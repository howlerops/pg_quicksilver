// Package ddl handles schema change — the part every logical-CDC pipeline
// leaks on.
//
// pgoutput and wal2json do not replicate DDL, so every CDC product hand-rolls
// drift detection and that is where they all fail: the pipeline keeps running
// against a stale schema and silently writes wrong data.
//
// Approach: an event trigger records each DDL statement into
// quicksilver.ddl_log, and that table is itself mirrored. DDL therefore arrives
// INSIDE the change stream, in commit order, as a BARRIER — the ingest applies
// up to and including the DDL transaction, re-reads the catalog, evolves, then
// continues. No polling, no race between "schema changed" and "rows in the new
// shape arrived".
//
// Detecting the barrier is not enough: it must STOP the apply. Applying
// post-DDL rows with the pre-DDL column list silently drops the new column —
// row counts still match and only the values are wrong, which is the worst
// shape a bug can take.
//
// Policy per docs/06: changes we can apply faithfully are applied; anything
// else halts mirroring for that table LOUDLY rather than corrupting it.
package ddl

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const LogTable = "quicksilver.ddl_log"

const setupSQL = `
CREATE SCHEMA IF NOT EXISTS quicksilver;

CREATE TABLE IF NOT EXISTS quicksilver.ddl_log (
    id        bigserial PRIMARY KEY,
    at        timestamptz NOT NULL DEFAULT now(),
    tag       text NOT NULL,
    object    text,
    statement text
);

CREATE OR REPLACE FUNCTION quicksilver.on_ddl() RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        -- never log our own bookkeeping, or the log recurses
        IF r.schema_name IS DISTINCT FROM 'quicksilver' THEN
            INSERT INTO quicksilver.ddl_log(tag, object, statement)
            VALUES (r.command_tag, r.object_identity, current_query());
        END IF;
    END LOOP;
END $$;

DROP EVENT TRIGGER IF EXISTS quicksilver_ddl;
CREATE EVENT TRIGGER quicksilver_ddl ON ddl_command_end
    EXECUTE FUNCTION quicksilver.on_ddl();
`

const teardownSQL = `
DROP EVENT TRIGGER IF EXISTS quicksilver_ddl;
DROP SCHEMA IF EXISTS quicksilver CASCADE;
`

func Setup(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, setupSQL)
	return err
}

func Teardown(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, teardownSQL)
	return err
}

// LiveColumns returns the source's current column list in ordinal order — the
// authority against which the mirror's schema is diffed.
func LiveColumns(ctx context.Context, conn *pgx.Conn, schema, table string) (map[string]string, []string, error) {
	rows, err := conn.Query(ctx,
		`SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		 FROM pg_attribute a
		 JOIN pg_class c ON c.oid = a.attrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2
		   AND a.attnum > 0 AND NOT a.attisdropped
		 ORDER BY a.attnum`, schema, table)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := map[string]string{}
	var order []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, nil, err
		}
		cols[name] = typ
		order = append(order, name)
	}
	return cols, order, rows.Err()
}

type Diff struct {
	Added   map[string]string
	Dropped []string
	Retyped map[string][2]string
}

func (d Diff) Empty() bool {
	return len(d.Added) == 0 && len(d.Dropped) == 0 && len(d.Retyped) == 0
}

// Safe reports whether the change can be applied without risking data. A
// retype can silently change values (numeric -> text, a widened decimal);
// adds and drops cannot.
func (d Diff) Safe() bool { return len(d.Retyped) == 0 }

func (d Diff) String() string {
	var bits []string
	if len(d.Added) > 0 {
		var a []string
		for k, v := range d.Added {
			a = append(a, k+" "+v)
		}
		bits = append(bits, "added "+strings.Join(a, ", "))
	}
	if len(d.Dropped) > 0 {
		bits = append(bits, "dropped "+strings.Join(d.Dropped, ", "))
	}
	if len(d.Retyped) > 0 {
		var a []string
		for k, v := range d.Retyped {
			a = append(a, fmt.Sprintf("%s %s->%s", k, v[0], v[1]))
		}
		bits = append(bits, "RETYPED "+strings.Join(a, ", "))
	}
	if len(bits) == 0 {
		return "no change"
	}
	return strings.Join(bits, "; ")
}

func Compare(old, new map[string]string) Diff {
	d := Diff{Added: map[string]string{}, Retyped: map[string][2]string{}}
	for k, v := range new {
		if _, ok := old[k]; !ok {
			d.Added[k] = v
		}
	}
	for k, ov := range old {
		nv, ok := new[k]
		if !ok {
			d.Dropped = append(d.Dropped, k)
		} else if ov != nv {
			d.Retyped[k] = [2]string{ov, nv}
		}
	}
	return d
}

// ---- backfilled columns ---------------------------------------------------

// Backfill describes what an ADD COLUMN did to rows that already existed.
//
// This is the case that made the mirror diverge silently in the first
// end-to-end run of the production sidecar, and it is invisible from the change
// stream by construction:
//
//	ALTER TABLE events ADD COLUMN channel text DEFAULT 'web'
//
// produces NO row-level WAL. PostgreSQL stores one value in pg_attribute's
// attmissingval and every pre-existing row reads back as that value without
// being touched. A logical consumer therefore sees the DDL and nothing else,
// leaves its own rows NULL, and diverges from the source on a column nobody
// will think to check. Row counts stay identical, which is what makes it bad.
//
// Two shapes, and only one of them is recoverable:
//
//   - MissingVal set: PostgreSQL used the fast-default path, so the value that
//     every pre-existing row takes is right there in the catalog. The mirror
//     records it and its read path substitutes it for files written before the
//     column existed — exactly what PostgreSQL itself does.
//   - Rewritten: a volatile default or a stored generated column made
//     PostgreSQL rewrite the heap. Every pre-existing row got its own value,
//     none of it reached the WAL as row changes, and there is nothing to
//     reconstruct from. The mirror must stop rather than serve NULLs.
type Backfill struct {
	MissingVals map[string]string // column -> value pre-existing rows read as
	Rewritten   []string          // columns whose values are unrecoverable
}

// InspectBackfill reports, for the named columns, how ADD COLUMN affected rows
// that predate them. Columns that were added with no default are absent from
// both maps: NULL really is their value, and the mirror is already correct.
func InspectBackfill(
	ctx context.Context, conn *pgx.Conn, schema, table string, columns []string,
) (Backfill, error) {
	b := Backfill{MissingVals: map[string]string{}}
	if len(columns) == 0 {
		return b, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT a.attname,
		       a.atthasmissing,
		       (SELECT v FROM unnest(a.attmissingval::text::text[]) v LIMIT 1),
		       a.attgenerated <> '',
		       d.adbin IS NOT NULL
		  FROM pg_attribute a
		  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		 WHERE a.attrelid = format('%I.%I', $1::text, $2::text)::regclass
		   AND a.attnum > 0 AND NOT a.attisdropped
		   AND a.attname = ANY($3)`, schema, table, columns)
	if err != nil {
		return b, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var hasMissing, generated, hasDefault bool
		var missing *string
		if err := rows.Scan(&name, &hasMissing, &missing, &generated, &hasDefault); err != nil {
			return b, err
		}
		switch {
		case hasMissing && missing != nil:
			b.MissingVals[name] = *missing
		case generated || hasDefault:
			// A default that did NOT leave a missing value means the heap was
			// rewritten, so each row has its own value and none of it is in the
			// change stream.
			b.Rewritten = append(b.Rewritten, name)
		}
	}
	return b, rows.Err()
}

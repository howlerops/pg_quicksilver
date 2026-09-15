package mirror

// Initial snapshot / bootstrap.
//
// Needed in three situations, and the same code serves all of them:
//   - a table newly matches the `tables:` pattern
//   - a mirror node is created or its storage is lost
//   - a failover invalidated the replication slot, so the stream cannot be
//     resumed and the mirror must be rebuilt (docs/05 lifecycle matrix)
//
// The correctness requirement is that snapshot and stream join up with neither
// gap nor overlap. Postgres gives us exactly the tool: CREATE_REPLICATION_SLOT
// returns a consistent point, and a REPEATABLE READ snapshot taken at that LSN
// sees precisely the state the stream will start from. Take the snapshot first,
// then stream from the consistent point, and every row is applied exactly once.
//
// This implementation takes the simpler route available when the slot already
// exists: snapshot at the CURRENT LSN and start the stream there. Overlap is
// then possible but harmless — the mirror is an upsert-by-key store, so
// replaying a row that the snapshot already contains converges to the same
// state. A gap would not be harmless, which is why the order is snapshot-then-
// stream and never the reverse.

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Snapshot replaces the mirror's contents with the table's current rows and
// sets applied_lsn to the LSN the caller should stream from.
func (t *Table) Snapshot(ctx context.Context, conn *pgx.Conn, atLSN string) (int, error) {
	if err := t.truncate(); err != nil {
		return 0, err
	}

	sel := ""
	for i, c := range t.Order {
		if i > 0 {
			sel += ", "
		}
		sel += `"` + c + `"::text`
	}
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT %s FROM %s", sel, t.Qualified))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(t.Order))
		ptrs := make([]any, len(t.Order))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return 0, err
		}
		m := make(map[string]any, len(t.Order))
		for i, c := range t.Order {
			m[c] = vals[i]
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	if len(out) > 0 {
		if err := t.writeParquet(fmt.Sprintf("%s/base/%s", t.Dir, name), out); err != nil {
			return 0, err
		}
		t.State.BaseFiles = []string{name}
		if err := t.writeIndex(name, out); err != nil {
			return 0, err
		}
	}
	t.State.DeltaFiles = nil
	t.State.BaseRows = len(out)
	t.State.DeltaRows = 0
	t.State.AppliedLSN = atLSN
	return len(out), t.saveState()
}

func (t *Table) writeIndex(baseName string, rows []map[string]any) error {
	index := make(map[string]int, len(rows))
	for i, r := range rows {
		index[keyString(r[t.Key])] = i
	}
	return writeJSON(fmt.Sprintf("%s/index/%s.idx.json", t.Dir,
		trimParquet(baseName)), index)
}

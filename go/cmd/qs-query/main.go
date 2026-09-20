// qs-query publishes the mirror as something a query engine can read.
//
// The mirror's directory is not a table: deletion vectors retire rows, partial
// deltas carry only the columns a change touched, and the manifest decides
// which files count. An engine pointed at a glob of the directory gets a
// plausible-looking answer that is wrong in three different ways at once.
//
// This prints a SELECT that reconstructs the logical table, for any engine that
// reads Parquet and JSON:
//
//	qs-query -mirror /var/lib/postgresql/data/quicksilver -table public.events
//	qs-query -mirror ... -table ... -view events   # wrapped in CREATE OR REPLACE VIEW
//
// The SQL reflects one manifest. A compaction landing mid-query deletes files
// the query names; ask again and re-run, which is the same contract the
// in-process reader has.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/ddl"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
	"github.com/howlerops/pg_quicksilver/go/internal/pgtext"
)

func main() {
	root := flag.String("mirror", "", "mirror root directory")
	table := flag.String("table", "", "qualified table name, e.g. public.events")
	view := flag.String("view", "", "wrap the SELECT in CREATE OR REPLACE VIEW <name>")
	engine := flag.String("engine", engineDuckDB, "duckdb | postgres (a view readable through pg_duckdb)")
	dsn := flag.String("dsn", "", "optional source connection, to read the column list from the catalog")
	lsn := flag.Bool("lsn", false, "print only the applied LSN, for freshness gating")
	flag.Parse()
	if *root == "" || *table == "" {
		flag.Usage()
		os.Exit(2)
	}
	schema, name, ok := strings.Cut(*table, ".")
	if !ok {
		fail("table must be schema.table")
	}

	cols, order, err := columns(*dsn, *root, schema, name)
	if err != nil {
		fail("%v", err)
	}
	t, err := mirror.New(*root, schema, name, order[0], cols, order)
	if err != nil {
		fail("open mirror: %v", err)
	}

	if *lsn {
		fmt.Println(t.State.AppliedLSN)
		return
	}

	sql, err := t.ViewSQL()
	if err != nil {
		fail("%v", err)
	}
	// The LSN is part of the answer, not decoration: a view of a mirror is only
	// usable if the reader can decide whether it is fresh enough.
	fmt.Printf("-- quicksilver mirror %s applied_lsn=%s\n", t.Qualified, t.State.AppliedLSN)
	if *engine == enginePostgres {
		if *view == "" {
			fail("-engine postgres emits a view; pass -view <name>")
		}
		fmt.Print(PostgresView(*view, sql, cols, order))
		fmt.Fprintln(os.Stderr, pgNullOrderNote)
		return
	}
	if *view != "" {
		// -view emits statements to run, so it emits the engine settings the
		// view depends on as well. Without -view the caller is embedding the
		// SELECT in something of their own and the settings would corrupt it,
		// so they go to stderr instead — see nullOrderNote.
		fmt.Println(duckdbPreamble)
		fmt.Printf("CREATE OR REPLACE VIEW %s AS\n%s;\n", *view, sql)
		return
	}
	fmt.Println(sql)
	fmt.Fprintln(os.Stderr, nullOrderNote)
}

// ORDER BY does not mean the same thing to both engines, and the difference is
// silent.
//
// PostgreSQL sorts NULLs last on ASC and FIRST on DESC. DuckDB defaults to
// NULLS_LAST for both. So `ORDER BY ts DESC LIMIT 10` over a nullable column
// returns different rows from the mirror than from the source, with no error
// and no warning — the 20M-row run caught it only because the harness compares
// the two:
//
//	postgres [NULL, 3, 1]   duckdb [3, 1, NULL]
//
// This is the same class of problem as docs/22: a value that two engines agree
// they hold and disagree how to present. That one was fixed by pinning the
// rendering on every connection (internal/pgtext). This one cannot be — the
// ORDER BY belongs to the reader's query, not to the view — so the mirror's job
// is to say so rather than to hope.
//
// DuckDB has the exact setting. NULLS_LAST_ON_ASC_FIRST_ON_DESC reproduces
// PostgreSQL in both directions, verified against PostgreSQL 17.
const duckdbPreamble = `-- ORDER BY differs between the engines unless this is set: PostgreSQL sorts
-- NULLs last on ASC and first on DESC, DuckDB defaults to last on both.
SET default_null_order = 'NULLS_LAST_ON_ASC_FIRST_ON_DESC';`

const nullOrderNote = "note: ORDER BY over a NULLABLE column will not match the source " +
	"unless the reader\n      sets PostgreSQL's null ordering. In DuckDB:\n" +
	"      SET default_null_order = 'NULLS_LAST_ON_ASC_FIRST_ON_DESC';\n" +
	"      PostgreSQL sorts NULLs last on ASC and FIRST on DESC; DuckDB defaults to last on both."

// columns prefers the live catalog, because the mirror's own column order is
// only recoverable from a file and the catalog is authoritative. Without a DSN
// it falls back to the Parquet schema, so the tool still works against a mirror
// whose source is unreachable — which is exactly when someone is most likely to
// be querying it.
func columns(dsn, root, schema, name string) (map[string]string, []string, error) {
	if dsn != "" {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, pgtext.PinDSN(dsn))
		if err != nil {
			return nil, nil, fmt.Errorf("connect: %w", err)
		}
		defer conn.Close(ctx)
		cols, order, err := ddl.LiveColumns(ctx, conn, schema, name)
		if err != nil {
			return nil, nil, fmt.Errorf("read columns: %w", err)
		}
		return cols, order, nil
	}
	cols, order, err := mirror.ColumnsFromFiles(root, schema, name)
	if err != nil {
		return nil, nil, fmt.Errorf("read the mirror's own schema: %w "+
			"(pass -dsn to read the column list from the source instead)", err)
	}
	return cols, order, nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(2)
}

package main

// -ducklake: the statements a reader needs when the sidecar publishes the
// mirror as a DuckLake (QS_DUCKLAKE=on, internal/ducklake).
//
// Unlike everything else this tool prints, none of it describes a manifest.
// The ATTACH names the catalog and the table name is the table's own; both
// are the same after every apply, merge and rewrite. A view over the lake
// table therefore never has to be generated again, which is the point: the
// generated SELECT of the other modes names files, and is wrong the moment
// one of them is retired.
//
//	qs-query -mirror M -table public.orders -ducklake
//	qs-query -mirror M -table public.orders -ducklake -view orders
//	qs-query -mirror M -table public.orders -ducklake -lsn
//	qs-query -mirror M -table public.orders -ducklake -engine postgres -view qs_lake.orders [-attach-on-login]
//
// PostgreSQL reads it through pg_duckdb, whose DuckDB lives in the backend:
// the ATTACH has to run once in every session before the view is used. The
// tool prints that statement, and with -attach-on-login a PostgreSQL 17 login
// event trigger that runs it for every session.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/howlerops/pg_quicksilver/go/internal/ducklake"
)

const lakeAlias = "lake"

func emitDuckLake(root, schema, name, engine, view, dsn string, lsnOnly, attachOnLogin bool) {
	// Absolute: the ATTACH is run by another process, somewhere else.
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
	}
	catalog := ducklake.CatalogPath(root)
	st, err := ducklake.ReadTable(context.Background(), catalog, schema, name)
	if err != nil {
		fail("read the lake catalog %s: %v (is the sidecar running with QS_DUCKLAKE=on?)", catalog, err)
	}
	if lsnOnly {
		fmt.Println(st.AppliedLSN)
		return
	}
	if st.Refusal != "" {
		fmt.Fprintf(os.Stderr, "warning: the lake table is not served at present; a query fails with: %s\n", st.Refusal)
	}
	table := lakeAlias + "." + quoteIdent(schema) + "." + quoteIdent(name)
	header := fmt.Sprintf("-- quicksilver mirror %s.%s as a DuckLake table (catalog format %s)\n"+
		"-- applied_lsn=%s manifest_seq=%d ducklake_snapshot=%d published_at=%s",
		schema, name, ducklake.FormatVersion, st.AppliedLSN, st.ManifestSeq, st.Snapshot, st.PublishedAt)
	attach := strings.TrimSuffix(ducklake.AttachSQL(catalog, lakeAlias), ";")
	attachOnce := strings.Replace(attach, "ATTACH ", "ATTACH IF NOT EXISTS ", 1)

	if engine != enginePostgres {
		fmt.Println(header)
		fmt.Println("-- once per machine: INSTALL ducklake; INSTALL sqlite;")
		fmt.Println(attach + ";")
		fmt.Println(duckdbPreamble)
		if view != "" {
			fmt.Printf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s;\n", view, table)
		} else {
			fmt.Printf("-- the table: %s\n", table)
		}
		fmt.Println("-- how fresh it is, and whether it is being served:")
		fmt.Println(ducklake.FreshnessSQL(lakeAlias, schema, name))
		return
	}

	if view == "" {
		fail("-engine postgres emits a view; pass -view <name>")
	}
	cols, order, err := columns(dsn, root, schema, name)
	if err != nil {
		fail("%v", err)
	}
	fmt.Println(header)
	fmt.Print(pgLakeNote)
	fmt.Println("SELECT duckdb.install_extension('ducklake');")
	fmt.Println("SELECT duckdb.install_extension('sqlite');")
	fmt.Println("-- once per SESSION, before the view is used (and in this one, to create it):")
	fmt.Printf("SELECT duckdb.raw_query($qs$ %s $qs$);\n", attachOnce)
	if attachOnLogin {
		fmt.Print(pgLoginTrigger(catalog, attachOnce))
	}
	fmt.Print(PostgresLakeView(view, table, cols, order))
	fmt.Fprintln(os.Stderr, pgNullOrderNote)
}

const pgLakeNote = `-- Readable through pg_duckdb (1.2.0 was verified), as a superuser or a role
-- that may read the mirror's files. The view names the lake table, not files:
-- it is created once and reads the current snapshot from then on.
-- Once per database:
`

// PostgresLakeView is the view over the lake table: the same projection as
// PostgresView, the source's own types put back on each column, over a SELECT
// that names no file.
func PostgresLakeView(view, table string, cols map[string]string, order []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE OR REPLACE VIEW %s AS\nSELECT ", view)
	for i, c := range order {
		if i > 0 {
			b.WriteString(",\n       ")
		}
		fmt.Fprintf(&b, "(r[%s])::%s AS %s", sqlLiteral(c), cols[c], quoteIdent(c))
	}
	fmt.Fprintf(&b, "\nFROM duckdb.query($qs$ SELECT * FROM %s $qs$) r;\n", table)
	return b.String()
}

// pgLoginTrigger attaches the lake in every new session. Three things about
// it were found by running it:
//
//   - It must not have an EXCEPTION block. pg_duckdb refuses subtransactions
//     ("SAVEPOINT and subtransactions are not supported in DuckDB"), so the
//     block that was meant to make a failed attach harmless made every attach
//     fail.
//   - Without one, an error here is an error at login, for everyone. So it
//     attaches only when the catalog file is there — which also makes it a
//     no-op on a node that has no mirror, such as the primary.
//   - ENABLE ALWAYS, or it does not fire on a standby, which is where the
//     mirror is.
//
// If it ever does lock sessions out: event_triggers = off (PostgreSQL 17).
func pgLoginTrigger(catalog, attachOnce string) string {
	return fmt.Sprintf(`-- every session attaches the lake at login (PostgreSQL 17+):
CREATE SCHEMA IF NOT EXISTS qs_lake;
CREATE OR REPLACE FUNCTION qs_lake.attach() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER AS $fn$
BEGIN
  IF (pg_stat_file(%s, true)).size IS NULL THEN
    RETURN;  -- no mirror on this node
  END IF;
  SET LOCAL client_min_messages = warning;
  PERFORM duckdb.raw_query($qs$ %s $qs$);
END $fn$;
DROP EVENT TRIGGER IF EXISTS qs_lake_attach;
CREATE EVENT TRIGGER qs_lake_attach ON login EXECUTE FUNCTION qs_lake.attach();
ALTER EVENT TRIGGER qs_lake_attach ENABLE ALWAYS;
`, sqlLiteral(catalog), attachOnce)
}

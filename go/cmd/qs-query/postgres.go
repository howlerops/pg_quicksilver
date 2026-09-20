package main

import (
	"fmt"
	"strings"
)

const (
	engineDuckDB   = "duckdb"
	enginePostgres = "postgres"
)

// PostgresView wraps the mirror's DuckDB SELECT so PostgreSQL can answer from
// it, through pg_duckdb.
//
// The SELECT that ViewSQL produces is valid DuckDB and is NOT valid here, in
// two ways that both fail late and confusingly:
//
//  1. `SET default_null_order` is a DuckDB setting. PostgreSQL answers
//     "unrecognized configuration parameter", so the preamble that makes the
//     DuckDB view correct is the thing that stops the PostgreSQL one existing.
//
//  2. Columns inside read_parquet are not PostgreSQL columns. pg_duckdb hands
//     back a single `duckdb."row"`, and a bare reference to one of its fields
//     fails with `column "id" does not exist` — the error names the column the
//     mirror definitely has, which sends you looking at the mirror.
//
// So the bridge is duckdb.query($$ ... $$), which runs the DuckDB SQL verbatim
// — the same string already verified against the mirror, no second dialect to
// keep in step — and then projects r['col'] back out with the source's own
// types, read from the catalog. That projection is what makes the result a
// table to PostgreSQL rather than an opaque row.
//
// duckdb.query accepts exactly one SELECT, which ViewSQL is.
func PostgresView(view, selectSQL string, cols map[string]string, order []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n", pgViewComment)
	fmt.Fprintf(&b, "CREATE OR REPLACE VIEW %s AS\nSELECT ", view)
	for i, c := range order {
		if i > 0 {
			b.WriteString(",\n       ")
		}
		// The cast is the source's declared type, so numeric(12,2) stays
		// numeric(12,2) and a timestamptz does not arrive as text. Without it
		// every column would be whatever duckdb."row" indexing yields, and
		// sum(amount) would either fail or quietly change type.
		fmt.Fprintf(&b, "(r[%s])::%s AS %s", sqlLiteral(c), cols[c], quoteIdent(c))
	}
	// $qs$ rather than $$: the mirror's SELECT contains file paths, and a path
	// is allowed to contain $$.
	fmt.Fprintf(&b, "\nFROM duckdb.query($qs$\n%s\n$qs$) r;\n", selectSQL)
	return b.String()
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func sqlLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

const pgViewComment = `-- Readable through pg_duckdb. Requires the allowed_directories patch in
-- bench/patches/ and duckdb.postgres_role set, or only superusers can read it.`

// The DuckDB view can fix null ordering with one SET. This one cannot, and
// saying so is the whole point of printing it.
//
// pg_duckdb exposes no equivalent GUC, and duckdb.query takes a single SELECT,
// so there is nowhere to put the setting. Whether it matters depends on where
// the ORDER BY ends up running: PostgreSQL sorting the projected rows agrees
// with the source, and an ORDER BY pushed down into DuckDB does not, over a
// NULLABLE column. bench/scripts/serving_pg17.sh measures which happens rather
// than assuming, because the difference is silent either way.
const pgNullOrderNote = "note: ORDER BY over a NULLABLE column may not match the source.\n" +
	"      PostgreSQL sorts NULLs last on ASC and FIRST on DESC; DuckDB defaults to\n" +
	"      last on both, and pg_duckdb has no setting to change it. Add an explicit\n" +
	"      NULLS FIRST / NULLS LAST to any ORDER BY that must agree."

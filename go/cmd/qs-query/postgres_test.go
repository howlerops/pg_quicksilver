package main

import (
	"strings"
	"testing"
)

func TestPostgresViewIsValidForPgDuckDB(t *testing.T) {
	sel := "SELECT \"id\" FROM read_parquet('/m/base/000001.parquet')"
	cols := map[string]string{
		"id":     "bigint",
		"sku":    "text",
		"amount": "numeric(12,2)",
		"ts":     "timestamp with time zone",
	}
	got := PostgresView("events_pq", sel, cols, []string{"id", "sku", "amount", "ts"})

	// The three things that make it work through pg_duckdb rather than only in
	// DuckDB. Each was a real error message before it was a test.
	for _, want := range []string{
		"duckdb.query($qs$",           // the bridge; without it, "column id does not exist"
		"(r['id'])::bigint AS \"id\"", // r[...] indexing, with the source's type
		"(r['amount'])::numeric(12,2) AS \"amount\"",
		"(r['ts'])::timestamp with time zone AS \"ts\"",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("view is missing %q:\n%s", want, got)
		}
	}

	// SET default_null_order is DuckDB-only. PostgreSQL rejects it with
	// "unrecognized configuration parameter", which kills the whole statement
	// batch — so the preamble that makes the DuckDB view correct must not be
	// anywhere near this one.
	if strings.Contains(got, "default_null_order") {
		t.Error("the PostgreSQL view carries SET default_null_order, which PostgreSQL refuses")
	}

	// Column order is the source's, not a map's iteration order, or the view's
	// shape would change between runs of the same command.
	idxID := strings.Index(got, "AS \"id\"")
	idxTS := strings.Index(got, "AS \"ts\"")
	if idxID < 0 || idxTS < 0 || idxID > idxTS {
		t.Errorf("columns are not in the source's order:\n%s", got)
	}
}

// A path may contain $$, and the mirror's SELECT is full of paths. Quoting the
// body with $$ would end the string early and produce SQL that neither engine
// can parse, in a way that depends on where the mirror happens to live.
func TestPostgresViewSurvivesADollarQuotedPath(t *testing.T) {
	sel := "SELECT \"id\" FROM read_parquet('/mnt/$$weird/base/000001.parquet')"
	got := PostgresView("v", sel, map[string]string{"id": "bigint"}, []string{"id"})
	if !strings.Contains(got, "$qs$") {
		t.Fatalf("expected $qs$ quoting:\n%s", got)
	}
	if strings.Count(got, "$qs$") != 2 {
		t.Errorf("the $qs$ delimiters are unbalanced:\n%s", got)
	}
}

func TestIdentifiersAndLiteralsAreQuoted(t *testing.T) {
	got := PostgresView("v", "SELECT 1",
		map[string]string{`we"ird`: "text"}, []string{`we"ird`})
	if !strings.Contains(got, `AS "we""ird"`) {
		t.Errorf("identifier not quoted:\n%s", got)
	}
	if !strings.Contains(got, `r['we"ird']`) {
		t.Errorf("literal not rendered:\n%s", got)
	}
}

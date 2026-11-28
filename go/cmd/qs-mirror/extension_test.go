package main

import (
	"log/slog"
	"os"
	"strings"
	"testing"
)

// The mirror relation's text is what every replica sees: it must name the
// lake table and nothing local (no path, no file), give the heap's columns in
// the heap's order with the heap's types, and publish as text the types a
// pg_duckdb row field cannot be cast to.
func TestMirrorRelationNamesTheLakeTableAndNoFile(t *testing.T) {
	cols := []extColumn{
		{Name: "id", Type: "bigint", TypeOID: 20},
		{Name: "amount", Type: "numeric(12,2)", TypeOID: 1700},
		{Name: `we"ird`, Type: "text", TypeOID: 25},
		{Name: "mood", Type: "ex.mood", TypeOID: 99999}, // an enum: not castable
		{Name: "tags", Type: "text[]", TypeOID: 1009},   // an array: not castable
	}
	got := extViewSQL("quicksilver_mirror", "public", "orders", cols, nil)
	want := `CREATE VIEW "quicksilver_mirror"."public.orders" AS
SELECT (r['id'])::bigint AS "id",
       (r['amount'])::numeric(12,2) AS "amount",
       (r['we"ird'])::text AS "we""ird",
       (r['mood'])::text AS "mood",
       (r['tags'])::text AS "tags"
FROM duckdb.query($qs$ SELECT * FROM lake."public"."orders" $qs$) r`
	if got != want {
		t.Fatalf("mirror relation:\n%s\nwant:\n%s", got, want)
	}
	if strings.Contains(got, "/") || strings.Contains(got, ".parquet") {
		t.Error("the mirror relation names a path or a file; it would differ between Pods and go stale at a compaction")
	}
	// Idempotence rests on the signature: the same columns give the same one,
	// a changed column a different one.
	if extSignature(got) != extSignature(extViewSQL("quicksilver_mirror", "public", "orders", cols, nil)) {
		t.Error("the same definition signed differently")
	}
	cols[1].Type = "numeric(14,2)"
	if extSignature(got) == extSignature(extViewSQL("quicksilver_mirror", "public", "orders", cols, nil)) {
		t.Error("a changed column type did not change the signature, so the relation would not be re-created at a DDL barrier")
	}
	if !strings.HasPrefix(extSignature(got), extViewComment) {
		t.Error("the signature does not carry the marker that says the relation is the sidecar's to drop")
	}
}

func TestMirrorRelationNamesAreUniqueAndFit(t *testing.T) {
	if got := extViewName("public.orders"); got != "public.orders" {
		t.Errorf("view name %q", got)
	}
	long1 := "a_schema_with_a_long_name." + strings.Repeat("t", 50) + "_one"
	long2 := "a_schema_with_a_long_name." + strings.Repeat("t", 50) + "_two"
	n1, n2 := extViewName(long1), extViewName(long2)
	if len(n1) > 63 || len(n2) > 63 {
		t.Errorf("names longer than an identifier: %d, %d", len(n1), len(n2))
	}
	if n1 == n2 {
		t.Error("two tables whose names share 63 bytes got the same mirror relation")
	}
}

// Before this process has vouched for a table, its report is 0/0: a report left
// by an earlier process or attempt is not an answer about this one (blank.go).
// A halted table is 0/0 whatever else is true. (lake is nil here.)
func TestUnvouchedTablesAreZeroAndHaltedOnesToo(t *testing.T) {
	e := newExtLink(slog.New(slog.NewTextHandler(os.Stderr, nil)), options{tables: []string{"public.a", "public.b"}})
	got := e.lsns()
	if len(got) != 2 || got["public.a"] != "0/0" || got["public.b"] != "0/0" {
		t.Fatalf("unvouched tables must be reported 0/0; got %v", got)
	}
	e.mu.Lock()
	e.blocked["public.b"] = true
	e.mu.Unlock()
	got = e.lsns()
	if got["public.b"] != "0/0" {
		t.Fatalf("a halted table must be reported as 0/0; got %v", got)
	}
}

// A table that was vouched for in an earlier attempt is 0/0 again the moment the
// next attempt begins, with no lake state needed, so nothing of the old attempt
// can be read in the window before the new one has looked at the table.
func TestAttemptStartForgetsVouching(t *testing.T) {
	e := newExtLink(slog.New(slog.NewTextHandler(os.Stderr, nil)), options{tables: []string{"public.a"}})
	e.mu.Lock()
	e.vouched["public.a"] = true
	e.mu.Unlock()
	e.unvouch()
	if got := e.lsns(); got["public.a"] != "0/0" {
		t.Fatalf("after an attempt begins: %v", got)
	}
}

// The position and LSN parsing helpers, which this file also covers.
func TestPositionAndLSNParsing(t *testing.T) {
	e := newExtLink(slog.New(slog.NewTextHandler(os.Stderr, nil)), options{tables: []string{"public.a", "public.b"}})
	if e.position() != 0 {
		t.Error("a position was vouched for with no stream attached")
	}
	if parseLSNBytes("1/A0") != 1<<32|0xA0 || parseLSNBytes("junk") != 0 {
		t.Error("parseLSNBytes")
	}
}

func TestExtensionCanBeTurnedOff(t *testing.T) {
	t.Setenv("QS_EXTENSION", "off")
	e := newExtLink(slog.New(slog.NewTextHandler(os.Stderr, nil)), options{tables: []string{"public.a"}})
	if !e.off {
		t.Fatal("QS_EXTENSION=off was not honoured")
	}
	if e.Metrics() != "" {
		t.Error("an extension link that is off exports metrics")
	}
	var none *extLink
	none.pumpBusy()
	none.pumpIdle(1)
	none.markDirty()
	none.observe(nil)
	none.attachStream(nil) // every hook the pump calls is safe before main has set ext
}

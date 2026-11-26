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
	got := extViewSQL("quicksilver_mirror", "public", "orders", cols)
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
	if extSignature(got) != extSignature(extViewSQL("quicksilver_mirror", "public", "orders", cols)) {
		t.Error("the same definition signed differently")
	}
	cols[1].Type = "numeric(14,2)"
	if extSignature(got) == extSignature(extViewSQL("quicksilver_mirror", "public", "orders", cols)) {
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

// Before anything is known about a table, nothing is reported for it: a
// restarted sidecar must not take every table off the mirror until its first
// publish. A halted table is reported unreadable at once, whatever the lake
// still serves. (lake is nil here: no publisher state at all.)
func TestUnknownTablesAreNotReportedAndHaltedOnesAreZero(t *testing.T) {
	e := newExtLink(slog.New(slog.NewTextHandler(os.Stderr, nil)), options{tables: []string{"public.a", "public.b"}})
	if got := e.lsns(); len(got) != 0 {
		t.Fatalf("reported %v for tables nothing is known about", got)
	}
	e.mu.Lock()
	e.blocked["public.b"] = true
	e.mu.Unlock()
	got := e.lsns()
	if len(got) != 1 || got["public.b"] != "0/0" {
		t.Fatalf("a halted table must be reported as 0/0 and nothing else reported; got %v", got)
	}
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

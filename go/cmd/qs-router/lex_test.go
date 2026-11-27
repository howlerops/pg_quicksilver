package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func an(sql string) stmtInfo { return analyse(sql, "public", "qs_mirror") }

func TestPlainRead(t *testing.T) {
	cases := []struct {
		sql string
		ok  bool
		why string
	}{
		{"SELECT * FROM orders WHERE id = 1", true, ""},
		{"  select 1 ; ", true, ""},
		{"(SELECT 1) UNION ALL (SELECT 2)", true, ""},
		{"WITH x AS (SELECT 1) SELECT * FROM x", true, ""},
		{"TABLE orders", true, ""},
		{"VALUES (1)", true, ""},
		{"/* hint */ SELECT 1 -- trailing", true, ""},
		{"", false, "empty"},
		{"-- nothing", false, "empty"},
		{"INSERT INTO orders VALUES (1)", false, "INSERT"},
		{"UPDATE orders SET note = 'x'", false, "UPDATE"},
		{"SET search_path = x", false, "SET"},
		{"BEGIN", false, "BEGIN"},
		{"EXECUTE p(1)", false, "EXECUTE"},
		{"EXPLAIN SELECT 1", false, "EXPLAIN"},
		{"COPY orders TO STDOUT", false, "COPY"},
		{"DECLARE c CURSOR FOR SELECT 1", false, "DECLARE"},
		{"SELECT 1; SELECT 2", false, "more than one"},
		{"SELECT 1; DROP TABLE orders", false, "more than one"},
		{"SELECT * INTO copy_of FROM orders", false, "INTO"},
		{"SELECT * FROM public.orders", false, "names schema public"},
		{`SELECT * FROM "public" . orders`, false, "names schema public"},
		{"SELECT * FROM qs_mirror.orders", false, "names schema qs_mirror"},
		{"SELECT * FROM orders ORDER BY id", false, "ORDER BY"},
		{"SELECT id/2 FROM orders", false, "division"},
		{"SELECT AVG(amount) FROM orders", false, "uses avg"},
		{"SELECT avgerage, 'avg' FROM orders", true, ""},
		{"SELECT 'unterminated", false, "tokenise"},
		{"SELECT /* open", false, "tokenise"},
		{`SELECT "open`, false, "tokenise"},
		{"SELECT $q$ open", false, "tokenise"},
	}
	for _, c := range cases {
		in := an(c.sql)
		ok, why := in.plainRead(false, []string{"avg"})
		if ok != c.ok || !strings.Contains(why, c.why) {
			t.Errorf("%q: got %v %q, want %v containing %q", c.sql, ok, why, c.ok, c.why)
		}
	}
	in := an("SELECT * FROM orders ORDER BY id")
	if ok, _ := in.plainRead(true, nil); !ok {
		t.Error("-allow-order-by did not allow ORDER BY")
	}
}

// What is inside a literal, a comment or a quoted identifier is not SQL.
func TestLiteralsAreNotRead(t *testing.T) {
	for _, sql := range []string{
		"SELECT 'a; b' FROM orders",
		"SELECT 'public.orders into order by /' FROM orders",
		"SELECT $$ ; public.x into $$ FROM orders",
		"SELECT $tag$ it's ; $$ $tag$ FROM orders",
		"SELECT E'it\\'s ; public.x' FROM orders",
		"SELECT 'it''s ; fine' FROM orders",
		`SELECT "into", "order by", "a;b" FROM orders`,
		"SELECT 1 /* ; public.x /* nested ; */ into */ FROM orders",
		"SELECT 1 -- ; public.x into\n FROM orders",
		"SELECT publicity.x FROM orders publicity",
	} {
		in := an(sql)
		if ok, why := in.plainRead(false, []string{"avg"}); !ok {
			t.Errorf("%q: refused (%s)", sql, why)
		}
	}
	// And the other way round: a quote that does not close a string.
	in := an(`SELECT 'a\'; DROP TABLE orders; --'`)
	if !in.Multi {
		t.Errorf("a backslash closed nothing in a standard string, and the rest is a second statement: %+v", in)
	}
}

func TestShapeAndExact(t *testing.T) {
	a := an("SELECT * FROM orders WHERE id = 42 AND note = 'x'")
	b := an("select *\n  from ORDERS where id=7 and note='it''s' -- c")
	if a.Shape != b.Shape {
		t.Errorf("shapes differ:\n %q\n %q", a.Shape, b.Shape)
	}
	if a.Exact == b.Exact {
		t.Error("exact keys are equal for different constants")
	}
	// Quoted identifiers keep their case, and a parameter is not a literal.
	if an(`SELECT "A" FROM t`).Shape == an(`SELECT "a" FROM t`).Shape {
		t.Error(`"A" and "a" got one shape`)
	}
	if an("SELECT $1").Shape == an("SELECT $2").Shape {
		t.Error("$1 and $2 got one shape")
	}
	if an("SELECT 1.5e-3, .5, 0x1F").Shape != "select ? , ? , ?" {
		t.Errorf("numbers: %q", an("SELECT 1.5e-3, .5, 0x1F").Shape)
	}
}

func TestRouteSetting(t *testing.T) {
	cases := map[string]string{
		"SET qs.route = 'mirror'":       "mirror",
		"set qs.route to heap":          "heap",
		"SET SESSION qs.route = 'AUTO'": "auto",
		"SET LOCAL qs.route = 'heap';":  "heap",
		"SET qs.route = 'sideways'":     "sideways", // the session treats an unknown value as heap
		"SET qs.route = 1":              "?",
		"SET qs.router = 'mirror'":      "",
		"SELECT 'SET qs.route = heap'":  "",
		"SET qs.route = 'heap'; SELECT": "",
	}
	for sql, want := range cases {
		if got := an(sql).Route; got != want {
			t.Errorf("%q: Route = %q, want %q", sql, got, want)
		}
	}
	for _, sql := range []string{"RESET qs.route", "RESET ALL", "DISCARD ALL", "SET qs.route TO DEFAULT"} {
		if !an(sql).RouteReset {
			t.Errorf("%q did not reset", sql)
		}
	}
	if !an("show qs.last_route;").ShowLast || an("show qs.route").ShowLast {
		t.Error("SHOW qs.last_route")
	}
	for _, sql := range []string{"PREPARE p AS SELECT 1", "DEALLOCATE ALL", "DISCARD ALL", "deallocate p"} {
		if !an(sql).TouchesPrep {
			t.Errorf("%q does not invalidate prepared statements", sql)
		}
	}
}

func TestStartupRoute(t *testing.T) {
	cases := []struct {
		p    map[string]string
		want string
	}{
		{map[string]string{}, ""},
		{map[string]string{"qs.route": "heap"}, "heap"},
		{map[string]string{"options": "-c qs.route=mirror"}, "mirror"},
		{map[string]string{"options": "-c statement_timeout=5 --qs.route=heap"}, "heap"},
		{map[string]string{"qs.route": "nonsense"}, "heap"},
	}
	for _, c := range cases {
		if got := startupRoute(c.p); got != c.want {
			t.Errorf("%v: %q, want %q", c.p, got, c.want)
		}
	}
}

type fixedLSN struct {
	lsn uint64
	err error
}

func (f fixedLSN) AppliedLSN(context.Context) (uint64, error) { return f.lsn, f.err }

func TestFreshnessGate(t *testing.T) {
	lsn := func(s string) uint64 {
		v, err := ParseLSN(s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	if lsn("1/0") != 1<<32 || lsn("0/F065CC0") != 0xF065CC0 {
		t.Fatal("ParseLSN")
	}
	for _, bad := range []string{"", "1", "x/y", "1/"} {
		if _, err := ParseLSN(bad); err == nil {
			t.Errorf("ParseLSN accepted %q", bad)
		}
	}
	f := &freshness{src: fixedLSN{lsn: lsn("0/1000")}, maxBytes: 100}
	if ok, _ := f.check(context.Background(), lsn("0/1064")); !ok {
		t.Error("100 bytes behind with a bound of 100 was refused")
	}
	if ok, why := f.check(context.Background(), lsn("0/1065")); ok || !strings.Contains(why, "101 WAL bytes behind") {
		t.Errorf("101 bytes behind was accepted: %q", why)
	}
	if ok, _ := f.check(context.Background(), lsn("0/500")); !ok {
		t.Error("a mirror ahead of the server it is asked about was refused")
	}
	// Not knowing is not fresh.
	f = &freshness{src: fixedLSN{err: os.ErrNotExist}, maxBytes: 1 << 40}
	if ok, why := f.check(context.Background(), 0); ok || !strings.Contains(why, "unknown") {
		t.Errorf("an unreadable position passed the gate: %q", why)
	}

	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	if err := os.WriteFile(p, []byte(`{"applied_lsn":"0/2000","base_files":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := newLSNSource("file:" + p)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := src.AppliedLSN(context.Background()); err != nil || got != 0x2000 {
		t.Errorf("file source: %x %v", got, err)
	}
	if _, err := newLSNSource("ftp://x"); err == nil {
		t.Error("accepted an unknown source")
	}
}

// A table nothing writes to keeps its manifest at its last change, and the
// mirror root's watermark says how far it has applied since. The file source
// reads the effective LSN, so the gate does not refuse a table for being
// quiet; and it stops believing the watermark the moment the manifest moves.
func TestFileSourceReadsTheEffectiveLSNOfAQuietTable(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("public.quiet/state.json", `{"applied_lsn":"0/2000","base_files":[]}`)
	src, err := newLSNSource("file:" + filepath.Join(root, "public.quiet", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	read := func() uint64 {
		t.Helper()
		got, err := src.AppliedLSN(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := read(); got != 0x2000 {
		t.Fatalf("no watermark: %x", got)
	}
	// The server is 1 MB of WAL past the table's last change, all of it in
	// other tables. By its manifest the table is stale; it is not.
	server := uint64(0x2000 + 1<<20)
	gate := func() bool {
		f := &freshness{src: src, maxBytes: 4096}
		ok, _ := f.check(context.Background(), server)
		return ok
	}
	if gate() {
		t.Fatal("the test needs the manifest alone to fail the gate")
	}
	write("watermark.json", `{"applied_lsn":"0/101F00","tables":{"public.quiet":"0/2000","public.other":"0/9"}}`)
	if got := read(); got != 0x101F00 {
		t.Fatalf("under a watermark that speaks for this manifest: %x", got)
	}
	if !gate() {
		t.Fatal("a quiet table that has applied through the watermark was refused as stale")
	}
	// The table changes: its manifest is at a new LSN the watermark has not
	// seen, and the watermark no longer speaks for it.
	write("public.quiet/state.json", `{"applied_lsn":"0/3000","base_files":[]}`)
	if got := read(); got != 0x3000 {
		t.Fatalf("a manifest the watermark does not match: %x", got)
	}
	// A table the watermark does not name answers for itself.
	write("public.halted/state.json", `{"applied_lsn":"0/10","base_files":[]}`)
	other, _ := newLSNSource("file:" + filepath.Join(root, "public.halted", "state.json"))
	if got, err := other.AppliedLSN(context.Background()); err != nil || got != 0x10 {
		t.Fatalf("a table the watermark does not name: %x %v", got, err)
	}
	// And a table with no manifest is unknown, which is not fresh.
	missing, _ := newLSNSource("file:" + filepath.Join(root, "public.none", "state.json"))
	if _, err := missing.AppliedLSN(context.Background()); err == nil {
		t.Fatal("a table with no manifest reported a position")
	}
}

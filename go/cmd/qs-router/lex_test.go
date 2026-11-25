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

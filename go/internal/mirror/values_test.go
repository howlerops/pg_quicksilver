package mirror

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// liveImage is everything a reader of the mirror can observe about its values:
// the checksum, and per key the rendered text AND the Go type of every column.
// The type is in there because rows leave this package as plain strings and
// numbers, and a wrapper escaping through Live would be a change of contract
// that Render alone cannot see.
type liveImage struct {
	n    int
	sum  uint64
	rows map[string]string
}

func imageOf(t *testing.T, tbl *Table) liveImage {
	t.Helper()
	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	img := liveImage{rows: map[string]string{}}
	img.n, img.sum = Checksum(rows, tbl.Order)
	for _, r := range rows {
		var b strings.Builder
		for _, c := range tbl.Order {
			fmt.Fprintf(&b, "%s=%T(%s)|", c, r[c], Render(r[c]))
		}
		img.rows[Render(r[tbl.Key])] = b.String()
	}
	return img
}

func (a liveImage) diff(t *testing.T, step string, b liveImage) {
	t.Helper()
	if a.n != b.n || a.sum != b.sum {
		t.Errorf("%s: checksum changed: %d rows %x -> %d rows %x", step, a.n, a.sum, b.n, b.sum)
	}
	keys := map[string]bool{}
	for k := range a.rows {
		keys[k] = true
	}
	for k := range b.rows {
		keys[k] = true
	}
	for k := range keys {
		if a.rows[k] != b.rows[k] {
			t.Errorf("%s: row %s changed\n  before %s\n  after  %s", step, k, a.rows[k], b.rows[k])
		}
	}
}

// A rewrite must not change one observable thing about a value. This is the
// contract the typed read path (values.go) has to keep: every supported type,
// through every way a row is read from one file and written to another.
func TestRewritesPreserveEveryValue(t *testing.T) {
	if !TemporalTypes {
		t.Skip("QS_TEMPORAL_TYPES=0")
	}
	cols := map[string]string{
		"id": "bigint", "n": "integer", "amt": "numeric(12,2)", "whole": "numeric(10,0)",
		"u": "numeric", "f": "double precision", "b": "boolean",
		"at": "timestamptz", "naive": "timestamp", "day": "date", "clock": "time",
		"span": "interval", "s": "text",
	}
	order := []string{"id", "n", "amt", "whole", "u", "f", "b", "at", "naive", "day", "clock", "span", "s"}
	dir := t.TempDir()
	tbl, err := New(dir, "public", "v", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	src := []map[string]any{
		{"id": "1", "n": "7", "amt": "1.50", "whole": "42", "u": "12345678901234567890.123456789",
			"f": "1.5", "b": "t", "at": "2026-01-02 03:04:05.123456+00", "naive": "2026-01-02 03:04:05",
			"day": "2024-02-29", "clock": "12:34:56.5", "span": "1 year 3 mons 19 days -04:23:45.536088", "s": "007.500"},
		{"id": "2", "n": "-2147483648", "amt": "-0.05", "whole": "-7", "u": "-0.000000001",
			"f": "-0.25", "b": "f", "at": "1969-12-31 23:59:59.5+00", "naive": "1900-01-01 00:00:00.000001",
			"day": "1969-12-31", "clock": "00:00:00.000001", "span": "-1 years", "s": "1e5"},
		{"id": "3", "n": "0", "amt": "0.00", "whole": "0", "u": "0",
			"f": "0", "b": "t", "at": "infinity", "naive": "-infinity",
			"day": "infinity", "clock": "24:00:00", "span": "00:00:00", "s": ""},
		{"id": "4", "n": nil, "amt": nil, "whole": nil, "u": nil,
			"f": nil, "b": nil, "at": nil, "naive": nil,
			"day": nil, "clock": nil, "span": nil, "s": nil},
		{"id": "5", "n": "2147483647", "amt": "9999999999.99", "whole": "9999999999", "u": "3",
			"f": "NaN", "b": "f", "at": "2026-12-31 23:59:59.999999+00", "naive": "2026-06-15 12:00:00.25",
			"day": "0001-01-01", "clock": "23:59:59.999999", "span": "-5 mons +12 days 03:46:59.379269", "s": "true"},
		{"id": "6", "n": "1", "amt": "-9999999999.99", "whole": "1", "u": "-99999999999999999999999999999.999999999",
			"f": "1e+20", "b": "t", "at": "2000-02-29 00:00:00+00", "naive": "2000-02-29 00:00:00",
			"day": "2000-02-29", "clock": "00:00:00", "span": "3 days", "s": "2026-01-02 03:04:05+00"},
	}
	lsn := 0
	apply := func(op changestream.Op, rows ...map[string]any) {
		t.Helper()
		var ch []changestream.Change
		for _, r := range rows {
			ch = append(ch, changestream.Change{Op: op, Schema: "public", Table: "v", Row: r, Key: r})
		}
		lsn++
		pos := fmt.Sprintf("0/%X", lsn)
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: ch}}); err != nil {
			t.Fatal(err)
		}
	}
	apply(changestream.OpInsert, src...)

	// What the mirror holds is what the source sent. A boolean is the one
	// column whose text differs (the source's own reader returns a bool, and
	// both render "true"), so it is compared as one.
	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(src) {
		t.Fatalf("mirror holds %d rows, source sent %d", len(rows), len(src))
	}
	byID := map[string]map[string]any{}
	for _, r := range rows {
		byID[Render(r["id"])] = r
	}
	for _, want := range src {
		got := byID[want["id"].(string)]
		for _, c := range order {
			w := want[c]
			if c == "b" && w != nil {
				w = w == "t"
			}
			if Render(got[c]) != Render(w) || (got[c] == nil) != (w == nil) {
				t.Errorf("id=%v column %s: mirror=%#v source=%#v", want["id"], c, got[c], want[c])
			}
			switch got[c].(type) {
			case nil, string, int64, float64, bool:
			default:
				t.Errorf("id=%v column %s left the package as %T", want["id"], c, got[c])
			}
		}
	}
	// Text that merely looks like a number is stored as the text it is.
	if got := byID["1"]["s"]; got != "007.500" {
		t.Errorf("text column: stored %#v, source sent \"007.500\"", got)
	}

	before := imageOf(t, tbl)

	// 1. The synchronous compaction: rows read from deltas, written to a base.
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	before.diff(t, "Compact", imageOf(t, tbl))

	// 2. The streaming compaction, over a base it reads back natively plus
	// deltas and a deletion vector. Rewriting row 1 with its own values and
	// leaving the rest is the case where an image must not move at all.
	apply(changestream.OpUpdate, src[0], src[4])
	before.diff(t, "re-applying two rows", imageOf(t, tbl))
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction started")
	}
	<-c.done
	if _, err := tbl.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
	before.diff(t, "streaming compaction", imageOf(t, tbl))
	if len(tbl.State.DeltaFiles) != 0 {
		t.Fatalf("compaction left deltas: %v", tbl.State.DeltaFiles)
	}

	// 3. A delta merge: several delta files read back and written as one.
	for _, r := range src {
		apply(changestream.OpUpdate, r)
	}
	if _, err := tbl.MergeDeltas(); err != nil {
		t.Fatal(err)
	}
	before.diff(t, "MergeDeltas", imageOf(t, tbl))

	// 4. And once more over files that were themselves written from native
	// values, so nothing in this pass has ever been text.
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	before.diff(t, "second Compact", imageOf(t, tbl))

	// 5. A restart reads the same thing.
	again, err := New(dir, "public", "v", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	before.diff(t, "reopen", imageOf(t, again))
}

// The wrappers must print as the strings they replaced, through every route a
// value is turned into text.
func TestTypedValuesRenderAsTheirText(t *testing.T) {
	for _, c := range []struct {
		prec, scale int32
		in          string
	}{
		{12, 2, "1.50"}, {12, 2, "-0.05"}, {12, 2, "0.00"}, {10, 0, "42"}, {10, 0, "-7"},
		{38, 9, "12345678901234567890.123456789"}, {38, 9, "-0.000000001"}, {38, 0, "99999999999999999999999999999999999999"},
	} {
		want, err := decimal128.FromString(c.in, c.prec, c.scale)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := parseDecimalExact(c.in, c.prec, c.scale)
		if !ok || got != want {
			t.Errorf("parseDecimalExact(%q, %d, %d) = %v, %v; decimal128 says %v", c.in, c.prec, c.scale, got, ok, want)
		}
		d := decimalValue{num: got, prec: c.prec, scale: c.scale}
		text := want.ToString(c.scale)
		if d.String() != text || fmt.Sprint(d) != text || renderValue(d) != renderValue(text) ||
			renderValue(d) != renderValue(c.in) {
			t.Errorf("decimal %q: String=%q Sprint=%q render=%q, the string form was %q (render %q)",
				c.in, d.String(), fmt.Sprint(d), renderValue(d), text, renderValue(text))
		}
	}
	// The same number written other ways is the same stored integer.
	for in, same := range map[string]string{"1.5": "1.50", "+1.50000": "1.50", "15e-1": "1.50", "0.015E2": "1.50",
		" 1.50 ": "1.50", "1500e-3": "1.50", "-0": "0", "0e99": "0", ".5": "0.50", "5.": "5.00"} {
		a, ok1 := parseDecimalExact(in, 12, 2)
		b, ok2 := parseDecimalExact(same, 12, 2)
		if !ok1 || !ok2 || a != b {
			t.Errorf("parseDecimalExact(%q) = %v,%v but (%q) = %v,%v", in, a, ok1, same, b, ok2)
		}
	}
	for _, c := range []struct {
		k  temporalKind
		in string
	}{
		{kindTimestampTZ, "2026-01-02 03:04:05.123456+00"}, {kindTimestampTZ, "infinity"},
		{kindTimestamp, "1900-01-01 00:00:00.000001"}, {kindDate, "2024-02-29"}, {kindDate, "-infinity"},
		{kindTime, "24:00:00"}, {kindTime, "12:34:56.5"},
	} {
		n, ok := parseTemporal(c.k, c.in)
		if !ok {
			t.Fatalf("parseTemporal(%q)", c.in)
		}
		v := temporalValue{k: c.k, v: n}
		if v.String() != c.in || fmt.Sprint(v) != c.in || renderValue(v) != renderValue(c.in) || plainValue(v) != c.in {
			t.Errorf("temporal %q: String=%q Sprint=%q render=%q", c.in, v.String(), fmt.Sprint(v), renderValue(v))
		}
	}
	// The native fast paths in renderValue say what fmt said.
	for _, f := range []float64{0, 1.5, -0.25, 1e20, 1e21, 1e-7, 123456789.125, 100, 1e6, math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, 5e-324} {
		if got, want := renderValue(f), normalizeNumeric(fmt.Sprint(f)); got != want {
			t.Errorf("renderValue(%v) = %q, was %q", f, got, want)
		}
	}
	for _, n := range []int64{0, -1, 42, math.MaxInt64, math.MinInt64} {
		if got, want := renderValue(n), normalizeNumeric(fmt.Sprint(n)); got != want {
			t.Errorf("renderValue(%d) = %q, was %q", n, got, want)
		}
	}
}

// Each of these used to be stored as NULL or as a different number. Now the
// batch is refused, the mirror halts naming the value, and no file is written —
// including for the perfectly good row that arrived in the same batch.
func TestUnrepresentableValuesHaltAndWriteNothing(t *testing.T) {
	for _, c := range []struct{ pgType, value string }{
		{"numeric", "0.123456789012345"},                 // was stored as 0.123456789
		{"numeric", "1e-12"},                             // was stored as 0
		{"numeric", "NaN"},                               // was NULL
		{"numeric(12,2)", "NaN"},                         // was NULL
		{"numeric", "123456789012345678901234567890123"}, // 33 digits; was NULL
		{"numeric", "Infinity"},
		{"numeric", "-Infinity"},
		{"numeric(12,2)", "1.005"},
		{"numeric(4,2)", "100.00"},
		{"numeric(10,0)", "0.5"},
		{"numeric", "12 apples"},
		{"numeric", ""},
		{"bigint", "9223372036854775808"},
		{"bigint", "1.5"},
		{"integer", "12x"},
		{"double precision", "one"},
		{"double precision", "1e999"},
		{"boolean", "maybe"},
	} {
		t.Run(c.pgType+"/"+c.value, func(t *testing.T) {
			cols := map[string]string{"id": "bigint", "v": c.pgType}
			order := []string{"id", "v"}
			dir := t.TempDir()
			tbl, err := New(dir, "public", "r", "id", cols, order)
			if err != nil {
				t.Fatal(err)
			}
			good := map[string]any{"id": "1", "v": nil}
			bad := map[string]any{"id": "2", "v": c.value}
			_, err = tbl.Apply([]changestream.Transaction{{CommitLSN: "0/1", NextLSN: "0/1", Changes: []changestream.Change{
				{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: good, Key: good},
				{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: bad, Key: bad},
			}}})
			if err == nil {
				rows, _ := tbl.Live()
				t.Fatalf("the mirror accepted %s %q and now holds %v", c.pgType, c.value, rows)
			}
			h := tbl.Halted()
			if !strings.Contains(h, fmt.Sprintf("%q", c.value)) || !strings.Contains(h, `"v"`) || !strings.Contains(h, "public.r") {
				t.Errorf("halt reason does not name the table, column and value: %q", h)
			}
			for _, sub := range []string{"base", "delta"} {
				if m, _ := filepath.Glob(filepath.Join(tbl.Dir, sub, "*")); len(m) != 0 {
					t.Errorf("a refused batch left files in %s: %v", sub, m)
				}
			}
			// What a restart sees: halted, and empty.
			again, err := New(dir, "public", "r", "id", cols, order)
			if err != nil {
				t.Fatal(err)
			}
			if again.Halted() == "" {
				t.Error("the halt did not survive a restart")
			}
			if rows, err := again.Live(); err != nil || len(rows) != 0 {
				t.Errorf("after restart the mirror holds %v (err %v), expected nothing", rows, err)
			}
		})
	}

	// The snapshot's path into the writer, which no part of Apply covers.
	t.Run("AppendValues", func(t *testing.T) {
		cols := map[string]string{"id": "bigint", "v": "numeric"}
		dir := t.TempDir()
		tbl, err := New(dir, "public", "r", "id", cols, []string{"id", "v"})
		if err != nil {
			t.Fatal(err)
		}
		w, err := tbl.newParquetWriter(filepath.Join(dir, "direct.parquet"), nil, true)
		if err != nil {
			t.Fatal(err)
		}
		err = w.AppendValues([]any{"1", "NaN"})
		_ = w.Close()
		if err == nil || tbl.Halted() == "" {
			t.Fatalf("bootstrap would have loaded numeric NaN as NULL: err=%v halted=%q", err, tbl.Halted())
		}
	})
}

// The refusal must not reach values the column holds exactly, however they are
// spelled.
func TestRepresentableValuesAreStillAccepted(t *testing.T) {
	cases := []struct{ pgType, value, want string }{
		{"numeric", "0.123456789", "0.123456789"},
		{"numeric", "0.1234567890000", "0.123456789"}, // zeros below the scale lose nothing
		{"numeric", "1e3", "1000"},
		{"numeric", "12345678901234567890123456789", "12345678901234567890123456789"}, // 29 digits: the most Decimal(38,9) has room for
		{"numeric(12,2)", "-9999999999.99", "-9999999999.99"},
		{"numeric(12,2)", "5", "5"},
		{"bigint", " 42 ", "42"},
		{"bigint", "-9223372036854775808", "-9223372036854775808"},
		{"double precision", "NaN", "NaN"},
		{"double precision", "1e308", "1e+308"},
		{"real", "0.1", "0.1"},
		{"boolean", "TRUE", "true"},
		{"boolean", "f", "false"},
		{"text", "NaN", "NaN"},
	}
	for i, c := range cases {
		cols := map[string]string{"id": "bigint", "v": c.pgType}
		tbl, err := New(t.TempDir(), "public", "r", "id", cols, []string{"id", "v"})
		if err != nil {
			t.Fatal(err)
		}
		row := map[string]any{"id": fmt.Sprint(i), "v": c.value}
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/1", NextLSN: "0/1", Changes: []changestream.Change{
			{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row},
		}}}); err != nil {
			t.Errorf("%s %q was refused: %v", c.pgType, c.value, err)
			continue
		}
		rows, err := tbl.Live()
		if err != nil || len(rows) != 1 {
			t.Fatalf("%s %q: live = %v, %v", c.pgType, c.value, rows, err)
		}
		if got := Render(rows[0]["v"]); got != c.want || rows[0]["v"] == nil {
			t.Errorf("%s %q: mirror holds %#v (renders %q), want %q", c.pgType, c.value, rows[0]["v"], got, c.want)
		}
	}
}

// A UUID key takes the compact form, prints as the text it came from, and
// anything that is not exactly that text stays a string.
func TestUUIDKeysAreCompactAndLossless(t *testing.T) {
	const u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	k := keyFor(false, u)
	if !k.isUUID || k.str != "" || k.String() != u {
		t.Fatalf("keyFor(%q) = %+v, String %q", u, k, k.String())
	}
	if k != keyStringFor(false, u) || k != keyFor(true, u) {
		t.Error("the same UUID produced different keys by different routes")
	}
	for _, s := range []string{"00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
		"01234567-89ab-cdef-0123-456789abcdef", costUUID(1), costUUID(123456789)} {
		if got := keyFor(false, s); !got.isUUID || got.String() != s {
			t.Errorf("round trip of %q gave %+v / %q", s, got, got.String())
		}
	}
	// In a text column these are different keys from the canonical spelling,
	// and from each other. None may be folded onto it.
	seen := map[rowKey]string{k: u}
	for _, s := range []string{
		"A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", "a0eebc999c0b4ef8bb6d6bb9bd380a11",
		"{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11 ",
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1g", "a0eebc99-9c0b-4ef8-bb6d6-bb9bd380a11", "a0eebc99-9c0b-4ef8-bb6d--bb9bd380a11",
		"-0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "",
	} {
		got := keyFor(false, s)
		if got.isUUID || got.String() != s {
			t.Errorf("%q is not canonical UUID text but became %+v (String %q)", s, got, got.String())
		}
		if prev, dup := seen[got]; dup {
			t.Errorf("%q and %q are the same key", s, prev)
		}
		seen[got] = s
	}

	x := newKeyIndex(4)
	x.set(k, loc{File: 3, Pos: 7})
	x.set(keyFor(false, "plain"), loc{File: 3, Pos: 8})
	x.set(keyFor(true, "9"), loc{File: 3, Pos: 9})
	if x.len() != 3 || x.strs[u] != (loc{}) || len(x.uuids) != 1 {
		t.Fatalf("index holds nums=%d uuids=%d strs=%d", len(x.nums), len(x.uuids), len(x.strs))
	}
	if l, ok := x.get(keyFor(false, u)); !ok || l.Pos != 7 {
		t.Errorf("get = %+v, %v", l, ok)
	}
	var got []string
	x.each(func(k rowKey, _ loc) { got = append(got, k.String()) })
	sort.Strings(got)
	if strings.Join(got, ",") != "9,"+u+",plain" {
		t.Errorf("each visited %v", got)
	}
	x.deleteWhere(func(l loc) bool { return l.Pos == 7 })
	if _, ok := x.get(k); ok || x.len() != 2 {
		t.Error("deleteWhere left the UUID key behind")
	}
	x.set(k, loc{File: 3, Pos: 7})
	x.del(k)
	if _, ok := x.get(k); ok {
		t.Error("del left the UUID key behind")
	}
}

// End to end on a table keyed by uuid: updates and deletes find their rows, a
// compaction keeps them, and an index rebuilt from the files after a restart
// agrees with the one that was built from the stream.
func TestUUIDKeyedTable(t *testing.T) {
	cols := map[string]string{"id": "uuid", "v": "text"}
	order := []string{"id", "v"}
	dir := t.TempDir()
	tbl, err := New(dir, "public", "u", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	lsn := 0
	apply := func(op changestream.Op, rows ...map[string]any) {
		t.Helper()
		var ch []changestream.Change
		for _, r := range rows {
			ch = append(ch, changestream.Change{Op: op, Schema: "public", Table: "u", Row: r, Key: r})
		}
		lsn++
		pos := fmt.Sprintf("0/%X", lsn)
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: ch}}); err != nil {
			t.Fatal(err)
		}
	}
	const n = 500
	var rows []map[string]any
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{"id": costUUID(i), "v": "old"})
	}
	apply(changestream.OpInsert, rows...)
	check := func(step string) {
		t.Helper()
		live, err := tbl.Live()
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, r := range live {
			got[r["id"].(string)] = r["v"].(string)
		}
		if len(live) != n-10 || len(got) != n-10 {
			t.Fatalf("%s: %d live rows, %d distinct keys, want %d", step, len(live), len(got), n-10)
		}
		for i := 10; i < n; i++ {
			want := "old"
			if i < 110 {
				want = "new"
			}
			if got[costUUID(i)] != want {
				t.Fatalf("%s: key %s holds %q, want %q", step, costUUID(i), got[costUUID(i)], want)
			}
		}
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	var upd, del []map[string]any
	for i := 0; i < 110; i++ {
		upd = append(upd, map[string]any{"id": costUUID(i), "v": "new"})
	}
	for i := 0; i < 10; i++ {
		del = append(del, map[string]any{"id": costUUID(i)})
	}
	apply(changestream.OpUpdate, upd...)
	apply(changestream.OpDelete, del...)
	check("after updates and deletes")
	if tbl.index == nil || len(tbl.index.uuids) == 0 || len(tbl.index.strs) != 0 || len(tbl.index.nums) != 0 {
		t.Errorf("the index is not holding these keys as UUIDs: %+v", tbl.index)
	}

	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction started")
	}
	<-c.done
	if _, err := tbl.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
	check("after compaction")

	tbl, err = New(dir, "public", "u", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := tbl.BuildIndex(); err != nil || got != n-10 {
		t.Fatalf("rebuilt index holds %d keys (err %v), want %d", got, err, n-10)
	}
	apply(changestream.OpUpdate, map[string]any{"id": costUUID(200), "v": "old"})
	check("after restart and one more update")
	if _, err := os.Stat(tbl.Dir); err != nil {
		t.Fatal(err)
	}
}

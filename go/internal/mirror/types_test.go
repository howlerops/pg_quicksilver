package mirror

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/pgtext"
	"github.com/jackc/pgx/v5"
)

// ---- helpers ----------------------------------------------------------------

// duck runs one statement in DuckDB and returns its rows, tab-separated. The
// tests that call it are opt-in, like TestPublishedSnapshotInDuckDB:
//
//	QS_DUCKDB_PYTHON=python3 go test ./internal/mirror/
func duck(t *testing.T, sql string) []string {
	t.Helper()
	py := os.Getenv("QS_DUCKDB_PYTHON")
	if py == "" {
		t.Skip("set QS_DUCKDB_PYTHON to a python with duckdb installed")
	}
	script, _ := filepath.Abs(filepath.Join("testdata", "duck_rows.py"))
	out, err := exec.Command(py, script, sql).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", script, err, out)
	}
	s := strings.TrimRight(string(out), "\n")
	if strings.HasPrefix(s, "ERROR") {
		t.Fatalf("DuckDB refused the statement: %s\n%s", s, sql)
	}
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func haveDuck() bool { return os.Getenv("QS_DUCKDB_PYTHON") != "" }

// withLegacyWriter runs fn with the writer producing the representation it
// produced before types.go: every integer INT64, every float DOUBLE, uuid and
// bytea as text, unconstrained numeric DECIMAL(38,9), -infinity as the type's
// minimum, no bloom filters.
func withLegacyWriter(fn func()) {
	legacyTypes = true
	defer func() { legacyTypes = false }()
	fn()
}

func typedChange(op changestream.Op, table string, row map[string]any) changestream.Change {
	return changestream.Change{Op: op, Schema: "public", Table: table, Row: row, Key: row}
}

func tapply(t *testing.T, tbl *Table, lsn int, changes ...changestream.Change) {
	t.Helper()
	pos := fmt.Sprintf("0/%X", lsn)
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: pos, NextLSN: pos, Changes: changes,
	}}); err != nil {
		t.Fatal(err)
	}
}

// fileTypes is each column of a Parquet file as an engine reads it.
func fileTypes(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	out := map[string]string{}
	s := rdr.MetaData().Schema
	for i := 0; i < s.NumColumns(); i++ {
		out[s.Column(i).Name()] = parquetSQLType(s.Column(i))
	}
	return out
}

func dataFiles(tbl *Table) []string {
	var out []string
	for _, b := range tbl.State.BaseFiles {
		out = append(out, filepath.Join(tbl.Dir, "base", b))
	}
	for _, d := range tbl.State.DeltaFiles {
		out = append(out, filepath.Join(tbl.Dir, "delta", d))
	}
	return out
}

// liveText is the mirror's contents as text, keyed by the key column.
func liveText(t *testing.T, tbl *Table) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	if err := tbl.ForEachLive(func(r map[string]any) error {
		m := map[string]string{}
		for c, v := range r {
			if v == nil {
				m[c] = "<null>"
			} else {
				m[c] = Render(v)
			}
		}
		k := m[tbl.Key]
		if _, dup := out[k]; dup {
			t.Errorf("key %s is live twice", k)
		}
		out[k] = m
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func sameContents(t *testing.T, step string, got, want map[string]map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: mirror holds %d rows, expected %d", step, len(got), len(want))
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Errorf("%s: key %s is missing", step, k)
			continue
		}
		for c, wv := range w {
			if g[c] != wv {
				t.Errorf("%s: key %s column %s: mirror %q, expected %q", step, k, c, g[c], wv)
			}
		}
	}
}

// ---- the mapping --------------------------------------------------------------

var everyType = allTypes

var allTypes = []struct{ name, pg, sql, value, alt string }{
	{"id", "uuid", "UUID", "", ""},
	{"si", "smallint", "SMALLINT", "-32768", "12"},
	{"i", "integer", "INTEGER", "2147483647", "-7"},
	{"bi", "bigint", "BIGINT", "-9223372036854775808", "99"},
	{"r", "real", "FLOAT", "1e+06", "0.1"},
	{"d", "double precision", "DOUBLE", "123456789.12345679", "1000000"},
	{"b", "boolean", "BOOLEAN", "t", "f"},
	{"ref", "uuid", "UUID", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "00000000-0000-0000-0000-000000000000"},
	{"raw", "bytea", "BLOB", `\xdeadbeef00ff`, `\x`},
	{"n", "numeric", "VARCHAR", "0.123456789012345", "NaN"},
	{"n2", "numeric(12,2)", "DECIMAL(12,2)", "-9999999999.99", "1.50"},
	{"n50", "numeric(50,2)", "VARCHAR", "123456789012345678901234567890123456789012345678.25", "1.50"},
	{"dt", "date", "DATE", "-infinity", "2024-02-29"},
	{"tm", "time without time zone", "TIME", "24:00:00", "12:34:56.5"},
	{"ts", "timestamp without time zone", "TIMESTAMP", "-infinity", "1900-01-01 00:00:00.000001"},
	{"tz", "timestamp with time zone", "TIMESTAMP WITH TIME ZONE", "infinity", "2026-01-02 03:04:05.123456+00"},
	{"ch", "character(8)", "VARCHAR", "ab      ", "abcdefgh"},
	{"ip", "inet", "VARCHAR", "192.168.1.7", "::1/64"},
	{"iv", "interval", "VARCHAR", "1 year 2 mons", "-00:00:01"},
	{"ttz", "time with time zone", "VARCHAR", "12:30:00+05:30", "00:00:00+00"},
	{"m", "money", "VARCHAR", "$12.34", "-$1.00"},
	{"bt", "bit(4)", "VARCHAR", "1010", "0000"},
	{"arr", "integer[]", "VARCHAR", "{1,2,NULL}", "{}"},
	{"js", "jsonb", "VARCHAR", `{"a": 1}`, "null"},
	{"tx", "text", "VARCHAR", "1.50", ""},
}

func everyTypeTable(t *testing.T, dir string) *Table {
	t.Helper()
	cols := map[string]string{}
	var order []string
	for _, c := range everyType {
		cols[c.name] = c.pg
		order = append(order, c.name)
	}
	tbl, err := New(dir, "public", "every", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func everyRow(i int, alt bool) map[string]any {
	row := map[string]any{}
	for _, c := range everyType {
		row[c.name] = c.value
		if alt {
			row[c.name] = c.alt
		}
	}
	row["id"] = fmt.Sprintf("%08x-1111-4222-8333-%012x", uint32(i)*2654435761, i)
	return row
}

func expectRow(row map[string]any) map[string]string {
	m := map[string]string{}
	for c, v := range row {
		if v == nil {
			m[c] = "<null>"
		} else {
			m[c] = Render(v)
		}
	}
	if m["b"] == "t" {
		m["b"] = "true"
	} else if m["b"] == "f" {
		m["b"] = "false"
	}
	return m
}

// Each declared type reaches the file as the type in the table at the top of
// types.go, and reads back as the text PostgreSQL printed.
func TestEveryTypeIsStoredAsDeclared(t *testing.T) {
	tbl := everyTypeTable(t, t.TempDir())
	want := map[string]map[string]string{}
	var changes []changestream.Change
	for i := 0; i < 6; i++ {
		row := everyRow(i, i%2 == 1)
		if i == 5 {
			for _, c := range everyType[1:] {
				row[c.name] = nil
			}
		}
		changes = append(changes, typedChange(changestream.OpInsert, "every", row))
		want[row["id"].(string)] = expectRow(row)
	}
	tapply(t, tbl, 1, changes...)
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	sameContents(t, "after compaction", liveText(t, tbl), want)

	stored := map[string]string{ // where the file's type is not the view's
		"n": "VARCHAR", "n50": "VARCHAR",
	}
	got := fileTypes(t, dataFiles(tbl)[0])
	for _, c := range everyType {
		wantFile := c.sql
		if s, ok := stored[c.name]; ok {
			wantFile = s
		}
		if got[c.name] != wantFile {
			t.Errorf("%s (%s) is stored as %s, expected %s", c.name, c.pg, got[c.name], wantFile)
		}
		if s := sqlType(c.pg); s != c.sql {
			t.Errorf("%s (%s) is presented as %s, expected %s", c.name, c.pg, s, c.sql)
		}
	}

	// ...and an engine agrees, column by column.
	if !haveDuck() {
		return
	}
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	var sel []string
	for _, c := range everyType {
		sel = append(sel, "typeof("+sqlIdent(c.name)+") AS "+sqlIdent(c.name))
	}
	types := strings.Split(duck(t, "SELECT DISTINCT "+strings.Join(sel, ", ")+" FROM ("+sql+")")[0], "\t")
	for i, c := range everyType {
		if types[i] != c.sql {
			t.Errorf("DuckDB reads %s (%s) as %s, expected %s", c.name, c.pg, types[i], c.sql)
		}
	}
	// The values PostgreSQL and DuckDB spell the same way, compared as text.
	rows := duck(t, `SELECT id, si, i, bi, ref, lower(hex(raw)), n2, dt, ts, ch, ip, tx
		FROM (`+sql+`) WHERE si = -32768 ORDER BY id LIMIT 1`)
	wantRow := "00000000-1111-4222-8333-000000000000\t-32768\t2147483647\t-9223372036854775808\t" +
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11\tdeadbeef00ff\t-9999999999.99\t-infinity\t-infinity\t" +
		"ab      \t192.168.1.7\t1.50"
	if len(rows) != 1 || rows[0] != wantRow {
		t.Errorf("DuckDB reads\n  %q\nexpected\n  %q", rows, wantRow)
	}
	for q, want := range map[string]string{
		`r = CAST('0.1' AS FLOAT)`: "2",
		`d = 1000000`:              "2",
		`n = 'NaN'`:                "2",
		`ref = 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'`:         "3",
		`dt = '-infinity' AND ts < '0001-01-01' AND isinf(tz)`: "3",
		`b`:                     "3",
		`octet_length(raw) = 6`: "3",
		`n50 = '123456789012345678901234567890123456789012345678.25' AND n = '0.123456789012345' AND tm = '24:00:00'`: "3",
	} {
		if got := duck(t, `SELECT count(*) FROM (`+sql+`) WHERE `+q); len(got) != 1 || got[0] != want {
			t.Errorf("WHERE %s counts %v, expected %s", q, got, want)
		}
	}
}

// ---- coexistence --------------------------------------------------------------

// A mirror written by the OLD writer, then run by the new one: applied to,
// patched, merged, rewritten and restarted, with the two representations side
// by side in one table for most of it. The contents must be right at every
// step, by the Go read path and — with an engine available — by the view.
func TestOldFilesAndNewCoexist(t *testing.T) {
	for _, keyCol := range []string{"id", "bi"} {
		t.Run("key="+keyCol, func(t *testing.T) {
			dir := t.TempDir()
			cols := map[string]string{}
			var order []string
			// Without numeric(50,2): the old writer could not create a file
			// for one at all (TestNumericWiderThanADecimalIsText).
			var everyType []struct{ name, pg, sql, value, alt string }
			for _, c := range allTypes {
				if c.name == "n50" {
					continue
				}
				everyType = append(everyType, c)
				cols[c.name] = c.pg
				order = append(order, c.name)
			}
			open := func() *Table {
				tbl, err := New(dir, "public", "every", keyCol, cols, order)
				if err != nil {
					t.Fatal(err)
				}
				return tbl
			}
			row := func(i int, alt bool) map[string]any {
				r := everyRow(i, alt)
				delete(r, "n50")
				r["bi"] = fmt.Sprint(int64(i)*1_000_003 - 7)
				// What DECIMAL(38,9) holds: the old mirror halted on the rest.
				r["n"] = []string{"12.5", "0.000000001"}[i%2]
				return r
			}
			want := map[string]map[string]string{}
			raw := map[string]map[string]any{}
			put := func(r map[string]any) changestream.Change {
				want[r[keyCol].(string)] = expectRow(r)
				raw[r[keyCol].(string)] = r
				return typedChange(changestream.OpInsert, "every", r)
			}
			check := func(step string, tbl *Table) {
				t.Helper()
				sameContents(t, step, liveText(t, tbl), want)
				if !haveDuck() {
					return
				}
				sql, err := tbl.ViewSQL()
				if err != nil {
					t.Fatalf("%s: %v", step, err)
				}
				// Everything that has one spelling in both engines, as text,
				// ordered — plus the engine's own type for every column, which
				// must not depend on which files happen to be there.
				var typs []string
				for _, c := range everyType {
					typs = append(typs, "typeof("+sqlIdent(c.name)+")")
				}
				got := duck(t, `SELECT id, si, i, bi, "ref", concat(chr(92), 'x', lower(hex(raw))), n2, dt, ts, ch, ip, tx, `+
					strings.Join(typs, "||','||")+` FROM (`+sql+`) ORDER BY id`)
				var wantTypes []string
				for _, c := range everyType {
					wantTypes = append(wantTypes, c.sql)
				}
				var exp []string
				for k := range want {
					var f []string
					for _, c := range []string{"id", "si", "i", "bi", "ref", "raw", "n2", "dt", "ts", "ch", "ip", "tx"} {
						v, _ := raw[k][c].(string)
						if c == "n2" {
							v = decimal2(v)
						}
						f = append(f, v)
					}
					exp = append(exp, strings.Join(f, "\t")+"\t"+strings.Join(wantTypes, ","))
				}
				sort.Strings(exp)
				if len(got) != len(exp) {
					t.Errorf("%s: DuckDB reads %d rows, expected %d", step, len(got), len(exp))
				}
				for i := 0; i < len(got) && i < len(exp); i++ {
					if got[i] != exp[i] {
						t.Errorf("%s: DuckDB reads\n  %s\nexpected\n  %s", step, got[i], exp[i])
						break
					}
				}
			}

			// 1. The old writer builds a base file and leaves deltas on it.
			var tbl *Table
			withLegacyWriter(func() {
				tbl = open()
				var batch []changestream.Change
				for i := 0; i < 40; i++ {
					batch = append(batch, put(row(i, i%2 == 1)))
				}
				tapply(t, tbl, 1, batch...)
				if _, err := tbl.Compact(); err != nil {
					t.Fatal(err)
				}
				tapply(t, tbl, 2, put(row(40, false)), put(row(41, true)), put(row(3, false)))
				tapply(t, tbl, 3, put(row(42, false)))
				for _, f := range dataFiles(tbl) {
					if ft := fileTypes(t, f); ft["id"] != "VARCHAR" || ft["i"] != "BIGINT" ||
						ft["r"] != "DOUBLE" || ft["n"] != "DECIMAL(38,9)" && ft["n"] != "" {
						t.Fatalf("the legacy writer did not write the old representation: %v", ft)
					}
				}
				tbl.Close()
			})
			// The old writer stored an unconstrained numeric as DECIMAL(38,9),
			// which could not hold these; the old mirror never had them.
			check("old files, new reader", open())

			// 2. The new writer applies on top: an update and a delete of rows
			// in the old base, an update of a row in an old delta, an insert.
			tbl = open()
			tapply(t, tbl, 4, put(row(5, true)), put(row(41, false)), put(row(50, true)),
				typedChange(changestream.OpDelete, "every", row(7, false)))
			delete(want, row(7, false)[keyCol].(string))
			check("old base, old and new deltas", tbl)
			newest := dataFiles(tbl)[len(dataFiles(tbl))-1]
			if ft := fileTypes(t, newest); ft["ref"] != "UUID" || ft["i"] != "INTEGER" || ft["r"] != "FLOAT" {
				t.Fatalf("the new writer did not write the new representation: %v", ft)
			}

			// 3. A patch: one column of a row that lives in the old base.
			patch := map[string]any{keyCol: row(9, false)[keyCol], "tx": "patched"}
			tapply(t, tbl, 5, changestream.Change{Op: changestream.OpUpdate, Schema: "public",
				Table: "every", Row: patch, Key: map[string]any{keyCol: patch[keyCol]}})
			want[patch[keyCol].(string)]["tx"] = "patched"
			raw[patch[keyCol].(string)]["tx"] = "patched"
			check("a patch on the old base", tbl)

			// ...and the same change with patches off, which is carry-forward:
			// every other column is fetched from the old files — one row in a
			// delta, one in the base — and written to a new file.
			carried := map[string]any{keyCol: row(42, false)[keyCol], "tx": "carried"}
			carried2 := map[string]any{keyCol: row(11, false)[keyCol], "tx": "carried"}
			before := len(tbl.State.PartialCols)
			PartialDeltas = false
			tapply(t, tbl, 16, changestream.Change{Op: changestream.OpUpdate, Schema: "public",
				Table: "every", Row: carried, Key: map[string]any{keyCol: carried[keyCol]}},
				changestream.Change{Op: changestream.OpUpdate, Schema: "public",
					Table: "every", Row: carried2, Key: map[string]any{keyCol: carried2[keyCol]}})
			PartialDeltas = true
			if len(tbl.State.PartialCols) != before {
				t.Fatalf("the update was stored as a patch, so carry-forward was not exercised")
			}
			want[carried2[keyCol].(string)]["tx"] = "carried"
			raw[carried2[keyCol].(string)]["tx"] = "carried"
			want[carried[keyCol].(string)]["tx"] = "carried"
			raw[carried[keyCol].(string)]["tx"] = "carried"
			check("carry-forward from an old delta", tbl)

			// 4. The recovered schema is the newest file's, not the oldest's.
			rc, _, err := ColumnsFromFiles(filepath.Dir(tbl.Dir), "public", "every")
			if err != nil {
				t.Fatal(err)
			}
			if rc["ref"] != "uuid" || rc["i"] != "integer" || rc["r"] != "real" || rc["raw"] != "bytea" {
				t.Errorf("ColumnsFromFiles recovered the old representation: %v", rc)
			}

			// 5. Restart: the index is rebuilt from old and new files together.
			tbl.Close()
			tbl = open()
			if _, err := tbl.BuildIndex(); err != nil {
				t.Fatal(err)
			}
			tapply(t, tbl, 17, put(row(0, true)), put(row(40, true)), put(row(50, false)))
			check("after a restart", tbl)

			// 6. Old and new deltas merged into one new file.
			if _, err := tbl.MergeDeltas(); err != nil {
				t.Fatal(err)
			}
			check("after a delta merge", tbl)

			// 7. The base rewrite, which is where the old files end.
			if _, err := tbl.Compact(); err != nil {
				t.Fatal(err)
			}
			check("after the rewrite", tbl)
			for _, f := range dataFiles(tbl) {
				ft := fileTypes(t, f)
				for _, c := range everyType {
					wantFile := c.sql
					if c.name == "n" || c.name == "n50" {
						wantFile = "VARCHAR"
					}
					if ft[c.name] != wantFile {
						t.Errorf("after the rewrite %s still holds %s as %s", filepath.Base(f), c.name, ft[c.name])
					}
				}
			}
			tbl.Close()
			tbl = open()
			tapply(t, tbl, 18, put(row(1, false)), typedChange(changestream.OpDelete, "every", row(2, false)))
			delete(want, row(2, false)[keyCol].(string))
			check("after the rewrite and a restart", tbl)

			// 8. The background rewrite reads the same files the same way.
			tapply(t, tbl, 19, put(row(60, true)))
			if c := tbl.BeginCompaction(); c != nil {
				<-c.done
				if _, err := tbl.FinishCompaction(); err != nil {
					t.Fatal(err)
				}
			}
			check("after a background rewrite", tbl)
			tbl.Close()
		})
	}
}

// decimal2 is a numeric(12,2) as DuckDB prints a DECIMAL(12,2): two decimals.
func decimal2(s string) string {
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return s + ".00"
	}
	for len(s)-i-1 < 2 {
		s += "0"
	}
	return s
}

// A uuid is one key whether it arrives as text or as 16 bytes.
func TestUUIDKeyIsTheSameFromTextAndFromBytes(t *testing.T) {
	const s = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	u, ok := parseUUIDValue(s)
	if !ok || u.String() != s {
		t.Fatalf("parseUUIDValue(%q) = %v, %v", s, u, ok)
	}
	if keyFor(false, u) != keyFor(false, s) || !keyFor(false, u).isUUID {
		t.Errorf("a uuid keys differently as bytes (%+v) and as text (%+v)", keyFor(false, u), keyFor(false, s))
	}
	if kidxWidth(keyFor(false, u)) != 16 {
		t.Errorf("a 16-byte uuid key is not one the on-disk index can hold")
	}
	if up, ok := parseUUIDValue(strings.ToUpper(s)); !ok || up != u {
		t.Errorf("an upper-case uuid is a different uuid")
	}
}

// ---- (a) -infinity -----------------------------------------------------------

// -infinity is stored as the integer DuckDB uses for it, and an older file
// that holds the type's minimum still reads as -infinity — here and in the
// view.
func TestNegativeInfinityIsStoredAsTheEngineReadsIt(t *testing.T) {
	for k, want := range map[temporalKind]int64{
		kindTimestamp: -math.MaxInt64, kindTimestampTZ: -math.MaxInt64, kindDate: -math.MaxInt32,
	} {
		v, ok := storable(temporalArrowType(k), "-infinity")
		if !ok || v.(temporalValue).v != want {
			t.Errorf("-infinity is stored as %v, expected %d: DuckDB reads the type's "+
				"minimum as a finite date in 5877642 BC", v, want)
		}
		if v.(temporalValue).String() != "-infinity" {
			t.Errorf("stored -infinity renders as %q", v.(temporalValue).String())
		}
	}
	for k, want := range map[temporalKind]int64{kindTimestamp: math.MaxInt64, kindDate: math.MaxInt32} {
		if v, ok := storable(temporalArrowType(k), "infinity"); !ok || v.(temporalValue).v != want {
			t.Errorf("infinity is stored as %v, expected %d", v, want)
		}
	}

	cols := map[string]string{"id": "bigint", "dt": "date", "ts": "timestamp", "tz": "timestamptz"}
	order := []string{"id", "dt", "ts", "tz"}
	dir := t.TempDir()
	rows := func(from int) []changestream.Change {
		var out []changestream.Change
		for i, v := range []string{"-infinity", "infinity"} {
			out = append(out, typedChange(changestream.OpInsert, "inf", map[string]any{
				"id": fmt.Sprint(from + i), "dt": v, "ts": v, "tz": v}))
		}
		return append(out, typedChange(changestream.OpInsert, "inf", map[string]any{
			"id": fmt.Sprint(from + 2), "dt": "2024-02-29", "ts": "2024-02-29 00:00:00", "tz": "2024-02-29 00:00:00+00"}))
	}
	var tbl *Table
	withLegacyWriter(func() {
		tbl, _ = New(dir, "public", "inf", "id", cols, order)
		tapply(t, tbl, 1, rows(0)...)
	})
	tapply(t, tbl, 2, rows(10)...)
	for k, r := range liveText(t, tbl) {
		if k == "0" || k == "10" {
			if r["dt"] != "-infinity" || r["ts"] != "-infinity" || r["tz"] != "-infinity" {
				t.Errorf("row %s reads back as %v", k, r)
			}
		}
	}
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(sql, "CAST('-infinity' AS") != 3 {
		t.Errorf("the view should map the old sentinel in the old file's three columns and nowhere else:\n%s", sql)
	}
	if !haveDuck() {
		return
	}
	got := duck(t, "SELECT id, dt, ts, tz, isinf(dt) AND isinf(ts) AND isinf(tz) FROM ("+sql+") ORDER BY id")
	want := []string{
		"0\t-infinity\t-infinity\t-infinity\ttrue",
		"1\tinfinity\tinfinity\tinfinity\ttrue",
		"2\t2024-02-29\t2024-02-29 00:00:00\t2024-02-29 00:00:00+00\tfalse",
		"10\t-infinity\t-infinity\t-infinity\ttrue",
		"11\tinfinity\tinfinity\tinfinity\ttrue",
		"12\t2024-02-29\t2024-02-29 00:00:00\t2024-02-29 00:00:00+00\tfalse",
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("DuckDB reads\n%s\nexpected\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// ---- (b) the snapshot's text is the stream's --------------------------------

// The snapshot must read each type as its OUTPUT FUNCTION prints it, because
// that is what pgoutput sends. Needs a server:
//
//	QS_TEST_PG_DSN=postgres://... go test -run TestSnapshotTextIsTheOutputFunctions ./internal/mirror/
func TestSnapshotTextIsTheOutputFunctions(t *testing.T) {
	dsn := os.Getenv("QS_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("set QS_TEST_PG_DSN to a PostgreSQL the test may create a table in")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgtext.PinDSN(dsn))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	types := []struct{ typ, values string }{
		{"character(8)", `'ab', '', 'abcdefgh'`},
		{"inet", `'192.168.1.7', '192.168.1.7/24', '::1', '10.0.0.0/8'`},
		{"cidr", `'10.0.0.0/8', '192.168.1.0/24', '::/0'`},
		{"macaddr", `'08:00:2b:01:02:03'`},
		{"bit(4)", `B'1010'`},
		{"bit varying(8)", `B'1', B''`},
		{"money", `'12.34', '-1'`},
		{"interval", `'1 year 2 mons', '-1 day +02:00:00', '178000000 years', '0'`},
		{"time with time zone", `'12:30:00+05:30', '00:00:00Z'`},
		{"boolean", `true, false`},
		{"smallint", `-32768`}, {"integer", `2147483647`}, {"bigint", `-9223372036854775808`},
		{"real", `1000000, 0.1, 'NaN', '-Infinity', 3.4028235e+38, 1.17549435e-38`},
		{"double precision", `1000000, 123456789.12345679, 'Infinity', 'NaN', 1e15, 1e-5, 0.0001, -0.0`},
		{"numeric", `1.50, 'NaN', 0.33333333333333333333, 1e-12, 'Infinity'`},
		{"numeric(50,2)", `1.50`},
		{"uuid", `'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'`},
		{"bytea", `'\xdeadbeef', ''`},
		{"date", `'infinity', '-infinity', '2024-02-29'`},
		{"timestamp", `'-infinity', '2024-02-29 12:00:00.5'`},
		{"timestamptz", `'infinity', '2024-02-29 12:00:00+05'`},
		{"time", `'24:00:00', '12:34:56.789'`},
		{"character(3)[]", `'{ab,c}'`},
		{"text", `'  padded  ', 'true'`},
		{"jsonb", `'{"b":1,"a":[1.50]}'`},
		{"\"char\"", `'x'`},
		{"name", `'n'`},
	}
	for i, c := range types {
		tbl := fmt.Sprintf("qs_out_%d", i)
		if _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s; CREATE TABLE %s (id serial PRIMARY KEY, v %s)", tbl, tbl, c.typ)); err != nil {
			t.Fatalf("%s: %v", c.typ, err)
		}
		insert := fmt.Sprintf("INSERT INTO %s (v) SELECT unnest(ARRAY[%s]::%s[])", tbl, c.values, c.typ)
		if strings.HasSuffix(c.typ, "[]") { // one value: unnest would flatten it
			insert = fmt.Sprintf("INSERT INTO %s (v) VALUES ((%s)::%s)", tbl, c.values, c.typ)
		}
		if _, err := conn.Exec(ctx, insert); err != nil {
			t.Fatalf("%s: %v", c.typ, err)
		}
		// What the output function prints, asked for by name.
		var out string
		if err := conn.QueryRow(ctx, "SELECT typoutput::regproc::text FROM pg_type WHERE oid = $1::regtype", c.typ).Scan(&out); err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query(ctx, fmt.Sprintf("SELECT id::text, %s(v)::text, v::text FROM %s ORDER BY id", out, tbl))
		if err != nil {
			t.Fatal(err)
		}
		want, cast := map[string]string{}, map[string]string{}
		for rows.Next() {
			var id, o, c string
			if err := rows.Scan(&id, &o, &c); err != nil {
				t.Fatal(err)
			}
			want[id], cast[id] = o, c
		}
		rows.Close()
		n, castDiffers := 0, 0
		if err := SourceRows(ctx, conn, "public."+tbl, []string{"id", "v"}, func(vals []any) error {
			n++
			id := vals[0].(string)
			if vals[1] != want[id] {
				t.Errorf("%s: the snapshot reads %q, the output function prints %q", c.typ, vals[1], want[id])
			}
			if cast[id] != want[id] {
				castDiffers++
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if n != len(want) || n == 0 {
			t.Errorf("%s: read %d rows of %d", c.typ, n, len(want))
		}
		if castDiffers > 0 {
			t.Logf("%-22s col::text differs from the output function on %d of %d values", c.typ, castDiffers, n)
		}
		// And the mirror holds that text: a value must survive the writer as
		// what the source printed, or be refused — never be something else.
		m, err := New(t.TempDir(), "public", tbl, "id", map[string]string{"id": "integer", "v": c.typ}, []string{"id", "v"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Snapshot(ctx, conn, "0/1"); err != nil {
			t.Errorf("%s: snapshot: %v", c.typ, err)
			continue
		}
		for id, r := range liveText(t, m) {
			w := Render(want[id])
			if c.typ == "boolean" {
				w = map[string]string{"t": "true", "f": "false"}[want[id]]
			}
			if r["v"] != w {
				t.Errorf("%s: the mirror holds %q for %q", c.typ, r["v"], want[id])
			}
		}
		_, _ = conn.Exec(ctx, "DROP TABLE "+tbl)
	}
}

// ---- (c) numeric wider than a decimal ------------------------------------------

// numeric(50,2) asked Arrow for a Decimal128 of precision 50, which panics.
func TestNumericWiderThanADecimalIsText(t *testing.T) {
	for _, typ := range []string{"numeric(50,2)", "numeric(39,0)", "numeric(1000,500)", "numeric(3,-2)", "numeric(2,5)", "decimal(40,4)"} {
		tbl, err := New(t.TempDir(), "public", "wide", "id", map[string]string{"id": "bigint", "v": typ}, []string{"id", "v"})
		if err != nil {
			t.Fatal(err)
		}
		row := map[string]any{"id": "1", "v": "1.50"}
		tapply(t, tbl, 1, typedChange(changestream.OpInsert, "wide", row))
		if _, err := tbl.Compact(); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		if got := liveText(t, tbl)["1"]["v"]; got != "1.5" {
			t.Errorf("%s: mirror holds %q", typ, got)
		}
		if ft := fileTypes(t, dataFiles(tbl)[0]); ft["v"] != "VARCHAR" {
			t.Errorf("%s is stored as %s", typ, ft["v"])
		}
	}
	if got := arrowType("numeric(38,2)").String(); got != "decimal(38, 2)" {
		t.Errorf("numeric(38,2) is %s; 38 digits is what a Decimal128 holds", got)
	}
}

// ---- (d) floats as PostgreSQL prints them ---------------------------------------

// Each of these is the text float8out (float4out) prints for the value, and
// what the mirror must render for it — or qs-verify reports a divergence
// between two equal numbers.
func TestFloatsRenderAsPostgresPrintsThem(t *testing.T) {
	for _, s := range []string{"1000000", "123456789.12345679", "Infinity", "-Infinity", "NaN",
		"0", "-0", "1e+15", "100000000000000", "999999999999999.9", "1e-05", "0.0001", "1.5e-07",
		"1.7976931348623157e+308", "5e-324", "0.1", "-1401", "2.5e+15"} {
		v, ok := storable(arrowType("double precision"), s)
		if !ok {
			t.Fatalf("float8 %q refused", s)
		}
		if got := Render(v); got != s {
			t.Errorf("float8 %q renders as %q", s, got)
		}
	}
	for _, s := range []string{"1e+06", "100000", "999999.9", "0.1", "3.4028235e+38", "1e-45", "-Infinity", "NaN",
		"1.1754944e-38", "0.0001", "1e-05", "16777216" /* not below 1e6: */, "1.6777216e+07"} {
		v, ok := storable(arrowType("real"), s)
		if !ok {
			t.Fatalf("float4 %q refused", s)
		}
		want := s
		if s == "16777216" {
			want = "1.6777216e+07"
		}
		if got := Render(v); got != want {
			t.Errorf("float4 %q renders as %q", s, got)
		}
	}
	// Unequal values stay unequal.
	a, _ := storable(arrowType("double precision"), "0.1")
	b, _ := storable(arrowType("double precision"), "0.10000000000000002")
	if Render(a) == Render(b) {
		t.Error("two different float8 values render the same")
	}
	// A real that an older file holds as a DOUBLE renders as the real it is.
	if f := coerceFor(arrowType("real"), arrowType("double precision")); f == nil || Render(f(float64(1e6))) != "1e+06" {
		t.Error("a real read from an old DOUBLE column does not render as a float4")
	}
}

// ---- (e) NaN and statistics ---------------------------------------------------

// A float chunk holding NaN must not carry min/max statistics that exclude
// it; the chunks without one keep theirs.
func TestNaNChunkCarriesNoMisleadingStatistics(t *testing.T) {
	defer func(prev int64) { RowGroupRows = prev }(RowGroupRows)
	RowGroupRows = 100
	cols := map[string]string{"id": "bigint", "d": "double precision", "r": "real"}
	tbl, err := New(t.TempDir(), "public", "nan", "id", cols, []string{"id", "d", "r"})
	if err != nil {
		t.Fatal(err)
	}
	var changes []changestream.Change
	for i := 0; i < 300; i++ {
		d, r := fmt.Sprint(float64(i)+0.5), fmt.Sprint(i)
		if i == 150 {
			d = "NaN"
		}
		if i == 250 {
			r = "NaN"
		}
		changes = append(changes, typedChange(changestream.OpInsert, "nan",
			map[string]any{"id": fmt.Sprint(i), "d": d, "r": r}))
	}
	tapply(t, tbl, 1, changes...)
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	path := dataFiles(tbl)[0]
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatalf("the file is not readable after its footer was replaced: %v", err)
	}
	md := rdr.MetaData()
	if md.NumRowGroups() != 3 {
		t.Fatalf("%d row groups, expected 3", md.NumRowGroups())
	}
	for g := 0; g < 3; g++ {
		for col, nanGroup := range map[int]int{1: 1, 2: 2} {
			cc, _ := md.RowGroup(g).ColumnChunk(col)
			set, _ := cc.StatsSet()
			if g == nanGroup && set {
				st, _ := cc.Statistics()
				t.Errorf("row group %d column %d holds NaN and has statistics (min/max set: %v): "+
					"an engine will skip it for `= 'NaN'`", g, col, st != nil && st.HasMinMax())
			}
			if g != nanGroup && !set {
				t.Errorf("row group %d column %d holds no NaN and lost its statistics", g, col)
			}
		}
		if cc, _ := md.RowGroup(g).ColumnChunk(0); cc != nil {
			if set, _ := cc.StatsSet(); !set {
				t.Errorf("row group %d: the key column lost its statistics", g)
			}
		}
	}
	rdr.Close()
	f.Close()
	if n := len(liveText(t, tbl)); n != 300 {
		t.Errorf("read back %d rows of 300", n)
	}
	if !haveDuck() {
		return
	}
	sql, _ := tbl.ViewSQL()
	for q, want := range map[string]string{
		"SELECT id FROM (" + sql + ") WHERE d = 'NaN'":                "150",
		"SELECT id FROM (" + sql + ") WHERE r = 'NaN'":                "250",
		"SELECT count(*) FROM (" + sql + ") WHERE d > 1e300":          "1",
		"SELECT count(*) FROM (" + sql + ") WHERE d < 10.25":          "10",
		"SELECT count(*) FROM (" + sql + ") WHERE isnan(d)":           "1",
		"SELECT id FROM read_parquet('" + path + "') WHERE d = 'NaN'": "150",
	} {
		if got := duck(t, q); len(got) != 1 || got[0] != want {
			t.Errorf("%s\n  = %v, expected %s", q, got, want)
		}
	}
}

// ---- unconstrained numeric ------------------------------------------------------

func TestUnconstrainedNumericIsTextUnlessOptedIn(t *testing.T) {
	defer func(prev string) { NumericScale = prev }(NumericScale)
	cols := map[string]string{"id": "bigint", "amount": "numeric", "rate": "numeric"}
	order := []string{"id", "amount", "rate"}
	awkward := []string{"0.123456789012345", "1e-12", "NaN", "Infinity",
		"123456789012345678901234567890123456789012", "1.50"}

	// By default nothing an ordinary numeric column holds halts the table.
	NumericScale = ""
	dir := t.TempDir()
	tbl, _ := New(dir, "public", "num", "id", cols, order)
	for i, v := range awkward {
		tapply(t, tbl, i+1, typedChange(changestream.OpInsert, "num",
			map[string]any{"id": fmt.Sprint(i), "amount": v, "rate": v}))
	}
	if tbl.Halted() != "" {
		t.Fatalf("an unconstrained numeric halted the table: %s", tbl.Halted())
	}
	live := liveText(t, tbl)
	for i, v := range awkward {
		if got := live[fmt.Sprint(i)]["amount"]; got != Render(v) {
			t.Errorf("numeric %q read back as %q", v, got)
		}
	}
	if s := sqlType("numeric"); s != "VARCHAR" {
		t.Errorf("an unconstrained numeric is presented as %s", s)
	}

	// Opted in, a column is a decimal, takes what fits and halts on what does
	// not; the column beside it is untouched.
	NumericScale = "public.num.amount=4"
	tbl2, _ := New(t.TempDir(), "public", "num", "id", cols, order)
	if got := arrowType(tbl2.typeOf("amount")).String(); got != "decimal(38, 4)" {
		t.Fatalf("opted-in column is %s", got)
	}
	if got := arrowType(tbl2.typeOf("rate")).String(); got != "utf8" {
		t.Fatalf("the column that was not opted in is %s", got)
	}
	tapply(t, tbl2, 1, typedChange(changestream.OpInsert, "num", map[string]any{"id": "1", "amount": "12.3400", "rate": "NaN"}))
	bad := map[string]any{"id": "2", "amount": "0.00001", "rate": "1"}
	if _, err := tbl2.Apply([]changestream.Transaction{{CommitLSN: "0/2", NextLSN: "0/2",
		Changes: []changestream.Change{typedChange(changestream.OpInsert, "num", bad)}}}); err == nil || tbl2.Halted() == "" {
		t.Errorf("a value with more digits than the opted-in scale was accepted: %v", err)
	}
	NumericScale = "9"
	tbl4, _ := New(t.TempDir(), "public", "num", "id", cols, order)
	if got := arrowType(tbl4.typeOf("rate")).String(); got != "decimal(38, 9)" {
		t.Errorf("QS_NUMERIC_SCALE=9 should make every unconstrained numeric DECIMAL(38,9), got %s", got)
	}

	// The same mirror, opted in after the fact, once the rows that do not fit
	// are gone: the stored values are checked at the first manifest write
	// (numeric_optin.go), and from then the text files are cast by the view
	// and converted by the next rewrite.
	NumericScale = "public.num.rate=20"
	tbl3, _ := New(dir, "public", "num", "id", cols, order)
	if got := arrowType(tbl3.typeOf("rate")).String(); got != "utf8" {
		t.Fatalf("a column holding unchecked text was exposed as %s before any check", got)
	}
	tapply(t, tbl3, 10, typedChange(changestream.OpDelete, "num", map[string]any{"id": "2"}),
		typedChange(changestream.OpDelete, "num", map[string]any{"id": "3"}),
		typedChange(changestream.OpDelete, "num", map[string]any{"id": "4"}),
		typedChange(changestream.OpInsert, "num", map[string]any{"id": "9", "amount": "7", "rate": "0.25"}))
	sql, err := tbl3.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `CAST("rate" AS DECIMAL(38,20))`) || strings.Contains(sql, `AS DOUBLE`) {
		t.Errorf("the view does not cast text numerics to their declared types:\n%s", sql)
	}
	if haveDuck() {
		got := duck(t, "SELECT id, amount, rate, typeof(amount), typeof(rate) FROM ("+sql+") ORDER BY id")
		want := []string{
			"0\t0.123456789012345\t0.12345678901234500000\tVARCHAR\tDECIMAL(38,20)",
			"1\t1e-12\t0.00000000000100000000\tVARCHAR\tDECIMAL(38,20)",
			"5\t1.50\t1.50000000000000000000\tVARCHAR\tDECIMAL(38,20)",
			"9\t7\t0.25000000000000000000\tVARCHAR\tDECIMAL(38,20)",
		}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("DuckDB reads\n%s\nexpected\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
		// And with nothing opted in, the reader chooses the arithmetic: the
		// column is the text, exactly, and a cast in the query is exact too.
		NumericScale = ""
		defer func() { NumericScale = "" }()
		tbl4, _ := New(dir, "public", "num", "id", cols, order)
		sql4, _ := tbl4.ViewSQL()
		if got := duck(t, "SELECT count(*), sum(CAST(amount AS DECIMAL(38,15))), typeof(any_value(amount)) FROM ("+sql4+") WHERE id IN (0, 5, 9)"); len(got) != 1 ||
			got[0] != "3\t8.623456789012345\tVARCHAR" {
			t.Errorf("sum over a text numeric = %v", got)
		}
	}
	NumericScale = "public.num.rate=20"
	if _, err := tbl3.Compact(); err != nil {
		t.Fatal(err)
	}
	if ft := fileTypes(t, dataFiles(tbl3)[0]); ft["rate"] != "DECIMAL(38,20)" || ft["amount"] != "VARCHAR" {
		t.Errorf("after a rewrite the columns are stored as %v", ft)
	}
	if got := liveText(t, tbl3)["0"]["rate"]; got != "0.123456789012345" {
		t.Errorf("a text numeric rewritten as a decimal reads back as %q", got)
	}
}

// ---- bloom filters --------------------------------------------------------------

func bloomColumnsOf(t *testing.T, path string) map[string]int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer rdr.Close()
	out := map[string]int64{}
	md := rdr.MetaData()
	for g := 0; g < md.NumRowGroups(); g++ {
		for c, col := range md.RowGroups[g].Columns {
			if col.MetaData != nil && col.MetaData.BloomFilterOffset != nil {
				n := int64(0)
				if col.MetaData.BloomFilterLength != nil {
					n = int64(*col.MetaData.BloomFilterLength)
				}
				out[md.Schema.Column(c).Name()] += n
			}
		}
	}
	return out
}

func TestBloomFiltersOnKeyAndUUIDColumns(t *testing.T) {
	defer func(prev string) { BloomColumns = prev }(BloomColumns)
	cols := map[string]string{"id": "bigint", "customer": "uuid", "sku": "text", "qty": "integer"}
	order := []string{"id", "customer", "sku", "qty"}
	build := func(n int) (*Table, string) {
		tbl, err := New(t.TempDir(), "public", "orders", "id", cols, order)
		if err != nil {
			t.Fatal(err)
		}
		var changes []changestream.Change
		for i := 0; i < n; i++ {
			changes = append(changes, typedChange(changestream.OpInsert, "orders", map[string]any{
				"id": fmt.Sprint(i), "customer": fmt.Sprintf("%08x-0000-4000-8000-%012x", i%97, i%97),
				"sku": fmt.Sprintf("sku-%d", i%10), "qty": "1"}))
		}
		tapply(t, tbl, 1, changes...)
		return tbl, dataFiles(tbl)[0]
	}
	names := func(m map[string]int64) string {
		var out []string
		for c := range m {
			out = append(out, c)
		}
		sort.Strings(out)
		return strings.Join(out, ",")
	}

	BloomColumns = ""
	tbl, delta := build(40)
	if got := names(bloomColumnsOf(t, delta)); got != "customer,id" {
		t.Errorf("a delta carries bloom filters on [%s], expected the key and the uuid column", got)
	}
	for c, n := range bloomColumnsOf(t, delta) {
		if n > 256 {
			t.Errorf("a 40-row delta carries a %d-byte bloom filter on %s", n, c)
		}
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := names(bloomColumnsOf(t, dataFiles(tbl)[0])); got != "customer,id" {
		t.Errorf("a base file carries bloom filters on [%s]", got)
	}

	BloomColumns = "public.other=x;public.orders=sku, id"
	if _, f := build(40); names(bloomColumnsOf(t, f)) != "id,sku" {
		t.Errorf("QS_BLOOM_COLUMNS=%q gave [%s]", BloomColumns, names(bloomColumnsOf(t, f)))
	}
	BloomColumns = "public.orders=-"
	if _, f := build(40); names(bloomColumnsOf(t, f)) != "" {
		t.Errorf("QS_BLOOM_COLUMNS=%q gave [%s]", BloomColumns, names(bloomColumnsOf(t, f)))
	}
	BloomColumns = "off"
	_, plain := build(5000)
	BloomColumns = ""
	_, with := build(5000)
	a, _ := os.Stat(plain)
	b, _ := os.Stat(with)
	per := float64(b.Size()-a.Size()) / 5000 / 2
	t.Logf("5000 rows: %d bytes without, %d with two filters: %.2f bytes per row per filter", a.Size(), b.Size(), per)
	if per > 4 {
		t.Errorf("a bloom filter costs %.2f bytes per row", per)
	}
	if haveDuck() {
		if got := duck(t, "SELECT count(*) FROM read_parquet('"+with+"') WHERE id = 4321"); got[0] != "1" {
			t.Errorf("a key lookup through the filter found %v rows", got)
		}
		if got := duck(t, "SELECT count(*) FROM read_parquet('"+with+"') WHERE customer = '00000007-0000-4000-8000-000000000007'"); got[0] != "52" {
			t.Errorf("a uuid lookup through the filter found %v rows", got)
		}
	}
}

// TestWriteUUIDBenchMirrors writes the mirrors the uuid numbers in
// docs/16-deploying.md were measured on: the same 2M uuid-keyed rows in the old
// representation, in the new one without bloom filters, and in the new one,
// each with the view a reader would be given (view.sql). Opt-in:
//
//	QS_TYPES_BENCH_DIR=/some/dir QS_TYPES_BENCH_ROWS=2000000 go test -run TestWriteUUIDBenchMirrors ./internal/mirror/
func TestWriteUUIDBenchMirrors(t *testing.T) {
	root := os.Getenv("QS_TYPES_BENCH_DIR")
	if root == "" {
		t.Skip("set QS_TYPES_BENCH_DIR")
	}
	n := int(envInt("QS_TYPES_BENCH_ROWS", 2_000_000))
	defer func(prev string) { BloomColumns = prev }(BloomColumns)
	uuid := func(salt, i uint64) string {
		x := (i + 1) * 0x9E3779B97F4A7C15
		x ^= x >> 29
		y := (x + salt) * 0xBF58476D1CE4E5B9
		y ^= y >> 31
		return fmt.Sprintf("%08x-%04x-4%03x-8%03x-%012x", uint32(x>>32), uint16(x>>16), uint16(x)&0xfff, uint16(y>>48)&0xfff, y&0xffffffffffff)
	}
	write := func(dir string) {
		orders, err := New(dir, "public", "orders", "id", map[string]string{"id": "uuid", "customer": "uuid",
			"qty": "integer", "amount": "numeric(12,2)", "note": "text"}, []string{"id", "customer", "qty", "amount", "note"})
		if err != nil {
			t.Fatal(err)
		}
		customers, _ := New(dir, "public", "customers", "id", map[string]string{"id": "uuid", "name": "text",
			"tier": "smallint"}, []string{"id", "name", "tier"})
		for _, job := range []struct {
			tbl  *Table
			rows int
			row  func(i uint64) []any
		}{
			{orders, n, func(i uint64) []any {
				return []any{uuid(1, i), uuid(2, i%uint64(n/10)), fmt.Sprint(i % 17), fmt.Sprintf("%d.%02d", i%9973, i%100), fmt.Sprintf("note %d", i%1000)}
			}},
			{customers, n / 10, func(i uint64) []any {
				return []any{uuid(2, i), fmt.Sprintf("customer %d", i), fmt.Sprint(i % 5)}
			}},
		} {
			job.tbl.State.Seq++
			name := fmt.Sprintf("%06d.parquet", job.tbl.State.Seq)
			w, err := job.tbl.newParquetWriter(filepath.Join(job.tbl.Dir, "base", name), nil, true)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < job.rows; i++ {
				if err := w.AppendValues(job.row(uint64(i))); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			job.tbl.State.BaseFiles = []string{name}
			job.tbl.State.BaseRows = job.rows
			if err := job.tbl.saveState(); err != nil {
				t.Fatal(err)
			}
			sql, err := job.tbl.ViewSQL()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(job.tbl.Dir, "view.sql"), []byte(sql), 0o644); err != nil {
				t.Fatal(err)
			}
			job.tbl.Close()
		}
		if err := os.WriteFile(filepath.Join(dir, "keys.txt"), []byte(uuid(1, uint64(n/2))+"\n"+uuid(1, uint64(n/3))+
			"\n"+uuid(1, 7)+"\n"+uuid(1, uint64(n-1))+"\n"+uuid(1, uint64(n/7))+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	withLegacyWriter(func() { write(filepath.Join(root, "old")) })
	BloomColumns = "off"
	write(filepath.Join(root, "new-nobloom"))
	BloomColumns = ""
	write(filepath.Join(root, "new"))
}

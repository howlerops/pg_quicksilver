package mirror

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
)

// ---- the round-trip matrix ---------------------------------------------------
//
// Every type in types.go's table x {nullable, NOT NULL} x {empty, one row, all
// NULL, extremes, 200k random rows}, written by the writer as it is now and
// read back twice: by this package's reader, value by value against what was
// handed to the writer, and by DuckDB, value by value and by checksum against
// the same input written as JSON, which no code under test produced.

type matrixCol struct {
	name, pg string
	extreme  []string
	random   func(r *rand.Rand) string
}

func randDigits(r *rand.Rand, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('0' + r.Intn(10))
	}
	if b[0] == '0' {
		b[0] = '1'
	}
	return string(b)
}

func randNumeric(r *rand.Rand, prec, scale int) string {
	s := ""
	if r.Intn(2) == 0 {
		s = "-"
	}
	ip := 1 + r.Intn(prec-scale)
	s += randDigits(r, ip)
	if scale > 0 {
		f := make([]byte, scale)
		for i := range f {
			f[i] = byte('0' + r.Intn(10))
		}
		s += "." + string(f)
	}
	return s
}

func matrixTimestamp(r *rand.Rand) string {
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", 1+r.Intn(9999), 1+r.Intn(12), 1+r.Intn(28),
		r.Intn(24), r.Intn(60), r.Intn(60), r.Intn(1000000))
}

func matrixColumns() []matrixCol {
	words := []string{"", "a", "paid", "shipped", "żółć", "日本語", "line\nbreak", `quote"and\slash`, "tab\there", " padded "}
	return []matrixCol{
		{"c_smallint", "smallint", []string{"-32768", "32767", "0", "-1"},
			func(r *rand.Rand) string { return strconv.Itoa(r.Intn(65536) - 32768) }},
		{"c_integer", "integer", []string{"-2147483648", "2147483647", "0", "-1"},
			func(r *rand.Rand) string { return strconv.FormatInt(int64(int32(r.Uint32())), 10) }},
		// low cardinality on purpose: the rule keeps a dictionary for it
		{"c_tenant", "integer", []string{"1", "200"},
			func(r *rand.Rand) string { return strconv.Itoa(1 + r.Intn(200)) }},
		{"c_bigint", "bigint", []string{"-9223372036854775808", "9223372036854775807", "0", "-1"},
			func(r *rand.Rand) string { return strconv.FormatInt(int64(r.Uint64()), 10) }},
		{"c_real", "real", []string{"NaN", "Infinity", "-Infinity", "0", "-0", "1e-45", "3.4028235e+38", "0.1"},
			func(r *rand.Rand) string {
				if r.Intn(50000) == 0 {
					return "NaN"
				}
				return pgFloat(float64(float32(r.NormFloat64()*1e6)), 32)
			}},
		{"c_double", "double precision", []string{"NaN", "Infinity", "-Infinity", "0", "-0", "5e-324", "1.7976931348623157e+308", "0.1"},
			func(r *rand.Rand) string { return pgFloat(r.NormFloat64()*1e12, 64) }},
		{"c_bool", "boolean", []string{"t", "f"},
			func(r *rand.Rand) string { return []string{"t", "f"}[r.Intn(2)] }},
		{"c_uuid", "uuid", []string{"00000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff",
			"7fffffff-ffff-ffff-ffff-ffffffffffff", "80000000-0000-0000-0000-000000000000"},
			func(r *rand.Rand) string { return formatUUID(r.Uint64(), r.Uint64()) }},
		{"c_bytea", "bytea", []string{`\x`, `\x00`, `\xff00ff`, `\xdeadbeef`},
			func(r *rand.Rand) string {
				b := make([]byte, r.Intn(12))
				r.Read(b)
				return `\x` + hex.EncodeToString(b)
			}},
		{"c_num9", "numeric(9,2)", []string{"-9999999.99", "9999999.99", "0.00", "-0.01"},
			func(r *rand.Rand) string { return randNumeric(r, 9, 2) }},
		{"c_num12", "numeric(12,2)", []string{"-9999999999.99", "9999999999.99", "0.00", "0.01"},
			func(r *rand.Rand) string { return fmt.Sprintf("%d.%02d", 5+r.Intn(495), r.Intn(100)) }},
		{"c_num18", "numeric(18,4)", []string{"-99999999999999.9999", "99999999999999.9999", "0.0000", "-0.0001"},
			func(r *rand.Rand) string { return randNumeric(r, 18, 4) }},
		{"c_num18int", "numeric(18,0)", []string{"-999999999999999999", "999999999999999999", "0"},
			func(r *rand.Rand) string { return randNumeric(r, 18, 0) }},
		{"c_num19", "numeric(19,4)", []string{"-999999999999999.9999", "999999999999999.9999", "0.0000"},
			func(r *rand.Rand) string { return randNumeric(r, 19, 4) }},
		{"c_num38", "numeric(38,10)", []string{"-9999999999999999999999999999.9999999999", "9999999999999999999999999999.9999999999", "0.0000000000"},
			func(r *rand.Rand) string { return randNumeric(r, 38, 10) }},
		{"c_num50", "numeric(50,2)", []string{"-" + strings.Repeat("9", 48) + ".99", "0.00"},
			func(r *rand.Rand) string { return randNumeric(r, 50, 2) }},
		{"c_numeric", "numeric", []string{"NaN", "Infinity", "1e-12", "0.1234567890123", "12345678901234567890123456789012345678901234"},
			func(r *rand.Rand) string { return randNumeric(r, 20, r.Intn(8)) }},
		{"c_date", "date", []string{"0001-01-01", "9999-12-31", "infinity", "-infinity", "1970-01-01", "1969-12-31"},
			func(r *rand.Rand) string { return fmt.Sprintf("%04d-%02d-%02d", 1+r.Intn(9999), 1+r.Intn(12), 1+r.Intn(28)) }},
		{"c_time", "time without time zone", []string{"00:00:00", "23:59:59.999999", "12:00:00"},
			func(r *rand.Rand) string {
				return fmt.Sprintf("%02d:%02d:%02d.%06d", r.Intn(24), r.Intn(60), r.Intn(60), r.Intn(1000000))
			}},
		{"c_ts", "timestamp without time zone", []string{"0001-01-01 00:00:00", "9999-12-31 23:59:59.999999", "infinity", "-infinity",
			"1970-01-01 00:00:00", "1969-12-31 23:59:59.999999"}, matrixTimestamp},
		{"c_tstz", "timestamp with time zone", []string{"0001-01-01 00:00:00+00", "9999-12-31 23:59:59.999999+00", "infinity", "-infinity",
			"1970-01-01 00:00:00+00"}, func(r *rand.Rand) string { return matrixTimestamp(r) + "+00" }},
		// rises with the row, as a created_at does
		{"c_created", "timestamp with time zone", []string{"2024-01-01 00:00:00+00"}, nil},
		{"c_text", "text", words, func(r *rand.Rand) string { return words[r.Intn(len(words))] + strconv.Itoa(r.Intn(1000)) }},
		{"c_status", "text", []string{"paid", ""}, func(r *rand.Rand) string { return words[2+r.Intn(2)] }},
		{"c_jsonb", "jsonb", []string{`{}`, `{"a": [1, 2, {"b": null}]}`},
			func(r *rand.Rand) string { return fmt.Sprintf(`{"k": %d, "s": "v%d"}`, r.Intn(100), r.Intn(1e6)) }},
		{"c_char", "character(3)", []string{"a  ", "   ", "abc"},
			func(r *rand.Rand) string { return string(rune('a'+r.Intn(26))) + "  " }},
		{"c_interval", "interval", []string{"1 year 2 mons 3 days 04:05:06", "00:00:00"},
			func(r *rand.Rand) string { return fmt.Sprintf("%d days", r.Intn(1000)) }},
		{"c_intarr", "integer[]", []string{"{}", "{1,2,NULL}"},
			func(r *rand.Rand) string { return fmt.Sprintf("{%d,%d}", r.Intn(10), r.Intn(10)) }},
	}
}

// matrixRows is the input of one cell: rows of PostgreSQL text, nil for NULL.
func matrixRows(cols []matrixCol, shape string, notNull bool) [][]any {
	r := rand.New(rand.NewSource(42))
	n := 0
	switch shape {
	case "one":
		n = 1
	case "allnull":
		n = 1000
	case "extremes":
		n = 8
	case "random":
		n = 200000
	}
	rows := make([][]any, n)
	for i := range rows {
		row := make([]any, len(cols)+1)
		row[0] = strconv.Itoa(i)
		for ci, c := range cols {
			var v any
			switch {
			case shape == "allnull":
			case shape == "extremes" || shape == "one":
				if i < len(c.extreme) {
					v = c.extreme[i]
				} else if notNull {
					v = c.extreme[i%len(c.extreme)]
				}
			case c.random == nil:
				v = fmt.Sprintf("2024-%02d-%02d %02d:%02d:%02d.%06d+00", 1+i/20000, 1+(i/800)%25, (i/40)%20, (i/2)%20, i%2*30, r.Intn(1000000))
			case !notNull && r.Intn(4) == 0:
				// a quarter NULL: "NULL-heavy" for every column of the nullable half
			default:
				v = c.random(r)
			}
			row[ci+1] = v
		}
		rows[i] = row
	}
	return rows
}

func matrixTable(dir string, cols []matrixCol) *Table {
	t := &Table{Qualified: "public.m", Key: "i", Dir: dir, Columns: map[string]string{"i": "bigint"}, Order: []string{"i"}, detached: true}
	for _, c := range cols {
		t.Columns[c.name] = c.pg
		t.Order = append(t.Order, c.name)
	}
	return t
}

func sameValue(a, b any) bool {
	if isNaN(a) && isNaN(b) {
		return true
	}
	return reflect.DeepEqual(a, b)
}

func TestRoundTripMatrix(t *testing.T) {
	cols := matrixColumns()
	dir := t.TempDir()
	tbl := matrixTable(dir, cols)
	for _, notNull := range []bool{false, true} {
		for _, shape := range []string{"empty", "one", "allnull", "extremes", "random"} {
			if notNull && shape == "allnull" {
				continue // not a thing a NOT NULL column can be; TestNotNull* refuse it
			}
			if shape == "random" && testing.Short() {
				continue
			}
			name := fmt.Sprintf("%s_notnull=%v", shape, notNull)
			t.Run(name, func(t *testing.T) {
				tbl.SetNotNull()
				if notNull {
					tbl.SetNotNull(tbl.Order...)
				}
				input := matrixRows(cols, shape, notNull)
				path := filepath.Join(dir, name+".parquet")
				w, err := tbl.newParquetWriter(path, nil, true)
				if err != nil {
					t.Fatal(err)
				}
				want := make([][]any, len(input))
				for i, row := range input {
					vals := append([]any(nil), row...)
					if err := w.AppendValues(vals); err != nil {
						t.Fatal(err)
					}
					want[i] = vals // converted in place: what the file is to hold
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}

				// The Go reader.
				got := 0
				err = tbl.forEachRowGroup(path, nil, nil, func(rows []map[string]any, first int) error {
					for k, m := range rows {
						for ci, c := range tbl.Order {
							if !sameValue(m[c], want[first+k][ci]) {
								return fmt.Errorf("row %d column %s (%s): read %#v, wrote %#v (from %#v)",
									first+k, c, tbl.Columns[c], m[c], want[first+k][ci], input[first+k][ci])
							}
						}
					}
					got += len(rows)
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				if got != len(input) {
					t.Fatalf("read %d rows, wrote %d", got, len(input))
				}

				// What the footer says.
				checkLayout(t, tbl, path, notNull, shape)

				// DuckDB, against the input as JSON.
				if !haveDuck() {
					return
				}
				jpath := filepath.Join(dir, name+".json")
				jf, err := os.Create(jpath)
				if err != nil {
					t.Fatal(err)
				}
				enc := json.NewEncoder(jf)
				enc.SetEscapeHTML(false)
				for _, row := range input {
					m := map[string]any{}
					for ci, c := range tbl.Order {
						m[c] = row[ci]
					}
					if err := enc.Encode(m); err != nil {
						t.Fatal(err)
					}
				}
				jf.Close()
				var jcols, sel []string
				for i, c := range tbl.Order {
					jcols = append(jcols, fmt.Sprintf("%s: 'VARCHAR'", sqlString(c)))
					typ := sqlType(tbl.typeOf(c))
					e := fmt.Sprintf("CAST(j.%s AS %s)", sqlIdent(c), typ)
					if typ == "BLOB" {
						e = fmt.Sprintf("from_hex(substr(j.%s, 3))", sqlIdent(c))
					}
					p := "p." + sqlIdent(c)
					sel = append(sel, fmt.Sprintf(`SELECT %d AS ord, %s AS col, typeof(any_value(%s)) AS typ,
						count(*) FILTER (WHERE %s IS DISTINCT FROM %s) AS different,
						coalesce(sum(hash(%s)::HUGEINT), 0) AS sum_file, coalesce(sum(hash(%s)::HUGEINT), 0) AS sum_input,
						count(%s) AS n_file, count(%s) AS n_input, count(*) AS n FROM p JOIN j ON j.i = p.i::VARCHAR`,
						i, sqlString(c), p, p, e, p, e, p, e))
				}
				src := "(SELECT 1 WHERE false)"
				if len(input) > 0 {
					src = fmt.Sprintf("read_json(%s, format = 'newline_delimited', columns = {%s})", sqlString(jpath), strings.Join(jcols, ", "))
				}
				q := fmt.Sprintf("WITH p AS MATERIALIZED (SELECT * FROM read_parquet(%s)), j AS MATERIALIZED (SELECT * FROM %s) %s ORDER BY ord",
					sqlString(path), src, strings.Join(sel, " UNION ALL "))
				if len(input) == 0 {
					// Nothing to compare; the file must still read, with its types.
					out := duck(t, fmt.Sprintf("SELECT count(*) FROM read_parquet(%s)", sqlString(path)))
					if len(out) != 1 || out[0] != "0" {
						t.Fatalf("empty file: DuckDB counted %v", out)
					}
					desc := duck(t, fmt.Sprintf("SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet(%s))", sqlString(path)))
					for i, c := range tbl.Order {
						if want := c + "\t" + sqlType(tbl.typeOf(c)); desc[i] != want {
							t.Errorf("empty file: DuckDB describes %q, want %q", desc[i], want)
						}
					}
					return
				}
				out := duck(t, q)
				if len(out) != len(tbl.Order) {
					t.Fatalf("DuckDB returned %d rows for %d columns:\n%s", len(out), len(tbl.Order), strings.Join(out, "\n"))
				}
				for i, line := range out {
					f := strings.Split(line, "\t")
					c := tbl.Order[i]
					wantTyp := sqlType(tbl.typeOf(c))
					if f[2] != wantTyp {
						t.Errorf("%s: DuckDB reads it as %s, want %s", c, f[2], wantTyp)
					}
					if f[3] != "0" || f[4] != f[5] || f[6] != f[7] || f[8] != strconv.Itoa(len(input)) {
						t.Errorf("%s (%s): %s values differ; checksum %s in the file, %s in the input; "+
							"%s non-NULL in the file, %s in the input; %s rows joined of %d",
							c, tbl.Columns[c], f[3], f[4], f[5], f[6], f[7], f[8], len(input))
					}
				}
			})
		}
	}
}

// checkLayout holds a file to what encoding.go and nullability.go say it is.
func checkLayout(t *testing.T, tbl *Table, path string, notNull bool, shape string) {
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
	md := rdr.MetaData()
	sc := md.Schema
	wantDelta := map[string]bool{"i": true, "c_smallint": true, "c_integer": true, "c_bigint": true, "c_num9": true, "c_num12": true,
		"c_num18": true, "c_num18int": true, "c_date": true, "c_time": true, "c_ts": true, "c_tstz": true, "c_created": true}
	for i := 0; i < sc.NumColumns(); i++ {
		col := sc.Column(i)
		name := col.Name()
		dt := arrowType(tbl.typeOf(name))
		rep := col.SchemaNode().RepetitionType()
		if notNull && rep != parquet.Repetitions.Required {
			t.Errorf("%s: declared NOT NULL and written %v", name, rep)
		}
		if !notNull && rep != parquet.Repetitions.Optional {
			t.Errorf("%s: not declared NOT NULL and written %v", name, rep)
		}
		if p, _, _, ok := numericSpec(tbl.typeOf(name)); ok && dt.ID() != 0 {
			want := parquet.Types.FixedLenByteArray
			switch {
			case isTextNumeric(tbl.typeOf(name)):
				want = parquet.Types.ByteArray
			case p <= 9:
				want = parquet.Types.Int32
			case p <= 18:
				want = parquet.Types.Int64
			}
			if col.PhysicalType() != want {
				t.Errorf("%s (%s): stored on %v, want %v", name, tbl.Columns[name], col.PhysicalType(), want)
			}
		}
		for g := 0; g < md.NumRowGroups(); g++ {
			cc, err := md.RowGroup(g).ColumnChunk(i)
			if err != nil {
				t.Fatal(err)
			}
			if shape == "random" {
				delta := false
				for _, e := range cc.Encodings() {
					delta = delta || e == parquet.Encodings.DeltaBinaryPacked
				}
				if delta != wantDelta[name] {
					t.Errorf("%s group %d: DELTA_BINARY_PACKED is %v, want %v (encodings %v)", name, g, delta, wantDelta[name], cc.Encodings())
				}
				if delta != (cc.Compression() == compress.Codecs.Uncompressed) {
					t.Errorf("%s group %d: codec %v with encodings %v", name, g, cc.Compression(), cc.Encodings())
				}
			}
			// Exactness, read from the Thrift footer itself: arrow-go's reader
			// does not surface the flags.
			st := md.RowGroups[g].Columns[i].MetaData.Statistics
			if st == nil || st.MinValue == nil {
				continue
			}
			exact := st.IsMinValueExact != nil && *st.IsMinValueExact && st.IsMaxValueExact != nil && *st.IsMaxValueExact
			if exact != exactStatistics(dt) {
				t.Errorf("%s group %d: min/max marked exact is %v, want %v", name, g, exact, exactStatistics(dt))
			}
		}
	}
}

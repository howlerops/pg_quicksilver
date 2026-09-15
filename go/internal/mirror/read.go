package mirror

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// readParquet reads a file into row maps. `dead` positions are skipped — that
// is the deletion vector applied at read time, with no join and no window
// function.
//
// Files written before an ADD COLUMN lack that column; the caller's current
// column list wins and missing columns come back nil, so old files NULL-fill
// rather than erroring.
func (t *Table) readParquet(path string, dead map[int]bool) ([]map[string]any, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()

	ar, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	tbl, err := ar.ReadTable(context.Background())
	if err != nil {
		return nil, err
	}
	defer tbl.Release()

	present := map[string]int{}
	for i := 0; i < int(tbl.NumCols()); i++ {
		present[tbl.Schema().Field(i).Name] = i
	}

	out := make([]map[string]any, 0, tbl.NumRows())
	for row := 0; row < int(tbl.NumRows()); row++ {
		if dead[row] {
			continue
		}
		m := make(map[string]any, len(t.Order))
		for _, name := range t.Order {
			ci, ok := present[name]
			if !ok {
				m[name] = nil // predates an ADD COLUMN
				continue
			}
			m[name] = chunkedValue(tbl.Column(ci).Data(), row)
		}
		out = append(out, m)
	}
	return out, nil
}

func chunkedValue(cd *arrow.Chunked, row int) any {
	for _, ch := range cd.Chunks() {
		if row < ch.Len() {
			return arrayValue(ch, row)
		}
		row -= ch.Len()
	}
	return nil
}

func arrayValue(a arrow.Array, i int) any {
	if a.IsNull(i) {
		return nil
	}
	switch v := a.(type) {
	case *array.Int64:
		return v.Value(i)
	case *array.Float64:
		return v.Value(i)
	case *array.Boolean:
		return v.Value(i)
	case *array.String:
		return v.Value(i)
	case *array.Decimal128:
		dt := v.DataType().(*arrow.Decimal128Type)
		return v.Value(i).ToString(dt.Scale)
	default:
		return v.ValueStr(i)
	}
}

// Live returns the current logical contents: base files minus their deletion
// vectors, plus the deltas.
func (t *Table) Live() ([]map[string]any, error) {
	var out []map[string]any
	for _, base := range t.State.BaseFiles {
		stem := strings.TrimSuffix(base, ".parquet")
		dead := map[int]bool{}
		if b, err := os.ReadFile(filepath.Join(t.Dir, "dv", stem+".dv.json")); err == nil {
			var pos []int
			_ = json.Unmarshal(b, &pos)
			for _, p := range pos {
				dead[p] = true
			}
		}
		rows, err := t.readParquet(filepath.Join(t.Dir, "base", base), dead)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	for _, d := range t.State.DeltaFiles {
		rows, err := t.readParquet(filepath.Join(t.Dir, "delta", d), nil)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

// ---- verification (goal G7) ----------------------------------------------

// Checksum is order-independent: rows are hashed individually and summed, so
// mirror and source can be compared without sorting either side.
func Checksum(rows []map[string]any, order []string) (int, uint64) {
	var sum uint64
	for _, r := range rows {
		h := fnv.New64a()
		for _, c := range order {
			fmt.Fprintf(h, "%s|", renderValue(r[c]))
		}
		sum += h.Sum64()
	}
	return len(rows), sum
}

// renderValue must produce the SAME text for a value read back from Parquet as
// for the same value read from Postgres, or the checksum reports false
// divergence. Numerics are compared as trimmed decimal strings.
func renderValue(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case bool:
		if x {
			return "true"
		}
		return "false"
	case string:
		return normalizeNumeric(x)
	default:
		return normalizeNumeric(fmt.Sprint(x))
	}
}

// normalizeNumeric strips trailing zeros from a decimal so that "10.50" from
// Postgres and "10.5000" from a decimal128 round-trip compare equal.
func normalizeNumeric(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	for _, c := range s {
		if (c < '0' || c > '9') && c != '.' && c != '-' && c != '+' {
			return s // not a plain number; leave it alone
		}
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}

// SortedKeys is a deterministic column order for a map, used when the caller
// has no explicit ordinal order to hand in.
func SortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

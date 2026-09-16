package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
				// This file predates an ADD COLUMN. NULL is right only when the
				// column was added without a default; otherwise PostgreSQL
				// reads these rows back as attmissingval and so must we.
				if mv, has := t.State.MissingVals[name]; has {
					m[name] = mv
				} else {
					m[name] = nil
				}
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
	// Deltas are no longer rewritten when a row in them is superseded, so they
	// carry deletion vectors exactly as base files do. Reading a delta without
	// its vector would resurrect every superseded row.
	read := func(rel string) error {
		rows, err := t.readParquet(filepath.Join(t.Dir, rel), t.deadPositions(rel))
		if err != nil {
			if os.IsNotExist(err) {
				return ErrStaleManifest
			}
			return err
		}
		out = append(out, rows...)
		return nil
	}
	for _, base := range t.State.BaseFiles {
		if err := read(filepath.Join("base", base)); err != nil {
			return nil, err
		}
	}
	for _, d := range t.State.DeltaFiles {
		if err := read(filepath.Join("delta", d)); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ---- verification (goal G7) ----------------------------------------------

// Checksum is order-independent: rows are hashed individually and summed, so
// mirror and source can be compared without sorting either side.
func Checksum(rows []map[string]any, order []string) (int, uint64) {
	a := NewAccumulator(order)
	for _, r := range rows {
		a.Add(r)
	}
	return a.Result()
}

func rowHash(r map[string]any, order []string) uint64 {
	h := fnv.New64a()
	for _, c := range order {
		fmt.Fprintf(h, "%s|", renderValue(r[c]))
	}
	return h.Sum64()
}

// Accumulator computes the same order-independent checksum as Checksum, but
// incrementally, so neither side of a comparison has to be held in memory.
type Accumulator struct {
	order []string
	n     int
	sum   uint64
}

func NewAccumulator(order []string) *Accumulator { return &Accumulator{order: order} }

func (a *Accumulator) Add(row map[string]any) {
	a.n++
	a.sum += rowHash(row, a.order)
}

func (a *Accumulator) Result() (int, uint64) { return a.n, a.sum }

// ErrStaleManifest means the file list a read started from no longer matches
// what is on disk, because a compaction swapped it mid-read.
//
// This has to be an error and not a skip. Compaction saves the new manifest and
// then deletes the old files, so a reader that loaded the manifest a moment
// earlier can find the file it is about to open already gone — and treating
// that as "no rows here" silently returns a fraction of the table. It is how a
// 200,302-row mirror read back as 363 rows while being entirely intact on disk.
// Every read path returns this instead, and callers reload and retry.
var ErrStaleManifest = errors.New("mirror manifest is stale: a file was removed by compaction mid-read")

// Reload re-reads the durable manifest. Callers use it to recover from
// ErrStaleManifest; it is the only supported way for a reader outside the apply
// goroutine to pick up a compaction.
func (t *Table) Reload() error {
	b, err := os.ReadFile(t.statePath())
	if err != nil {
		return err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	t.State = st
	t.index = nil
	return nil
}

// ReadLiveWithRetry streams the live rows, reloading the manifest and starting
// over if a compaction swaps it underneath.
//
// reset is called before every attempt and MUST discard whatever the previous
// attempt accumulated. It is a required argument rather than an optional one
// because forgetting it is silent: a retry replays rows the caller already saw,
// so a checksum double-counts and a "divergence" appears that is purely the
// reader's own doing. Bounded attempts, because a manifest that never settles
// is something to report rather than spin on.
func (t *Table) ReadLiveWithRetry(reset func(), fn func(map[string]any) error) error {
	for attempt := 0; attempt < 5; attempt++ {
		reset()
		err := t.ForEachLive(fn)
		if !errors.Is(err, ErrStaleManifest) {
			return err
		}
		if err := t.Reload(); err != nil {
			return err
		}
	}
	return ErrStaleManifest
}

// ForEachLive streams the live rows one file at a time, so a caller never holds
// more than a single file's worth.
//
// Live() materialises the whole table. That is fine for a small mirror and is
// how the verifier used to work — until a 5.7M-row mirror got the verifier
// OOM-killed, and the harness reported the dead process as a DIVERGENCE. A
// verification tool that cannot run at the size it is verifying is worse than
// none: it turns "too big to check" into "wrong".
func (t *Table) ForEachLive(fn func(map[string]any) error) error {
	visit := func(rel string) error {
		rows, err := t.readParquet(filepath.Join(t.Dir, rel), t.deadPositions(rel))
		if err != nil {
			if os.IsNotExist(err) {
				return ErrStaleManifest
			}
			return err
		}
		for _, r := range rows {
			if err := fn(r); err != nil {
				return err
			}
		}
		return nil
	}
	for _, base := range t.State.BaseFiles {
		if err := visit(filepath.Join("base", base)); err != nil {
			return err
		}
	}
	for _, d := range t.State.DeltaFiles {
		if err := visit(filepath.Join("delta", d)); err != nil {
			return err
		}
	}
	return nil
}

// renderValue must produce the SAME text for a value read back from Parquet as
// for the same value read from Postgres, or the checksum reports false
// divergence. Numerics are compared as trimmed decimal strings.
// Render is how the mirror compares a value to a source value: as the text
// PostgreSQL would print, so that two engines returning different Go types for
// the same number still compare equal. Exported because divergence diagnostics
// outside this package must use exactly the comparison the checksum uses,
// otherwise they disagree with it and send the reader hunting a phantom.
func Render(v any) string { return renderValue(v) }

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

// ---- targeted reads --------------------------------------------------------

// RowGroupRows is the row-group size written by writeParquet, and it is the
// single knob that trades scan efficiency against point-read cost.
//
// A row group is the smallest unit Parquet can decode. Carry-forward has to
// fetch one row out of a file, so its cost is (row group size x bytes per row)
// regardless of how few rows it wants: on a table with a 3 KB document per row,
// a 64k-row group means decoding ~200 MB to read back one column of 200 rows.
// Scans want the opposite — large groups compress better, carry less metadata
// and prune more coarsely but more cheaply.
//
// Overridable so the trade can be measured rather than asserted; see docs/19.
var RowGroupRows = envInt("QS_ROW_GROUP", 65536)

func envInt(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// readRowsAt fetches specific rows, by global position, reading ONLY the named
// columns and ONLY the row groups those positions fall in.
//
// This exists for unchanged-TOAST carry-forward: pgoutput does not resend a
// large value that an update did not change, so the mirror has to supply the
// previous one, and it must do so without reading the table.
func (t *Table) readRowsAt(rel string, positions []int, cols []string) (map[int]map[string]any, error) {
	if len(positions) == 0 || len(cols) == 0 {
		return nil, nil
	}
	f, err := os.Open(filepath.Join(t.Dir, rel))
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
	schema, err := ar.Schema()
	if err != nil {
		return nil, err
	}

	// column indices for the names we actually need
	want := map[string]bool{}
	for _, c := range cols {
		want[c] = true
	}
	var colIdx []int
	var colName []string
	for i := 0; i < schema.NumFields(); i++ {
		if want[schema.Field(i).Name] {
			colIdx = append(colIdx, i)
			colName = append(colName, schema.Field(i).Name)
		}
	}
	if len(colIdx) == 0 {
		return nil, nil
	}

	// group the wanted positions by the row group that holds them
	md := rdr.MetaData()
	starts := make([]int64, md.NumRowGroups()+1)
	for g := 0; g < md.NumRowGroups(); g++ {
		starts[g+1] = starts[g] + md.RowGroup(g).NumRows()
	}
	byGroup := map[int][]int{}
	for _, p := range positions {
		g := sort.Search(len(starts)-1, func(i int) bool { return starts[i+1] > int64(p) })
		if g < md.NumRowGroups() {
			byGroup[g] = append(byGroup[g], p)
		}
	}

	out := make(map[int]map[string]any, len(positions))
	for g, ps := range byGroup {
		tbl, err := ar.ReadRowGroups(context.Background(), colIdx, []int{g})
		if err != nil {
			return nil, err
		}
		base := int(starts[g])
		for _, p := range ps {
			local := p - base
			if int64(local) >= tbl.NumRows() {
				continue
			}
			m := make(map[string]any, len(colName))
			for ci := range colName {
				m[colName[ci]] = chunkedValue(tbl.Column(ci).Data(), local)
			}
			out[p] = m
		}
		tbl.Release()
	}
	return out, nil
}

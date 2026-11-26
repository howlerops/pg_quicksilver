package mirror

// A rewrite that keeps the base file's clustering.
//
// The base file a snapshot writes is in heap order, and for most tables heap
// order is roughly insertion order: the file is clustered on its time column
// without anyone having asked for it. Measured on ten million rows, a filter on
// one month read 8 of 153 row groups and took 15 ms.
//
// The rewrite used to read the base and then each delta file, and write rows in
// that order — so every row updated since the last rewrite went to the END of
// the new file, whatever its values. After one rewrite the same filter
// overlapped 28 row groups and took 57 ms, and each further rewrite made it
// worse.
//
// Sorting the table is not the answer: a full sort by the time column gained
// nothing over snapshot order, and a sort by key made the time filter 13x
// worse. What is wanted is the order the file ALREADY has, with the changed
// rows put back where their values say they belong. So:
//
//   - The clustering column is read off the base file itself: the column whose
//     row-group min/max ranges overlap least (chooseCluster). Nothing is
//     persisted; a restart reads the same footer and reaches the same answer.
//   - The base is treated as a stream approximately sorted on that column, and
//     the live delta rows — sorted on it — are merged in (clusterRewrite).
//
// With no column that is meaningfully clustered, or no delta rows to place, the
// rewrite is the loop it always was, row for row.

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// ---- ordering ---------------------------------------------------------------
//
// Cluster values are compared as whatever arrayValue hands back, and that is
// not a fixed set of Go types: integers may come at their declared width, a
// uuid as sixteen bytes, a timestamp as a temporalValue. So the order is
// defined on CLASSES of value, and a column is merged on only while its values
// stay in one class. Anything else — NULL included — is "unordered": it is
// never compared, never moves a row, and is written where it always was.

const (
	ordNone = iota
	ordInt
	ordFloat
	ordBytes
	ordBool
	ordTime
	ordTemporal      // + temporalKind
	ordDecimal  = 64 // + scale
)

// ordClass is the class a value is ordered in, or ordNone if it has no order
// here.
func ordClass(v any) int {
	switch x := v.(type) {
	case nil:
		return ordNone
	case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint:
		return ordInt
	case float64:
		if math.IsNaN(x) {
			return ordNone
		}
		return ordFloat
	case float32:
		if x != x {
			return ordNone
		}
		return ordFloat
	case string, []byte:
		return ordBytes
	case bool:
		return ordBool
	case time.Time:
		return ordTime
	case temporalValue:
		return ordTemporal + int(x.k)
	case decimalValue:
		return ordDecimal + int(x.scale)
	}
	// A fixed-size byte array: [16]byte for a uuid, whatever its width.
	if rt := reflect.TypeOf(v); rt.Kind() == reflect.Array && rt.Elem().Kind() == reflect.Uint8 {
		return ordBytes
	}
	return ordNone
}

// ordInt64 widens any integer. big is set for an unsigned value past int64,
// which sorts above every signed one.
func ordInt64(v any) (n int64, big bool) {
	switch x := v.(type) {
	case int64:
		return x, false
	case int32:
		return int64(x), false
	case int16:
		return int64(x), false
	case int8:
		return int64(x), false
	case int:
		return int64(x), false
	case uint32:
		return int64(x), false
	case uint16:
		return int64(x), false
	case uint8:
		return int64(x), false
	case uint64:
		return int64(x), x > math.MaxInt64
	case uint:
		return int64(x), uint64(x) > math.MaxInt64
	}
	return 0, false
}

func ordBytesOf(v any) []byte {
	switch x := v.(type) {
	case string:
		return []byte(x)
	case []byte:
		return x
	}
	rv := reflect.ValueOf(v)
	b := make([]byte, rv.Len())
	reflect.Copy(reflect.ValueOf(b), rv)
	return b
}

// ordCmp orders two values of the SAME class (ordClass(a) == ordClass(b), and
// not ordNone). Text and bytes compare bytewise, which is how Parquet orders
// the statistics the column was chosen by.
func ordCmp(a, b any) int {
	switch x := a.(type) {
	case int64:
		if y, ok := b.(int64); ok {
			return cmp.Compare(x, y)
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y)
		}
	case temporalValue:
		return cmp.Compare(x.v, b.(temporalValue).v)
	case float64:
		if y, ok := b.(float64); ok {
			return cmp.Compare(x, y)
		}
	case decimalValue:
		return x.num.Cmp(b.(decimalValue).num)
	case time.Time:
		return x.Compare(b.(time.Time))
	case bool:
		y := b.(bool)
		switch {
		case x == y:
			return 0
		case y:
			return -1
		}
		return 1
	}
	switch ordClass(a) {
	case ordInt:
		x, xb := ordInt64(a)
		y, yb := ordInt64(b)
		if xb || yb {
			if xb && yb {
				return cmp.Compare(uint64(x), uint64(y))
			}
			if xb {
				return 1
			}
			return -1
		}
		return cmp.Compare(x, y)
	case ordFloat:
		return cmp.Compare(reflect.ValueOf(a).Float(), reflect.ValueOf(b).Float())
	case ordBytes:
		return bytes.Compare(ordBytesOf(a), ordBytesOf(b))
	}
	return 0
}

// ---- choosing the column ------------------------------------------------------

// ClusterBy overrides the choice, per table:
//
//	QS_CLUSTER_BY="public.orders=o_date,public.events=none"
//
// An entry is table=column; the table may be `*`, and a bare word is the same
// as `*=word`. `none` keeps the rewrite as it was (base, then deltas) for that
// table. A table with no entry chooses for itself.
var ClusterBy = os.Getenv("QS_CLUSTER_BY")

const (
	// A column counts as clustered if fewer than this fraction of row-group
	// pairs have overlapping ranges. A file with a fifth of its rows appended
	// out of place by an old rewrite scores about 0.36, and is still worth
	// keeping tight; an unclustered column scores close to 1.
	clusterMaxOverlap = 0.5
	// Columns this close to the best are a tie, decided by clusterRank.
	clusterTie = 0.02
	// With fewer row groups than this there is nothing to prune and nothing to
	// measure.
	clusterMinGroups = 4
	// clusterMaxSample bounds the pairwise comparison on a very large file.
	clusterMaxSample = 1024
)

// statRange is one row group's [min, max] on one column, in a form ordCmp
// orders: int64, uint64, float64 or []byte.
type statRange struct{ lo, hi any }

// overlapFraction is the fraction of row-group pairs whose ranges overlap.
// Ranges that only touch are not counted — a file sorted on a date has the
// same date either side of most boundaries — but two IDENTICAL ranges are, or
// a column holding one value everywhere would look perfectly clustered.
func overlapFraction(r []statRange) float64 {
	if len(r) > clusterMaxSample {
		s := make([]statRange, clusterMaxSample)
		for i := range s {
			s[i] = r[i*len(r)/clusterMaxSample]
		}
		r = s
	}
	if len(r) < 2 {
		return 1
	}
	n := 0
	for i := range r {
		for j := i + 1; j < len(r); j++ {
			a, b := r[i], r[j]
			if ordCmp(a.hi, b.lo) > 0 && ordCmp(b.hi, a.lo) > 0 ||
				ordCmp(a.lo, b.lo) == 0 && ordCmp(a.hi, b.hi) == 0 {
				n++
			}
		}
	}
	return float64(n) / float64(len(r)*(len(r)-1)/2)
}

// columnRanges reads, from the footers alone, the row-group ranges of every
// named column that has an order and statistics. groups is the number of
// non-empty row groups.
func columnRanges(paths []string, names []string) (ranges map[string][]statRange, groups int, err error) {
	ranges = map[string][]statRange{}
	bad := map[string]bool{}
	for _, path := range paths {
		rdr, err := file.OpenParquetFile(path, false)
		if err != nil {
			return nil, 0, err
		}
		md := rdr.MetaData()
		for _, name := range names {
			if bad[name] {
				continue
			}
			ci := md.Schema.ColumnIndexByName(name)
			if ci < 0 || md.Schema.Column(ci).MaxRepetitionLevel() > 0 {
				bad[name] = true
				continue
			}
			so := md.Schema.Column(ci).SortOrder()
			for g := 0; g < md.NumRowGroups() && !bad[name]; g++ {
				rg := md.RowGroup(g)
				if rg.NumRows() == 0 {
					continue
				}
				r, ok, allNull := statRangeOf(rg, ci, so)
				switch {
				case ok:
					ranges[name] = append(ranges[name], r)
				case !allNull:
					bad[name] = true
				}
			}
		}
		for g := 0; g < md.NumRowGroups(); g++ {
			if md.RowGroup(g).NumRows() > 0 {
				groups++
			}
		}
		rdr.Close()
	}
	for name := range bad {
		delete(ranges, name)
	}
	return ranges, groups, nil
}

func statRangeOf(rg *metadata.RowGroupMetaData, ci int, so schema.SortOrder) (r statRange, ok, allNull bool) {
	cc, err := rg.ColumnChunk(ci)
	if err != nil {
		return r, false, false
	}
	if set, err := cc.StatsSet(); err != nil || !set {
		return r, false, false
	}
	st, err := cc.Statistics()
	if err != nil || st == nil {
		return r, false, false
	}
	if !st.HasMinMax() {
		return r, false, st.NullCount() == rg.NumRows()
	}
	signed, unsigned := so == schema.SortSIGNED, so == schema.SortUNSIGNED
	switch s := st.(type) {
	case *metadata.Int32Statistics:
		if signed {
			return statRange{int64(s.Min()), int64(s.Max())}, true, false
		}
		if unsigned {
			return statRange{int64(uint32(s.Min())), int64(uint32(s.Max()))}, true, false
		}
	case *metadata.Int64Statistics:
		if signed {
			return statRange{s.Min(), s.Max()}, true, false
		}
		if unsigned {
			return statRange{uint64(s.Min()), uint64(s.Max())}, true, false
		}
	case *metadata.Float32Statistics:
		return statRange{float64(s.Min()), float64(s.Max())}, true, false
	case *metadata.Float64Statistics:
		return statRange{s.Min(), s.Max()}, true, false
	case *metadata.ByteArrayStatistics:
		if unsigned {
			return statRange{bytes.Clone(s.Min()), bytes.Clone(s.Max())}, true, false
		}
	case *metadata.FixedLenByteArrayStatistics:
		// Signed here is a decimal, whose bytes are not in value order.
		if unsigned {
			return statRange{bytes.Clone(s.Min()), bytes.Clone(s.Max())}, true, false
		}
	}
	return r, false, false
}

// chooseCluster picks the column a file is most clustered on: the least
// overlap among its row groups' ranges. rank breaks a tie (higher wins), and
// order breaks what is left.
//
// The tie matters more than it looks. A table loaded in insertion order is
// clustered on its serial key AND on its creation time, usually to within a
// rounding error of each other, and the two are not equally useful: a key
// lookup goes through the key index and never reads a range, while a time
// filter prunes on nothing else.
func chooseCluster(order []string, ranges map[string][]statRange, groups int, rank func(string) int) (col, why string) {
	if groups < clusterMinGroups {
		return "", fmt.Sprintf("%d row groups, too few to tell", groups)
	}
	type cand struct {
		name string
		f    float64
	}
	var cs []cand
	for _, name := range order {
		r := ranges[name]
		if len(r) < clusterMinGroups || len(r) < groups/2 {
			continue
		}
		cs = append(cs, cand{name, overlapFraction(r)})
	}
	if len(cs) == 0 {
		return "", "no orderable column with row-group statistics"
	}
	best := cs[0]
	for _, c := range cs[1:] {
		if c.f < best.f {
			best = c
		}
	}
	if best.f >= clusterMaxOverlap {
		return "", fmt.Sprintf("no column is clustered: the least overlapping is %s, on %.0f%% of row-group pairs",
			best.name, 100*best.f)
	}
	pick := best
	for _, c := range cs {
		if c.f <= best.f+clusterTie && rank(c.name) > rank(pick.name) {
			pick = c
		}
	}
	slices.SortStableFunc(cs, func(a, b cand) int { return cmp.Compare(a.f, b.f) })
	var others []string
	for _, c := range cs {
		if c.name != pick.name && len(others) < 3 {
			others = append(others, fmt.Sprintf("%s %.1f%%", c.name, 100*c.f))
		}
	}
	why = fmt.Sprintf("row-group ranges overlap on %.1f%% of pairs over %d groups", 100*pick.f, groups)
	if len(others) > 0 {
		why += " (" + strings.Join(others, ", ") + ")"
	}
	return pick.name, why
}

// clusterOverride is the QS_CLUSTER_BY entry for a table, if it has one.
func clusterOverride(spec, qualified string) (col string, ok bool) {
	for _, e := range strings.Split(spec, ",") {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		tbl, c, has := strings.Cut(e, "=")
		if !has {
			tbl, c = "*", e
		}
		tbl, c = strings.TrimSpace(tbl), strings.TrimSpace(c)
		if tbl == qualified {
			return c, true
		}
		if tbl == "*" && !ok {
			col, ok = c, true
		}
	}
	return col, ok
}

// clusterLogged remembers what was last logged per table, so that the choice
// is reported when it is made and when it changes, not on every rewrite.
var clusterLogged sync.Map

// clusterColumn decides what this rewrite keeps the output clustered on. ""
// means nothing: write the base and then the deltas, as before.
func (t *Table) clusterColumn(baseRels []string) (col, why string) {
	defer func() {
		said := col + "\x00" + why
		if prev, _ := clusterLogged.Swap(t.Dir, said); prev != said {
			slog.Info("rewrite clustering", "table", t.Qualified, "column", cmp.Or(col, "none"), "why", why)
		}
	}()
	// Nothing to keep the clustering OF. An override does not change that: it
	// names the column a base is clustered on, it is not a request to sort.
	if len(baseRels) == 0 {
		return "", "no base file yet"
	}
	if c, ok := clusterOverride(ClusterBy, t.Qualified); ok {
		if c == "none" || c == "" {
			return "", "QS_CLUSTER_BY says none"
		}
		if _, known := t.Columns[c]; !known {
			return "", fmt.Sprintf("QS_CLUSTER_BY names %q, which is not a column", c)
		}
		return c, "QS_CLUSTER_BY"
	}
	paths := make([]string, len(baseRels))
	for i, rel := range baseRels {
		paths[i] = filepath.Join(t.Dir, rel)
	}
	ranges, groups, err := columnRanges(paths, t.Order)
	if err != nil {
		return "", "base file statistics unreadable: " + err.Error()
	}
	return chooseCluster(t.Order, ranges, groups, func(name string) int {
		switch {
		case temporalOf(t.Columns[name]) != notTemporal:
			return 2
		case name == t.Key:
			return 1
		}
		return 0
	})
}

// ---- reading by row group -------------------------------------------------------

// groupView is one decoded row group, restricted to the columns asked for.
type groupView struct {
	start, n int
	names    []string
	idx      []int // per requested column: its column in tbl, or -1 if the file lacks it
	tbl      arrow.Table
}

// value is column c (an index into the requested names) of row i of the group.
// ok is false for a column the file does not have.
func (v *groupView) value(c, i int) (val any, ok bool) {
	if v.idx[c] < 0 {
		return nil, false
	}
	return chunkedValue(v.tbl.Column(v.idx[c]).Data(), i), true
}

// eachGroup decodes the named columns of a file one row group at a time. want,
// if given, says which groups are worth decoding.
func eachGroup(path string, names []string, want func(start, n int) bool, fn func(*groupView) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return err
	}
	defer rdr.Close()
	ar, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	sc, err := ar.Schema()
	if err != nil {
		return err
	}
	field := map[string]int{}
	for i := 0; i < sc.NumFields(); i++ {
		field[sc.Field(i).Name] = i
	}
	var colIdx []int
	idx := make([]int, len(names))
	at := map[int]int{}
	for i, name := range names {
		fi, ok := field[name]
		if !ok {
			idx[i] = -1
			continue
		}
		ti, seen := at[fi]
		if !seen {
			ti = len(colIdx)
			colIdx = append(colIdx, fi)
			at[fi] = ti
		}
		idx[i] = ti
	}
	md := rdr.MetaData()
	start := 0
	for g := 0; g < md.NumRowGroups(); g++ {
		n := int(md.RowGroup(g).NumRows())
		if want == nil || want(start, n) {
			v := &groupView{start: start, n: n, names: names, idx: idx}
			if len(colIdx) > 0 {
				tbl, err := ar.ReadRowGroups(context.Background(), colIdx, []int{g})
				if err != nil {
					return err
				}
				v.tbl = tbl
			}
			err := fn(v)
			if v.tbl != nil {
				v.tbl.Release()
			}
			if err != nil {
				return err
			}
		}
		start += n
	}
	return nil
}

// ---- the merge --------------------------------------------------------------
//
// THE RULE. Delta rows are sorted on the cluster column. A delta row is written
// immediately before the first base row whose value exceeds it — but only base
// rows that are IN ORDER get to call delta rows forward: a base row counts only
// if its value is at most the cap of the row group it is in, and the cap of
// group g is the largest MEDIAN of any group up to and including g+1.
//
// The cap is what makes "approximately sorted" safe. Without it, one stray
// base row with a late value, sitting early in the file, calls forward every
// delta row below it and they land hundreds of row groups from where they
// belong; and a base that already carries a tail of rows appended by the old
// rewrite is nothing but strays. A median does not move for a stray, and the
// running maximum of medians does not move for a whole row group of them. On a
// base that IS sorted every row is under its cap, and this is an exact merge.
// The last group has no cap, and what is left when the base ends follows it.
//
// Rows with no ordered value — NULL, or a value outside the column's class —
// are never moved: base rows stay where they are, and delta rows are written
// last, in file order, exactly as before. Parquet keeps NULLs out of min/max,
// so they widen no range wherever they are.
//
// NEWEST WINS, BY CONSTRUCTION. The old order made the newest copy of a key the
// one at the highest position, and the key index and the swap both lean on
// that. This order does not keep it — a delta row can land far in front of the
// base row it supersedes — so a key is never written twice across the merge at
// all: of the delta copies of a key only the newest is kept (newest file, then
// highest position), and a base row whose key is live in a delta is dropped.
// What duplicates remain are within the base stream, which is still written in
// its own order.
//
// MEMORY. Per live delta row: one clusterEnt (24 bytes plus the boxed value,
// about 40) and its key for the dropped-base-row test (16 bytes, sorted, behind
// a filter). That is about 56 bytes a row, against the kilobyte a row costs as
// a Go map; at the compaction trigger, a fifth of ten million rows, about
// 110 MB. Whole rows are never held by the table or by the delta: next to the
// one base row group the rewrite always held there is one BUCKET of delta
// rows, a row group's worth, and nothing else.
//
// The buckets are the one extra copy this costs. Delta files are in arrival
// order and the merge wants their rows in cluster order, so reading them a
// batch at a time in that order re-reads every delta row group once per batch
// — a number of passes that grows with the table, each over a fifth of it at
// the trigger. (Measured at 700,000 delta rows in ten million it was the
// cheaper of the two, by about seven seconds in seventy; it is quadratic.) So
// the delta files are read ONCE, in their own order, and each row is written
// to the bucket its place in the sorted run falls in: bucket b holds entries
// [b*per, (b+1)*per) of the run. A bucket is a Parquet file in the table's own
// schema, so a row comes back from it exactly as it would have from the delta.
// They live beside the new base's key index under its name, and so share its
// fate: removed when read, and by dropKidx if the rewrite is abandoned. They
// are scratch, and are not fsynced. A delta that fits in one bucket is not
// written anywhere: it is simply held.
//
// What that copy costs is one more trip through the writer for each delta row
// — in proportion to the delta, whatever the size of the table.

// clusterEmitRows is how many rows are handed to the writer at a time.
const clusterEmitRows = 4096

type clusterEnt struct {
	val  any
	file int32 // index into deltaRels; -1 once superseded by a newer copy
	pos  int32
}

type clusterRewrite struct {
	t       *Table
	col     string
	cls     int
	numeric bool
	hashing bool
	overlay map[rowKey]map[string]any
	kb      *kidxBuilder
	w       *parquetWriter
	out     int
	buf     []map[string]any

	rels []string     // delta files holding whole rows, oldest first
	ents []clusterEnt // ordered ones sorted by (val, file, pos), then the rest
	nOrd int
	next int

	// The bucket being merged: entries [batchLo, batchHi) of the run, each
	// at batch[ent.pos].
	prefix           string
	buckets          int
	batch            []map[string]any
	batchLo, batchHi int

	// Where the time went, for the one line a rewrite logs.
	scanTime, spillTime, loadTime time.Duration

	// Every key live in a delta.
	fixed  []kkey
	bloom  []uint64
	shift  uint
	others map[rowKey]int32
}

func kkeyCmp(a, b kkey) int {
	if c := cmp.Compare(a.hi, b.hi); c != 0 {
		return c
	}
	return cmp.Compare(a.lo, b.lo)
}

func (cr *clusterRewrite) bloomBit(k kkey) (word int, bit uint64) {
	h := (k.hi ^ bits.RotateLeft64(k.lo, 31)) * 0x9E3779B97F4A7C15
	h >>= cr.shift
	return int(h >> 6), 1 << (h & 63)
}

// inDelta reports whether a key has a live copy in a delta file.
func (cr *clusterRewrite) inDelta(k rowKey) bool {
	if kidxWidth(k) == 0 {
		_, ok := cr.others[k]
		return ok
	}
	if len(cr.fixed) == 0 {
		return false
	}
	kk := toKKey(k)
	if w, b := cr.bloomBit(kk); cr.bloom[w]&b == 0 {
		return false
	}
	_, ok := slices.BinarySearchFunc(cr.fixed, kk, kkeyCmp)
	return ok
}

// scan builds the sorted run: one entry per live delta row, the newest copy of
// each key only, from two columns of each delta file.
func (cr *clusterRewrite) scan(deadOf func(string) map[int]bool, medCls int) error {
	t := cr.t
	type fk struct {
		k kkey
		i int32
	}
	var fks []fk
	missing := t.State.MissingVals[cr.col]
	for fi, rel := range cr.rels {
		dead := deadOf(rel)
		err := eachGroup(filepath.Join(t.Dir, rel), []string{t.Key, cr.col}, nil, func(v *groupView) error {
			for i := 0; i < v.n; i++ {
				pos := v.start + i
				if dead[pos] {
					continue
				}
				kv, _ := v.value(0, i)
				k := keyFor(cr.numeric, kv)
				cv, ok := v.value(1, i)
				if !ok {
					cv = missing
				}
				// The value that will be WRITTEN, which a patch may have changed.
				if p, ok := cr.overlay[k]; ok {
					if pv, has := p[cr.col]; has {
						cv = pv
					}
				}
				idx := int32(len(cr.ents))
				cr.ents = append(cr.ents, clusterEnt{val: cv, file: int32(fi), pos: int32(pos)})
				if kidxWidth(k) != 0 {
					fks = append(fks, fk{toKKey(k), idx})
					continue
				}
				if cr.others == nil {
					cr.others = map[rowKey]int32{}
				}
				if prev, dup := cr.others[k]; dup {
					cr.ents[prev].file = -1
				}
				cr.others[k] = idx
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// Entries were appended oldest file first and in position order, so of two
	// copies of a key the one with the larger index is the newer.
	slices.SortFunc(fks, func(a, b fk) int {
		if c := kkeyCmp(a.k, b.k); c != 0 {
			return c
		}
		return cmp.Compare(a.i, b.i)
	})
	cr.fixed = make([]kkey, 0, len(fks))
	for i, f := range fks {
		if i+1 < len(fks) && fks[i+1].k == f.k {
			cr.ents[f.i].file = -1
			continue
		}
		cr.fixed = append(cr.fixed, f.k)
	}
	fks = nil
	if n := len(cr.fixed); n > 0 {
		lg := max(bits.Len(uint(n*16)), 10)
		cr.shift = uint(64 - lg)
		cr.bloom = make([]uint64, 1<<(lg-6))
		for _, k := range cr.fixed {
			w, b := cr.bloomBit(k)
			cr.bloom[w] |= b
		}
	}

	// The column's class: the base's, or failing that the first delta value's.
	cr.cls = medCls
	live := cr.ents[:0]
	for _, e := range cr.ents {
		if e.file < 0 {
			continue
		}
		if cr.cls == ordNone {
			cr.cls = ordClass(e.val)
		}
		live = append(live, e)
	}
	clear(cr.ents[len(live):])
	cr.ents = live
	// Ordered entries first, by value; the unordered keep file order behind.
	slices.SortStableFunc(cr.ents, func(a, b clusterEnt) int {
		ao, bo := cr.ordered(a.val), cr.ordered(b.val)
		switch {
		case ao && bo:
			return ordCmp(a.val, b.val)
		case ao:
			return -1
		case bo:
			return 1
		}
		return 0
	})
	cr.nOrd = sort.Search(len(cr.ents), func(i int) bool { return !cr.ordered(cr.ents[i].val) })
	return nil
}

func (cr *clusterRewrite) ordered(v any) bool {
	return cr.cls != ordNone && ordClass(v) == cr.cls
}

// bucketRows is how many delta rows are held as whole rows at once.
func (cr *clusterRewrite) bucketRows() int { return int(RowGroupRows) }

func (cr *clusterRewrite) bucketPath(b int) string {
	return fmt.Sprintf("%s%s%d", cr.prefix, scratchBucket, b)
}

// A rewrite's buckets are scratch: index/<N>.rewrite-<pid>-<n>.bucket<b>,
// where N is the base file being written. They used to be <N>.kidx.delta<b>,
// which reads as part of an index, and which nothing removed after a crash:
// dropKidx clears the names of ONE sequence number and the replay has moved
// past it, so up to a fifth of the table stayed on disk for good.
//
// The token after "rewrite-" is this process and a counter, so no two
// rewrites share a prefix, and the prefix is in liveScratch from before the
// first bucket exists until after the last is removed. The sweep of index/
// (publish.go) removes every scratch name whose prefix is not there: it
// cannot take a running rewrite's, whatever it knows about the Table.
const (
	scratchMark   = ".rewrite-"
	scratchBucket = ".bucket"
)

var (
	scratchSeq  atomic.Int64
	liveScratch sync.Map // prefix (absolute path up to scratchBucket) -> struct{}
)

// newScratchPrefix names and registers the scratch of one rewrite of base.
func (t *Table) newScratchPrefix(base string) string {
	p := filepath.Join(t.Dir, "index", fmt.Sprintf("%s%s%d-%d",
		trimParquet(filepath.Base(base)), scratchMark, os.Getpid(), scratchSeq.Add(1)))
	liveScratch.Store(p, struct{}{})
	return p
}

// scratchLive reports whether an index/ entry is a running rewrite's bucket.
func scratchLive(path string) bool {
	i := strings.LastIndex(path, scratchBucket)
	if i < 0 {
		return false
	}
	_, ok := liveScratch.Load(path[:i])
	return ok
}

// spill reads every delta file once and writes each row of the run to its
// bucket. Afterwards an entry's pos is its row within the bucket, not within
// the delta file.
func (cr *clusterRewrite) spill() (err error) {
	t := cr.t
	defer func(t0 time.Time) { cr.spillTime = time.Since(t0) }(time.Now())
	n, per := len(cr.ents), cr.bucketRows()
	if n == 0 {
		return nil
	}
	type ref struct{ pos, idx int32 }
	byFile := make([][]ref, len(cr.rels))
	for i, e := range cr.ents {
		byFile[e.file] = append(byFile[e.file], ref{e.pos, int32(i)})
	}
	cr.buckets = (n + per - 1) / per
	if cr.buckets == 1 {
		// Small enough to hold: the bucket is this slice.
		cr.buckets = 0
		cr.batch, cr.batchHi = make([]map[string]any, n), n
	} else if err := os.MkdirAll(filepath.Dir(cr.prefix), 0o755); err != nil {
		return err
	}
	ws := make([]*parquetWriter, cr.buckets)
	files := make([]*os.File, cr.buckets)
	defer func() {
		for b, w := range ws {
			if w == nil {
				continue
			}
			// The writer would fsync a file it owns. This one is scratch.
			if cerr := w.Close(); err == nil {
				err = cerr
			}
			if cerr := files[b].Close(); err == nil {
				err = cerr
			}
		}
	}()
	for b := range ws {
		if ws[b], err = t.newParquetWriter(cr.bucketPath(b), nil, true); err != nil {
			return err
		}
		files[b], ws[b].f = ws[b].f, nil
	}
	// Rows wait here, a row group's worth across ALL the buckets, and are then
	// written out as a row group of their own: the writer would otherwise hold
	// a full one per bucket.
	hold := max(int(RowGroupRows)/max(cr.buckets, 1), 256)
	pend := make([][]map[string]any, cr.buckets)
	count := make([]int32, cr.buckets)
	put := func(b int) error {
		if err := ws[b].Append(pend[b]); err != nil {
			return err
		}
		clear(pend[b])
		pend[b] = pend[b][:0]
		return ws[b].flush(len(ws[b].pending))
	}
	for fi, refs := range byFile {
		if len(refs) == 0 {
			continue
		}
		slices.SortFunc(refs, func(a, b ref) int { return cmp.Compare(a.pos, b.pos) })
		from := func(start int) int {
			return sort.Search(len(refs), func(j int) bool { return int(refs[j].pos) >= start })
		}
		err := eachGroup(filepath.Join(t.Dir, cr.rels[fi]), t.Order,
			func(start, n int) bool {
				j := from(start)
				return j < len(refs) && int(refs[j].pos) < start+n
			},
			func(v *groupView) error {
				for j := from(v.start); j < len(refs) && int(refs[j].pos) < v.start+v.n; j++ {
					row := int(refs[j].pos) - v.start
					m := make(map[string]any, len(t.Order))
					for c, name := range v.names {
						if val, ok := v.value(c, row); ok {
							m[name] = val
						}
					}
					m = t.fillMissing(m, t.Order, true)
					if cr.buckets == 0 {
						cr.ents[refs[j].idx].pos = refs[j].idx
						cr.batch[refs[j].idx] = m
						continue
					}
					b := int(refs[j].idx) / per
					cr.ents[refs[j].idx].pos = count[b]
					count[b]++
					pend[b] = append(pend[b], m)
					if len(pend[b]) >= hold {
						if err := put(b); err != nil {
							return err
						}
					}
				}
				return nil
			})
		if err != nil {
			return err
		}
		byFile[fi] = nil
	}
	for b := range ws {
		if want := min(per, n-b*per); int(count[b]) != want {
			return fmt.Errorf("rewrite: %d delta rows to place in bucket %d, %d read", want, b, count[b])
		}
		if err := put(b); err != nil {
			return err
		}
	}
	return nil
}

// load reads bucket b, which is then gone.
func (cr *clusterRewrite) load(b int) error {
	defer func(t0 time.Time) { cr.loadTime += time.Since(t0) }(time.Now())
	path := cr.bucketPath(b)
	rows, err := cr.t.readParquet(path, nil)
	if err != nil {
		return err
	}
	_ = os.Remove(path)
	cr.batchLo = b * cr.bucketRows()
	cr.batchHi = min(cr.batchLo+cr.bucketRows(), len(cr.ents))
	if len(rows) != cr.batchHi-cr.batchLo {
		return fmt.Errorf("rewrite: bucket %d holds %d rows, want %d", b, len(rows), cr.batchHi-cr.batchLo)
	}
	cr.batch = rows
	return nil
}

func (cr *clusterRewrite) flush() error {
	if len(cr.buf) == 0 {
		return nil
	}
	err := cr.w.Append(cr.buf)
	clear(cr.buf)
	cr.buf = cr.buf[:0]
	return err
}

// emit writes one row at the next output position. The caller has applied any
// patch.
func (cr *clusterRewrite) emit(k rowKey, r map[string]any) error {
	var h uint64
	if cr.hashing {
		h, _ = largeHash(r, cr.t.Order)
	}
	if err := cr.kb.add(k, int32(cr.out), h, false); err != nil {
		return err
	}
	cr.out++
	cr.buf = append(cr.buf, r)
	if len(cr.buf) >= clusterEmitRows {
		return cr.flush()
	}
	return nil
}

// emitDelta writes the next delta row of the run.
func (cr *clusterRewrite) emitDelta() error {
	if cr.next >= cr.batchHi {
		if err := cr.load(cr.next / cr.bucketRows()); err != nil {
			return err
		}
	}
	at := cr.ents[cr.next].pos
	r := cr.batch[at]
	if r == nil {
		return fmt.Errorf("rewrite: delta row %d of the run was not read", cr.next)
	}
	cr.batch[at] = nil
	cr.next++
	k := keyFor(cr.numeric, r[cr.t.Key])
	if p, ok := cr.overlay[k]; ok {
		applyPatch(r, p)
	}
	return cr.emit(k, r)
}

// groupMedians samples the cluster column of every row group of a base file.
// The result maps a group's first position to an approximate median, nil where
// the group has no ordered value. cls is the class the medians are in, and is
// fixed by the first ordered value seen.
func (cr *clusterRewrite) groupMedians(rel string, cls *int) (starts []int, meds []any, err error) {
	const sample = 255
	err = eachGroup(filepath.Join(cr.t.Dir, rel), []string{cr.col}, nil, func(v *groupView) error {
		var vals []any
		step := max(v.n/sample, 1)
		for i := step / 2; i < v.n; i += step {
			val, ok := v.value(0, i)
			if !ok {
				break
			}
			c := ordClass(val)
			if c == ordNone {
				continue
			}
			if *cls == ordNone {
				*cls = c
			}
			if c == *cls {
				vals = append(vals, val)
			}
		}
		var med any
		if len(vals) > 0 {
			slices.SortFunc(vals, ordCmp)
			med = vals[len(vals)/2]
		}
		starts, meds = append(starts, v.start), append(meds, med)
		return nil
	})
	return starts, meds, err
}

// clusteredRewrites counts rewrites that merged on a column, for tests that
// must know the path they mean to exercise was the one taken.
var clusteredRewrites atomic.Int64

// afterSpill is for tests: it is called on the rewrite's goroutine once the
// buckets are on disk, which is where a crash leaves the most behind.
var afterSpill func(*clusterRewrite)

// rewriteClustered writes the new base with the delta rows merged into the
// base stream on col. It returns the number of rows written.
func (t *Table) rewriteClustered(col string, baseRels, deltaRels []string,
	deadOf func(string) map[int]bool, overlay map[rowKey]map[string]any,
	numeric, hashing bool, kb *kidxBuilder, w *parquetWriter,
) (int, error) {
	clusteredRewrites.Add(1)
	start := time.Now()
	cr := &clusterRewrite{
		t: t, col: col, numeric: numeric, hashing: hashing,
		overlay: overlay, kb: kb, w: w, rels: deltaRels,
		prefix: t.newScratchPrefix(kb.name),
	}
	defer func() {
		for b := 0; b < cr.buckets; b++ {
			_ = os.Remove(cr.bucketPath(b))
		}
		liveScratch.Delete(cr.prefix)
	}()

	// The cap of each base row group, in stream order.
	type groupKey struct {
		rel   string
		start int
	}
	groupAt := map[groupKey]int{}
	var meds []any
	medCls := ordNone
	for _, rel := range baseRels {
		starts, m, err := cr.groupMedians(rel, &medCls)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		for i, s := range starts {
			groupAt[groupKey{rel, s}] = len(meds) + i
		}
		meds = append(meds, m...)
	}
	// caps[g] is the running maximum of medians through g+1; nil means no
	// base row of the group calls anything forward.
	caps := make([]any, len(meds))
	var run any
	for g, m := range meds {
		if m != nil && (run == nil || ordCmp(m, run) > 0) {
			run = m
		}
		if g > 0 {
			caps[g-1] = run
		}
	}

	t0 := time.Now()
	if err := cr.scan(deadOf, medCls); err != nil {
		return 0, err
	}
	cr.scanTime = time.Since(t0)
	defer func() {
		slog.Info("clustered rewrite", "table", t.Qualified, "column", col,
			"rows", cr.out, "delta_rows", len(cr.ents), "unordered", len(cr.ents)-cr.nOrd,
			"scan_seconds", cr.scanTime.Seconds(), "buckets", cr.buckets,
			"spill_seconds", cr.spillTime.Seconds(), "load_seconds", cr.loadTime.Seconds(),
			"seconds", time.Since(start).Seconds())
	}()
	if err := cr.spill(); err != nil {
		return 0, err
	}
	if afterSpill != nil {
		afterSpill(cr)
	}

	for _, rel := range baseRels {
		err := t.forEachRowGroup(filepath.Join(t.Dir, rel), deadOf(rel), nil,
			func(rows []map[string]any, first int) error {
				g, known := groupAt[groupKey{rel, first}]
				last := known && g == len(meds)-1
				var limit any
				if known && !last {
					limit = caps[g]
				}
				for _, r := range rows {
					k := keyFor(numeric, r[t.Key])
					if cr.inDelta(k) {
						continue // a newer copy is in the run
					}
					if p, ok := overlay[k]; ok {
						applyPatch(r, p)
					}
					if v := r[col]; cr.next < cr.nOrd && cr.ordered(v) &&
						(last || limit != nil && ordCmp(v, limit) <= 0) {
						for cr.next < cr.nOrd && ordCmp(cr.ents[cr.next].val, v) < 0 {
							if err := cr.emitDelta(); err != nil {
								return err
							}
						}
					}
					if err := cr.emit(k, r); err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil && !os.IsNotExist(err) {
			return 0, err
		}
	}
	for cr.next < len(cr.ents) {
		if err := cr.emitDelta(); err != nil {
			return 0, err
		}
	}
	return cr.out, cr.flush()
}

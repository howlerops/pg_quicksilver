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
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
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
	return chooseCluster(t.Order, ranges, groups, t.clusterRank)
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
// MEMORY, MEASURED AT THE TRIGGER. Per live delta row: one clusterEnt (24
// bytes and the boxed value, 16), its key for the dropped-base-row test (16
// bytes, sorted, behind a filter), its place in its delta file while the
// buckets are written (8), and what a slice grown by append leaves spare:
// 75 bytes a row, from a heap profile. Whole rows are never held by the table
// or by the delta. A base row group is decoded to Arrow and turned into rows a
// few thousand at a time; next to it is one BUCKET of delta rows, whose size
// is chosen against the number of bucket writers it implies (bucketRows), and
// the two together cost about 85 KB x sqrt(D). So the live heap of a process
// in the middle of a rewrite is
//
//	77 B x D   the table's own index of the keys in its deltas (a uuid key)
//	75 B x D   the rewrite's run
//	85 KB x sqrt(D)   the bucket being merged, and the writers of the rest
//	116 MB     what grows with nothing: the row group being read and the one
//	           being written, the key index's sort run
//
// D the live delta rows — at the churn trigger, a fifth of the table. Measured
// with TestRewriteAtTrigger on the orders shape (uuid key, jsonb payload,
// 65,536-row groups), both AT the trigger. The 116 MB is fitted; with it the
// formula is within 5% of the measurement at both sizes:
//
//	rows    delta D   live heap    peak RSS (anon), default GC: twice that
//	                  by formula   by formula    measured
//	1.5M     300k       208 MB       416 MB       399 MB
//	10M        2M       540 MB      1080 MB      1070 MB
//
// Peak RSS is twice the live heap because that is what Go's collector allows
// itself by default; the sidecar runs with GOMEMLIMIT at 80% of its limit, and
// it is the live heap that has to fit under that. The unclustered rewrite
// (QS_CLUSTER_BY=none) has no 75 B x D and no buckets: 684 MB at ten million.
//
// This paragraph used to say 56 bytes a row and "about 110 MB" at the trigger,
// from a run with 700,000 changed rows. At the trigger the same rewrite peaked
// at 1,950 to 2,040 MB resident (1,650 to 1,740 anonymous), and four things it
// did not mention were most of that: a bloom filter built for every small row
// group of every bucket and held until the bucket closed (480 MB, growing with
// the square of the delta; see newScratchWriter); the base file's dead
// positions as a map (74 MB for what a bitmap says in 1.25); every base row
// group as 65,536 Go maps at once (80 MB; forEachRows); and a 40 MB sort run
// for the key index (kidxRunRecords). It is 1,314 MB resident now, 466 of
// which the process held before the rewrite began.
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

// A base that is NOT approximately sorted any more — one an older release's
// rewrites appended to — is not merged into at all: it is sorted, once. See
// "re-clustering" at the end of this file.

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
	numeric keyMode
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
	per              int // rows in a bucket; 0 is a row group's worth
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
func (cr *clusterRewrite) scan(deadOf func(string) func(int) bool, medCls int) error {
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
				if dead(pos) {
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

// bucketRows is how many delta rows are held as whole rows at once: the size of
// a bucket.
//
// It was a row group's worth whatever the delta. But a bucket is paid for
// twice over: once as the rows of the one being merged, about 1.2 KB each as Go
// maps, and once as a writer held open for every bucket while they are filled,
// about 1.5 MB each. For D delta rows in buckets of b that is 1.2 KB x b +
// 1.5 MB x D/b, least at b = sqrt(1250 x D) — 16,000 rows for a delta of
// 200,000, where a full row group was 80 MB of rows to save four writers.
func (cr *clusterRewrite) bucketRows() int {
	if cr.per > 0 {
		return cr.per
	}
	return int(RowGroupRows)
}

const (
	bucketMinRows = 8192
	// writerBytes / rowBytes, as measured: see bucketRows.
	bucketRatio = 1250
	// spillHoldRows is how many rows wait, across all the buckets, to be
	// written as row groups of their own.
	spillHoldRows = 16384
)

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

// newScratchWriter opens a bucket: a Parquet file in the table's schema that
// this rewrite will read back, whole, and remove. It is the table's writer
// with three things taken away, because nothing but this rewrite reads it:
//
//   - No bloom filters. A filter is built per column chunk and held until the
//     file is closed, and a bucket is written in many small row groups — one
//     each time its share of the hold fills. With the adaptive builder that
//     was a ladder of candidates, 256 KB for each of two columns, for every
//     one of those row groups in every bucket: at two million delta rows, 31
//     buckets of 31 row groups, 480 MB, growing with the SQUARE of the delta.
//     It was most of the rewrite's peak, and it was thrown away unread.
//   - No fsync: the writer syncs a file it owns, so it is not given this one.
//     The caller closes the file it gets back.
//   - No claim on the path. It is not a data file and no snapshot names it.
func (t *Table) newScratchWriter(path string) (*parquetWriter, *os.File, error) {
	schema := t.arrowSchemaFor(t.Order)
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(ZstdLevel),
		parquet.WithMaxRowGroupLength(RowGroupRows),
		parquet.WithStats(false),
	)
	w, err := pqarrow.NewFileWriter(schema, keepOpen{f}, props, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return &parquetWriter{w: w, schema: schema, cols: t.Order, tbl: t}, f, nil
}

// spill reads every delta file once and writes each row of the run to its
// bucket. Afterwards an entry's pos is its row within the bucket, not within
// the delta file.
func (cr *clusterRewrite) spill() (err error) {
	t := cr.t
	defer func(t0 time.Time) { cr.spillTime = time.Since(t0) }(time.Now())
	n := len(cr.ents)
	if n == 0 {
		return nil
	}
	if want := int(math.Sqrt(float64(n) * bucketRatio)); want < int(RowGroupRows) {
		cr.per = min(max(want, bucketMinRows), int(RowGroupRows))
	}
	per := cr.bucketRows()
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
			if cerr := w.Close(); err == nil {
				err = cerr
			}
			if cerr := files[b].Close(); err == nil {
				err = cerr
			}
		}
	}()
	for b := range ws {
		if ws[b], files[b], err = t.newScratchWriter(cr.bucketPath(b)); err != nil {
			return err
		}
	}
	// Rows wait here, a few thousand across ALL the buckets, and are then
	// written out as a row group of their own: the writer would otherwise hold
	// a full one per bucket.
	hold := max(min(spillHoldRows, int(RowGroupRows))/max(cr.buckets, 1), 64)
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
	err = cr.eachDeltaRow(func(idx int32, m map[string]any) error {
		if cr.buckets == 0 {
			cr.ents[idx].pos = idx
			cr.batch[idx] = m
			return nil
		}
		b := int(idx) / per
		cr.ents[idx].pos = count[b]
		count[b]++
		pend[b] = append(pend[b], m)
		if len(pend[b]) >= hold {
			return put(b)
		}
		return nil
	})
	if err != nil {
		return err
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

// eachDeltaRow reads every delta file once, in its own order, and hands over
// each row of the run — whole, as the table's schema has it — with its index in
// cr.ents. Rows that are dead or superseded by a newer copy are not in the run
// and are not read.
func (cr *clusterRewrite) eachDeltaRow(fn func(idx int32, row map[string]any) error) error {
	t := cr.t
	type ref struct{ pos, idx int32 }
	byFile := make([][]ref, len(cr.rels))
	for i, e := range cr.ents {
		byFile[e.file] = append(byFile[e.file], ref{e.pos, int32(i)})
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
					if err := fn(refs[j].idx, t.fillMissing(m, t.Order, true)); err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil {
			return err
		}
		byFile[fi] = nil
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
	deadOf func(string) func(int) bool, overlay map[rowKey]map[string]any,
	numeric keyMode, hashing bool, kb *kidxBuilder, w *parquetWriter, sortAll bool,
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
			"sorted", sortAll, "seconds", time.Since(start).Seconds())
	}()
	if sortAll {
		// The base is not in the order this merge would keep: sort all of it.
		if err := cr.rewriteSorted(baseRels, deadOf); err != nil {
			return 0, err
		}
		return cr.out, cr.flush()
	}
	if err := cr.spill(); err != nil {
		return 0, err
	}
	if afterSpill != nil {
		afterSpill(cr)
	}

	for _, rel := range baseRels {
		err := t.forEachRows(filepath.Join(t.Dir, rel), deadOf(rel), nil, rowChunk,
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

// ---- re-clustering ----------------------------------------------------------
//
// The merge above keeps the order a base file HAS. A mirror that ran an older
// release has a base that no longer has one worth keeping: every rewrite that
// release did put the changed rows at the end, and those rows stay where they
// are until something updates them again. Their row groups each span most of
// the column's range, every range filter reads all of them, and the merge
// faithfully carries them forward.
//
// DETECTION is from the footer and nothing else, and it asks two questions of
// each column's row-group ranges.
//
// Is the file declustered on it? A file of G row groups that is clustered has
// ranges that overlap their neighbours and no others: about 2·w/(G-1) of all
// pairs, for a group overlapping w on each side. A file with a fraction f of
// its rows appended out of place has about f·G groups that overlap everything,
// which is 2f - f² of all pairs. So the measure is the EXCESS — overlapFraction
// less what a clustered file of that many groups would show — and ReclusterOverlap
// is how much of it is too much. One old rewrite at the churn trigger (f = 1/6)
// scores about 0.3, two score 0.5; a snapshot scores 0.
//
// And was it ever clustered on it? That cannot be chooseCluster's test, which
// wants fewer than half of all pairs overlapping: two old rewrites are past
// that, and then the column that overlaps LEAST is whichever happens to hold
// three values in long runs, and sorting on that would throw away the order
// the file still has. What an old rewrite leaves is a file in two parts, and
// the test is for exactly that shape: at least half the row groups are NARROW
// (their range is a few G-ths of the column's whole span — the part still in
// order), and those narrow groups do not overlap one another beyond what
// neighbours do. A column with no order has no narrow groups. A column of a
// few repeated values has nothing but, and they overlap each other entirely.
//
// THE CURE is the one thing the top of this file says not to do routinely: sort
// the whole table on the column. Once. It is an external merge sort (see
// rewriteSorted) with the memory of an ordinary rewrite, and what it writes is
// exactly sorted, so the next look at the footer finds nothing to do.
//
// WHEN. Any rewrite that finds its base declustered sorts instead of merging;
// the trigger that started it does not matter. And a table that is otherwise
// quiet gets a rewrite for this alone ("recluster", CompactReason) — but never
// sooner than ReclusterIntervalSeconds after its base file was WRITTEN, which
// is the file's own mtime and therefore survives restarts, and never for a
// table with no column that passes the second test: there is nothing to sort
// by. QS_CLUSTER_BY names the column for a table the test is wrong about, and
// `none` there turns this off for it.
//
// If a sort does not bring the measure down, sorting again would not either.
// So a table is sorted on a column at most once per process, and a rewrite for
// this alone is started at most once per interval whatever happens.

// ReclusterOverlap is the excess overlap (see above) at which a base file is
// sorted by the next rewrite. Zero or less turns re-clustering off.
var ReclusterOverlap = envFloat("QS_RECLUSTER_OVERLAP", 0.10)

// ReclusterIntervalSeconds is the least age of a base file before a rewrite is
// started for no reason but to re-cluster it.
var ReclusterIntervalSeconds = envFloat("QS_RECLUSTER_INTERVAL_SECONDS", 24*3600)

const (
	// reclusterNeighbours is how many groups either side a clustered file's
	// row group may overlap before it counts against the file.
	reclusterNeighbours = 2
	// A row group is narrow if its range is at most this many G-ths of the
	// column's span, G being the number of groups (and never more than a
	// quarter of it).
	reclusterNarrow = 8
	// The narrow groups must be at least this fraction of the file...
	reclusterCore = 0.5
	// ...and overlap each other by no more than this beyond their neighbours.
	reclusterCoreOverlap = 0.05
)

// clusteredOverlap is the overlap fraction a clustered file of this many row
// groups shows.
func clusteredOverlap(groups int) float64 {
	if groups < 2 {
		return 1
	}
	return min(1, 2*reclusterNeighbours/float64(groups-1))
}

// statNum places a statistics value on a line, so that the width of a range
// can be compared with the span of a column. skip is the bytes every value of
// the column begins with.
func statNum(v any, skip int) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case uint64:
		return float64(x)
	case float64:
		return x
	case []byte:
		var n uint64
		for i := 0; i < 8; i++ {
			n <<= 8
			if skip+i < len(x) {
				n |= uint64(x[skip+i])
			}
		}
		return float64(n)
	}
	return 0
}

// declustering is what the footer says about one column.
type declustering struct {
	groups int
	excess float64 // overlap beyond a clustered file's, over all groups
	narrow float64 // fraction of groups whose range is narrow
	core   float64 // overlap beyond a clustered file's, among the narrow ones
}

// wasClustered: most of the file is still in order on this column.
func (d declustering) wasClustered() bool {
	return d.narrow >= reclusterCore && d.core <= reclusterCoreOverlap
}

// isDeclustered: enough of it is not.
func (d declustering) isDeclustered() bool {
	return ReclusterOverlap > 0 && d.groups >= clusterMinGroups && d.excess >= ReclusterOverlap
}

func measureDeclustering(r []statRange) (d declustering) {
	d.groups = len(r)
	if len(r) < clusterMinGroups {
		return d
	}
	d.excess = overlapFraction(r) - clusteredOverlap(len(r))
	lo, hi := r[0].lo, r[0].hi
	for _, g := range r[1:] {
		if ordCmp(g.lo, lo) < 0 {
			lo = g.lo
		}
		if ordCmp(g.hi, hi) > 0 {
			hi = g.hi
		}
	}
	skip := 0
	if a, ok := lo.([]byte); ok {
		b, _ := hi.([]byte)
		for skip < len(a) && skip < len(b) && a[skip] == b[skip] {
			skip++
		}
	}
	span := statNum(hi, skip) - statNum(lo, skip)
	if !(span > 0) {
		return d
	}
	limit := span * min(reclusterNarrow/float64(len(r)), 0.25)
	var core []statRange
	for _, g := range r {
		if statNum(g.hi, skip)-statNum(g.lo, skip) <= limit {
			core = append(core, g)
		}
	}
	d.narrow = float64(len(core)) / float64(len(r))
	d.core = 1
	if len(core) >= clusterMinGroups {
		d.core = overlapFraction(core) - clusteredOverlap(len(core))
	}
	return d
}

// reclusterSorted remembers, per table directory, the column a sort was last
// done on, so that one which did not help is not repeated by this process.
var reclusterSorted sync.Map

// clusterRank breaks ties between columns: the time column is the one a filter
// prunes on, and a key lookup goes through the key index.
func (t *Table) clusterRank(name string) int {
	switch {
	case temporalOf(t.Columns[name]) != notTemporal:
		return 2
	case name == t.Key:
		return 1
	}
	return 0
}

// reclusterColumn is the column the base files should be SORTED on because
// they are badly declustered on it, or "" if there is none — which is the
// answer for a clustered file, for a file with no clustered column, and for a
// table this process has already sorted on that column.
func (t *Table) reclusterColumn(baseRels []string) (col, why string) {
	if len(baseRels) == 0 || ReclusterOverlap <= 0 {
		return "", ""
	}
	cands, named := t.Order, false
	if c, ok := clusterOverride(ClusterBy, t.Qualified); ok {
		if _, known := t.Columns[c]; !known || c == "none" {
			return "", ""
		}
		// The operator has said which column the base is clustered on.
		cands, named = []string{c}, true
	}
	paths := make([]string, len(baseRels))
	for i, rel := range baseRels {
		paths[i] = filepath.Join(t.Dir, rel)
	}
	ranges, groups, err := columnRanges(paths, cands)
	if err != nil {
		return "", ""
	}
	var best declustering
	for _, name := range cands {
		r := ranges[name]
		if len(r) < clusterMinGroups || len(r) < groups/2 {
			continue
		}
		d := measureDeclustering(r)
		if !d.isDeclustered() || !(named || d.wasClustered()) {
			continue
		}
		if col == "" || t.clusterRank(name) > t.clusterRank(col) ||
			t.clusterRank(name) == t.clusterRank(col) && d.narrow > best.narrow {
			col, best = name, d
		}
	}
	if col == "" {
		return "", ""
	}
	if done, _ := reclusterSorted.Load(t.Dir); done == col {
		return "", ""
	}
	return col, fmt.Sprintf("declustered on %s: %.0f%% of row-group pairs overlap beyond the %.1f%% a clustered file of %d groups shows; "+
		"%.0f%% of the groups are still in order", col, 100*best.excess, 100*clusteredOverlap(best.groups), best.groups, 100*best.narrow)
}

// reclusterCheck is what reclusterDue last found for a table's base file.
type reclusterCheck struct {
	base    string
	spec    string
	col     string
	written time.Time
}

var reclusterChecked sync.Map // table directory -> reclusterCheck

// reclusterDue reports whether the table should be rewritten for no other
// reason than that its base file is declustered. It is asked on every tick, so
// the footer is read once per base file and the answer kept.
func (t *Table) reclusterDue() bool {
	if ReclusterOverlap <= 0 || len(t.State.BaseFiles) != 1 || t.compacting != nil {
		return false
	}
	if !t.retryAt.IsZero() && t.now().Before(t.retryAt) {
		return false
	}
	base := t.State.BaseFiles[0]
	chk, _ := reclusterChecked.Load(t.Dir)
	c, ok := chk.(reclusterCheck)
	if !ok || c.base != base || c.spec != ClusterBy {
		c = reclusterCheck{base: base, spec: ClusterBy}
		rels := []string{filepath.Join("base", base)}
		if st, err := os.Stat(filepath.Join(t.Dir, rels[0])); err == nil {
			c.written = st.ModTime()
			c.col, _ = t.reclusterColumn(rels)
		}
		reclusterChecked.Store(t.Dir, c)
	}
	if c.col == "" {
		return false
	}
	return t.now().Sub(c.written) >= time.Duration(ReclusterIntervalSeconds*float64(time.Second))
}

// CompactReason is why a rewrite of the base is worth its cost right now, or
// "" if it is not: one of the three triggers of compactReason (mirror.go), or
// "recluster" for a base file that is badly declustered and old enough.
func (t *Table) CompactReason() string {
	if why := t.compactReason(); why != "" {
		return why
	}
	if t.reclusterDue() {
		return "recluster"
	}
	return ""
}

// sortRunRowGroup bounds the rows a run holds in one row group, which is what
// the merge holds per run.
const (
	sortRunGroupMin = 64
	sortRunGroupMax = 8192
)

// sortedRuns counts runs written by rewriteSorted, for tests.
var sortedRuns atomic.Int64

type sortRow struct {
	val any
	row map[string]any
}

// sortCursor is one sorted run being merged: a row group of it at a time.
type sortCursor struct {
	idx  int
	ch   chan []map[string]any
	errc chan error
	rows []map[string]any
	i    int
	val  any
	ord  bool
}

// rewriteSorted writes every live row in the order of the cluster column: an
// external merge sort.
//
// PASS ONE reads the table as any rewrite does — the base a row group at a
// time, less the rows a delta supersedes, then the newest delta copy of every
// key — and cuts it into RUNS of one row group's worth of rows. Each run is
// sorted in memory and written as a scratch Parquet file in the table's own
// schema, in small row groups. PASS TWO merges the runs through a heap, holding
// one small row group of each.
//
// MEMORY. Pass one holds the run being filled: R whole rows, R = RowGroupRows,
// which is what the clustered merge held before its buckets were sized to the
// delta. Pass two holds 2·g rows of each of the
// k = rows/R runs, with g = R/(2k) clamped to [64, 8192]: R rows, until the
// table passes R²/128 rows (33 million at the default) and the clamp makes it
// k·128. Ordering is as the merge's: values of the column's class in order,
// everything else — NULL included — after them, in the order it was read.
//
// A key is written once, as in the merge: a base row whose key is live in a
// delta is dropped, and only the newest delta copy is in the run.
func (cr *clusterRewrite) rewriteSorted(baseRels []string, deadOf func(string) func(int) bool) error {
	t := cr.t
	R := int(RowGroupRows)
	total := len(cr.ents)
	for _, rel := range baseRels {
		if n, err := parquetRows(filepath.Join(t.Dir, rel)); err == nil {
			total += int(n)
		}
	}
	k := max((total+R-1)/R, 1)
	g := min(max(R/(2*k), sortRunGroupMin), sortRunGroupMax)
	if err := os.MkdirAll(filepath.Dir(cr.prefix), 0o755); err != nil {
		return err
	}

	t0 := time.Now()
	run := make([]sortRow, 0, R)
	less := func(a, b sortRow) int {
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
	}
	writeRun := func() (err error) {
		if len(run) == 0 {
			return nil
		}
		slices.SortStableFunc(run, less)
		w, f, err := t.newScratchWriter(cr.bucketPath(cr.buckets))
		if err != nil {
			return err
		}
		cr.buckets++
		sortedRuns.Add(1)
		defer func() {
			if cerr := w.Close(); err == nil {
				err = cerr
			}
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}()
		rows := make([]map[string]any, 0, g)
		for lo := 0; lo < len(run); lo += g {
			rows = rows[:0]
			for _, r := range run[lo:min(lo+g, len(run))] {
				rows = append(rows, r.row)
			}
			if err := w.Append(rows); err != nil {
				return err
			}
			if err := w.flush(len(w.pending)); err != nil {
				return err
			}
		}
		clear(run)
		run = run[:0]
		return nil
	}
	add := func(r map[string]any) error {
		run = append(run, sortRow{val: r[cr.col], row: r})
		if len(run) >= R {
			return writeRun()
		}
		return nil
	}
	for _, rel := range baseRels {
		err := t.forEachRows(filepath.Join(t.Dir, rel), deadOf(rel), nil, rowChunk,
			func(rows []map[string]any, _ int) error {
				for _, r := range rows {
					k := keyFor(cr.numeric, r[t.Key])
					if cr.inDelta(k) {
						continue // a newer copy is in a delta
					}
					if p, ok := cr.overlay[k]; ok {
						applyPatch(r, p)
					}
					if err := add(r); err != nil {
						return err
					}
				}
				return nil
			})
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	err := cr.eachDeltaRow(func(_ int32, r map[string]any) error {
		if p, ok := cr.overlay[keyFor(cr.numeric, r[t.Key])]; ok {
			applyPatch(r, p)
		}
		return add(r)
	})
	if err != nil {
		return err
	}
	if err := writeRun(); err != nil {
		return err
	}
	run = nil
	cr.next = len(cr.ents) // the delta rows are in the runs
	cr.spillTime = time.Since(t0)

	// The merge. Each run is read by its own goroutine, which hands over one
	// row group and waits: forEachRowGroup pushes, and a merge has to pull.
	defer func(t0 time.Time) { cr.loadTime = time.Since(t0) }(time.Now())
	stop := make(chan struct{})
	defer close(stop)
	curs := make([]*sortCursor, cr.buckets)
	for b := range curs {
		c := &sortCursor{idx: b, ch: make(chan []map[string]any), errc: make(chan error, 1)}
		curs[b] = c
		go func(path string) {
			defer close(c.ch)
			err := t.forEachRowGroup(path, nil, nil, func(rows []map[string]any, _ int) error {
				select {
				case c.ch <- rows:
					return nil
				case <-stop:
					return errSortStopped
				}
			})
			_ = os.Remove(path)
			c.errc <- err
		}(cr.bucketPath(b))
	}
	// advance moves a cursor to its next row; false at the end of its run.
	advance := func(c *sortCursor) (bool, error) {
		c.i++
		for c.i >= len(c.rows) {
			rows, ok := <-c.ch
			if !ok {
				return false, <-c.errc
			}
			c.rows, c.i = rows, 0
		}
		c.val = c.rows[c.i][cr.col]
		c.ord = cr.ordered(c.val)
		return true, nil
	}
	before := func(a, b *sortCursor) bool {
		switch {
		case a.ord && b.ord:
			if d := ordCmp(a.val, b.val); d != 0 {
				return d < 0
			}
		case a.ord != b.ord:
			return a.ord
		}
		return a.idx < b.idx // runs were cut in the order the rows were read
	}
	var h []*sortCursor
	down := func(i int) {
		for {
			l, m := 2*i+1, i
			if l < len(h) && before(h[l], h[m]) {
				m = l
			}
			if l+1 < len(h) && before(h[l+1], h[m]) {
				m = l + 1
			}
			if m == i {
				return
			}
			h[i], h[m] = h[m], h[i]
			i = m
		}
	}
	for _, c := range curs {
		c.i = -1
		ok, err := advance(c)
		if err != nil {
			return err
		}
		if ok {
			h = append(h, c)
		}
	}
	for i := len(h)/2 - 1; i >= 0; i-- {
		down(i)
	}
	for len(h) > 0 {
		c := h[0]
		r := c.rows[c.i]
		c.rows[c.i] = nil
		if err := cr.emit(keyFor(cr.numeric, r[t.Key]), r); err != nil {
			return err
		}
		ok, err := advance(c)
		if err != nil {
			return err
		}
		if !ok {
			h[0] = h[len(h)-1]
			h = h[:len(h)-1]
		}
		down(0)
	}
	return nil
}

var errSortStopped = fmt.Errorf("rewrite: sort abandoned")

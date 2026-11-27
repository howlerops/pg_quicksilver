package mirror

// What the on-disk base index costs and buys, against the map it replaced.
//
// It builds a UUID-keyed mirror of QS_KIDX_N rows (default 4,000,000) with no
// index file, as a mirror written by the previous version would arrive, and
// then measures both implementations over it: heap held by the index, the time
// to have one at start, a lookup, and a 10,000-row update batch through Apply.
// A measurement, so it does not run unless asked:
//
//	QS_COST=kidx taskset -c 2 go test ./internal/mirror/ -run TestKidxCost -v -timeout 60m
//
// As root it also drops the page cache to time a lookup that has to fault.
//
// On a 4-vCPU VM shared with other work, pinned to one core (so the Apply
// figures are the pair-by-pair ones; a batch varies more between runs than the
// two implementations differ):
//
//	                                         4M UUID keys          10M UUID keys
//	                                         map       file        map       file
//	Go heap in use, index loaded          320.6 MB    1.6 MB    641.9 MB    4.1 MB
//	  with 410,000 keys moved to deltas   320.3 MB   17.2 MB
//	index file, mapped                        -     106.8 MB        -     267.0 MB
//	index at start, from the key column    3.9 s      3.3 s      11.0 s     8.0 s
//	index at start, file present              -       47 ms         -       96 ms
//	  the same, page cache dropped            -      713 ms         -      806 ms
//	lookup, warm                           345 ns     389 ns      384 ns    426 ns
//	lookup, page cache dropped                -       65 us         -       85 us
//	Apply of 10k updates, median of 40    79.1 ms    85.9 ms    85.3 ms   82.6 ms
//	  new/old, pair by pair, median            +7.0%                 +2.0%
//
// The cold lookup is one read of one page from this VM's disk, and it is the
// number to remember: an index that does not fit in the page cache turns every
// update of a base row into one of those.

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func heapInUse() float64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapInuse) / (1 << 20)
}

func TestKidxCost(t *testing.T) {
	if testing.Short() || os.Getenv("QS_COST") != "kidx" {
		t.Skip("builds a multi-million-row mirror; set QS_COST=kidx")
	}
	N := 4_000_000
	if v, err := strconv.Atoi(os.Getenv("QS_KIDX_N")); err == nil && v > 0 {
		N = v
	}
	defer func(v bool) { DiskIndex = v }(DiskIndex)

	cols := map[string]string{"id": "uuid", "v": "text", "n": "bigint"}
	order := []string{"id", "v", "n"}
	root := t.TempDir()
	if d := os.Getenv("QS_KIDX_DIR"); d != "" {
		root = d
		defer os.RemoveAll(filepath.Join(d, "public.r"))
	}
	open := func() *Table {
		tbl, err := New(root, "public", "r", "id", cols, order)
		if err != nil {
			t.Fatal(err)
		}
		return tbl
	}

	// The base file, written as Snapshot writes it but without an index.
	{
		DiskIndex = false
		tbl := open()
		tbl.State.Seq++
		name := fmt.Sprintf("%06d.parquet", tbl.State.Seq)
		w, err := tbl.newParquetWriter(filepath.Join(tbl.Dir, "base", name), nil, true)
		if err != nil {
			t.Fatal(err)
		}
		t0 := time.Now()
		for i := 0; i < N; i++ {
			if err := w.AppendValues([]any{testUUID(i), "x", strconv.Itoa(i)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		tbl.State.BaseFiles, tbl.State.BaseRows = []string{name}, N
		if err := tbl.saveState(); err != nil {
			t.Fatal(err)
		}
		st, _ := os.Stat(filepath.Join(tbl.Dir, "base", name))
		t.Logf("%d UUID rows, base file %.1f MB, written in %s", N, float64(st.Size())/(1<<20), time.Since(t0).Round(time.Millisecond))
	}

	rng := rand.New(rand.NewSource(1))
	probe := make([]rowKey, 200_000)
	for i := range probe {
		hi, lo, _ := parseUUID(testUUID(rng.Intn(N)))
		probe[i] = rowKey{num: int64(hi), lo: lo, isUUID: true}
	}
	lookups := func(tbl *Table, keys []rowKey) float64 {
		t0 := time.Now()
		for _, k := range keys {
			if _, ok := tbl.index.get(k); !ok {
				t.Fatalf("key %s not found", k)
			}
		}
		return float64(time.Since(t0).Nanoseconds()) / float64(len(keys))
	}
	lsn := 0
	batch := func(b int) []changestream.Change {
		ch := make([]changestream.Change, 0, 10_000)
		seen := map[int]bool{}
		for len(ch) < 10_000 {
			i := rng.Intn(N)
			if seen[i] {
				continue
			}
			seen[i] = true
			r := map[string]any{"id": testUUID(i), "v": "y", "n": strconv.Itoa(b)}
			ch = append(ch, changestream.Change{Op: changestream.OpUpdate, Schema: "public", Table: "r", Row: r, Key: r})
		}
		return ch
	}
	// Page faults taken inside Apply, by table: the mapped file is the one
	// place the new index can wait on the kernel where the old one could not.
	faults := map[*Table][2]int64{}
	applyOne := func(tbl *Table, ch []changestream.Change) time.Duration {
		lsn++
		pos := fmt.Sprintf("0/%X", lsn)
		var r0, r1 syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r0)
		t0 := time.Now()
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: ch}}); err != nil {
			t.Fatal(err)
		}
		d := time.Since(t0)
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r1)
		f := faults[tbl]
		faults[tbl] = [2]int64{f[0] + r1.Minflt - r0.Minflt, f[1] + r1.Majflt - r0.Majflt}
		return d
	}
	stats := func(ds []time.Duration) (med, lo time.Duration) {
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		return ds[len(ds)/2], ds[0]
	}
	dropCaches := func() bool {
		// Dirty pages are not dropped, and these files were just written.
		syscall.Sync()
		return os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0o200) == nil
	}
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

	// The second copy of the table, so each implementation applies its batches
	// to a mirror in the same state.
	other := filepath.Join(root, "copy")
	if err := os.CopyFS(filepath.Join(other, "public.r"), os.DirFS(filepath.Join(root, "public.r"))); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(other)

	// ---- the map ----
	DiskIndex = false
	hBase := heapInUse()
	tbl := open()
	t0 := time.Now()
	if _, err := tbl.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	oldBuild := time.Since(t0)
	oldHeap := heapInUse() - hBase
	lookups(tbl, probe)
	lookups(tbl, probe)
	oldLookup := lookups(tbl, probe)
	oldTbl := tbl
	// Dropped while the other is measured, and built again for the batches.
	oldTbl.index, oldTbl.patch = nil, nil

	// ---- the file ----
	DiskIndex = true
	root = other
	tbl = open()
	t0 = time.Now()
	if _, err := tbl.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	newBuild := time.Since(t0)
	st, err := os.Stat(tbl.kidxPath(tbl.State.BaseFiles[0]))
	if err != nil {
		t.Fatal(err)
	}
	tbl.index.close()
	tbl = nil

	tbl = open()
	t0 = time.Now()
	if _, err := tbl.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	newLoad := time.Since(t0)
	newHeap := heapInUse() - hBase
	// Nothing is mapped in yet (the load reads, it does not fault), so dropping
	// the cache here makes every lookup that follows a real page fault.
	cold := -1.0
	if dropCaches() {
		cold = lookups(tbl, probe[:20_000])
		// And back to what a start leaves behind: the whole file in the page
		// cache. Without this the "warm" numbers below pay for the pages the
		// cold probes happened not to touch (3,240 major faults in 41 Applies
		// at ten million, the first time this was run).
		if _, err := os.ReadFile(tbl.kidxPath(tbl.State.BaseFiles[0])); err != nil {
			t.Fatal(err)
		}
	}
	lookups(tbl, probe)
	lookups(tbl, probe)
	newLookup := lookups(tbl, probe)

	// The same batches to both, turn and turn about, so that whatever else the
	// machine is doing lands on both. Rows are built outside the timing.
	DiskIndex = false
	if _, err := oldTbl.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	DiskIndex = true
	if oldTbl.index.disk != nil || tbl.index.disk == nil {
		t.Fatal("the two tables are not one of each")
	}
	oldFirst, newFirst := applyOne(oldTbl, batch(0)), applyOne(tbl, batch(0))
	var oldDs, newDs []time.Duration
	for b := 1; b <= 40; b++ {
		ch1, ch2 := batch(b), batch(b)
		if b%2 == 0 {
			oldDs, newDs = append(oldDs, applyOne(oldTbl, ch1)), append(newDs, applyOne(tbl, ch2))
		} else {
			newDs, oldDs = append(newDs, applyOne(tbl, ch2)), append(oldDs, applyOne(oldTbl, ch1))
		}
	}
	// On a machine doing other things the spread between batches is wider than
	// the difference being measured, so the pairs are compared too.
	ratios := make([]float64, len(oldDs))
	for i := range oldDs {
		ratios[i] = float64(newDs[i]) / float64(oldDs[i])
	}
	sort.Float64s(ratios)
	oldFaults, newFaults := faults[oldTbl], faults[tbl]
	oldApply, oldMin := stats(oldDs)
	newApply, newMin := stats(newDs)
	oldTbl.index, oldTbl.patch = nil, nil
	newHeapAfter := heapInUse() - hBase
	DiskIndex = false
	if _, err := oldTbl.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	DiskIndex = true
	oldHeapAfter := heapInUse() - hBase - newHeapAfter
	runtime.KeepAlive(oldTbl)
	oldTbl = nil

	coldLoad := time.Duration(-1)
	tbl.index.close()
	tbl = open()
	if dropCaches() {
		t0 = time.Now()
		if _, err := tbl.BuildIndex(); err != nil {
			t.Fatal(err)
		}
		coldLoad = time.Since(t0)
	}

	t.Logf("")
	t.Logf("%-46s %14s %14s", "", "map (before)", "file (after)")
	t.Logf("%-46s %11.1f MB %11.1f MB", "Go heap in use, index loaded", oldHeap, newHeap)
	t.Logf("%-46s %11.1f MB %11.1f MB", "  and with 41 batches of 10k updates in deltas", oldHeapAfter, newHeapAfter)
	t.Logf("%-46s %14s %11.1f MB", "index file, mapped", "-", float64(st.Size())/(1<<20))
	t.Logf("%-46s %11.0f ms %11.0f ms", "index at start: scan of the key column", ms(oldBuild), ms(newBuild))
	t.Logf("%-46s %14s %11.0f ms", "index at start: file present, warm cache", "-", ms(newLoad))
	if coldLoad >= 0 {
		t.Logf("%-46s %14s %11.0f ms", "index at start: file present, cache dropped", "-", ms(coldLoad))
	}
	t.Logf("%-46s %11.0f ns %11.0f ns", "lookup, warm", oldLookup, newLookup)
	if cold >= 0 {
		t.Logf("%-46s %14s %11.0f ns", "lookup, page cache dropped", "-", cold)
	} else {
		t.Logf("lookup with the page cache dropped: not measured (not root)")
	}
	t.Logf("%-46s %11.1f ms %11.1f ms", "Apply of 10k updates, first batch", ms(oldFirst), ms(newFirst))
	t.Logf("%-46s %11.1f ms %11.1f ms  (%+.1f%%)", "Apply of 10k updates, median of 40", ms(oldApply), ms(newApply),
		100*(float64(newApply)/float64(oldApply)-1))
	t.Logf("%-46s %11.1f ms %11.1f ms  (%+.1f%%)", "Apply of 10k updates, fastest of 40", ms(oldMin), ms(newMin),
		100*(float64(newMin)/float64(oldMin)-1))
	t.Logf("%-46s %14d %14d", "  minor page faults in those 41 Applies", oldFaults[0], newFaults[0])
	t.Logf("%-46s %14d %14d", "  major page faults", oldFaults[1], newFaults[1])
	t.Logf("%-46s %+.1f%% (quartiles %+.1f%% .. %+.1f%%)", "  new/old, pair by pair, median",
		100*(ratios[len(ratios)/2]-1), 100*(ratios[len(ratios)/4]-1), 100*(ratios[3*len(ratios)/4]-1))
	tbl.index.close()
}

// TestKidxTextCost is the same question for a key that is not fixed-width: a
// table keyed by text, which before the index file held such keys kept every
// one in a Go map of strings and read the key column at every start.
//
//	QS_COST=kidxtext go test ./internal/mirror/ -run TestKidxTextCost -v -timeout 60m
//
// It prints three lines: the map (QS_DISK_INDEX=0), the file being built from
// the key column, and the file being loaded as a restart loads it.
func TestKidxTextCost(t *testing.T) {
	if testing.Short() || os.Getenv("QS_COST") != "kidxtext" {
		t.Skip("builds a multi-million-row mirror; set QS_COST=kidxtext")
	}
	N := 2_000_000
	if v, err := strconv.Atoi(os.Getenv("QS_KIDX_N")); err == nil && v > 0 {
		N = v
	}
	defer func(v bool) { DiskIndex = v }(DiskIndex)
	cols := map[string]string{"id": "text", "v": "text", "n": "bigint"}
	order := []string{"id", "v", "n"}
	root := t.TempDir()
	key := func(i int) string { return "order-" + testUUID(i)[:18] + "-" + strconv.Itoa(i) }
	open := func() *Table {
		tbl, err := New(root, "public", "r", "id", cols, order)
		if err != nil {
			t.Fatal(err)
		}
		return tbl
	}
	{
		DiskIndex = false
		tbl := open()
		tbl.State.Seq++
		name := fmt.Sprintf("%06d.parquet", tbl.State.Seq)
		w, err := tbl.newParquetWriter(filepath.Join(tbl.Dir, "base", name), nil, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < N; i++ {
			if err := w.AppendValues([]any{key(i), "x", strconv.Itoa(i)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		tbl.State.BaseFiles, tbl.State.BaseRows = []string{name}, N
		if err := tbl.saveState(); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(1))
	probe := make([]rowKey, 200_000)
	for i := range probe {
		probe[i] = rowKey{str: key(rng.Intn(N))}
	}
	for _, disk := range []bool{false, true, true} {
		DiskIndex = disk
		runtime.GC()
		h0 := heapInUse()
		tbl := open()
		t0 := time.Now()
		if n, err := tbl.BuildIndex(); err != nil || n != N {
			t.Fatal(n, err)
		}
		took := time.Since(t0)
		runtime.GC()
		h1 := heapInUse()
		t1 := time.Now()
		for _, k := range probe {
			if _, ok := tbl.index.get(k); !ok {
				t.Fatalf("key %s not found", k)
			}
		}
		ns := float64(time.Since(t1).Nanoseconds()) / float64(len(probe))
		var size int64
		if d := tbl.index.disk; d != nil {
			st, _ := os.Stat(d.path)
			size = st.Size()
		}
		t.Logf("%d text keys, disk=%v: index at start %.2fs, Go heap %.1f MB (%.0f bytes a key), index file %.1f MB, lookup %.0f ns",
			N, disk, took.Seconds(), h1-h0, (h1-h0)*(1<<20)/float64(N), float64(size)/(1<<20), ns)
		runtime.KeepAlive(tbl)
		tbl.index.close()
	}
}

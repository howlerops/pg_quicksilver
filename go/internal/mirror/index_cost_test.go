package mirror

// What the key index costs per row, and what the split key buys.
//
// index.go's premise is that a Go map whose key and value types contain no
// pointers is invisible to the garbage collector — scanned as plain memory
// rather than walked entry by entry — and that this is why `loc` is two
// integers and why `keyIndex` keeps `nums map[int64]loc` separate from
// `strs map[string]loc` instead of keying both on `rowKey`.
//
// That reasoning was never measured, and the obvious cheaper-looking design —
// one `map[rowKey]loc`, with the string half left empty for integer-keyed
// tables — is a trap that reads as correct. Go decides scanability from the
// TYPE, not the values: `rowKey` has a string field, a string header contains a
// pointer, so a `map[rowKey]loc` is walked on every cycle for every entry
// whether or not a single one of those strings is ever set.
//
// This test exists so the split is defended by a number instead of a paragraph,
// and so that collapsing the two maps back into one shows up as a measurement
// rather than as a tidier-looking struct.
//
// The heap profile of the sidecar against the 20.5M-row mirror says this is
// where the memory is, so it is worth defending:
//
//	862.64MB 95.76%  mirror.newKeyIndex
//	 18.80MB  2.09%  mirror.forEachRowGroup
//	  8.50MB  0.94%  changestream.decodeTuple
//
//	go test ./internal/mirror/ -run TestIndexCost -v

import (
	"runtime"
	"testing"
	"time"
	"unsafe"
)

func TestIndexCost(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates several hundred megabytes")
	}

	t.Logf("sizeof(rowKey) = %d, sizeof(loc) = %d, sizeof(int64) = %d",
		unsafe.Sizeof(rowKey{}), unsafe.Sizeof(loc{}), unsafe.Sizeof(int64(0)))

	const n = 4_000_000

	// What the index would cost keyed on rowKey, the design that looks simpler.
	withStr, gcStr := measureMap(func() any {
		m := make(map[rowKey]loc, n)
		for i := 0; i < n; i++ {
			m[rowKey{num: int64(i), isNum: true}] = loc{File: 3, Pos: int32(i)}
		}
		return m
	})

	// What it costs as keyIndex actually builds it for an integer-keyed table.
	withoutStr, gcNum := measureMap(func() any {
		x := newKeyIndex(n)
		for i := 0; i < n; i++ {
			x.set(rowKey{num: int64(i), isNum: true}, loc{File: 3, Pos: int32(i)})
		}
		return x
	})

	t.Logf("%-28s %7.1f MB  %6.1f B/row   GC %v", "map[rowKey]loc",
		float64(withStr)/1e6, float64(withStr)/n, gcStr.Round(time.Millisecond))
	t.Logf("%-28s %7.1f MB  %6.1f B/row   GC %v", "keyIndex (nums, today)",
		float64(withoutStr)/1e6, float64(withoutStr)/n, gcNum.Round(time.Millisecond))

	// The memory difference is worth having; the COLLECTOR difference is the
	// one index.go is about, and it is the one that would come back as a
	// latency tail rather than as a bigger number in `kubectl top`.
	if withoutStr >= withStr {
		t.Errorf("the split key is not smaller than map[rowKey]loc "+
			"(%d vs %d bytes for %d rows) — index.go's premise no longer holds",
			withoutStr, withStr, n)
	}
	if gcNum >= gcStr {
		t.Errorf("the split key does not collect faster than map[rowKey]loc "+
			"(%v vs %v per cycle) — that is the whole reason index.go keeps "+
			"two maps instead of one", gcNum, gcStr)
	}
}

// measureMap reports the live heap a structure occupies and how long a
// collection takes while it is reachable.
//
// The GC number is the one a size comparison misses: a map with pointer data in
// its key type is WALKED, so its cost is paid on every cycle for the life of
// the process, not once at build time.
func measureMap(build func() any) (uint64, time.Duration) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	m := build()

	runtime.GC()
	runtime.ReadMemStats(&after)
	live := after.HeapAlloc - min(after.HeapAlloc, before.HeapAlloc)

	// Several collections with the structure reachable; one cycle is noise.
	start := time.Now()
	for i := 0; i < 5; i++ {
		runtime.GC()
	}
	d := time.Since(start) / 5
	runtime.KeepAlive(m)
	return live, d
}

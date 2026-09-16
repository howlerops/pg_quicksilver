package mirror

// The key -> location index, and why both halves are integers.
//
// This map holds one entry per row of the mirror, which on the benchmark shapes
// is a few million, and a profile of the deletes shape said it had become the
// dominant cost on both axes:
//
//	heap (1.09 GB live)              cpu (40 s)
//	  190 MB  the compaction swap      25%  string-keyed map hashing and probing
//	  188 MB  the compactor's index     30%  garbage collection
//	   77 MB  key strings
//
// Nothing there is Parquet, compression or decoding. It is `map[string]loc`
// with `loc{File string, Pos int}` — two pointers per entry, several million
// entries, held twice during a compaction swap, and the garbage collector
// walking every one of them on every cycle.
//
// So neither half carries a pointer any more:
//
//   - a location is (fileID, row), both integers;
//   - a key is an int64 when the table's primary key is an integer type, which
//     is most tables, and only falls back to a string otherwise.
//
// A Go map whose key and value types contain no pointers is invisible to the
// garbage collector: it is scanned as plain memory, not walked entry by entry.
// That is the whole point of this file.

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// fileID identifies a data file without a pointer.
//
// Data files are named from ONE monotonic sequence — base/000007.parquet and
// delta/000008.parquet never collide and a number is never reused — so (kind,
// seq) is already the file's identity. That makes the id a pure function of the
// name, which matters twice over: there is no interning table to grow without
// bound in a process that runs for weeks, and the background compactor can
// compute an id without touching state the apply goroutine is writing.
type fileID int32

const (
	noFile    fileID = 0 // the zero loc points nowhere, which is what we want
	kindBase         = 0
	kindDelta        = 1
)

func makeFileID(kind, seq int) fileID { return fileID(seq*2 + kind + 1) }

func (f fileID) valid() bool { return f != noFile }

func (f fileID) kind() int { return (int(f) - 1) % 2 }

func (f fileID) seq() int { return (int(f) - 1) / 2 }

// name renders the id back to a path relative to the table directory.
// Allocates, so callers keep it off per-row paths.
func (f fileID) name() string {
	if !f.valid() {
		return ""
	}
	dir := "base"
	if f.kind() == kindDelta {
		dir = "delta"
	}
	return filepath.Join(dir, fmt.Sprintf("%06d.parquet", f.seq()))
}

// stem is the file's bare name without the extension, which is how state.json
// keys its per-file maps.
func (f fileID) stem() string { return fmt.Sprintf("%06d", f.seq()) }

// parseFileID reads an id back out of a relative path.
func parseFileID(rel string) fileID {
	kind := kindBase
	switch {
	case strings.HasPrefix(rel, "base"+string(filepath.Separator)), !strings.ContainsRune(rel, filepath.Separator):
		kind = kindBase
	case strings.HasPrefix(rel, "delta"+string(filepath.Separator)):
		kind = kindDelta
	default:
		return noFile
	}
	seq, err := strconv.Atoi(trimParquet(filepath.Base(rel)))
	if err != nil {
		return noFile
	}
	return makeFileID(kind, seq)
}

func deltaID(name string) fileID { return parseFileID(filepath.Join("delta", name)) }

func baseID(name string) fileID { return parseFileID(filepath.Join("base", name)) }

// loc is where a key's current row physically lives, plus what the mirror knows
// about that row's large columns.
//
// Hash is a digest of the row's columns that are big enough to be worth not
// rewriting (see elide.go), or 0 for "unknown". Keeping it here rather than in
// a second map costs 8 bytes per row and keeps the entry pointer-free, which is
// the property this whole file exists to preserve.
type loc struct {
	File fileID
	Pos  int32
	Hash uint64
}

// sameRow compares only the location. Two locs can differ in Hash while naming
// the same row — the compaction swap asks "did this key MOVE", and answering
// that with a full struct comparison would tombstone rows that merely had their
// digest learned or forgotten.
func (a loc) sameRow(b loc) bool { return a.File == b.File && a.Pos == b.Pos }

// rowKey is a primary-key value in whichever form avoids allocating.
//
// pgoutput delivers every value as text and Parquet gives back an int64, so
// both have to land on the same key; when the column is an integer type they
// land on `num`, and the string half stays empty — which is what keeps a
// several-million-entry map free of pointers.
type rowKey struct {
	num   int64
	str   string
	isNum bool
}

// String renders the key as PostgreSQL would print it. Used for the persisted
// index (JSON keys are strings) and for diagnostics, never on a hot path.
func (k rowKey) String() string {
	if k.isNum {
		return strconv.FormatInt(k.num, 10)
	}
	return k.str
}

// keyOf converts a raw key value — text from pgoutput, an int64 from Parquet —
// into the table's key form.
func (t *Table) keyOf(v any) rowKey { return keyFor(t.numericKey, v) }

// keyFor is keyOf without a Table, so the background compactor can build keys
// from a flag captured before it started rather than reading a live field.
func keyFor(numeric bool, v any) rowKey {
	switch x := v.(type) {
	case nil:
		return rowKey{}
	case int64:
		if numeric {
			return rowKey{num: x, isNum: true}
		}
		return rowKey{str: strconv.FormatInt(x, 10)}
	case int32:
		return keyFor(numeric, int64(x))
	case int:
		return keyFor(numeric, int64(x))
	case string:
		return keyStringFor(numeric, x)
	default:
		return keyStringFor(numeric, fmt.Sprint(x))
	}
}

// keyOfString is keyOf for a value already known to be text.
//
// A numeric key column whose text will not parse falls back to the string form
// rather than erroring. That is a value PostgreSQL could not have stored in an
// integer column, so it should not arise; if it somehow does, a key that fails
// to match is visible as a divergence, whereas a panic in the apply loop takes
// the mirror down.
func (t *Table) keyOfString(s string) rowKey { return keyStringFor(t.numericKey, s) }

func keyStringFor(numeric bool, s string) rowKey {
	if numeric {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return rowKey{num: n, isNum: true}
		}
	}
	return rowKey{str: s}
}

// keyIndex maps keys to locations, with the integer keys held separately so
// that the common case is a map the garbage collector never has to walk.
type keyIndex struct {
	nums map[int64]loc
	strs map[string]loc
}

func newKeyIndex(hint int) *keyIndex {
	return &keyIndex{nums: make(map[int64]loc, hint)}
}

func (x *keyIndex) get(k rowKey) (loc, bool) {
	if k.isNum {
		l, ok := x.nums[k.num]
		return l, ok
	}
	if x.strs == nil {
		return loc{}, false
	}
	l, ok := x.strs[k.str]
	return l, ok
}

func (x *keyIndex) set(k rowKey, l loc) {
	if k.isNum {
		x.nums[k.num] = l
		return
	}
	if x.strs == nil {
		x.strs = map[string]loc{}
	}
	x.strs[k.str] = l
}

func (x *keyIndex) del(k rowKey) {
	if k.isNum {
		delete(x.nums, k.num)
		return
	}
	delete(x.strs, k.str)
}

func (x *keyIndex) len() int { return len(x.nums) + len(x.strs) }

func (x *keyIndex) each(fn func(rowKey, loc)) {
	for n, l := range x.nums {
		fn(rowKey{num: n, isNum: true}, l)
	}
	for s, l := range x.strs {
		fn(rowKey{str: s}, l)
	}
}

// deleteWhere removes every entry whose location satisfies pred. Deleting
// during a range is defined behaviour in Go, and doing it in one pass avoids
// materialising the key list of a several-million-entry map.
func (x *keyIndex) deleteWhere(pred func(loc) bool) {
	for n, l := range x.nums {
		if pred(l) {
			delete(x.nums, n)
		}
	}
	for s, l := range x.strs {
		if pred(l) {
			delete(x.strs, s)
		}
	}
}

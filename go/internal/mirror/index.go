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
	"math/bits"
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
//
// A UUID is the other key people actually use, and as text it was the worst
// case this file had: 36 bytes on the heap per key behind a pointer the
// collector follows, 190 bytes a key all told and 169 ms of every GC cycle at
// four million of them. It is 128 bits, so it is held as 128 bits — the high
// half in `num`, the low half in `lo` — and indexed in a map as pointer-free as
// the integer one.
type rowKey struct {
	num    int64
	lo     uint64
	str    string
	isNum  bool
	isUUID bool
}

// String renders the key as PostgreSQL would print it. Used for the persisted
// index (JSON keys are strings) and for diagnostics, never on a hot path.
func (k rowKey) String() string {
	if k.isNum {
		return strconv.FormatInt(k.num, 10)
	}
	if k.isUUID {
		return formatUUID(uint64(k.num), k.lo)
	}
	return k.str
}

const hexDigits = "0123456789abcdef"

// parseUUID reads a UUID in exactly the form PostgreSQL prints one: 36 bytes,
// lowercase hex, hyphens after the 8th, 12th, 16th and 20th digit.
//
// EXACTLY that form, and the strictness is what makes the compact key safe to
// use without knowing the column's type. PostgreSQL accepts braces, upper case
// and no hyphens on input and prints none of them for a `uuid` column, so a
// uuid key always parses. A `text` key can hold any of those spellings, and
// there 'A0…' and 'a0…' are DIFFERENT keys — so anything that is not already
// canonical is refused here and stays a string, and formatUUID gives back
// byte for byte what was parsed.
func parseUUID(s string) (hi, lo uint64, ok bool) {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return 0, 0, false
	}
	n := 0
	for i := 0; i < 36; i++ {
		c := s[i]
		var d uint64
		switch {
		case c >= '0' && c <= '9':
			d = uint64(c - '0')
		case c >= 'a' && c <= 'f':
			d = uint64(c-'a') + 10
		case c == '-' && (i == 8 || i == 13 || i == 18 || i == 23):
			continue
		default:
			return 0, 0, false
		}
		if n < 16 {
			hi = hi<<4 | d
		} else {
			lo = lo<<4 | d
		}
		n++
	}
	return hi, lo, true
}

func formatUUID(hi, lo uint64) string {
	var b [36]byte
	j := 0
	for i := 0; i < 32; i++ {
		if i == 8 || i == 12 || i == 16 || i == 20 {
			b[j] = '-'
			j++
		}
		w, sh := hi, uint(60-4*i)
		if i >= 16 {
			w, sh = lo, uint(60-4*(i-16))
		}
		b[j] = hexDigits[(w>>sh)&0xf]
		j++
	}
	return string(b[:])
}

// keyOf converts a raw key value — text from pgoutput, an int64 or a 16-byte
// uuid from Parquet — into the table's key form.
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
	case uuidValue:
		// A uuid from a file that stores it as 16 bytes. The same key as its
		// text, which is how it arrives from pgoutput and from an older file.
		hi, lo := x.halves()
		return rowKey{num: int64(hi), lo: lo, isUUID: true}
	case float64:
		// As PostgreSQL prints it, which is how the same key arrives from
		// pgoutput: fmt's 1e+06 is not the text of 1000000.
		return keyStringFor(numeric, pgFloat(x, 64))
	case float32:
		return keyStringFor(numeric, pgFloat(float64(x), 32))
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
	if hi, lo, ok := parseUUID(s); ok {
		return rowKey{num: int64(hi), lo: lo, isUUID: true}
	}
	return rowKey{str: s}
}

// keyIndex maps keys to locations, with the integer and UUID keys held
// separately so that the common cases are maps the garbage collector never has
// to walk. Only a key that is neither falls through to the string map.
//
// A table's keys are all of one form, so only one of the three is ever built,
// and it is built on the first set rather than in newKeyIndex: the hint is the
// table's row count, and spending it on the integer map of a table keyed by
// UUID bought several hundred megabytes of empty buckets and left the map that
// was actually filled to grow from nothing.
//
// The maps are no longer the whole index. When `disk` is set, the keys of the
// base file are in a sorted file (kidx.go) — integers, uuids, and keys of any
// other type as their exact bytes — and the maps hold only what is NOT there:
// every key whose current row is in a delta file, and a key that is not of the
// table's kind at all. The rule that keeps the two from disagreeing is one
// invariant:
//
//	a key is in a map  =>  none of its records in the file is live
//
// and "live" is one bit per base position, `gone`. Deleting a key, or moving it
// to a delta, sets the bit; nothing ever clears one, because a base position
// that stopped being a key's current row never becomes it again — the next
// rewrite makes a new file with a new index. So the answer to "where is this
// key" is the map if it is there, and otherwise the file's record for it unless
// its bit is set, which are exactly the answers the one big map gave.
//
// `gone` is seeded from the deletion vector the manifest names and thereafter
// kept by set and del themselves, rather than read from the table's dead set on
// every lookup. The two say the same thing for the base file, but markDead and
// the index are updated at different moments of an Apply (and of a swap), and
// an index whose answers depend on which of the two ran first is one every
// caller would have to be re-read against. It costs a bit per row: 1.25 MB at
// ten million.
type keyIndex struct {
	nums  map[int64]loc
	uuids map[[2]uint64]loc
	strs  map[string]loc
	hint  int

	disk *diskIndex
	gone []uint64 // one bit per base position: this row is not its key's current one
	live int      // records of the file whose bit is clear
	// hashOv holds the digest of a base row where it differs from the one in
	// the file: learned, or forgotten, since the file was written (elide.go).
	// Only a patched or elided row gets one, so it is bounded by the deltas.
	hashOv map[int32]uint64
	// deadExtras is how many rows of the base file were dead and of a key form
	// the file does not hold, so that seedGone does not count them against it.
	deadExtras int
}

func newKeyIndex(hint int) *keyIndex { return &keyIndex{hint: hint} }

// attach puts a base file's index underneath the maps. Every record starts
// live; seedGone and markGone say otherwise.
func (x *keyIndex) attach(d *diskIndex) {
	x.disk = d
	x.gone = make([]uint64, (d.rows+63)/64)
	x.live = d.n
}

// close releases the mapping. The index must not be used afterwards.
func (x *keyIndex) close() {
	if x == nil || x.disk == nil {
		return
	}
	x.disk.close()
}

func (x *keyIndex) isGone(p int32) bool {
	w := int(p) >> 6
	return w < len(x.gone) && x.gone[w]&(1<<(uint(p)&63)) != 0
}

// setGone marks a base position and reports whether it was clear.
func (x *keyIndex) setGone(p int) bool {
	w := p >> 6
	if w >= len(x.gone) {
		return false
	}
	bit := uint64(1) << (uint(p) & 63)
	if x.gone[w]&bit != 0 {
		return false
	}
	x.gone[w] |= bit
	return true
}

// seedGone applies the base file's dead set as the manifest names it.
func (x *keyIndex) seedGone(d *deadSet) {
	if x.disk == nil || d == nil {
		return
	}
	n := 0
	for w, word := range d.words {
		for word != 0 {
			if x.setGone(w<<6 | bits.TrailingZeros64(word)) {
				n++
			}
			word &= word - 1
		}
	}
	x.live -= n - x.deadExtras
	x.deadExtras = 0
}

// markGone marks positions of records in the file as not live.
func (x *keyIndex) markGone(positions []int) {
	if x.disk == nil {
		return
	}
	for _, p := range positions {
		if x.setGone(p) {
			x.live--
		}
	}
}

// diskLive finds the key's live record in the file, if it has one.
//
// A key can have more than one record — a rewrite folds in both copies of a row
// it caught mid-supersede — and at most one of them is live. If the dead set
// has not said which, the highest position is the newest, as it always was.
func (x *keyIndex) diskLive(k rowKey) (i int, pos int32, ok bool) {
	d := x.disk
	if d == nil {
		return 0, 0, false
	}
	kk, text, held := kidxKeyOf(d.kind, k)
	if !held {
		return 0, 0, false
	}
	for j := d.lowerBound(kk); j < d.n && d.keyAt(j) == kk; j++ {
		// For a key of any length kk is a hash, and the record is this key's
		// only if the key in the heap is these bytes.
		if !d.holds(j, text) {
			continue
		}
		if p := d.posAt(j); !x.isGone(p) {
			i, pos, ok = j, p, true
		}
	}
	return i, pos, ok
}

func (x *keyIndex) diskLoc(i int, pos int32) loc {
	l := loc{File: x.disk.file, Pos: pos, Hash: x.disk.hashAt(i)}
	if len(x.hashOv) > 0 {
		if h, ok := x.hashOv[pos]; ok {
			l.Hash = h
		}
	}
	return l
}

func (x *keyIndex) get(k rowKey) (loc, bool) {
	// Reading a nil map is defined and finds nothing, which is the answer.
	var l loc
	var ok bool
	switch {
	case k.isNum:
		l, ok = x.nums[k.num]
	case k.isUUID:
		l, ok = x.uuids[[2]uint64{uint64(k.num), k.lo}]
	default:
		l, ok = x.strs[k.str]
	}
	if ok || x.disk == nil {
		return l, ok
	}
	if i, pos, ok := x.diskLive(k); ok {
		return x.diskLoc(i, pos), true
	}
	return loc{}, false
}

func (x *keyIndex) set(k rowKey, l loc) {
	// A key already in a map has no live record in the file (the invariant
	// above), so there is nothing to look for there: this is every write after
	// a key's first since the last rewrite, and every digest noted on a row
	// that was just written.
	if x.disk != nil && !x.memHas(k) {
		if i, pos, ok := x.diskLive(k); ok {
			if l.File == x.disk.file && l.Pos == pos {
				// The same row, with something new known about it.
				if x.diskLoc(i, pos).Hash != l.Hash {
					if x.hashOv == nil {
						x.hashOv = map[int32]uint64{}
					}
					x.hashOv[pos] = l.Hash
				}
				return
			}
			// The key's current row is somewhere else now.
			x.setGone(int(pos))
			x.live--
			delete(x.hashOv, pos)
		}
	}
	x.memSet(k, l)
}

func (x *keyIndex) memHas(k rowKey) bool {
	var ok bool
	switch {
	case k.isNum:
		_, ok = x.nums[k.num]
	case k.isUUID:
		_, ok = x.uuids[[2]uint64{uint64(k.num), k.lo}]
	default:
		_, ok = x.strs[k.str]
	}
	return ok
}

func (x *keyIndex) memSet(k rowKey, l loc) {
	switch {
	case k.isNum:
		if x.nums == nil {
			x.nums = make(map[int64]loc, x.hint)
		}
		x.nums[k.num] = l
	case k.isUUID:
		if x.uuids == nil {
			x.uuids = make(map[[2]uint64]loc, x.hint)
		}
		x.uuids[[2]uint64{uint64(k.num), k.lo}] = l
	default:
		if x.strs == nil {
			x.strs = make(map[string]loc, x.hint)
		}
		x.strs[k.str] = l
	}
}

func (x *keyIndex) del(k rowKey) {
	inMem := x.disk != nil && x.memHas(k)
	switch {
	case k.isNum:
		delete(x.nums, k.num)
	case k.isUUID:
		delete(x.uuids, [2]uint64{uint64(k.num), k.lo})
	default:
		delete(x.strs, k.str)
	}
	if x.disk != nil && !inMem {
		if _, pos, ok := x.diskLive(k); ok {
			x.setGone(int(pos))
			x.live--
			delete(x.hashOv, pos)
		}
	}
}

func (x *keyIndex) len() int { return len(x.nums) + len(x.uuids) + len(x.strs) + x.live }

func (x *keyIndex) each(fn func(rowKey, loc)) {
	for n, l := range x.nums {
		fn(rowKey{num: n, isNum: true}, l)
	}
	for u, l := range x.uuids {
		fn(rowKey{num: int64(u[0]), lo: u[1], isUUID: true}, l)
	}
	for s, l := range x.strs {
		fn(rowKey{str: s}, l)
	}
	if d := x.disk; d != nil {
		for i := 0; i < d.n; i++ {
			if p := d.posAt(i); !x.isGone(p) {
				fn(d.rowKeyAt(i), x.diskLoc(i, p))
			}
		}
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
	for u, l := range x.uuids {
		if pred(l) {
			delete(x.uuids, u)
		}
	}
	for s, l := range x.strs {
		if pred(l) {
			delete(x.strs, s)
		}
	}
	if d := x.disk; d != nil {
		for i := 0; i < d.n; i++ {
			if p := d.posAt(i); !x.isGone(p) && pred(x.diskLoc(i, p)) {
				x.setGone(int(p))
				x.live--
				delete(x.hashOv, p)
			}
		}
	}
}

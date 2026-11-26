package mirror

// The key index of the base file, on disk.
//
// index.go made the key -> location map pointer-free, and that fixed what the
// garbage collector paid for it. It did not change what it IS: one map entry
// per live row, 56 bytes for an integer key and 84 for a UUID, in a sidecar
// whose default limit is 256Mi. At ten million UUID rows that was 1.4 GB
// resident after load, rebuilt from the key column at every start (8.5 s), and
// held TWICE for the length of a background rewrite.
//
// Nearly all of those entries describe the base file, and the base file does
// not change between rewrites. A structure that never changes does not need to
// be a hash map in the heap. So for the base file the index is a file:
//
//	index/<stem>.kidx     header, then one fixed-width record per ROW of the
//	                      base file, sorted by key:  key | position | hash
//
// mapped read-only. A lookup is a search of a small in-memory fence (the top
// eight bytes of every kidxFence-th key) and then of the half page of records
// the fence names, so a cold lookup is one page fault and a warm one reads two
// or three cache lines. Keys in delta files stay in the Go maps of index.go,
// which are bounded by the compaction trigger and not by the table.
//
// THE FILE IS A FUNCTION OF THE BASE FILE AND NOTHING ELSE. Every physical row
// has a record — dead ones, and both copies of a key a rewrite folded in twice.
// Which of them is live is not the file's business: the keyIndex holding it
// keeps one bit per base position ("gone"), seeded from the deletion vector the
// manifest names and set whenever a key is deleted or moves to a delta. That is
// what lets the file be rebuilt from the key column at any time and come out
// byte for byte the same, and it is why it is safe for it not to be named by
// the manifest: it is derived data, and it says so in its header.
//
// IT IS NEVER TRUSTED ON ITS NAME. The header binds it to the base file it
// indexes — sequence number, size, modification time, row count — and carries a
// checksum of the records and of itself; the load also checks the records are
// in order. A file that fails any of that is removed and rebuilt from the key
// column, which is exactly what happened at every start before this existed.
// A base file's name CAN be reused with different content (a rewrite that died
// before its manifest was written leaves State.Seq where it was), which is why
// the name alone binds nothing, and why every writer of a base file removes
// the index of that name before it starts.
//
// WHAT IT DOES NOT COVER. Fixed-width keys only: an integer primary key
// (8 bytes) or a UUID (16). A table keyed by any other text keeps the in-memory
// maps exactly as before, and a table that mixes UUID-shaped and other text
// keys holds the odd ones in memory and rebuilds its file at every start.
// A table with more than one base file (nothing writes one today) also keeps
// the old path. QS_DISK_INDEX=0 turns all of this off.

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/parquet/file"
)

// DiskIndex is whether a fixed-width-keyed base file is indexed on disk rather
// than in a Go map. Off is the previous behaviour, kept so the two can be
// measured against each other.
var DiskIndex = os.Getenv("QS_DISK_INDEX") != "0"

const (
	kidxMagic  = "QSKIDX01"
	kidxHeader = 72
	// kidxFence is how many records one in-memory fence key stands for: eight
	// bytes of heap per 64 rows, 1.25 MB at ten million, and a search under one
	// fence stays inside half a page of the file.
	kidxFence = 64
)

// kidxRunRecords is how many records the builder sorts in memory before it
// spills a run to disk: 32 bytes each, so 32 MB. A variable so a test can make
// a small table take the merge path.
var kidxRunRecords = 1 << 20

// kidxRebuilds counts index files that were missing, stale or corrupt and were
// rebuilt from the base file. Tests read it to prove a bad file was detected.
var kidxRebuilds atomic.Int64

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// kkey is a fixed-width key in the form the file sorts by.
type kkey struct{ hi, lo uint64 }

func (a kkey) less(b kkey) bool { return a.hi < b.hi || (a.hi == b.hi && a.lo < b.lo) }

// krec is one record while it is being sorted.
type krec struct {
	k    kkey
	hash uint64
	pos  uint32
}

func krecCmp(a, b krec) int {
	switch {
	case a.k.hi != b.k.hi:
		if a.k.hi < b.k.hi {
			return -1
		}
		return 1
	case a.k.lo != b.k.lo:
		if a.k.lo < b.k.lo {
			return -1
		}
		return 1
	case a.pos != b.pos:
		if a.pos < b.pos {
			return -1
		}
		return 1
	}
	return 0
}

// kidxWidth is the key width a rowKey would have on disk, or 0 if it has none.
func kidxWidth(k rowKey) int {
	switch {
	case k.isNum:
		return 8
	case k.isUUID:
		return 16
	}
	return 0
}

// toKKey maps a key to its sort form. An integer has its sign bit flipped so
// that unsigned byte order is numeric order.
func toKKey(k rowKey) kkey {
	if k.isNum {
		return kkey{hi: uint64(k.num) ^ (1 << 63)}
	}
	return kkey{hi: uint64(k.num), lo: k.lo}
}

func fromKKey(width int, k kkey) rowKey {
	if width == 8 {
		return rowKey{num: int64(k.hi ^ (1 << 63)), isNum: true}
	}
	return rowKey{num: int64(k.hi), lo: k.lo, isUUID: true}
}

// kidxBind is what ties an index file to one base file.
type kidxBind struct {
	seq   uint64 // the base file's sequence number, i.e. its name
	size  uint64
	mtime int64
	rows  uint64 // physical rows in the base file
}

func kidxBindFor(parquetPath string, id fileID, rows uint64) (kidxBind, error) {
	st, err := os.Stat(parquetPath)
	if err != nil {
		return kidxBind{}, err
	}
	return kidxBind{
		seq: uint64(id.seq()), size: uint64(st.Size()),
		mtime: st.ModTime().UnixNano(), rows: rows,
	}, nil
}

type kidxHdr struct {
	width   int
	records uint64
	extras  uint64
	bind    kidxBind
	crc     uint32
}

func (h kidxHdr) encode() []byte {
	b := make([]byte, kidxHeader)
	copy(b, kidxMagic)
	le := binary.LittleEndian
	le.PutUint32(b[8:], uint32(h.width))
	le.PutUint64(b[16:], h.records)
	le.PutUint64(b[24:], h.bind.rows)
	le.PutUint64(b[32:], h.extras)
	le.PutUint64(b[40:], h.bind.seq)
	le.PutUint64(b[48:], h.bind.size)
	le.PutUint64(b[56:], uint64(h.bind.mtime))
	le.PutUint32(b[64:], h.crc)
	le.PutUint32(b[68:], crc32.Checksum(b[:68], crcTable))
	return b
}

func decodeKidxHdr(b []byte) (kidxHdr, error) {
	if len(b) < kidxHeader || string(b[:8]) != kidxMagic {
		return kidxHdr{}, errors.New("kidx: not an index file")
	}
	le := binary.LittleEndian
	if le.Uint32(b[68:]) != crc32.Checksum(b[:68], crcTable) {
		return kidxHdr{}, errors.New("kidx: header checksum")
	}
	h := kidxHdr{
		width:   int(le.Uint32(b[8:])),
		records: le.Uint64(b[16:]),
		extras:  le.Uint64(b[32:]),
		crc:     le.Uint32(b[64:]),
	}
	h.bind = kidxBind{
		rows: le.Uint64(b[24:]), seq: le.Uint64(b[40:]),
		size: le.Uint64(b[48:]), mtime: int64(le.Uint64(b[56:])),
	}
	if h.width != 8 && h.width != 16 {
		return kidxHdr{}, errors.New("kidx: key width")
	}
	return h, nil
}

func putRec(b []byte, width int, r krec) {
	binary.BigEndian.PutUint64(b, r.k.hi)
	o := 8
	if width == 16 {
		binary.BigEndian.PutUint64(b[8:], r.k.lo)
		o = 16
	}
	binary.LittleEndian.PutUint32(b[o:], r.pos)
	binary.LittleEndian.PutUint64(b[o+4:], r.hash)
}

func getRec(b []byte, width int) (r krec) {
	r.k.hi = binary.BigEndian.Uint64(b)
	o := 8
	if width == 16 {
		r.k.lo = binary.BigEndian.Uint64(b[8:])
		o = 16
	}
	r.pos = binary.LittleEndian.Uint32(b[o:])
	r.hash = binary.LittleEndian.Uint64(b[o+4:])
	return r
}

// kidxScan is the one pass every record makes, whether it is being written or
// being checked: it insists on the order, collects the fence, and notices a key
// that appears twice.
type kidxScan struct {
	n      int
	prev   krec
	fence  []uint64
	dups   []int // the lower position of each pair of equal keys
	maxPos uint32
}

func (s *kidxScan) note(r krec) error {
	if s.n > 0 {
		if c := krecCmp(s.prev, r); c >= 0 {
			return errors.New("kidx: records out of order")
		}
		if s.prev.k == r.k {
			s.dups = append(s.dups, int(s.prev.pos))
		}
	}
	if s.n%kidxFence == 0 {
		s.fence = append(s.fence, r.k.hi)
	}
	if r.pos > s.maxPos {
		s.maxPos = r.pos
	}
	s.prev = r
	s.n++
	return nil
}

// diskIndex is one index file, mapped.
type diskIndex struct {
	file  fileID // the base file it indexes
	path  string
	width int
	rec   int // bytes per record
	n     int
	rows  int // physical rows in the base file
	data  []byte
	recs  []byte
	fence []uint64 // the top half of every kidxFence-th key
	unmap func()
	// noHash says the digests in the file are not to be believed: the index
	// was loaded by a later process than the one that wrote it. What a digest
	// was cleared for since (a patch that left a large column unknown) lived
	// only in memory, and a digest written by another version of largeHash
	// compares equal to nothing true. A rebuilt index knew no digests either,
	// so this is what a restart always meant.
	noHash bool
	memo   []kidxMemo
}

// kidxMemo is one remembered lowerBound; see there.
type kidxMemo struct {
	k  kkey
	i  int32
	ok bool
}

const kidxMemoBits = 14

func (d *diskIndex) close() {
	if d == nil || d.unmap == nil {
		return
	}
	d.unmap()
	d.unmap, d.data, d.recs, d.n = nil, nil, nil, 0
}

func (d *diskIndex) keyAt(i int) kkey {
	o := i * d.rec
	k := kkey{hi: binary.BigEndian.Uint64(d.recs[o:])}
	if d.width == 16 {
		k.lo = binary.BigEndian.Uint64(d.recs[o+8:])
	}
	return k
}

func (d *diskIndex) posAt(i int) int32 {
	return int32(binary.LittleEndian.Uint32(d.recs[i*d.rec+d.width:]))
}

func (d *diskIndex) hashAt(i int) uint64 {
	if d.noHash {
		return 0
	}
	return binary.LittleEndian.Uint64(d.recs[i*d.rec+d.width+4:])
}

// lowerBound is the index of the first record whose key is not below k.
//
// Measured at four million UUIDs, a plain binary search of the fence and then
// of the block came to 640-940 ns a lookup against the map's 340-660, and an
// update asks more than once per row. Two things bring it back:
//
//   - The fence, and then the block under it, is entered by INTERPOLATION.
//     Keys that are spread evenly — a random UUID, a serial integer — sit
//     within a few entries of where their value says they should, so the
//     search starts there and gallops outward; on keys that are not spread
//     evenly the gallop is a binary search that started in the wrong place,
//     which costs a few steps and no more.
//   - The answer is REMEMBERED. Apply asks where a key is, and a moment later
//     tells the index where it went; the second question is the first one
//     again. The file never changes, so the first record not below a key is a
//     fact for as long as the mapping lives, and a small direct-mapped table of
//     the last few thousand answers is always right. (Which record is LIVE is
//     not cached: that is the gone bitmap's to say, each time.)
func (d *diskIndex) lowerBound(k kkey) int {
	slot := &d.memo[((k.hi^k.lo)*0x9e3779b97f4a7c15)>>(64-kidxMemoBits)]
	if slot.ok && slot.k == k {
		return int(slot.i)
	}
	i := d.search(k)
	*slot = kidxMemo{k: k, i: int32(i), ok: true}
	return i
}

func (d *diskIndex) search(k kkey) int {
	// The fence holds the top 64 bits of every kidxFence-th key. L is the first
	// fence not below k's top half and U the first one above it — the same
	// entry or the next, unless many keys share a top half. The record wanted
	// is after fence L-1, whose key is below k, and no later than fence U,
	// whose key is above it.
	f, nf := d.fence, len(d.fence)
	lo, hi := 0, nf
	if nf > 8 {
		first, last := f[0], f[nf-1]
		g := 0
		switch {
		case k.hi <= first || last == first:
		case k.hi >= last:
			g = nf - 1
		default:
			g = int(float64(k.hi-first) / float64(last-first) * float64(nf-1))
			g = max(0, min(g, nf-1))
		}
		if f[g] < k.hi {
			// Somewhere to the right: widen until a fence is not below k.
			lo = g + 1
			for step := 1; g+step < nf; step *= 2 {
				if f[g+step] >= k.hi {
					hi = g + step
					break
				}
				lo = g + step + 1
			}
		} else {
			hi = g
			for step := 1; g-step >= 0; step *= 2 {
				if f[g-step] < k.hi {
					lo = g - step + 1
					break
				}
				hi = g - step
			}
		}
	}
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if f[m] < k.hi {
			lo = m + 1
		} else {
			hi = m
		}
	}
	u := lo
	for u < nf && f[u] == k.hi {
		u++
	}
	a, b := 0, min(u*kidxFence, d.n)
	if lo > 0 {
		a = (lo-1)*kidxFence + 1
	}
	if a >= b {
		return a
	}

	// The same again inside the block: guess from the value, then gallop. The
	// guess is from the fences either side, so it costs no read of the file.
	if lo > 0 && u < nf && f[u] > f[lo-1] && b-a > 8 {
		g := a + int(float64(k.hi-f[lo-1])/float64(f[u]-f[lo-1])*float64(b-a))
		g = max(a, min(g, b-1))
		if d.keyAt(g).less(k) {
			a = g + 1
			for step := 1; g+step < b; step *= 2 {
				if !d.keyAt(g + step).less(k) {
					b = g + step
					break
				}
				a = g + step + 1
			}
		} else {
			b = g
			for step := 1; g-step >= a; step *= 2 {
				if d.keyAt(g - step).less(k) {
					a = g - step + 1
					break
				}
				b = g - step
			}
		}
	}
	for a < b {
		m := int(uint(a+b) >> 1)
		if d.keyAt(m).less(k) {
			a = m + 1
		} else {
			b = m
		}
	}
	return a
}

// openKidx maps the index of one base file if, and only if, it is the index of
// THAT file and is intact. Any error means "rebuild".
func openKidx(path, parquetPath string, id fileID, width int) (*diskIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	hb := make([]byte, kidxHeader)
	if _, err := io.ReadFull(f, hb); err != nil {
		return nil, errors.New("kidx: short header")
	}
	h, err := decodeKidxHdr(hb)
	if err != nil {
		return nil, err
	}
	if h.width != width {
		return nil, errors.New("kidx: key width is not this table's")
	}
	if h.extras != 0 {
		return nil, errors.New("kidx: not every key of the base file is in it")
	}
	rows, err := parquetRows(parquetPath)
	if err != nil {
		return nil, err
	}
	want, err := kidxBindFor(parquetPath, id, uint64(rows))
	if err != nil {
		return nil, err
	}
	if h.bind != want || h.records != want.rows {
		return nil, errors.New("kidx: describes a different base file")
	}
	rec := width + 12
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if uint64(st.Size()) != kidxHeader+h.records*uint64(rec) {
		return nil, errors.New("kidx: wrong length")
	}

	// One sequential pass, by read and not through the mapping: it checks the
	// checksum and the order and collects the fence, and it leaves the pages in
	// the page cache without leaving them mapped into this process.
	var scan kidxScan
	scan.fence = make([]uint64, 0, int(h.records)/kidxFence+1)
	crc := uint32(0)
	buf := make([]byte, rec*(1<<15))
	for left := int(h.records); left > 0; {
		n := min(left, len(buf)/rec)
		if _, err := io.ReadFull(f, buf[:n*rec]); err != nil {
			return nil, errors.New("kidx: short read")
		}
		crc = crc32.Update(crc, crcTable, buf[:n*rec])
		for i := 0; i < n; i++ {
			if err := scan.note(getRec(buf[i*rec:], width)); err != nil {
				return nil, err
			}
		}
		left -= n
	}
	if crc != h.crc {
		return nil, errors.New("kidx: checksum")
	}
	if h.records > 0 && uint64(scan.maxPos) >= h.bind.rows {
		return nil, errors.New("kidx: position beyond the base file")
	}
	return mapKidx(path, id, width, int(h.records), int(h.bind.rows), scan.fence)
}

func mapKidx(path string, id fileID, width, n, rows int, fence []uint64) (*diskIndex, error) {
	d := &diskIndex{file: id, path: path, width: width, rec: width + 12, n: n, rows: rows, fence: fence}
	d.memo = make([]kidxMemo, 1<<kidxMemoBits)
	if n == 0 {
		d.unmap = func() {}
		return d, nil
	}
	data, unmap, err := mapFile(path, kidxHeader+n*d.rec)
	if err != nil {
		return nil, err
	}
	d.data, d.recs, d.unmap = data, data[kidxHeader:], unmap
	// Every swap site closes the index it replaces. This is for the one that
	// forgets: a mapping of an unlinked file holds its blocks until it goes.
	runtime.SetFinalizer(d, (*diskIndex).close)
	return d, nil
}

func parquetRows(path string) (int64, error) {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return 0, err
	}
	defer rdr.Close()
	return rdr.NumRows(), nil
}

// kidxPath is where the index of a base file lives.
func (t *Table) kidxPath(baseName string) string {
	return filepath.Join(t.Dir, "index", trimParquet(filepath.Base(baseName))+".kidx")
}

// dropKidx removes the index of a base file and anything a build of it left.
func (t *Table) dropKidx(baseName string) {
	p := t.kidxPath(baseName)
	_ = os.Remove(p)
	if m, _ := filepath.Glob(p + ".*"); len(m) > 0 {
		for _, f := range m {
			_ = os.Remove(f)
		}
	}
}

// kidxBuilder turns "this key is at this position", said once per row of a base
// file in the order the rows were written, into an index file — without ever
// holding the keys as map entries. Records are sorted a run at a time in one
// pointer-free slice and the runs merged, so its memory is the run, not the
// table.
//
// A key that has no fixed width (see kidxWidth) cannot go in the file and is
// held in an ordinary keyIndex instead — which, for a table keyed by text, is
// all of them, and is exactly what there was before.
type kidxBuilder struct {
	t       *Table
	name    string // base file name
	id      fileID
	width   int // 0: build in memory only
	path    string
	recs    []krec
	runs    []string
	records int
	rows    int
	// extras are the keys held in memory. keepExtras false means the caller
	// only wants the file and will rebuild if there turn out to be any.
	extras     *keyIndex
	keepExtras bool
	nExtras    int
	deadExtras int
	extraDups  []int
	hint       int
}

// newKidxBuilder starts an index for the base file `name`, which the caller is
// about to write or is reading. Any index already under that name is removed
// first: it describes a file that is being replaced.
func (t *Table) newKidxBuilder(name string, hint int, keepExtras bool) *kidxBuilder {
	b := &kidxBuilder{t: t, name: name, id: baseID(name), keepExtras: keepExtras, hint: hint}
	t.dropKidx(name)
	if DiskIndex {
		b.width = 16
		if t.numericKey {
			b.width = 8
		}
		b.path = t.kidxPath(name)
	}
	if keepExtras {
		b.extras = newKeyIndex(0)
	}
	return b
}

// add records one row of the base file. dead says the row is already known not
// to be live, which matters only for a key held in memory: a record in the file
// is written regardless, and the caller's dead set decides.
func (b *kidxBuilder) add(k rowKey, pos int32, hash uint64, dead bool) error {
	b.rows++
	if b.width == 0 || kidxWidth(k) != b.width {
		b.nExtras++
		if dead {
			b.deadExtras++
			return nil
		}
		if b.keepExtras {
			if b.width == 0 && b.extras.hint == 0 {
				b.extras.hint = b.hint
			}
			if prev, ok := b.extras.get(k); ok {
				b.extraDups = append(b.extraDups, int(prev.Pos))
			}
			b.extras.set(k, loc{File: b.id, Pos: pos, Hash: hash})
		}
		return nil
	}
	if b.recs == nil {
		b.recs = make([]krec, 0, min(max(b.hint, 1024), kidxRunRecords))
	}
	b.recs = append(b.recs, krec{k: toKKey(k), hash: hash, pos: uint32(pos)})
	b.records++
	if len(b.recs) >= kidxRunRecords {
		return b.spill()
	}
	return nil
}

func (b *kidxBuilder) spill() error {
	slices.SortFunc(b.recs, krecCmp)
	if err := os.MkdirAll(filepath.Dir(b.path), 0o755); err != nil {
		return err
	}
	p := fmt.Sprintf("%s.run%d", b.path, len(b.runs))
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	b.runs = append(b.runs, p)
	w := bufio.NewWriterSize(f, 1<<20)
	rec := b.width + 12
	var buf [28]byte
	for _, r := range b.recs {
		putRec(buf[:], b.width, r)
		if _, err := w.Write(buf[:rec]); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	b.recs = b.recs[:0]
	return f.Close()
}

func (b *kidxBuilder) abort() {
	b.recs = nil
	if b.path != "" {
		b.t.dropKidx(b.name)
	}
}

// runReader is one sorted run being merged.
type runReader struct {
	r    *bufio.Reader
	f    *os.File
	cur  krec
	left int
}

type runHeap []*runReader

func (h runHeap) Len() int           { return len(h) }
func (h runHeap) Less(i, j int) bool { return krecCmp(h[i].cur, h[j].cur) < 0 }
func (h runHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)        { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// finish writes the file and returns the index over it.
//
// dups is the position of every row whose key appears again at a HIGHER
// position. For a rewrite, whose rows are written oldest source first, those
// are the copies superseded within the rewrite itself; the caller marks them
// dead. Nothing here marks them: the file records every row, and only a dead
// set says which is live.
//
// The base file must be complete on disk by now — the header binds to its size.
func (b *kidxBuilder) finish() (x *keyIndex, recDups, extraDups []int, err error) {
	x = b.extras
	if x == nil {
		x = newKeyIndex(0)
	}
	x.deadExtras = b.deadExtras
	if b.width == 0 || b.records == 0 || (!b.keepExtras && b.nExtras > 0) {
		b.abort()
		return x, nil, b.extraDups, nil
	}
	defer func() {
		if err != nil {
			b.abort()
		}
	}()
	bind, err := kidxBindFor(filepath.Join(b.t.Dir, "base", b.name), b.id, uint64(b.rows))
	if err != nil {
		return nil, nil, nil, err
	}
	if err := os.MkdirAll(filepath.Dir(b.path), 0o755); err != nil {
		return nil, nil, nil, err
	}
	// A name of its own, so that two builds of one index — which nothing does on
	// purpose — cannot write through each other into a file that is then mapped
	// without being read back.
	f, err := os.CreateTemp(filepath.Dir(b.path), filepath.Base(b.path)+".tmp*")
	if err != nil {
		return nil, nil, nil, err
	}
	tmp := f.Name()
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err := w.Write(make([]byte, kidxHeader)); err != nil {
		return nil, nil, nil, err
	}
	rec := b.width + 12
	var scan kidxScan
	scan.fence = make([]uint64, 0, b.records/kidxFence+1)
	crc := uint32(0)
	var buf [28]byte
	emit := func(r krec) error {
		if err := scan.note(r); err != nil {
			return err
		}
		putRec(buf[:], b.width, r)
		crc = crc32.Update(crc, crcTable, buf[:rec])
		_, err := w.Write(buf[:rec])
		return err
	}

	slices.SortFunc(b.recs, krecCmp)
	if len(b.runs) == 0 {
		for _, r := range b.recs {
			if err := emit(r); err != nil {
				return nil, nil, nil, err
			}
		}
	} else {
		// The tail still in memory is merged as it is, without a file.
		tail, ti := b.recs, 0
		h := &runHeap{}
		for _, p := range b.runs {
			rf, err := os.Open(p)
			if err != nil {
				return nil, nil, nil, err
			}
			defer rf.Close()
			rr := &runReader{r: bufio.NewReaderSize(rf, 1<<18), f: rf, left: kidxRunRecords}
			if _, err := io.ReadFull(rr.r, buf[:rec]); err != nil {
				return nil, nil, nil, err
			}
			rr.cur = getRec(buf[:], b.width)
			rr.left--
			*h = append(*h, rr)
		}
		heap.Init(h)
		var rb [28]byte
		for h.Len() > 0 || ti < len(tail) {
			if h.Len() == 0 || (ti < len(tail) && krecCmp(tail[ti], (*h)[0].cur) < 0) {
				if err := emit(tail[ti]); err != nil {
					return nil, nil, nil, err
				}
				ti++
				continue
			}
			rr := (*h)[0]
			if err := emit(rr.cur); err != nil {
				return nil, nil, nil, err
			}
			if rr.left == 0 {
				heap.Pop(h)
				continue
			}
			if _, err := io.ReadFull(rr.r, rb[:rec]); err != nil {
				return nil, nil, nil, err
			}
			rr.cur = getRec(rb[:], b.width)
			rr.left--
			heap.Fix(h, 0)
		}
	}
	b.recs = nil
	if err := w.Flush(); err != nil {
		return nil, nil, nil, err
	}
	hdr := kidxHdr{width: b.width, records: uint64(b.records), extras: uint64(b.nExtras), bind: bind, crc: crc}
	if _, err := f.WriteAt(hdr.encode(), 0); err != nil {
		return nil, nil, nil, err
	}
	// Not fsynced. It is derived data with a checksum: a file torn by a crash
	// fails the check and is rebuilt, which costs what a start cost before.
	if err := f.Close(); err != nil {
		return nil, nil, nil, err
	}
	if err := os.Rename(tmp, b.path); err != nil {
		return nil, nil, nil, err
	}
	for _, p := range b.runs {
		_ = os.Remove(p)
	}
	d, err := mapKidx(b.path, b.id, b.width, b.records, b.rows, scan.fence)
	if err != nil {
		return nil, nil, nil, err
	}
	x.attach(d)
	return x, scan.dups, b.extraDups, nil
}

// baseIndex returns the index of the table's one base file: the file on disk if
// it is there and is this base file's, and otherwise one rebuilt from the key
// column, which is what every start used to do.
func (t *Table) baseIndex(base string) (*keyIndex, error) {
	rel := filepath.Join("base", base)
	id := baseID(base)
	dead, err := t.deadSetFor(rel)
	if err != nil {
		return nil, err
	}
	width := 16
	if t.numericKey {
		width = 8
	}
	parquetPath := filepath.Join(t.Dir, rel)
	if DiskIndex {
		if d, err := openKidx(t.kidxPath(base), parquetPath, id, width); err == nil {
			d.noHash = true
			x := newKeyIndex(0)
			x.attach(d)
			x.seedGone(dead)
			return x, nil
		}
		kidxRebuilds.Add(1)
	}
	// ONE COLUMN, not all of them: the key and the row's position are the
	// entire content of the index, and this is a column store.
	b := t.newKidxBuilder(base, t.State.BaseRows, true)
	if err := t.forEachRowGroup(parquetPath, nil, []string{t.Key},
		func(rows []map[string]any, first int) error {
			for i, r := range rows {
				p := first + i
				if err := b.add(t.keyOf(r[t.Key]), int32(p), 0, dead.has(p)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		b.abort()
		return nil, err
	}
	x, _, _, err := b.finish()
	if err != nil {
		return nil, err
	}
	x.seedGone(dead)
	return x, nil
}

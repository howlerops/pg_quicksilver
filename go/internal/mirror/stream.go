package mirror

// Streaming Parquet I/O.
//
// A compaction used to read every live row into a []map[string]any before
// writing a byte. On the narrow shape that is four million Go maps, and it is
// where the 4-7 GB of resident memory in docs/19 came from. The cost is not
// only the memory: allocating and then collecting millions of maps is
// garbage-collector work, and a GC pause stops the apply goroutine as surely as
// it stops the compactor — which makes it a candidate for the multi-second p99
// that survived making the swap O(changed).
//
// Parquet's unit of decode is a row group, and the writer already emits one at
// a time. These two types make the reader match it, so a rewrite of any table
// holds one row group rather than one table.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// parquetWriter accepts rows in any-sized runs and emits fixed-size row groups.
//
// The sizing matters beyond tidiness: a row group is the smallest unit a later
// point read can decode, so a file made of whatever chunks the caller happened
// to hand over would make carry-forward's cost depend on the shape of an
// unrelated compaction.
type parquetWriter struct {
	w      *pqarrow.FileWriter
	schema *arrow.Schema
	cols   []string
	// tbl is held for one reason: the value check has to live where the
	// write happens. It used to sit in writeParquetCols, which is one of TWO
	// paths into this writer — the snapshot builds its own writer and appends
	// directly, so bootstrap never checked at all. A table carrying a BC date
	// would have been loaded with that column silently NULL, which is the
	// exact failure docs/23 added the halt to prevent, on the path where an
	// old date is most likely to live.
	tbl *Table
	// pending holds rows in COLUMN ORDER rather than as maps. The apply path
	// converts once on the way in; the snapshot, whose rows arrive from a scan
	// already in order, never builds a map at all.
	pending [][]any
	// f is the file underneath w. The writer keeps it, rather than letting
	// pqarrow close it, because a file is not finished when its footer is
	// written: it is finished when fsync has returned. See Close.
	f *os.File
	// groups counts the row groups written, and nanChunks names the float
	// column chunks among them that hold a NaN. See dropNaNStatistics.
	groups    int
	floatCols []int
	nanChunks [][2]int
}

// ---- making a write durable ------------------------------------------------
//
// After Apply returns, the caller confirms the LSN to PostgreSQL, and from then
// on the source will never resend what that batch held. So by the time the
// manifest naming a file is written, the file has to survive power loss — its
// bytes AND its name. Those are two separate promises on a POSIX filesystem:
// fsync on the file makes the contents durable, and only fsync on the
// DIRECTORY makes the entry that names it durable. A manifest that survives a
// crash while naming a zero-length file, or no file at all, is a mirror that
// cannot be read and cannot be replayed.
//
// Neither was happening. Parquet files were closed and never synced, and the
// three atomic writers reopened their temp file read-only, synced that, and
// threw the error away.
//
// The cost is paid once per file, on the descriptor that wrote it, and once
// per touched directory per MANIFEST WRITE rather than once per file created
// in it: a writer records that its directory is owed a sync (owesDirSync) and
// saveState pays every debt under the table immediately before it writes the
// manifest. An Apply whose updates land on rows in seventeen files publishes
// seventeen deletion vectors into one directory, and that is one directory
// sync, not seventeen.
//
// It is not free. An Apply of a small batch (BenchmarkApplySync: one delta
// file, one vector, the manifest) made 2 fsyncs before this and makes 6 now —
// the delta file, which was not synced at all, and the three directories — and
// on a shared ext4 disk where an fsync costs about 2 ms that took the Apply
// from roughly 8 ms to roughly 15.5 ms, old and new run alternately. Six is
// the floor for that layout: three files, in three directories.

// syncHook, when set, is told about every fsync before it happens and can
// refuse it. It exists for tests: "was this file synced before the manifest
// named it" cannot be observed from outside the process, and "what does the
// caller do when fsync fails" cannot be provoked.
var syncHook atomic.Pointer[func(path string, dir bool) error]

// syncFile makes the contents of an open file durable.
func syncFile(f *os.File) error {
	if h := syncHook.Load(); h != nil {
		if err := (*h)(f.Name(), false); err != nil {
			return err
		}
	}
	return f.Sync()
}

// syncDir makes the entries of a directory durable: files created in it,
// renamed into it, or removed from it since the last call.
func syncDir(dir string) error {
	if h := syncHook.Load(); h != nil {
		if err := (*h)(dir, true); err != nil {
			return err
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// dirsOwed is every directory that has gained an entry no manifest has yet
// been written after. It is package state rather than a field on Table because
// two of the writers have no Table to hang it on: writeDV is a function of a
// path, and a background rewrite must not write to the Table the apply loop
// owns. Keyed by path, so tables do not see each other's.
var dirsOwed = struct {
	sync.Mutex
	m map[string]bool
}{m: map[string]bool{}}

// owesDirSync records that dir has a new entry which is not durable until
// syncOwedDirs has run for the table it belongs to.
func owesDirSync(dir string) {
	dirsOwed.Lock()
	dirsOwed.m[dir] = true
	dirsOwed.Unlock()
}

// syncOwedDirs fsyncs every directory at or under root that is owed one. A
// directory whose sync fails stays owed.
func syncOwedDirs(root string) error {
	var due []string
	dirsOwed.Lock()
	for dir := range dirsOwed.m {
		if dir == root || strings.HasPrefix(dir, root+string(filepath.Separator)) {
			due = append(due, dir)
			delete(dirsOwed.m, dir)
		}
	}
	dirsOwed.Unlock()
	for i, dir := range due {
		if err := syncDir(dir); err != nil {
			for _, d := range due[i:] {
				owesDirSync(d)
			}
			return err
		}
	}
	return nil
}

// writeFileAtomic replaces path with data so that a reader — and a crash —
// sees either the old contents or the new, never a prefix: temp file, fsync,
// rename. The caller owes the directory a syncDir before relying on the name.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = syncFile(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// keepOpen hides a file's Close from pqarrow, whose FileWriter closes the sink
// it is handed — which would leave nothing to fsync.
type keepOpen struct{ io.Writer }

// ZstdLevel is the compression level every Parquet file is written at.
//
// One is not a compromise on this data; it is the point where the curve stops
// paying. Same 20,000-row jsonb batch, all four settings measured, write and
// read (TestCodecCost, docs/25):
//
//	snappy         61.4 MB   write  529 ms   read 197 ms
//	zstd level 1   33.8 MB   write  258 ms   read 190 ms
//	zstd default   33.4 MB   write  712 ms   read 222 ms
//	zstd level 6   32.7 MB   write 1369 ms   read 182 ms
//
// Level 1 writes 2.8x faster than the default for 1.2% more bytes. Everything
// above it buys a rounding error in size for multiples of the CPU, and the
// bootstrap profile put 41% of the sidecar inside zstd's encoder.
var ZstdLevel = int(envInt("QS_ZSTD_LEVEL", 1))

// SnappyDeltas restores the delta codec this used to use, for A/B only.
//
// The old reasoning was that delta files are written constantly and read until
// the next compaction folds them away, so they should get the codec that
// DEcompresses fastest. The reasoning was sound and the premise was not:
// measured, zstd level 1 beats Snappy on all three axes at once — 45% smaller,
// 51% faster to write, and no slower to read, because Snappy's file is 1.8x
// bigger and those bytes have to be fetched and touched too.
var SnappyDeltas = os.Getenv("QS_SNAPPY_DELTAS") == "1"

// durable distinguishes a base file from a delta, and despite its name selects
// only the codec: SnappyDeltas needs to know which is which. It does NOT decide
// whether the file is fsynced. Every file this writer produces is synced when
// it is closed, because a delta is named by the manifest exactly as a base
// file is.
func (t *Table) newParquetWriter(path string, cols []string, durable bool) (*parquetWriter, error) {
	return t.newParquetWriterFor(path, cols, durable, 0)
}

// newParquetWriterFor is newParquetWriter for a caller that knows how many
// rows it is about to write (0: it does not). The count sizes the bloom
// filters and nothing else.
func (t *Table) newParquetWriterFor(path string, cols []string, durable bool, rows int) (*parquetWriter, error) {
	if cols == nil {
		cols = t.Order
	}
	schema := t.arrowSchemaFor(cols)

	// A detached table is a rewrite's private copy; BeginCompaction claimed
	// its output for it.
	if !t.detached {
		t.claim(path)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	codec := compress.Codecs.Zstd
	if SnappyDeltas && !durable {
		codec = compress.Codecs.Snappy
	}
	opts := []parquet.WriterProperty{
		parquet.WithCompression(codec),
		parquet.WithCompressionLevel(ZstdLevel),
		parquet.WithMaxRowGroupLength(RowGroupRows),
	}
	// Bloom filters on the key and on uuid columns (types.go, BloomColumns),
	// sized by what a chunk holds rather than by the worst case, because most
	// files this writes are small: a delta of 40 rows must not carry a filter
	// built for 65,536.
	//
	// A delta's row count is known before it is opened, and its filter is
	// sized for exactly that many values — 32 bytes for a handful. A snapshot
	// or a rewrite streams, so it uses the ADAPTIVE builder: a ladder of sizes
	// from one full row group's worth down, of which the smallest that still
	// meets the false-positive rate is written. The ladder's bottom rung is
	// about 2 KB, which is why the known count is used where there is one.
	if bloom := t.bloomColumnsFor(cols); len(bloom) > 0 {
		opts = append(opts, parquet.WithMaxBloomFilterBytes(bloomMaxBytes()))
		for _, c := range bloom {
			opts = append(opts,
				parquet.WithBloomFilterEnabledFor(c, true),
				parquet.WithBloomFilterFPPFor(c, bloomFPP))
			if rows > 0 && int64(rows) < RowGroupRows {
				opts = append(opts, parquet.WithBloomFilterNDVFor(c, int64(rows)))
			} else {
				opts = append(opts,
					parquet.WithAdaptiveBloomFilterEnabledFor(c, true),
					parquet.WithBloomFilterCandidatesFor(c, bloomCandidates()))
			}
		}
	}
	props := parquet.NewWriterProperties(opts...)
	w, err := pqarrow.NewFileWriter(schema, keepOpen{f}, props, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}
	pw := &parquetWriter{w: w, schema: schema, cols: cols, tbl: t, f: f}
	for i, fld := range schema.Fields() {
		if id := fld.Type.ID(); id == arrow.FLOAT32 || id == arrow.FLOAT64 {
			pw.floatCols = append(pw.floatCols, i)
		}
	}
	return pw, nil
}

// bloomFPP is the false-positive rate a filter is sized for: one row group in
// a hundred is read for a value it does not hold.
const bloomFPP = 0.01

// bloomCandidates is how many sizes the adaptive builder keeps: one per
// halving from bloomMaxBytes down to 64 bytes.
func bloomCandidates() int {
	n := 1
	for b := bloomMaxBytes(); b > 64; b >>= 1 {
		n++
	}
	return n
}

// dropNaNStatistics removes the min/max statistics of every float column
// chunk that holds a NaN, by rewriting the footer of the file just closed.
//
// The Parquet writer computes a float chunk's min and max over its non-NaN
// values, as the format's specification asks. To PostgreSQL — and to DuckDB —
// NaN is a value, equal to itself and greater than every other, so a chunk
// holding {1, 2, NaN} with statistics [1, 2] tells the engine that `d = 'NaN'`
// and `d > 1e300` cannot match there, and it skips the row group. The row is
// in the file and the query does not return it.
//
// No statistics is the only answer the format has: a NaN in max is something
// readers are told to ignore, and +Infinity is still below NaN. The writer
// decides per FILE whether a column has statistics, and a file is being
// streamed when its first NaN arrives, so the decision is applied afterwards,
// to exactly the chunks concerned: the footer is the last thing in the file,
// and replacing it leaves every page where it was. Chunks without a NaN keep
// their statistics and prune as before.
func (pw *parquetWriter) dropNaNStatistics(f *os.File) error {
	md, err := pw.w.FileMetadata()
	if err != nil || md == nil {
		return err
	}
	for _, c := range pw.nanChunks {
		g, col := c[0], c[1]
		if g >= len(md.RowGroups) || col >= len(md.RowGroups[g].Columns) {
			continue
		}
		if cm := md.RowGroups[g].Columns[col].MetaData; cm != nil {
			cm.Statistics = nil
		}
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	// footer | 4-byte length | "PAR1"
	var tail [8]byte
	if _, err := f.ReadAt(tail[:], size-8); err != nil {
		return err
	}
	if string(tail[4:]) != "PAR1" {
		return fmt.Errorf("%s: not a plaintext Parquet footer", f.Name())
	}
	start := size - 8 - int64(binary.LittleEndian.Uint32(tail[:4]))
	if start < 4 {
		return fmt.Errorf("%s: footer length is larger than the file", f.Name())
	}
	var buf bytes.Buffer
	n, err := md.WriteTo(&buf, nil)
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(tail[:4], uint32(n))
	buf.Write(tail[:])
	if err := f.Truncate(start); err != nil {
		return err
	}
	_, err = f.WriteAt(buf.Bytes(), start)
	return err
}

// Append takes ownership of nothing: rows are flattened to column order on the
// way in, so the caller may reuse or drop the map immediately after.
//
// The whole batch is converted — which is also how it is checked, see
// values.go — before any of it is queued, so a batch holding one value the
// column cannot store writes none of its rows.
func (pw *parquetWriter) Append(rows []map[string]any) error {
	vals, err := pw.tbl.storableRows(pw.schema, pw.cols, rows)
	if err != nil {
		return err
	}
	return pw.appendStorable(vals)
}

// appendStorable queues rows that storableRows has already converted, for the
// caller that had to check them before it opened the file.
func (pw *parquetWriter) appendStorable(vals [][]any) error {
	pw.pending = append(pw.pending, vals...)
	return pw.drain()
}

// AppendValues takes a row already in column order and TAKES OWNERSHIP of it.
//
// This is the snapshot's path. Its rows arrive from a scan as a slice in column
// order, and the old route turned each one into a map so that the writer could
// look the values back out by name — one allocation and N inserts per row on
// the way in, N lookups on the way out, for a table whose whole row is four
// small columns. Bootstrap is per-row CPU bound (docs/25), and this is the
// per-row work.
func (pw *parquetWriter) AppendValues(vals []any) error {
	if err := pw.tbl.storableValues(pw.schema, pw.cols, vals); err != nil {
		return err
	}
	pw.pending = append(pw.pending, vals)
	return pw.drain()
}

func (pw *parquetWriter) drain() error {
	for len(pw.pending) >= int(RowGroupRows) {
		if err := pw.flush(int(RowGroupRows)); err != nil {
			return err
		}
	}
	return nil
}

func (pw *parquetWriter) flush(n int) error {
	if n == 0 {
		return nil
	}
	bld := array.NewRecordBuilder(memory.DefaultAllocator, pw.schema)
	defer bld.Release()
	for _, vals := range pw.pending[:n] {
		for i := range pw.cols {
			appendValue(bld.Field(i), pw.schema.Field(i).Type, vals[i])
		}
	}
	// n never exceeds RowGroupRows, so this record is exactly one row group.
	for _, i := range pw.floatCols {
		for _, vals := range pw.pending[:n] {
			if isNaN(vals[i]) {
				pw.nanChunks = append(pw.nanChunks, [2]int{pw.groups, i})
				break
			}
		}
	}
	pw.groups++
	rec := bld.NewRecord()
	err := pw.w.Write(rec)
	rec.Release()
	if err != nil {
		return err
	}
	// Drop the references so the rows just written can be collected rather than
	// pinned by the tail of the buffer for the length of the rewrite.
	rest := copy(pw.pending, pw.pending[n:])
	for i := rest; i < len(pw.pending); i++ {
		pw.pending[i] = nil
	}
	pw.pending = pw.pending[:rest]
	return nil
}

// Close flushes whatever is left, writes the footer, and makes the file
// durable. An empty table still needs a valid file with a footer, which is why
// this is called unconditionally rather than only when rows were written.
//
// The fsync is here, and its error is the caller's error, because every caller
// goes on to put this file's name in the manifest. The directory entry is the
// other half; it is recorded as owed and paid by saveState, immediately before
// the manifest is written, so that several files landing in one directory cost
// one directory sync between them.
//
// A writer built without a file (f == nil) leaves closing the sink to pqarrow,
// as before.
func (pw *parquetWriter) Close() error {
	err := pw.flush(len(pw.pending))
	if cerr := pw.w.Close(); err == nil {
		err = cerr
	}
	if pw.f == nil {
		return err
	}
	f := pw.f
	pw.f = nil
	if err == nil && len(pw.nanChunks) > 0 {
		err = pw.dropNaNStatistics(f)
	}
	if err == nil {
		err = syncFile(f)
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		owesDirSync(filepath.Dir(f.Name()))
	}
	return err
}

// forEachRowGroup decodes a file one row group at a time, handing each run of
// rows to fn along with the global position of the first row IN THE FILE — not
// in the output, because the caller is usually building a new file whose
// positions differ once deleted rows are dropped.
//
// dead positions are skipped, and cols == nil means the table's full column
// list with absent columns filled as PostgreSQL would read them back.
func (t *Table) forEachRowGroup(path string, dead map[int]bool, cols []string,
	fn func(rows []map[string]any, firstPos int) error,
) error {
	var isDead func(int) bool
	if len(dead) > 0 {
		isDead = func(p int) bool { return dead[p] }
	}
	return t.forEachRowGroupLive(path, isDead, cols, fn)
}

// forEachRowGroupLive is forEachRowGroup for a caller that holds the dead
// positions as something other than a map — a rewrite of a base file with two
// million of them holds a bitmap, which is 1.25 MB where the map was 74.
// isDead may be nil.
func (t *Table) forEachRowGroupLive(path string, isDead func(int) bool, cols []string,
	fn func(rows []map[string]any, firstPos int) error,
) error {
	return t.forEachRows(path, isDead, cols, 0, fn)
}

// rowChunk is how many rows a streaming reader holds as Go maps at a time.
const rowChunk = 4096

// forEachRows is forEachRowGroupLive that hands a row group over in pieces of
// at most chunk rows (0: whole), each with the position of the first row OF
// THE ROW GROUP.
//
// A row group decoded to Arrow is compact — about 250 bytes a row on the orders
// shape. The same rows as Go maps are over a kilobyte each, so a caller that
// only passes rows along (a rewrite, a delta merge) held 80 MB of maps per
// 65,536-row group for no reason but that they were made all at once. In
// pieces it holds 5. The slice is reused from one piece to the next: fn may
// keep the rows, not the slice.
func (t *Table) forEachRows(path string, isDead func(int) bool, cols []string, chunk int,
	fn func(rows []map[string]any, firstPos int) error,
) error {
	dead := func(p int) bool { return isDead != nil && isDead(p) }
	want, fill := cols, false
	if want == nil {
		want, fill = t.Order, true
	}

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
	schema, err := ar.Schema()
	if err != nil {
		return err
	}

	// Which columns this file actually has, in the order the caller asked for.
	pos := map[string]int{}
	for i := 0; i < schema.NumFields(); i++ {
		pos[schema.Field(i).Name] = i
	}
	var colIdx []int
	var colName []string
	for _, name := range want {
		if i, ok := pos[name]; ok {
			colIdx = append(colIdx, i)
			colName = append(colName, name)
		}
	}

	// An older file holds some types in another form; see coerceFor.
	coerce := t.coercions(schema, colIdx, colName)

	md := rdr.MetaData()
	start := 0
	for g := 0; g < md.NumRowGroups(); g++ {
		n := int(md.RowGroup(g).NumRows())
		if len(colIdx) == 0 {
			// A file with none of the wanted columns still contributes rows.
			rows := make([]map[string]any, 0, n)
			for i := 0; i < n; i++ {
				if dead(start + i) {
					continue
				}
				rows = append(rows, t.fillMissing(map[string]any{}, want, fill))
			}
			if len(rows) > 0 {
				if err := fn(rows, start); err != nil {
					return err
				}
			}
			start += n
			continue
		}
		tbl, err := ar.ReadRowGroups(context.Background(), colIdx, []int{g})
		if err != nil {
			return err
		}
		room := n
		if chunk > 0 && chunk < n {
			room = chunk
		}
		rows := make([]map[string]any, 0, room)
		for i := 0; i < int(tbl.NumRows()); i++ {
			if dead(start + i) {
				continue
			}
			m := make(map[string]any, len(want))
			for ci, name := range colName {
				v := chunkedValue(tbl.Column(ci).Data(), i)
				if coerce != nil && coerce[ci] != nil && v != nil {
					v = coerce[ci](v)
				}
				m[name] = v
			}
			rows = append(rows, t.fillMissing(m, want, fill))
			if chunk > 0 && len(rows) >= chunk {
				if err := fn(rows, start); err != nil {
					tbl.Release()
					return err
				}
				clear(rows)
				rows = rows[:0]
			}
		}
		tbl.Release()
		if len(rows) > 0 {
			if err := fn(rows, start); err != nil {
				return err
			}
		}
		start += n
	}
	return nil
}

// fillMissing supplies columns a file predates. NULL is right only when the
// column was added without a default; otherwise PostgreSQL reads those rows
// back as attmissingval and so must the mirror.
func (t *Table) fillMissing(m map[string]any, want []string, fill bool) map[string]any {
	if !fill {
		return m
	}
	for _, name := range want {
		if _, ok := m[name]; ok {
			continue
		}
		if mv, has := t.State.MissingVals[name]; has {
			m[name] = mv
		} else {
			m[name] = nil
		}
	}
	return m
}

func isNaN(v any) bool {
	switch x := v.(type) {
	case float64:
		return math.IsNaN(x)
	case float32:
		return x != x
	}
	return false
}

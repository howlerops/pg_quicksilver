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
	"context"
	"os"

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
	w       *pqarrow.FileWriter
	schema  *arrow.Schema
	cols    []string
	pending []map[string]any
}

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

// durable distinguishes a base file from a delta. Both are zstd now; the flag
// still exists because it also selects the fsync behaviour, and because
// SnappyDeltas needs to know which is which.
func (t *Table) newParquetWriter(path string, cols []string, durable bool) (*parquetWriter, error) {
	if cols == nil {
		cols = t.Order
	}
	schema := t.arrowSchemaFor(cols)

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	codec := compress.Codecs.Zstd
	if SnappyDeltas && !durable {
		codec = compress.Codecs.Snappy
	}
	props := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithCompressionLevel(ZstdLevel),
		parquet.WithMaxRowGroupLength(RowGroupRows),
	)
	w, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return nil, err
	}
	return &parquetWriter{w: w, schema: schema, cols: cols}, nil
}

// Append takes ownership of nothing: rows are copied into Arrow builders at
// flush time, so the caller may reuse or drop the slice immediately after.
func (pw *parquetWriter) Append(rows []map[string]any) error {
	pw.pending = append(pw.pending, rows...)
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
	for _, r := range pw.pending[:n] {
		for i, name := range pw.cols {
			appendValue(bld.Field(i), pw.schema.Field(i).Type, r[name])
		}
	}
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

// Close flushes whatever is left and writes the footer. An empty table still
// needs a valid file with a footer, which is why this is called unconditionally
// rather than only when rows were written.
//
// pqarrow's FileWriter closes the sink it was handed, so the os.File must not
// be closed again here.
func (pw *parquetWriter) Close() error {
	if err := pw.flush(len(pw.pending)); err != nil {
		_ = pw.w.Close()
		return err
	}
	return pw.w.Close()
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

	md := rdr.MetaData()
	start := 0
	for g := 0; g < md.NumRowGroups(); g++ {
		n := int(md.RowGroup(g).NumRows())
		if len(colIdx) == 0 {
			// A file with none of the wanted columns still contributes rows.
			rows := make([]map[string]any, 0, n)
			for i := 0; i < n; i++ {
				if dead[start+i] {
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
		rows := make([]map[string]any, 0, n)
		for i := 0; i < int(tbl.NumRows()); i++ {
			if dead[start+i] {
				continue
			}
			m := make(map[string]any, len(want))
			for ci, name := range colName {
				m[name] = chunkedValue(tbl.Column(ci).Data(), i)
			}
			rows = append(rows, t.fillMissing(m, want, fill))
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

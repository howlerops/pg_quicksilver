package mirror

// How a deletion vector is stored.
//
// A deletion vector is a sorted list of row positions in one data file that are
// no longer live. It is read by two completely different consumers, and only
// one of them was ever considered:
//
//   - the mirror itself, in Go, a few times per compaction
//   - the QUERY ENGINE, on EVERY query, as part of the view — because the view
//     resolves deletes by anti-joining against it
//
// The original format was a JSON array of integers, which is a fine choice for
// the first consumer and a bad one for the second. At twenty million rows the
// measurement is not subtle:
//
//	                        load the vector     count(*) through the view
//	  JSON array                   95.7 ms                      269.4 ms
//	  one-column Parquet            0.6 ms                      141.6 ms
//
//	  on disk: 13.1 MB of JSON against 1.59 MB of Parquet, the same 1,563,904
//	  positions
//
// Parsing 13 MB of decimal text was over a third of the cost of every query
// against a file with deletes. The rest is the anti-join itself, which no
// encoding fixes — see ShouldCompact, where the other half of this lives.
//
// A tempting third option was RANGE encoding, because the vector on the machine
// that produced those numbers collapsed from 1,563,904 positions to 77
// contiguous runs. That is not a property of deletion vectors; it is a property
// of a benchmark whose workload updates `id BETWEEN lo AND lo+20000`. Priced
// against a scattered vector of the same cardinality the anti-join cost the
// same to within noise (208 ms scattered, 247 ms clustered), so the runs were
// an artefact of the measurement and encoding for them would have bought
// nothing anywhere else.
//
// BACK COMPATIBILITY. Vectors already on disk are JSON, and a mirror that
// cannot read them resurrects every deleted row in the file it describes. So
// both are readable, and the extension on disk says which: new generations are
// written as .dv.parquet, old ones stay .dv.json and are read as they are until
// the file they describe is retired.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// dvSchema is the one-column layout every Parquet deletion vector uses.
//
// The column is named "p" and holds int64 positions in ascending order. Sorted
// order is not required for correctness — an anti-join does not care — but it
// costs nothing to maintain and it is most of why the file compresses eightfold
// against the same numbers written as text.
var dvSchema = arrow.NewSchema([]arrow.Field{
	{Name: "p", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
}, nil)

// dvRowGroup is how many positions go in a row group. A vector is one column of
// integers, so this is chosen to keep even a very large one to a handful of
// groups rather than to bound decode memory.
const dvRowGroup = 1 << 20

// writeDV writes a deletion vector to path, atomically.
//
// Atomicity is not a nicety here. A deletion vector is opened by the query
// engine the instant it appears, and a truncate-then-write leaves a window in
// which the file parses as empty — which reads as "nothing in this file is
// dead" and silently undoes every delete it described. So: temp file, fsync,
// rename, exactly as writeJSON does and for exactly the same reason.
func writeDV(path string, positions []int) error {
	tmp := path + ".tmp"
	if err := writeDVFile(tmp, positions); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	_ = f.Sync()
	_ = f.Close()
	return os.Rename(tmp, path)
}

func writeDVFile(path string, positions []int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(ZstdLevel),
		parquet.WithMaxRowGroupLength(dvRowGroup),
	)
	w, err := pqarrow.NewFileWriter(dvSchema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}

	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Reserve(len(positions))
	for _, p := range positions {
		b.Append(int64(p))
	}
	col := b.NewArray()
	defer col.Release()

	rec := array.NewRecord(dvSchema, []arrow.Array{col}, int64(len(positions)))
	defer rec.Release()
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

// readDVParquet reads the positions back out of a Parquet vector.
func readDVParquet(path string) ([]int, error) {
	rdr, err := file.OpenParquetFile(path, false)
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

	out := make([]int, 0, tbl.NumRows())
	if tbl.NumCols() == 0 {
		return out, nil
	}
	chunked := tbl.Column(0).Data()
	for _, c := range chunked.Chunks() {
		vals, ok := c.(*array.Int64)
		if !ok {
			return nil, fmt.Errorf("deletion vector %s has a %s column where int64 was expected",
				path, c.DataType())
		}
		for i := 0; i < vals.Len(); i++ {
			out = append(out, int(vals.Value(i)))
		}
	}
	return out, nil
}

// readDVFile reads either encoding, choosing by what the name says it is.
func readDVFile(path string) ([]int, error) {
	if filepath.Ext(path) == ".json" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var pos []int
		if err := json.Unmarshal(b, &pos); err != nil {
			return nil, fmt.Errorf("deletion vector %s is unreadable: %w", path, err)
		}
		return pos, nil
	}
	return readDVParquet(path)
}

// dvSorted returns the positions of a set, ascending.
func dvSorted(set map[int]bool) []int {
	out := make([]int, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Ints(out)
	return out
}

package ducklake

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// writeDecimalParquet writes a numeric(12,2) column two ways: integerBacked
// stores it on INT64 as this release does, otherwise on FIXED_LEN_BYTE_ARRAY
// as the previous one did.
func writeDecimalParquet(t testing.TB, path string, ids []int64, cents []int64, integerBacked bool) {
	t.Helper()
	dt := &arrow.Decimal128Type{Precision: 12, Scale: 2}
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "amt", Type: dt}}, nil)
	b := array.NewDecimal128Builder(memory.DefaultAllocator, dt)
	defer b.Release()
	for _, c := range cents {
		b.Append(decimal128.FromI64(c))
	}
	props := parquet.NewWriterProperties()
	if integerBacked {
		props = parquet.NewWriterProperties(parquet.WithStoreDecimalAsInteger(true))
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{int64s(ids...), arr}, int64(len(ids)))
	defer rec.Release()
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// The test is about the two forms, so it checks that it has them.
	pf, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer pf.Close()
	want := parquet.Types.FixedLenByteArray
	if integerBacked {
		want = parquet.Types.Int64
	}
	if got := pf.MetaData().Schema.Column(1).PhysicalType(); got != want {
		t.Fatalf("%s is stored as %v, want %v", path, got, want)
	}
}

// The physical form of a decimal's files is not something a reader may
// answer differently for: a table whose files are fixed-length bytes (every
// file before this release) and integers (every file from it on, or a table
// that has both) must give the answers the single-form table gives. This is
// the check the mix in widen (stats.go) rests on.
func TestDecimalPhysicalFormsReadAsOne(t *testing.T) {
	ctx := context.Background()
	id := arrow.PrimitiveTypes.Int64
	dt := &arrow.Decimal128Type{Precision: 12, Scale: 2}
	// Negative, zero, a repeated value, the extremes of numeric(12,2), and
	// values that differ only in their last digit.
	cents := []int64{-99999999999, 99999999999, 0, 1, -1, 1234, 1234, 1235, 1234, -1234, 5000, 0, 99, 100, 101, -100, 7, 7, 7, 250}
	ids := make([]int64, len(cents))
	for i := range ids {
		ids[i] = int64(i)
	}
	half := len(cents) / 2
	lake := func(name string, forms ...bool) string {
		root := t.TempDir()
		dir := filepath.Join(root, "public."+name)
		var rels []string
		pos := 0
		for f, integer := range forms {
			n := half
			if f == len(forms)-1 {
				n = len(cents) - pos
			}
			rel := fmt.Sprintf("delta/%06d.parquet", f+1)
			if f == 0 {
				rel = "base/000001.parquet"
			}
			writeDecimalParquet(t, filepath.Join(dir, rel), ids[pos:pos+n], cents[pos:pos+n], integer)
			pos += n
			rels = append(rels, rel)
		}
		pub := openLake(t, root, time.Minute, nil)
		res, err := pub.Publish(ctx, &mirror.LakeManifest{Schema: "public", Table: name, Dir: dir, Seq: 1, AppliedLSN: "0/1",
			Columns: []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: id}, {Name: "amt", PGType: "numeric(12,2)", Type: dt}},
			Files:   lakeFiles(rels...)})
		if err != nil || res.Refused != "" {
			t.Fatalf("%s: publish %+v, %v", name, res, err)
		}
		return CatalogPath(root)
	}
	fixed := startReader(t, lake("fixed", false, false))
	mixed := startReader(t, lake("mixed", false, true))
	integer := startReader(t, lake("integer", true, true))
	queries := []string{
		"SELECT count(*) FROM lake.public.%[1]s WHERE amt = 12.34",
		"SELECT count(*) FROM lake.public.%[1]s WHERE amt = 7",
		"SELECT count(*) FROM lake.public.%[1]s WHERE amt >= 0 AND amt < 100",
		"SELECT count(*) FROM lake.public.%[1]s WHERE amt > -1",
		"SELECT count(DISTINCT amt) FROM lake.public.%[1]s",
		"SELECT amt, count(*) FROM lake.public.%[1]s GROUP BY amt ORDER BY amt",
		"SELECT sum(amt)::VARCHAR, avg(amt)::VARCHAR FROM lake.public.%[1]s",
		"SELECT min(amt)::VARCHAR, max(amt)::VARCHAR FROM lake.public.%[1]s",
		"SELECT count(*) FROM lake.public.%[1]s a JOIN lake.public.%[1]s b ON a.amt = b.amt",
		"SELECT count(*) FROM lake.public.%[1]s a JOIN lake.public.%[1]s b ON a.amt = b.amt AND a.id < b.id",
	}
	for _, q := range queries {
		want, err := fixed.query(false, fmt.Sprintf(q, "fixed"))
		if err != nil {
			t.Fatalf("%s on the fixed-length table: %v", q, err)
		}
		for _, c := range []struct {
			name string
			rd   *reader
		}{{"mixed", mixed}, {"integer", integer}} {
			got, err := c.rd.query(false, fmt.Sprintf(q, c.name))
			if err != nil || flat(got) != flat(want) {
				t.Errorf("%s\n  %s = %v (%v)\n  fixed-length table = %v", q, c.name, flat(got), err, flat(want))
			}
		}
	}
}

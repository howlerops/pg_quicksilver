package mirror

// Not a test — a MEASUREMENT, skipped unless asked for.
//
// The bootstrap profile (bench/scripts/bootstrap_profile.sh) put 41% of the
// sidecar's CPU in zstd's encoder while snapshotting the jsonb shape: 25.4s to
// move 1.6 GB, at 67 MB/s, on essentially one core. That makes the compression
// level the largest single lever on bootstrap time.
//
// It is also a TRADE, not a win. A base file is written once and scanned many
// times, and docs/24 showed the read path converging on the bytes-read ratio as
// caches get colder — so every byte saved at write time is paid back on every
// query afterwards. Neither half of that is worth guessing at.
//
//	QS_CODEC_COST=1 go test ./internal/mirror -run TestCodecCost -v
//
// See docs/25.

import (
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestCodecCost(t *testing.T) {
	if os.Getenv("QS_CODEC_COST") == "" {
		t.Skip("QS_CODEC_COST unset")
	}
	// The document has to match what the jsonb shape actually generates, and
	// the first version of this measurement did not. A synthetic document made
	// only of repeated structure compressed 50x, zstd barely had to work, and
	// all four codecs came back within noise of each other — which would have
	// read as "compression is not the bottleneck" and been an artifact of the
	// test data.
	//
	// qs-matrix's jsonb row is a structured head plus 3,200 characters of
	// concatenated md5 hex, chosen precisely so it does NOT compress and does
	// NOT dictionary-encode away. So is this one.
	const n = 20_000
	doc := func(i int) string {
		s := fmt.Sprintf(`{"Id":"OPP%d","AccountId":"001%d","Name":"Deal %d",`+
			`"StageName":"Prospecting","Amount":%d.0,"Products":[`, i, i, i, i%50000)
		for j := 1; j <= 12; j++ {
			s += fmt.Sprintf(`{"Sku":"P%d","Qty":%d},`, j, j)
		}
		s += `{"Sku":"P0","Qty":0}],"Blob":"`
		for p := 1; p <= 100; p++ {
			s += fmt.Sprintf("%x", md5.Sum([]byte(fmt.Sprint(i)+fmt.Sprint(p))))
		}
		return s + `"}`
	}
	docRows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		docRows = append(docRows, map[string]any{
			"id": fmt.Sprint(i), "status": "Negotiation", "doc": doc(i),
		})
	}
	// The narrow shape too, because the two are limited by different things:
	// jsonb bootstrap is bytes through the codec, narrow is per-row overhead,
	// and a codec that wins on one need not win on the other.
	narrowRows := make([]map[string]any, 0, n*10)
	for i := 0; i < n*10; i++ {
		narrowRows = append(narrowRows, map[string]any{
			"id": fmt.Sprint(i), "sku": fmt.Sprintf("SKU-%d", i%100000),
			"amount": fmt.Sprintf("%d.%02d", i%1000, i%100),
		})
	}

	for _, ds := range []struct {
		name  string
		rows  []map[string]any
		cols  map[string]string
		order []string
	}{
		{"jsonb-like", docRows,
			map[string]string{"id": "bigint", "status": "text", "doc": "jsonb"},
			[]string{"id", "status", "doc"}},
		{"narrow-like", narrowRows,
			map[string]string{"id": "bigint", "sku": "text", "amount": "numeric(12,2)"},
			[]string{"id", "sku", "amount"}},
	} {
		dir := t.TempDir()
		tbl, err := New(dir, "public", "c", "id", ds.cols, ds.order)
		if err != nil {
			t.Fatal(err)
		}
		schema := tbl.arrowSchemaFor(ds.order)

		raw := 0
		for _, r := range ds.rows {
			for _, c := range ds.order {
				raw += len(r[c].(string))
			}
		}
		t.Logf("=== %s: %d rows, %.1f MB uncompressed (%.0f B/row)",
			ds.name, len(ds.rows), float64(raw)/1e6, float64(raw)/float64(len(ds.rows)))

		for _, c := range []struct {
			label string
			codec compress.Compression
			level int
		}{
			{"snappy", compress.Codecs.Snappy, compress.DefaultCompressionLevel},
			{"zstd level 1", compress.Codecs.Zstd, 1},
			{"zstd default", compress.Codecs.Zstd, compress.DefaultCompressionLevel},
			{"zstd level 6", compress.Codecs.Zstd, 6},
		} {
			// Three runs, median reported. A single timing at this scale is
			// noise, and the gap being decided here is the difference between
			// changing a default and not.
			var times, reads []float64
			var size int64
			for r := 0; r < 3; r++ {
				p := filepath.Join(dir, fmt.Sprintf("%s-%d.parquet", c.label, r))
				start := time.Now()
				if err := writeWith(p, schema, ds.order, ds.rows, c.codec, c.level); err != nil {
					t.Fatalf("%s: %v", c.label, err)
				}
				times = append(times, float64(time.Since(start).Milliseconds()))
				fi, err := os.Stat(p)
				if err != nil {
					t.Fatal(err)
				}
				size = fi.Size()

				// READ too. Delta files are Snappy on the theory that they are
				// written once and read many times before compaction folds
				// them away, and that theory is about DEcompression speed —
				// which a write benchmark cannot see. Changing the codec on
				// write evidence alone would be exactly the kind of half-
				// measured optimisation this file exists to avoid.
				start = time.Now()
				nRead := 0
				if err := tbl.forEachRowGroup(p, nil, ds.order,
					func(rows []map[string]any, _ int) error {
						nRead += len(rows)
						return nil
					}); err != nil {
					t.Fatalf("%s read: %v", c.label, err)
				}
				reads = append(reads, float64(time.Since(start).Milliseconds()))
				if nRead != len(ds.rows) {
					t.Fatalf("%s: read back %d rows, wrote %d", c.label, nRead, len(ds.rows))
				}
				_ = os.Remove(p)
			}
			sort.Float64s(times)
			sort.Float64s(reads)
			t.Logf("  %-14s %6.1f MB  write %6.0f ms  read %6.0f ms  %.1fx smaller",
				c.label, float64(size)/1e6, times[1], reads[1],
				float64(raw)/float64(size))
		}
	}
}

func writeWith(path string, schema *arrow.Schema, cols []string,
	rows []map[string]any, codec compress.Compression, level int,
) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	props := parquet.NewWriterProperties(
		parquet.WithCompression(codec),
		parquet.WithCompressionLevel(level),
		parquet.WithMaxRowGroupLength(RowGroupRows),
	)
	w, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		f.Close()
		return err
	}
	pw := &parquetWriter{w: w, schema: schema, cols: cols}
	if err := pw.Append(rows); err != nil {
		pw.Close()
		return err
	}
	return pw.Close()
}

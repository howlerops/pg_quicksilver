package mirror

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// TestEncodingCrossover writes one bigint column both ways, dictionary+zstd
// and DELTA_BINARY_PACKED uncompressed, at a range of cardinalities, for a
// script outside this package to time with DuckDB. It is where
// deltaMinDistinct and deltaMinShare come from; the table is in encoding.go.
//
//	QS_ENC_SWEEP=/some/dir go test -run TestEncodingCrossover ./internal/mirror/
func TestEncodingCrossover(t *testing.T) {
	out := os.Getenv("QS_ENC_SWEEP")
	if out == "" {
		t.Skip("set QS_ENC_SWEEP to a directory to write the measurement files")
	}
	defer func() { forceEncoding = "" }()
	const rows = 2 << 20
	tbl := &Table{Qualified: "public.s", Key: "k", Columns: map[string]string{"k": "bigint", "v": "bigint"},
		Order: []string{"k", "v"}, detached: true}
	for _, shape := range []string{"wide", "narrow"} {
		for _, d := range []int{16, 256, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 1 << 20} {
			r := rand.New(rand.NewSource(int64(d)))
			vals := make([]int64, d)
			for i := range vals {
				if shape == "wide" {
					vals[i] = r.Int63n(1 << 40)
				} else {
					vals[i] = int64(i)
				}
			}
			for _, enc := range []string{"dict", "delta"} {
				forceEncoding = enc
				r := rand.New(rand.NewSource(7))
				path := filepath.Join(out, fmt.Sprintf("%s_%d_%s.parquet", shape, d, enc))
				w, err := tbl.newParquetWriter(path, nil, true)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < rows; i++ {
					if err := w.AppendValues([]any{int64(i), vals[r.Intn(d)]}); err != nil {
						t.Fatal(err)
					}
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

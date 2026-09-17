package mirror

// Not a test — a MEASUREMENT, skipped unless asked for.
//
// Storing a timestamp as an instant instead of as a sentence about one sounds
// like it must save space: eight bytes against twenty-nine characters. It does
// not always, and the claim was worth checking before docs/23 made it:
//
//	QS_SIZE=monotone go test ./internal/mirror -run TestTemporalStorageCost -v
//	QS_SIZE=random   go test ./internal/mirror -run TestTemporalStorageCost -v
//
// Same rows, written both ways, so the difference is the encoding and nothing
// else. See docs/23 for what the two shapes say.

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTemporalStorageCost(t *testing.T) {
	shape := os.Getenv("QS_SIZE")
	if shape == "" {
		t.Skip("QS_SIZE unset; set it to 'monotone' or 'random'")
	}
	const n = 1_000_000
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rnd := rand.New(rand.NewSource(1))
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		// monotone: an append-only event log, the shape the mirror sees most.
		at := base.Add(time.Duration(i) * 137 * time.Millisecond)
		if shape == "random" {
			// random: rows updated in no particular order over a quarter.
			at = base.Add(time.Duration(rnd.Int63n(int64(90 * 24 * time.Hour))))
		}
		rows = append(rows, map[string]any{
			"id":  fmt.Sprint(i),
			"at":  at.Format("2006-01-02 15:04:05.999999") + "+00",
			"day": at.Format("2006-01-02"),
		})
	}
	cols := map[string]string{"id": "bigint", "at": "timestamptz", "day": "date"}
	order := []string{"id", "at", "day"}

	for _, on := range []bool{false, true} {
		func() {
			defer func(prev bool) { TemporalTypes = prev }(TemporalTypes)
			TemporalTypes = on
			dir := t.TempDir()
			tbl, err := New(dir, "public", "s", "id", cols, order)
			if err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(dir, "x.parquet")
			if err := tbl.writeParquetCodec(p, rows, true); err != nil {
				t.Fatal(err)
			}
			fi, err := os.Stat(p)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%-8s TemporalTypes=%-5v %d rows  %5.1f MB  (%.1f bytes/row)",
				shape, on, n, float64(fi.Size())/1e6, float64(fi.Size())/float64(n))
		}()
	}
}

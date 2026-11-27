package mirror

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// BenchmarkApplySync prices one Apply of a small batch, which is the shape
// where fsync is the whole cost: 100 inserts and 20 updates of older rows, so
// each Apply writes one delta file, at least one deletion vector, and the
// manifest.
//
// The directory comes from QS_BENCH_DIR when set, because the default temp
// directory is frequently tmpfs and an fsync there costs nothing.
func BenchmarkApplySync(b *testing.B) {
	root := os.Getenv("QS_BENCH_DIR")
	if root == "" {
		root = b.TempDir()
	} else {
		var err error
		if root, err = os.MkdirTemp(root, "applysync"); err != nil {
			b.Fatal(err)
		}
		defer os.RemoveAll(root)
	}
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
	tbl, err := New(root, "public", "r", "id", cols, []string{"id", "v", "n"})
	if err != nil {
		b.Fatal(err)
	}
	next, lsn := 0, 0
	batch := func() []changestream.Transaction {
		ch := make([]changestream.Change, 0, 120)
		for i := 0; i < 100; i++ {
			r := map[string]any{"id": fmt.Sprint(next), "v": "x", "n": "1"}
			next++
			ch = append(ch, changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r})
		}
		for i := 0; i < 20 && next > 200; i++ {
			r := map[string]any{"id": fmt.Sprint(next - 150 - i), "v": "y", "n": "2"}
			ch = append(ch, changestream.Change{Op: changestream.OpUpdate, Schema: "public", Table: "r", Row: r, Key: r})
		}
		lsn++
		pos := fmt.Sprintf("0/%X", lsn)
		return []changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: ch}}
	}
	for i := 0; i < 4; i++ {
		if _, err := tbl.Apply(batch()); err != nil {
			b.Fatal(err)
		}
	}
	var syncs atomic.Int64
	count := func(string, bool) error { syncs.Add(1); return nil }
	syncHook.Store(&count)
	defer syncHook.Store(nil)
	defer func() { b.ReportMetric(float64(syncs.Load())/float64(b.N), "fsyncs/op") }()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txns := batch()
		if _, err := tbl.Apply(txns); err != nil {
			b.Fatal(err)
		}
		if tbl.ShouldMergeDeltas() { // what the pump does
			b.StopTimer()
			before := syncs.Load()
			if _, err := tbl.MergeDeltas(); err != nil {
				b.Fatal(err)
			}
			syncs.Store(before)
			b.StartTimer()
		}
	}
}

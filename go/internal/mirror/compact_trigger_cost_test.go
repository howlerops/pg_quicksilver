package mirror

// What a rewrite costs AT THE TRIGGER.
//
// compact_cluster.go's memory was measured with 700,000 changed rows in a
// ten-million-row table. The churn trigger does not fire there: it fires when
// the deltas hold a fifth of the base, two million rows, and a bound that was
// only ever checked at a third of the load it is stated for is not a bound.
//
// This is the measurement, not an assertion. It is in two phases because the
// number wanted is the peak resident memory of the rewrite and of nothing
// else: one process builds the table and exits, and another opens it and
// rewrites it.
//
//	QS_TRIGGER_DIR=/path QS_TRIGGER_PHASE=build   go test ./internal/mirror/ -run TestRewriteAtTrigger -v -timeout 0
//	QS_TRIGGER_DIR=/path QS_TRIGGER_PHASE=rewrite go test ./internal/mirror/ -run TestRewriteAtTrigger -v -timeout 0
//
// QS_TRIGGER_ROWS (default 10,000,000) is the base; a fifth of it is updated.
// QS_TRIGGER_KEY=text keys the table by text instead of uuid. The table is the
// `orders` shape of bench/engine_compare/four_engines/scripts/load.sql, built
// through this package (Apply, then a rewrite), not through PostgreSQL.
// QS_CLUSTER_BY=none in the rewrite phase is the unclustered rewrite;
// QS_TRIGGER_DECLUSTER=1 in the build phase leaves a base an old binary's
// rewrites would have left, for the re-cluster measurement.

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

var triggerCols = map[string]string{
	"id": "uuid", "tenant_id": "integer", "customer_id": "uuid", "status": "text",
	"amount": "numeric(12,2)", "created_at": "timestamp with time zone", "payload": "jsonb", "note": "text",
}
var triggerOrder = []string{"id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note"}

func triggerRow(rng *rand.Rand, g, rows int, text bool) map[string]any {
	id := testUUID(g)
	if text {
		id = "order-" + id[:13] + "-" + strconv.Itoa(g)
	}
	c := 1 + rng.Intn(200_000)
	r := rng.Float64()
	status := "refunded"
	switch {
	case r < 0.05:
		status = "pending"
	case r < 0.45:
		status = "paid"
	case r < 0.75:
		status = "shipped"
	case r < 0.93:
		status = "delivered"
	case r < 0.98:
		status = "cancelled"
	}
	// 730 days across the table, plus up to ten minutes of jitter, as load.sql.
	at := time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC).
		Add(time.Duration(float64(g) / float64(rows) * 730 * 24 * float64(time.Hour))).
		Add(time.Duration(rng.Intn(600)) * time.Second).Truncate(time.Microsecond)
	payload := fmt.Sprintf(`{"ref": "%032x", "gift": %v, "items": %d, "device": {"os": "%s", "version": "%d.%d"}, "source": "%s", "campaign": "cmp-%d"}`,
		mix64(uint64(g)), rng.Float64() < 0.1, 1+rng.Intn(9),
		[]string{"ios", "android", "macos", "windows", "linux"}[rng.Intn(5)], 10+rng.Intn(8), rng.Intn(10),
		[]string{"web", "ios", "android", "api", "pos", "email"}[rng.Intn(6)], rng.Intn(40))
	row := map[string]any{
		"id": id, "tenant_id": strconv.Itoa(1 + c%200), "customer_id": testUUID(1_000_000_000 + c),
		"status": status, "amount": fmt.Sprintf("%.2f", 5+rng.Float64()*495),
		"created_at": at.Format("2006-01-02 15:04:05.999999+00"), "payload": payload,
	}
	if rng.Float64() < 0.3 {
		row["note"] = "customer asked for " + []string{"gift wrap", "a call before delivery", "delivery after 5pm", "an invoice"}[rng.Intn(4)]
	} else {
		row["note"] = nil
	}
	return row
}

// procMem reads this process's resident memory, in MB, from /proc.
func procMem() (rss, anon, file, hwm float64) {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		kb, _ := strconv.ParseFloat(f[1], 64)
		switch f[0] {
		case "VmRSS:":
			rss = kb / 1024
		case "RssAnon:":
			anon = kb / 1024
		case "RssFile:":
			file = kb / 1024
		case "VmHWM:":
			hwm = kb / 1024
		}
	}
	return
}

func TestRewriteAtTrigger(t *testing.T) {
	root, phase := os.Getenv("QS_TRIGGER_DIR"), os.Getenv("QS_TRIGGER_PHASE")
	if root == "" || phase == "" {
		t.Skip("set QS_TRIGGER_DIR and QS_TRIGGER_PHASE=build|rewrite")
	}
	rows := 10_000_000
	if s := os.Getenv("QS_TRIGGER_ROWS"); s != "" {
		rows, _ = strconv.Atoi(s)
	}
	text := os.Getenv("QS_TRIGGER_KEY") == "text"
	cols := triggerCols
	if text {
		cols = map[string]string{}
		for k, v := range triggerCols {
			cols[k] = v
		}
		cols["id"] = "text"
	}
	tbl, err := New(root, "public", "orders", "id", cols, triggerOrder)
	if err != nil {
		t.Fatal(err)
	}
	lsn := 0
	apply := func(ch []changestream.Change) {
		lsn++
		pos := fmt.Sprintf("0/%X", lsn)
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: ch}}); err != nil {
			t.Fatal(err)
		}
		// No delta merges: a merge holds every row it merges, and the point
		// of the build is to get to the trigger, not to measure the way there.
	}
	rewrite := func() (time.Duration, float64, float64, float64) {
		var peak, peakAnon, peakFile atomic.Uint64
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			lastProf := 0.0
			for {
				rss, anon, file, _ := procMem()
				if uint64(rss) > peak.Load() {
					// QS_TRIGGER_PROF names a file for the heap profile at
					// the peak (as of the collection before it).
					if prof := os.Getenv("QS_TRIGGER_PROF"); prof != "" && rss > lastProf+100 {
						lastProf = rss
						var ms runtime.MemStats
						runtime.ReadMemStats(&ms)
						if f, err := os.Create(prof); err == nil {
							_ = pprof.WriteHeapProfile(f)
							f.Close()
						}
						t.Logf("at RSS %.0f MB: heap in use %d MB, heap alloc %d MB, heap sys %d MB, released %d MB, stacks %d MB, GCs %d",
							rss, ms.HeapInuse>>20, ms.HeapAlloc>>20, ms.HeapSys>>20, ms.HeapReleased>>20, ms.StackSys>>20, ms.NumGC)
					}
					peak.Store(uint64(rss))
					peakAnon.Store(uint64(anon))
					peakFile.Store(uint64(file))
				}
				select {
				case <-stop:
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
		}()
		t0 := time.Now()
		c := tbl.BeginCompaction()
		if c == nil {
			t.Fatal("no rewrite started")
		}
		<-c.done
		if _, err := tbl.FinishCompaction(); err != nil {
			t.Fatal(err)
		}
		d := time.Since(t0)
		close(stop)
		<-done
		t.Logf("rewrite clustered on %q (%s)", c.clusterBy, c.clusterWhy)
		return d, float64(peak.Load()), float64(peakAnon.Load()), float64(peakFile.Load())
	}

	switch phase {
	case "build":
		const per = 50_000
		rng := rand.New(rand.NewSource(42))
		t0 := time.Now()
		for lo := 0; lo < rows; lo += per {
			ch := make([]changestream.Change, 0, per)
			for g := lo; g < min(lo+per, rows); g++ {
				r := triggerRow(rng, g, rows, text)
				ch = append(ch, changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "orders", Row: r, Key: r})
			}
			apply(ch)
			// Folded into the base a fifth of the table at a time, so that the
			// keys held in memory are those of a fifth and not of all of it.
			if done := min(lo+per, rows); done%(rows/5) == 0 || done == rows {
				d, _, _, _ := rewrite()
				t.Logf("%d rows inserted after %.0fs; base rewritten in %.0fs", done, time.Since(t0).Seconds(), d.Seconds())
			}
		}

		// A fifth of the table, each row once, anywhere in it.
		update := func(seed int64) {
			rng := rand.New(rand.NewSource(seed))
			perm := rng.Perm(rows)[:rows/5]
			for lo := 0; lo < len(perm); lo += per {
				ch := make([]changestream.Change, 0, per)
				for _, g := range perm[lo:min(lo+per, len(perm))] {
					// The same row again with another status and amount: its
					// place on created_at does not move.
					r := triggerRow(rand.New(rand.NewSource(int64(g))), g, rows, text)
					r["status"], r["amount"] = "refunded", fmt.Sprintf("%.2f", 5+rng.Float64()*495)
					ch = append(ch, changestream.Change{Op: changestream.OpUpdate, Schema: "public", Table: "orders", Row: r, Key: r})
				}
				apply(ch)
			}
		}
		if os.Getenv("QS_TRIGGER_DECLUSTER") != "" {
			// What an older binary did, twice: fold a fifth of the table in
			// at the end of the base.
			defer func(s string) { ClusterBy = s }(ClusterBy)
			ClusterBy = "none"
			for i := int64(0); i < 2; i++ {
				update(100 + i)
				d, _, _, _ := rewrite()
				t.Logf("unclustered rewrite %d in %.0fs", i+1, d.Seconds())
			}
			tbl.Close()
			return
		}
		update(7)
		if why := tbl.CompactReason(); why != "churn" {
			t.Fatalf("the table is not at the churn trigger: %q (base %d, delta %d)", why, tbl.State.BaseRows, tbl.State.DeltaRows)
		}
		t.Logf("at the trigger: base %d rows, deltas %d rows in %d files", tbl.State.BaseRows, tbl.State.DeltaRows, len(tbl.State.DeltaFiles))
		tbl.Close()

	case "rewrite":
		if _, err := tbl.BuildIndex(); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		debug.FreeOSMemory()
		rss0, anon0, file0, _ := procMem()
		why := tbl.CompactReason()
		t.Logf("before: base %d rows, deltas %d rows in %d files, trigger %q, row group %d", tbl.State.BaseRows, tbl.State.DeltaRows,
			len(tbl.State.DeltaFiles), why, RowGroupRows)
		t.Logf("before: RSS %.0f MB (anon %.0f, file %.0f)", rss0, anon0, file0)
		d, peak, anon, file := rewrite()
		_, _, _, hwm := procMem()
		t.Logf("RESULT rows=%d delta=%d cluster_by=%q seconds=%.1f rss_before_mb=%.0f peak_rss_mb=%.0f (anon %.0f, file %.0f) vmhwm_mb=%.0f",
			rows, rows/5, ClusterBy, d.Seconds(), rss0, peak, anon, file, hwm)
		if pairs, groups, col := triggerOverlap(tbl); col != "" {
			t.Logf("after: %d row groups, %s ranges overlap on %d pairs", groups, col, pairs)
		}
		tbl.Close()
	default:
		t.Fatalf("QS_TRIGGER_PHASE=%q", phase)
	}
}

func triggerOverlap(tbl *Table) (pairs, groups int, col string) {
	if len(tbl.State.BaseFiles) != 1 {
		return 0, 0, ""
	}
	ranges, groups, err := columnRanges([]string{filepath.Join(tbl.Dir, "base", tbl.State.BaseFiles[0])}, []string{"created_at"})
	if err != nil || len(ranges["created_at"]) == 0 {
		return 0, groups, ""
	}
	r := ranges["created_at"]
	for i := range r {
		for j := i + 1; j < len(r); j++ {
			if ordCmp(r[i].hi, r[j].lo) > 0 && ordCmp(r[j].hi, r[i].lo) > 0 {
				pairs++
			}
		}
	}
	return pairs, groups, "created_at"
}

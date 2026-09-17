package mirror

// What does a compaction cost at the scale where the read path needs one?
//
// ShouldCompact triggers on DeltaRows >= BaseRows/5, which is a trigger sized
// entirely by the cost of the REWRITE: compaction reads every live row and
// writes a new base file, so triggering proportionally to the base keeps the
// amortised write cost constant. That reasoning is correct and it is only half
// the ledger. The other half is that every query pays for the deletion vector
// the rewrite has not folded away, and that cost scales with the ABSOLUTE
// number of dead positions, not the ratio.
//
// So the trigger tolerates more dead rows the larger the table gets — 80,000 on
// a 400k-row mirror, four million on a 20M-row one — while the per-query
// penalty for carrying them grows with exactly that number. The 20.4M-row
// large-scale run sat at 1,563,904 dead positions with compaction correctly
// declining to run, and count(*) went from 18.5 ms at 4M rows to 422.9 ms.
//
// This test exists to put the missing number on the other side of that trade.
// It is not an assertion about speed; it is a measurement printed with -v so
// the policy can be argued about with both costs in hand.
//
//	go test ./internal/mirror/ -run TestCompactionCost -v

import (
	"fmt"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestCompactionCost(t *testing.T) {
	if testing.Short() {
		t.Skip("writes several million rows")
	}

	for _, rows := range []int{100_000, 400_000, 1_000_000} {
		t.Run(fmt.Sprintf("%drows", rows), func(t *testing.T) {
			tbl := racyTable(t)

			apply := func(lsn int, op changestream.Op, from, to int) {
				const perTxn = 50_000
				for lo := from; lo < to; lo += perTxn {
					hi := min(lo+perTxn, to)
					changes := make([]changestream.Change, 0, hi-lo)
					for i := lo; i < hi; i++ {
						row := map[string]any{
							"id": fmt.Sprint(i),
							"v":  fmt.Sprintf("value-%d-%d", lsn, i),
							"n":  fmt.Sprint(i),
						}
						changes = append(changes, changestream.Change{
							Op: op, Schema: "public", Table: "r", Row: row, Key: row,
						})
					}
					if _, err := tbl.Apply([]changestream.Transaction{{
						CommitLSN: fmt.Sprintf("0/%d", lsn),
						NextLSN:   fmt.Sprintf("0/%d", lsn+1),
						Changes:   changes,
					}}); err != nil {
						t.Fatal(err)
					}
					lsn++
				}
			}

			apply(1, changestream.OpInsert, 0, rows)
			base := tbl.State.BaseRows + tbl.State.DeltaRows

			// Churn a fifth, which is exactly where ShouldCompact fires — so
			// this measures the rewrite the current policy actually schedules,
			// not an arbitrary one.
			churn := rows / compactChurnRatio
			apply(1000, changestream.OpUpdate, 0, churn)

			if !tbl.ShouldCompact() {
				t.Fatalf("%d rows with %d churned did not reach the trigger, so this "+
					"is no longer measuring what the policy schedules "+
					"(base_rows=%d delta_rows=%d)",
					rows, churn, tbl.State.BaseRows, tbl.State.DeltaRows)
			}

			start := time.Now()
			n, err := tbl.Compact()
			if err != nil {
				t.Fatal(err)
			}
			d := time.Since(start)

			t.Logf("%9d rows (%d live before), %7d churned -> compaction kept %d in %v, %.0f k rows/s",
				rows, base, churn, n, d.Round(time.Millisecond),
				float64(rows)/d.Seconds()/1000)

			if got := len(tbl.State.DeltaFiles); got != 0 {
				t.Errorf("compaction left %d delta files behind", got)
			}
			// The point of the rewrite, for the read path: the new base carries
			// no deletion vector, so a view over it is back on the footer path
			// and does not pay an anti-join at all.
			for _, b := range tbl.State.BaseFiles {
				if gen := tbl.State.DVGen[dvStem(b)]; gen != 0 {
					t.Errorf("the compacted base %s still carries a deletion vector "+
						"(gen %d), so the read path still pays the anti-join", b, gen)
				}
			}
		})
	}
}

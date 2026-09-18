package mirror

// Background compaction is the one place in this package where two things run
// at once, and the failure it produces is a silently wrong mirror rather than a
// crash: the first end-to-end run after it landed came back short by exactly
// one row out of 2.8 million.
//
// So this test does what the end-to-end harness cannot — it drives apply and
// compaction against each other deterministically, with an independently
// maintained expectation, and checks every row and every column.

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func racyTable(t *testing.T) *Table {
	t.Helper()
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
	order := []string{"id", "v", "n"}
	tbl, err := New(t.TempDir(), "public", "r", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

// TestCompactionRacesApply interleaves batches with a background rewrite,
// including the windows that matter: rows updated, deleted and re-inserted
// while the rewrite is reading the files they live in.
func TestCompactionRacesApply(t *testing.T) {
	testCompactionRaces(t, false)
}

// TestCompactionRacesPartialRows is the jsonb shape: every update omits a
// column, so carry-forward has to read the previous value out of the mirror —
// including out of files a background rewrite is reading at the same time.
func TestCompactionRacesPartialRows(t *testing.T) {
	testCompactionRaces(t, true)
}

func testCompactionRaces(t *testing.T, partial bool) {
	for _, seed := range []int64{1, 2, 3, 7, 11} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			tbl := racyTable(t)
			// Kept from the hunt for the torn-deletion-vector bug: knowing WHEN a
			// key was deleted relative to when a rewrite swapped in is what
			// turned "some rows are extra" into a mechanism.
			var lastCompaction *Compaction
			deletedAt := map[string]int{}
			swapAt := []int{}
			patchedDuringRewrite := false
			want := map[string]map[string]any{}
			lsn := 0

			apply := func(changes []changestream.Change) {
				lsn++
				pos := fmt.Sprintf("0/%X", lsn)
				if _, err := tbl.Apply([]changestream.Transaction{{
					CommitLSN: pos, NextLSN: pos, Changes: changes,
				}}); err != nil {
					t.Fatal(err)
				}
			}

			// seed
			var seedChanges []changestream.Change
			for i := 1; i <= 500; i++ {
				row := map[string]any{
					"id": fmt.Sprint(i), "v": fmt.Sprintf("v%d", i), "n": "0",
				}
				seedChanges = append(seedChanges, changestream.Change{
					Op: changestream.OpInsert, Schema: "public", Table: "r",
					Row: row, Key: row,
				})
				want[fmt.Sprint(i)] = copyRow(row)
			}
			apply(seedChanges)

			next := 501
			for round := 0; round < 60; round++ {
				// Start a rewrite roughly a third of the way in and again
				// later, so batches land both before and during it.
				if (round == 10 || round == 35) && !tbl.Compacting() {
					c := tbl.BeginCompaction()
					if c == nil {
						t.Fatal("BeginCompaction returned nil with files present")
					}
					lastCompaction = c
				}
				_ = lastCompaction

				var changes []changestream.Change
				for i := 0; i < 20; i++ {
					switch rng.Intn(10) {
					case 0, 1, 2: // insert
						id := fmt.Sprint(next)
						next++
						row := map[string]any{"id": id, "v": "new", "n": "1"}
						changes = append(changes, changestream.Change{
							Op: changestream.OpInsert, Schema: "public", Table: "r",
							Row: row, Key: row,
						})
						want[id] = copyRow(row)
					case 3, 4, 5, 6: // update an existing row
						id := pickKey(rng, want)
						if id == "" {
							continue
						}
						row := map[string]any{
							"id": id, "v": fmt.Sprintf("u%d", round), "n": fmt.Sprint(round),
						}
						if partial {
							// unchanged TOAST: the column is simply not sent,
							// and the mirror must carry the old value forward
							delete(row, "v")
							prev := want[id]
							row2 := map[string]any{"id": id, "n": fmt.Sprint(round)}
							changes = append(changes, changestream.Change{
								Op: changestream.OpUpdate, Schema: "public", Table: "r",
								Row: row2, Key: row2,
							})
							want[id] = map[string]any{
								"id": id, "v": prev["v"], "n": fmt.Sprint(round),
							}
							continue
						}
						changes = append(changes, changestream.Change{
							Op: changestream.OpUpdate, Schema: "public", Table: "r",
							Row: row, Key: row,
						})
						want[id] = copyRow(row)
					case 7, 8: // delete
						id := pickKey(rng, want)
						if id == "" {
							continue
						}
						row := map[string]any{"id": id}
						changes = append(changes, changestream.Change{
							Op: changestream.OpDelete, Schema: "public", Table: "r",
							Row: nil, Key: row,
						})
						delete(want, id)
						deletedAt[id] = round
					case 9: // re-insert a deleted key, the nastiest case
						id := fmt.Sprint(1 + rng.Intn(500))
						row := map[string]any{"id": id, "v": "re", "n": "9"}
						changes = append(changes, changestream.Change{
							Op: changestream.OpInsert, Schema: "public", Table: "r",
							Row: row, Key: row,
						})
						want[id] = copyRow(row)
					}
				}
				if len(changes) > 0 {
					apply(changes)
				}
				if tbl.patch.len() > 0 && tbl.Compacting() {
					patchedDuringRewrite = true
				}

				// swap in a finished rewrite, exactly as the apply loop does
				if tbl.Compacting() {
					n, err := tbl.FinishCompaction()
					if err != nil {
						t.Fatalf("FinishCompaction: %v", err)
					}
					if n > 0 {
						swapAt = append(swapAt, round)
					}
				}

				// ...and merge deltas on the same schedule the apply loop uses,
				// INCLUDING while a rewrite is in flight. That combination is
				// what keeps the file count bounded under churn, and it is the
				// one ordering where a merge could renumber rows a rewrite is
				// about to swap in.
				if tbl.ShouldMergeDeltas() {
					if _, err := tbl.MergeDeltas(); err != nil {
						t.Fatalf("MergeDeltas: %v", err)
					}
				}
			}

			if tbl.ShouldMergeDeltas() {
				if _, err := tbl.MergeDeltas(); err != nil {
					t.Fatalf("MergeDeltas: %v", err)
				}
			}

			// drain any rewrite still in flight
			for tbl.Compacting() {
				if _, err := tbl.FinishCompaction(); err != nil {
					t.Fatalf("FinishCompaction: %v", err)
				}
			}

			// The partial variant exists to drive column-partial deltas through
			// the swap. If none was ever written during a rewrite it is running
			// the whole-row path twice and proving half of what it claims.
			if partial && PartialDeltas && !patchedDuringRewrite {
				t.Fatal("no column-partial delta was written while a rewrite " +
					"was in flight")
			}

			got := map[string]map[string]any{}
			if err := tbl.ForEachLive(func(r map[string]any) error {
				k := fmt.Sprint(r["id"])
				if _, dup := got[k]; dup {
					// KNOWN BUG, see docs/31. This fires in roughly one run in
					// twenty, only in the partial-delta variant. It is NOT a
					// flaky test: the test is right and the mirror is wrong,
					// and a duplicate row makes count(*) and every aggregate
					// disagree with the source.
					t.Errorf("key %s appears twice in the mirror; a compaction "+
						"swap kept two copies of the same row (known bug, docs/31)", k)
				}
				got[k] = r
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if len(got) != len(want) {
				t.Errorf("mirror has %d rows, expected %d (difference %d)",
					len(got), len(want), len(got)-len(want))
			}
			missing, extra, wrong := 0, 0, 0
			for k, w := range want {
				g, ok := got[k]
				if !ok {
					if missing < 3 {
						t.Errorf("row %s missing from the mirror (expected v=%v n=%v)",
							k, w["v"], w["n"])
					}
					missing++
					continue
				}
				for _, c := range []string{"v", "n"} {
					if Render(g[c]) != Render(w[c]) {
						if wrong < 3 {
							t.Errorf("row %s column %s: mirror=%v want=%v",
								k, c, g[c], w[c])
						}
						wrong++
					}
				}
			}
			for k := range got {
				if _, ok := want[k]; !ok {
					if extra < 3 {
						where := "not in index"
						inDV := "n/a"
						if l, ok2 := tbl.index.get(tbl.keyOfString(k)); ok2 {
							where = fmt.Sprintf("index->%s#%d", l.File.name(), l.Pos)
							dp, _ := tbl.deadPositions(l.File.name())
							inDV = fmt.Sprint(dp[int(l.Pos)])
						}
						t.Errorf("row %s in mirror but deleted at round %d (swaps at %v) (%s; markedDead=%s)",
							k, deletedAt[k], swapAt, where, inDV)
					}
					extra++
				}
			}
			if missing+extra+wrong > 0 {
				t.Errorf("totals: %d missing, %d extra, %d wrong", missing, extra, wrong)
			}
		})
	}
}

func copyRow(r map[string]any) map[string]any {
	out := make(map[string]any, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}

func pickKey(rng *rand.Rand, m map[string]map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	n := rng.Intn(len(m))
	for k := range m {
		if n == 0 {
			return k
		}
		n--
	}
	return ""
}

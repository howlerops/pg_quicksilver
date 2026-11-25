package mirror

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// checkDeadCache asserts the property the cache exists under: for every file
// the manifest names, the dead set in memory is exactly the vector the manifest
// names on disk — and there is no entry for a file the manifest does not name.
func checkDeadCache(t *testing.T, tbl *Table, when string) {
	t.Helper()
	live := map[string]bool{}
	check := func(rel string) {
		live[dvStem(rel)] = true
		onDisk, err := tbl.deadPositions(rel)
		if err != nil {
			t.Fatalf("%s: reading %s from disk: %v", when, rel, err)
		}
		set, err := tbl.deadSetFor(rel)
		if err != nil {
			t.Fatalf("%s: dead set of %s: %v", when, rel, err)
		}
		if set.gen != tbl.State.DVGen[dvStem(rel)] {
			t.Fatalf("%s: %s cached at generation %d, manifest names %d",
				when, rel, set.gen, tbl.State.DVGen[dvStem(rel)])
		}
		want := dvSorted(onDisk)
		got := set.positions()
		if len(want) != set.n || !(len(want) == 0 && len(got) == 0) && !reflect.DeepEqual(got, want) {
			t.Fatalf("%s: %s: cache holds %d dead positions, disk holds %d\n cache %v\n disk  %v",
				when, rel, set.n, len(want), got, want)
		}
	}
	for _, b := range tbl.State.BaseFiles {
		check(filepath.Join("base", b))
	}
	for _, d := range tbl.State.DeltaFiles {
		check(filepath.Join("delta", d))
	}
	for stem := range tbl.dead {
		if !live[stem] {
			t.Fatalf("%s: cache still holds %s, which the manifest no longer names", when, stem)
		}
	}
}

// TestDeadCacheMatchesDisk drives everything that can change which vector
// belongs to which file — applies, delta merges, a compaction swap with writes
// racing it, a truncate, a Reload — and after every step compares the cache
// with the disk, and the mirror's contents with a model.
func TestDeadCacheMatchesDisk(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 5, 8} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			tbl := racyTable(t)
			want := map[string]map[string]any{}
			lsn, next := 0, 1

			apply := func(changes []changestream.Change) {
				lsn++
				pos := fmt.Sprintf("0/%X", lsn)
				if _, err := tbl.Apply([]changestream.Transaction{{
					CommitLSN: pos, NextLSN: pos, Changes: changes,
				}}); err != nil {
					t.Fatal(err)
				}
			}
			batch := func(n int) []changestream.Change {
				var changes []changestream.Change
				for i := 0; i < n; i++ {
					op := rng.Intn(10)
					if len(want) == 0 {
						op = 0
					}
					switch {
					case op < 3:
						id := fmt.Sprint(next)
						next++
						row := map[string]any{"id": id, "v": "new", "n": "1"}
						changes = append(changes, changestream.Change{
							Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
						})
						want[id] = copyRow(row)
					case op < 7:
						id := pickKey(rng, want)
						row := map[string]any{"id": id, "v": fmt.Sprintf("u%d", lsn), "n": fmt.Sprint(i)}
						changes = append(changes, changestream.Change{
							Op: changestream.OpUpdate, Schema: "public", Table: "r",
							Row: row, Key: map[string]any{"id": id},
						})
						want[id] = copyRow(row)
					default:
						id := pickKey(rng, want)
						changes = append(changes, changestream.Change{
							Op: changestream.OpDelete, Schema: "public", Table: "r",
							Key: map[string]any{"id": id},
						})
						delete(want, id)
					}
				}
				return changes
			}
			checkRows := func(when string) {
				t.Helper()
				rows, err := tbl.Live()
				if err != nil {
					t.Fatalf("%s: %v", when, err)
				}
				if len(rows) != len(want) {
					t.Fatalf("%s: mirror holds %d rows, model holds %d", when, len(rows), len(want))
				}
				for _, r := range rows {
					id := fmt.Sprint(r["id"])
					if w, ok := want[id]; !ok || fmt.Sprint(r["v"]) != fmt.Sprint(w["v"]) {
						t.Fatalf("%s: row %s is %v, model says %v", when, id, r, w)
					}
				}
			}

			apply(batch(400))
			checkDeadCache(t, tbl, "after the seed batch")

			for round := 0; round < 80; round++ {
				when := fmt.Sprintf("round %d", round)
				switch {
				case round%20 == 7 && !tbl.Compacting():
					if tbl.BeginCompaction() == nil {
						t.Fatal("BeginCompaction returned nil with files present")
					}
					when += " (rewrite started)"
				case round%20 == 11 && tbl.Compacting():
					<-tbl.compacting.done
					if _, err := tbl.FinishCompaction(); err != nil {
						t.Fatal(err)
					}
					when += " (rewrite swapped in)"
				case round%9 == 4:
					if _, err := tbl.MergeDeltas(); err != nil {
						t.Fatal(err)
					}
					when += " (deltas merged)"
				case round == 55:
					// Not during a rewrite: round%20 has one in flight from 7 to 11
					// only, and a truncate under a rewrite is a different
					// test's subject.
					apply([]changestream.Change{{
						Op: changestream.OpTruncate, Schema: "public", Table: "r",
					}})
					want = map[string]map[string]any{}
					when += " (truncated)"
				case round%13 == 6 && !tbl.Compacting():
					if err := tbl.Reload(); err != nil {
						t.Fatal(err)
					}
					when += " (reloaded)"
				}
				checkDeadCache(t, tbl, when)
				apply(batch(25))
				checkDeadCache(t, tbl, when+", then a batch")
				if round%5 == 4 {
					checkRows(when)
				}
			}
			checkRows("at the end")

			// A restart is a Table with an empty cache reading the same
			// manifest. It must arrive at the same sets.
			if tbl.Compacting() {
				<-tbl.compacting.done
				if _, err := tbl.FinishCompaction(); err != nil {
					t.Fatal(err)
				}
			}
			again, err := New(filepath.Dir(tbl.Dir), "public", "r", "id", tbl.Columns, tbl.Order)
			if err != nil {
				t.Fatal(err)
			}
			checkDeadCache(t, again, "after a restart")
			for stem, set := range tbl.dead {
				other := again.dead[stem]
				if other == nil || !reflect.DeepEqual(other.positions(), set.positions()) {
					t.Fatalf("restart disagrees with the running cache about %s", stem)
				}
			}
		})
	}
}

// TestDeadCacheSurvivesAStaleEntry is the generation check on its own: an
// entry left behind for a generation the manifest no longer names must be
// replaced from disk, not trusted.
func TestDeadCacheSurvivesAStaleEntry(t *testing.T) {
	tbl := racyTable(t)
	p := newPolicyRig(t, tbl)
	p.insert(0, 100)
	p.compact()
	p.del(0, 10)
	rel := filepath.Join("base", tbl.State.BaseFiles[0])

	// Poison the cache with the right generation number's predecessor.
	stale := &deadSet{gen: tbl.State.DVGen[dvStem(rel)] - 1}
	stale.add(99)
	tbl.dead[dvStem(rel)] = stale

	set, err := tbl.deadSetFor(rel)
	if err != nil {
		t.Fatal(err)
	}
	if set.n != 10 || set.has(99) {
		t.Fatalf("stale cache entry was served: %d dead, has(99)=%v", set.n, set.has(99))
	}
	checkDeadCache(t, tbl, "after replacing a stale entry")
}

// policyRig is a table with a clock that only moves when told to, and the part
// of the sidecar's tick that decides about rewrites.
type policyRig struct {
	t   *testing.T
	tbl *Table
	now time.Time
	lsn int
}

func newPolicyRig(t *testing.T, tbl *Table) *policyRig {
	p := &policyRig{t: t, tbl: tbl, now: time.Unix(1_700_000_000, 0)}
	tbl.clock = func() time.Time { return p.now }
	return p
}

func (p *policyRig) advance(d time.Duration) { p.now = p.now.Add(d) }

func (p *policyRig) apply(changes []changestream.Change) {
	p.t.Helper()
	p.lsn++
	pos := fmt.Sprintf("0/%X", p.lsn)
	if _, err := p.tbl.Apply([]changestream.Transaction{{
		CommitLSN: pos, NextLSN: pos, Changes: changes,
	}}); err != nil {
		p.t.Fatal(err)
	}
}

func (p *policyRig) insert(from, to int) {
	p.t.Helper()
	ch := make([]changestream.Change, 0, to-from)
	for i := from; i < to; i++ {
		r := map[string]any{"id": fmt.Sprint(i), "v": "x", "n": "0"}
		ch = append(ch, changestream.Change{
			Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r,
		})
	}
	p.apply(ch)
}

func (p *policyRig) update(ids []int) {
	p.t.Helper()
	ch := make([]changestream.Change, 0, len(ids))
	for _, i := range ids {
		r := map[string]any{"id": fmt.Sprint(i), "v": fmt.Sprintf("u%d", p.lsn), "n": "1"}
		ch = append(ch, changestream.Change{
			Op: changestream.OpUpdate, Schema: "public", Table: "r",
			Row: r, Key: map[string]any{"id": fmt.Sprint(i)},
		})
	}
	p.apply(ch)
}

func (p *policyRig) del(from, to int) {
	p.t.Helper()
	ch := make([]changestream.Change, 0, to-from)
	for i := from; i < to; i++ {
		ch = append(ch, changestream.Change{
			Op: changestream.OpDelete, Schema: "public", Table: "r",
			Key: map[string]any{"id": fmt.Sprint(i)},
		})
	}
	p.apply(ch)
}

// compact runs one rewrite to completion, as two ticks of the sidecar would.
func (p *policyRig) compact() {
	p.t.Helper()
	c := p.tbl.BeginCompaction()
	if c == nil {
		p.t.Fatal("BeginCompaction returned nil")
	}
	<-c.done
	if _, err := p.tbl.FinishCompaction(); err != nil {
		p.t.Fatal(err)
	}
}

func (p *policyRig) reason(want, when string) {
	p.t.Helper()
	if got := p.tbl.CompactReason(); got != want {
		p.t.Fatalf("%s: CompactReason() = %q, want %q (base_rows=%d delta_rows=%d)",
			when, got, want, p.tbl.State.BaseRows, p.tbl.State.DeltaRows)
	}
}

func (p *policyRig) baseDead() int {
	p.t.Helper()
	n, err := p.tbl.baseDead()
	if err != nil {
		p.t.Fatal(err)
	}
	return n
}

// TestIdleMirrorShedsItsBaseVector is the case that was measured: a burst of
// updates and deletes too small to reach the churn threshold, and then silence.
// The base used to carry its deletion vector until the next 12% of the table
// was written, however long that took.
func TestIdleMirrorShedsItsBaseVector(t *testing.T) {
	tbl := racyTable(t)
	p := newPolicyRig(t, tbl)
	const rows = 30_000
	p.insert(0, rows)
	p.compact()
	if p.baseDead() != 0 {
		t.Fatalf("a freshly rewritten base has %d dead rows", p.baseDead())
	}
	p.advance(24 * time.Hour)
	p.reason("", "a quiet base with no vector")

	// 8% of the table: 5% updated, 3% deleted.
	ids := make([]int, 0, 1500)
	for i := 0; i < 1500; i++ {
		ids = append(ids, i*7)
	}
	p.update(ids)
	p.del(20_000, 20_900)
	dead := p.baseDead()
	if dead != 2400 {
		t.Fatalf("expected 2400 dead in the base, have %d", dead)
	}
	sql, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "NOT IN") {
		t.Fatalf("the view should be reading the base through its vector:\n%s", sql)
	}

	p.reason("", "immediately after the burst")
	p.advance(29 * time.Second)
	p.reason("", "29s after the burst")
	p.advance(2 * time.Second)
	p.reason("idle", "31s after the burst")

	p.compact()
	if got := tbl.State.DVGen[dvStem(tbl.State.BaseFiles[0])]; got != 0 || p.baseDead() != 0 {
		t.Fatalf("the rewritten base still has a vector: generation %d, %d dead", got, p.baseDead())
	}
	if len(tbl.State.DeltaFiles) != 0 {
		t.Fatalf("the rewrite left %d delta files", len(tbl.State.DeltaFiles))
	}
	sql, err = tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "NOT IN") {
		t.Fatalf("the view still anti-joins after the idle rewrite:\n%s", sql)
	}
	n, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	if len(n) != rows-900 {
		t.Fatalf("mirror holds %d rows after the rewrite, want %d", len(n), rows-900)
	}

	// And having fired, it has nothing left to fire on.
	for i := 0; i < 10; i++ {
		p.advance(time.Hour)
		p.reason("", "idle with no vector, after the rewrite")
	}

	t.Run("off", func(t *testing.T) {
		defer func(v float64) { CompactIdleSeconds = v }(CompactIdleSeconds)
		CompactIdleSeconds = 0
		p.del(100, 110)
		p.advance(time.Hour)
		p.reason("", "idle trigger turned off")
	})
}

// TestSteadyWritesDoNotStormRewrites runs the sidecar's decision once a second
// against a table that is never quiet, and counts what it starts.
//
// The property is the one the churn trigger was built for and a time-based
// trigger would break: a rewrite is O(table), so it may only be bought by an
// amount of change proportional to the table — never by the clock.
func TestSteadyWritesDoNotStormRewrites(t *testing.T) {
	tbl := racyTable(t)
	p := newPolicyRig(t, tbl)
	const rows = 30_000
	p.insert(0, rows)
	p.compact()

	rng := rand.New(rand.NewSource(4))
	type start struct {
		tick      int
		why       string
		deltaRows int
	}
	var starts []start
	const ticks, perTick = 70, 1500
	for tick := 0; tick < ticks; tick++ {
		p.advance(time.Second)
		ids := make([]int, 0, perTick)
		seen := map[int]bool{}
		for len(ids) < perTick {
			if i := rng.Intn(rows); !seen[i] {
				seen[i] = true
				ids = append(ids, i)
			}
		}
		p.update(ids)

		// What maintain() in cmd/qs-mirror does, with the rewrite made
		// synchronous so the count does not depend on scheduling.
		if why := tbl.CompactReason(); why != "" {
			starts = append(starts, start{tick, why, tbl.State.DeltaRows})
			p.compact()
			// The pretend rewrite took 5s, so that the idle backoff is in play
			// for the rest of the run and not trivially zero.
			tbl.lastRewrite = 5 * time.Second
		}
	}

	// 1,500 rows a second against a threshold of max(30,000/5, 25,000) delta
	// rows is one rewrite about every 17 ticks.
	if len(starts) == 0 {
		t.Fatal("steady churn never triggered a rewrite at all")
	}
	for i, s := range starts {
		if s.why != "churn" {
			t.Fatalf("rewrite %d at tick %d was triggered by %q under steady writes; "+
				"only churn may buy one", i, s.tick, s.why)
		}
		if s.deltaRows < compactMinRows {
			t.Fatalf("rewrite %d at tick %d started with only %d delta rows", i, s.tick, s.deltaRows)
		}
		if i > 0 && s.tick-starts[i-1].tick < 10 {
			t.Fatalf("rewrites at ticks %d and %d are back to back", starts[i-1].tick, s.tick)
		}
	}
	if max := ticks*perTick/compactMinRows + 1; len(starts) > max {
		t.Fatalf("%d rewrites for %d changed rows; the churn threshold allows %d",
			len(starts), ticks*perTick, max)
	}
	t.Logf("%d ticks of %d updates: %d rewrites, all churn, at ticks %v",
		ticks, perTick, len(starts), func() (out []int) {
			for _, s := range starts {
				out = append(out, s.tick)
			}
			return
		}())

	// A slow trickle is still "not idle": one row every 20s, for twenty minutes, with
	// a vector on the base throughout, starts nothing.
	for i := 0; i < 60; i++ {
		p.advance(20 * time.Second)
		p.update([]int{i})
		p.reason("", "a trickle of writes")
	}
	if p.baseDead() == 0 {
		t.Fatal("the trickle left no vector on the base, so it tested nothing")
	}
}

// TestBurstsDoNotBuyARewriteEach is the workload idleness alone gets wrong:
// bursts separated by gaps just longer than the idle threshold. The backoff
// spaces idle rewrites by what the last one cost.
func TestBurstsDoNotBuyARewriteEach(t *testing.T) {
	tbl := racyTable(t)
	p := newPolicyRig(t, tbl)
	p.insert(0, 30_000)
	p.compact()
	// Say that rewrite took 20s. Ten times that is the spacing.
	tbl.lastRewrite = 20 * time.Second
	tbl.lastCompacted = p.now

	rewrites := 0
	for burst := 0; burst < 20; burst++ { // 20 bursts, 40s apart: 800s
		p.del(burst*10, burst*10+10)
		p.advance(40 * time.Second)
		if why := tbl.CompactReason(); why != "" {
			if why != "idle" {
				t.Fatalf("burst %d triggered %q", burst, why)
			}
			rewrites++
			p.compact()
			tbl.lastRewrite = 20 * time.Second
		}
	}
	// 800s at one rewrite per 200s of backoff.
	if rewrites < 2 || rewrites > 4 {
		t.Fatalf("%d idle rewrites across 20 bursts in 800s; the backoff allows about 4", rewrites)
	}

	// The backoff delays; it does not prevent. Once it has passed, a quiet
	// table with a vector is rewritten.
	p.del(1000, 1010)
	p.advance(31 * time.Second)
	if tbl.CompactReason() == "" {
		p.advance(200 * time.Second)
	}
	p.reason("idle", "after the backoff has passed")
}

// TestDeleteOnlyWorkloadCompacts is the gap the dead-fraction comment named and
// left: deletes with no accompanying writes produce no delta rows, so churn
// never fires — and with no delta FILES the policy used to return before
// looking at the base at all.
func TestDeleteOnlyWorkloadCompacts(t *testing.T) {
	tbl := racyTable(t)
	p := newPolicyRig(t, tbl)
	const rows = 150_000 // a fifth of which is above compactMinRows
	for lo := 0; lo < rows; lo += 50_000 {
		p.insert(lo, lo+50_000)
	}
	p.compact()

	// Never quiet for 30s, so idle cannot be what fires.
	const step = 2_000
	for lo := 0; lo < rows/5-step; lo += step {
		p.advance(time.Second)
		p.del(lo, lo+step)
		p.reason("", fmt.Sprintf("%d of %d dead", lo+step, rows))
	}
	if len(tbl.State.DeltaFiles) != 0 {
		t.Fatalf("a delete-only workload wrote %d delta files", len(tbl.State.DeltaFiles))
	}
	p.advance(time.Second)
	p.del(rows/5-step, rows/5)
	p.reason("dead-fraction", "a fifth of the base dead")

	// A rewrite that fails must not be retried on the very next tick.
	tbl.CompactionFailed()
	p.reason("", "straight after a failed rewrite")
	p.advance(compactRetryAfter)
	p.reason("dead-fraction", "once the retry pause has passed")

	p.compact()
	p.reason("", "after the rewrite")
	if tbl.State.BaseRows != rows-rows/5 || p.baseDead() != 0 {
		t.Fatalf("base holds %d rows and %d dead, want %d and 0",
			tbl.State.BaseRows, p.baseDead(), rows-rows/5)
	}
}

// TestIndexRebuildKeepsDeltaPositions: an index rebuilt from a delta file that
// has a deletion vector must name each surviving row's position IN THE FILE.
// It used to name its position among the survivors, so the first delete after
// a restart or a Reload hit the wrong row.
func TestIndexRebuildKeepsDeltaPositions(t *testing.T) {
	tbl := racyTable(t)
	p := newPolicyRig(t, tbl)
	p.insert(1, 5) // ids 1..4 at positions 0..3 of one delta file
	p.del(1, 2)    // position 0 dead

	if err := tbl.Reload(); err != nil {
		t.Fatal(err)
	}
	p.del(2, 3) // id 2 is at position 1, not 0
	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, r := range rows {
		ids = append(ids, fmt.Sprint(r["id"]))
	}
	sort.Strings(ids)
	if !reflect.DeepEqual(ids, []string{"3", "4"}) {
		t.Fatalf("after deleting 1, reloading and deleting 2, the mirror holds %v, want [3 4]", ids)
	}
	checkDeadCache(t, tbl, "after a delete against a rebuilt index")
}

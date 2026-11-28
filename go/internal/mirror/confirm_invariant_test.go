package mirror

// THE CONFIRM INVARIANT, as a test of the pump's sequence rather than of one
// function. The sidecar (cmd/qs-mirror/main.go, the confirm after Apply) may
// tell the server a position P is consumed only when every transaction that
// commits below P is durable in every table that is not halted. Durable means
// the table's manifest on disk, or the root watermark that speaks for it, says
// the table has applied through P-1. So, read from disk at any instant:
//
//	confirmed(server) <= min over live tables of (effective applied LSN) + 1
//
// The model below runs the pump's tick: commits arrive in commit order, the
// replay gate holds a commit until the standby has replayed it (gatedPrefix),
// a batch is applied to every table, and then the position is confirmed through
// RunBeforeConfirm. A kill can fall anywhere: between two ticks, between two
// tables of one batch (a partial apply), or after the watermark. A kill drops
// all memory: the pending queue, the tables' in-memory state, the stream. The
// server keeps only what it was told is confirmed. Restart resumes the stream
// from that position, as the server would resend it.
//
// It asserts the invariant after every step and at every kill, and at the end
// that every table holds exactly the rows of every commit it was given.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

type modelCommit struct {
	commit uint64 // CommitLSN
	table  []string
	id     []int
}

// openModelTables opens the tables without registering cleanups, so the test
// can close and reopen them at each simulated crash.
func openModelTables(t *testing.T, root string, n int) []*Table {
	t.Helper()
	cols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
	var out []*Table
	for i := 0; i < n; i++ {
		tbl, err := New(root, "public", fmt.Sprintf("t%d", i), "id", cols, []string{"id", "v", "n"})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, tbl)
	}
	return out
}

func closeAll(tables []*Table) {
	for _, tbl := range tables {
		tbl.Close()
	}
}

// durableFloor is min over tables of the effective applied LSN read from disk,
// +1 (a position). A table with no durable state reports 0.
func durableFloor(t *testing.T, root string, n int) uint64 {
	t.Helper()
	min := ^uint64(0)
	for i := 0; i < n; i++ {
		lsn, err := EffectiveAppliedLSN(root, "public", fmt.Sprintf("t%d", i))
		if err != nil {
			t.Fatal(err)
		}
		at := changestream.ParseLSN(lsn)
		if lsn == "" {
			at = 0
		} else {
			at++
		}
		if at < min {
			min = at
		}
	}
	return min
}

func TestConfirmNeverPassesWhatIsNotDurable(t *testing.T) {
	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runConfirmModel(t, seed, 20)
		})
	}
}

// runConfirmModel: nCommits commits, kills kill points, replay moving in
// bursts with holds.
func runConfirmModel(t *testing.T, seed int64, kills int) {
	const nTables = 3
	const nCommits = 400
	rng := rand.New(rand.NewSource(seed))
	root := t.TempDir()

	// The stream: commits in commit order. Some touch no table (an idle WAL
	// position for this mirror), some touch two.
	var stream []modelCommit
	next := 0x1000
	id := 0
	for i := 0; i < nCommits; i++ {
		next += 0x40 + rng.Intn(0x200)
		c := modelCommit{commit: uint64(next)}
		n := rng.Intn(3)
		for k := 0; k < n; k++ {
			id++
			c.table = append(c.table, fmt.Sprintf("t%d", rng.Intn(nTables)))
			c.id = append(c.id, id)
		}
		stream = append(stream, c)
	}
	// want[table] = ids the table must hold once every commit is applied.
	want := map[string][]int{}
	for _, c := range stream {
		for k := range c.table {
			want[c.table[k]] = append(want[c.table[k]], c.id[k])
		}
	}

	// Server side: what the slot is confirmed through, and the standby's
	// replay position. Both outlive the sidecar.
	var confirmed uint64
	var replay uint64
	tables := openModelTables(t, root, nTables)
	defer func() { closeAll(tables) }()

	// Sidecar memory, lost at every kill.
	var pending []changestream.Transaction
	streamPos := 0 // next commit the stream will deliver; restart resends from confirmed
	killsDone := 0
	steps := 0

	restart := func() {
		closeAll(tables)
		pending = nil
		streamPos = 0
		for streamPos < len(stream) && stream[streamPos].commit < confirmed {
			streamPos++
		}
		tables = openModelTables(t, root, nTables)
		// planResume's rule: the slot may resume only where every table is at
		// most one position behind it.
		for i := 0; i < nTables; i++ {
			lsn, err := EffectiveAppliedLSN(root, "public", fmt.Sprintf("t%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if lsn != "" && confirmed > changestream.ParseLSN(lsn)+1 {
				t.Fatalf("restart: confirmed %X but table t%d durably applied only %s", confirmed, i, lsn)
			}
		}
	}

	for {
		steps++
		if steps > 200000 {
			t.Fatal("model did not finish")
		}
		// Standby replay advances in bursts; sometimes it holds for a while.
		if rng.Intn(4) != 0 {
			replay += uint64(rng.Intn(0x300))
		}
		// The stream delivers a few commits into memory, up to a bound.
		for k := 0; k < 1+rng.Intn(4) && streamPos < len(stream); k++ {
			c := stream[streamPos]
			streamPos++
			txn := changestream.Transaction{
				CommitLSN: lsnString(c.commit), NextLSN: lsnString(c.commit + 1),
			}
			for k := range c.table {
				txn.Changes = append(txn.Changes, wmChange(c.table[k], c.id[k], fmt.Sprintf("c%X", c.commit)))
			}
			pending = append(pending, txn)
		}

		// THE REPLAY GATE (replaygate.go): only the prefix the standby has
		// replayed may be applied.
		applyable := modelGatedPrefix(pending, replay)
		if applyable == 0 {
			if streamPos == len(stream) && len(pending) == 0 {
				break
			}
			if streamPos == len(stream) && replay >= stream[len(stream)-1].commit+1 {
				// Everything replayed: the gate opens for the rest.
				applyable = len(pending)
			} else {
				// Held. A kill may still land here.
				if killsDone < kills && rng.Intn(50) == 0 {
					assertDurable(t, root, nTables, confirmed, "kill while held")
					killsDone++
					restart()
				}
				continue
			}
		}
		head := pending[:applyable]
		if m := 1 + rng.Intn(6); len(head) > m {
			head = head[:m]
		}
		pending = append([]changestream.Transaction(nil), pending[len(head):]...)

		// Apply to every table, in order. A kill may fall between two tables.
		crashAt := -1
		if killsDone < kills && rng.Intn(30) == 0 {
			crashAt = 1 + rng.Intn(nTables-1)
		}
		for i, tbl := range tables {
			if i == crashAt {
				break
			}
			if _, err := tbl.Apply(head); err != nil {
				t.Fatal(err)
			}
		}
		if crashAt >= 0 {
			// Some tables have the batch on disk, the rest do not. The server
			// was never told, so it still holds the older position.
			assertDurable(t, root, nTables, confirmed, "kill between tables of one batch")
			killsDone++
			restart()
			continue
		}

		// Confirm: the watermark is made durable first (BeforeConfirm), then
		// the server is told. Here the position is the last applied commit's end.
		last := head[len(head)-1]
		if err := changestream.RunBeforeConfirm(last.NextLSN); err != nil {
			t.Fatal(err)
		}
		if p := changestream.ParseLSN(last.NextLSN); p > confirmed {
			confirmed = p
		}
		assertDurable(t, root, nTables, confirmed, "after confirm")

		if killsDone < kills && steps%6 == 0 {
			assertDurable(t, root, nTables, confirmed, "kill between ticks")
			killsDone++
			restart()
		}
	}

	// Every commit applied and confirmed: each table holds exactly its rows.
	if confirmed < stream[len(stream)-1].commit+1 {
		t.Fatalf("finished with confirmed %X, want past the last commit %X", confirmed, stream[len(stream)-1].commit)
	}
	for i, tbl := range tables {
		rows, err := tbl.Live()
		if err != nil {
			t.Fatal(err)
		}
		var got []int
		for _, r := range rows {
			var v int
			if _, err := fmt.Sscan(fmt.Sprint(r["id"]), &v); err != nil {
				t.Fatal(err)
			}
			got = append(got, v)
		}
		exp := append([]int(nil), want[fmt.Sprintf("t%d", i)]...)
		sort.Ints(got)
		sort.Ints(exp)
		if fmt.Sprint(got) != fmt.Sprint(exp) {
			t.Fatalf("seed %d, table t%d after %d kills: %d rows, want %d; a commit was lost or duplicated",
				seed, i, killsDone, len(got), len(exp))
		}
	}
	if killsDone < kills {
		t.Logf("only %d of %d kill points were reached", killsDone, kills)
	}
}

// modelGatedPrefix is cmd/qs-mirror's gatedPrefix (replaygate.go), which the
// mirror package cannot import: the leading transactions whose commit ends at
// or below the standby's replay position.
func modelGatedPrefix(txns []changestream.Transaction, replay uint64) int {
	for i, t := range txns {
		if changestream.ParseLSN(t.NextLSN) > replay {
			return i
		}
	}
	return len(txns)
}

// assertDurable is the invariant, read from disk. The server's confirmed
// position may not be ahead of any table's durable applied position + 1.
func assertDurable(t *testing.T, root string, n int, confirmed uint64, where string) {
	t.Helper()
	if confirmed == 0 {
		return
	}
	if floor := durableFloor(t, root, n); confirmed > floor {
		t.Fatalf("%s: slot confirmed through %X, but the least durable table is only through %X",
			where, confirmed, floor-1)
	}
}

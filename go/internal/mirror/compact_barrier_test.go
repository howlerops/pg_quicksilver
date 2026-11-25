package mirror

// What the apply loop does to a rewrite it did not wait for.
//
// A background rewrite is abandoned in two places: a DDL barrier (the schema it
// was reading is about to change) and a TRUNCATE (the files it was reading are
// about to go). Both used to leave something behind. The first left a goroutine
// still reading the column list the barrier was replacing; the second left a
// rewrite that would later be swapped in over a table that had been emptied,
// naming a file the truncate had already deleted.

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

type barrierApplier struct {
	t   *testing.T
	tbl *Table
	lsn int
}

func (a *barrierApplier) apply(changes []changestream.Change) {
	a.t.Helper()
	a.lsn++
	pos := fmt.Sprintf("0/%X", a.lsn)
	if _, err := a.tbl.Apply([]changestream.Transaction{{CommitLSN: pos, NextLSN: pos, Changes: changes}}); err != nil {
		a.t.Fatal(err)
	}
}

func (a *barrierApplier) insertBatches(batches, per int) {
	a.t.Helper()
	for b := 0; b < batches; b++ {
		ch := make([]changestream.Change, 0, per)
		for i := 0; i < per; i++ {
			r := map[string]any{"id": fmt.Sprint(b*per + i), "v": "x", "n": "1"}
			ch = append(ch, changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r})
		}
		a.apply(ch)
	}
}

// TestTruncateDuringCompaction is TRUNCATE on the source while a rewrite is in
// flight, and again after the rewrite has finished but before the pump has
// swapped it in. The source ends holding exactly one row; so must the mirror,
// now and after a restart.
func TestTruncateDuringCompaction(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "rewrite running"
		if late {
			name = "rewrite finished, not yet swapped"
		}
		t.Run(name, func(t *testing.T) {
			tbl := racyTable(t)
			a := &barrierApplier{t: t, tbl: tbl}
			a.insertBatches(4, 20_000)
			c := tbl.BeginCompaction()
			if c == nil {
				t.Fatal("no compaction started")
			}
			if late {
				<-c.done
			}
			r := map[string]any{"id": "7", "v": "after-truncate", "n": "1"}
			a.apply([]changestream.Change{
				{Op: changestream.OpTruncate, Schema: "public", Table: "r"},
				{Op: changestream.OpInsert, Schema: "public", Table: "r", Row: r, Key: r},
			})
			<-c.done
			// the pump's next tick
			if _, err := tbl.FinishCompaction(); err != nil {
				t.Fatalf("FinishCompaction after a truncate: %v", err)
			}
			check := func(what string, tb *Table) {
				rows, err := tb.Live()
				if err != nil {
					t.Fatalf("%s: mirror is unreadable: %v (base=%v delta=%v)",
						what, err, tb.State.BaseFiles, tb.State.DeltaFiles)
				}
				if len(rows) != 1 || rows[0]["v"] != "after-truncate" {
					t.Fatalf("%s: source holds 1 row, mirror holds %d (base=%v delta=%v)",
						what, len(rows), tb.State.BaseFiles, tb.State.DeltaFiles)
				}
			}
			check("live", tbl)
			restarted, err := New(filepath.Dir(tbl.Dir), "public", "r", "id", tbl.Columns, tbl.Order)
			if err != nil {
				t.Fatal(err)
			}
			check("after restart", restarted)
			// and the mirror keeps working: the abandoned rewrite's output must
			// not come back as a file, and a new rewrite must be able to start
			a.apply([]changestream.Change{{Op: changestream.OpInsert, Schema: "public", Table: "r",
				Row: map[string]any{"id": "8", "v": "later", "n": "2"}, Key: map[string]any{"id": "8"}}})
			c2 := tbl.BeginCompaction()
			if c2 == nil {
				t.Fatal("no compaction could start after the truncate")
			}
			<-c2.done
			if n, err := tbl.FinishCompaction(); err != nil || n != 2 {
				t.Fatalf("rewrite after truncate: rows=%d err=%v", n, err)
			}
			if rows, err := tbl.Live(); err != nil || len(rows) != 2 {
				t.Fatalf("after the next rewrite: %d rows, err=%v", len(rows), err)
			}
		})
	}
}

// TestEvolveDuringAbortedCompaction is exactly what a DDL barrier does while a
// rewrite is in flight: abort it, which does not wait, and adopt the new column
// list. Most of what it is here for is the race detector, which used to report
// the abandoned goroutine reading the column list, the column types and the
// missing-value map underneath the barrier writing them.
func TestEvolveDuringAbortedCompaction(t *testing.T) {
	tbl := racyTable(t)
	a := &barrierApplier{t: t, tbl: tbl}
	a.insertBatches(6, 20_000)
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction started")
	}
	time.Sleep(20 * time.Millisecond) // let the rewrite get into its read loop
	tbl.AbortCompaction()
	newCols := map[string]string{"id": "bigint", "v": "text", "n": "bigint", "channel": "text"}
	if err := tbl.Evolve(newCols, []string{"id", "v", "n", "channel"}, map[string]string{"channel": "web"}); err != nil {
		t.Fatal(err)
	}
	// keep applying against the evolved schema, as the pump would
	r := map[string]any{"id": "1", "v": "y", "n": "2", "channel": "mobile"}
	a.apply([]changestream.Change{{Op: changestream.OpUpdate, Schema: "public", Table: "r", Row: r, Key: r}})
	<-c.done

	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 120_000 {
		t.Fatalf("mirror holds %d rows, want 120000", len(rows))
	}
	for _, row := range rows {
		want := "web"
		if row["id"] == int64(1) {
			want = "mobile"
		}
		if row["channel"] != want {
			t.Fatalf("row %v reads channel=%v, want %q", row["id"], row["channel"], want)
		}
	}
}

// TestRepeatedEvolveDuringAbortedCompaction repeats the barrier for as long as
// the abandoned rewrite is still reading, to give the detector many chances.
// Every call is one the pump makes; only the frequency is unrealistic.
func TestRepeatedEvolveDuringAbortedCompaction(t *testing.T) {
	tbl := racyTable(t)
	a := &barrierApplier{t: t, tbl: tbl}
	a.insertBatches(8, 20_000)
	c := tbl.BeginCompaction()
	if c == nil {
		t.Fatal("no compaction started")
	}
	tbl.AbortCompaction()
	for n := 1; !c.Done(); n++ {
		newCols := map[string]string{"id": "bigint", "v": "text", "n": "bigint"}
		newOrder := []string{"id", "v", "n"}
		miss := map[string]string{}
		for j := 0; j <= n%3; j++ {
			name := fmt.Sprintf("c%d", j)
			newCols[name] = "text"
			newOrder = append(newOrder, name)
			miss[name] = fmt.Sprint(n)
		}
		if err := tbl.Evolve(newCols, newOrder, miss); err != nil {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	if rows, err := tbl.Live(); err != nil || len(rows) != 160_000 {
		t.Fatalf("after the barriers: %d rows, err=%v", len(rows), err)
	}
}

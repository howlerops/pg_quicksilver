package mirror

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// An abandoned rewrite stops between row groups and gives the process-wide
// rewrite slot back; it does not run to the end of the table first.
func TestAbandonedRewriteStopsAndFreesTheSlot(t *testing.T) {
	prev := RowGroupRows
	RowGroupRows = 64
	defer func() { RowGroupRows = prev }()

	root := t.TempDir()
	cols := map[string]string{"id": "bigint", "v": "text"}
	tbl, err := New(root, "public", "c", "id", cols, []string{"id", "v"})
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	mk := func(lsn string, from, to int, v string) changestream.Transaction {
		var ch []changestream.Change
		for i := from; i < to; i++ {
			ch = append(ch, changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "c",
				LSN: lsn, Row: map[string]any{"id": fmt.Sprint(i), "v": v}})
		}
		return changestream.Transaction{CommitLSN: lsn, NextLSN: lsn, Changes: ch}
	}
	if _, err := tbl.Apply([]changestream.Transaction{mk("0/10", 0, 20000, "a")}); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Apply([]changestream.Transaction{mk("0/20", 20000, 20500, "b")}); err != nil {
		t.Fatal(err)
	}

	abandoned := false
	for try := 0; try < 20 && !abandoned; try++ {
		c := tbl.BeginCompaction()
		if c == nil {
			t.Fatal("no rewrite began")
		}
		name := c.name
		tbl.AbortCompaction()
		<-c.done
		tbl.aborting.Wait()
		if errors.Is(c.err, errRewriteAbandoned) {
			abandoned = true
		}
		if _, err := os.Stat(filepath.Join(tbl.Dir, "base", name)); err == nil {
			t.Fatalf("the abandoned rewrite's output %s was left on disk", name)
		}
		if !tryAcquireRewriteSlot() {
			t.Fatal("the rewrite slot was not given back")
		}
		releaseRewriteSlot()
	}
	if !abandoned {
		t.Fatal("in 20 tries no rewrite was stopped early: the cancel flag is not being checked")
	}
	rows, err := tbl.Live()
	if err != nil || len(rows) != 20500 {
		t.Fatalf("after abandoned rewrites: %d rows, err %v", len(rows), err)
	}
}

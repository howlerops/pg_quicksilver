package mirror

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func orphanTxn(lsn string, ch ...changestream.Change) changestream.Transaction {
	for i := range ch {
		ch[i].Schema, ch[i].Table, ch[i].LSN = "public", "r", lsn
	}
	return changestream.Transaction{CommitLSN: lsn, NextLSN: lsn, Changes: ch}
}

// An Apply that publishes its deletion vector and then fails before the
// manifest leaves a generation on disk that no manifest names. A rewrite started
// before the replay must not fold that vector in: nothing was in `touched` to
// put the row back, so the mirror lost it until the replay arrived, and for
// good if the replay differed.
func TestCompactionIgnoresAVectorNoManifestNames(t *testing.T) {
	root := t.TempDir()
	cols := map[string]string{"id": "bigint", "v": "text", "big": "text"}
	order := []string{"id", "v", "big"}
	tbl, err := New(root, "public", "r", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", 3000)
	var ins []changestream.Change
	for i := 1; i <= 10; i++ {
		ins = append(ins, changestream.Change{Op: changestream.OpInsert,
			Row: map[string]any{"id": fmt.Sprint(i), "v": "old", "big": big}})
	}
	if _, err := tbl.Apply([]changestream.Transaction{orphanTxn("0/10", ins...)}); err != nil {
		t.Fatal(err)
	}
	// An Apply that gets as far as publishing its deletion vector and then
	// fails to write the manifest (or the process dies there).
	hook := func(path string, dir bool) error {
		if !dir && filepath.Base(path) == "state.json.tmp" {
			return errors.New("injected")
		}
		return nil
	}
	syncHook.Store(&hook)
	_, err = tbl.Apply([]changestream.Transaction{orphanTxn("0/20", changestream.Change{
		Op: changestream.OpUpdate, Row: map[string]any{"id": "5", "v": "new", "big": big + "y"}})})
	syncHook.Store(nil)
	if err == nil {
		t.Fatal("expected the injected failure")
	}

	// Restart.
	t2, err := New(root, "public", "r", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := t2.Live()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after restart: applied=%s rows=%d", t2.State.AppliedLSN, len(rows))
	if _, err := t2.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	c := t2.BeginCompaction()
	<-c.done
	if _, err := t2.FinishCompaction(); err != nil {
		t.Fatal(err)
	}
	rows, err = t2.Live()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after compaction, no Apply in between: applied=%s rows=%d", t2.State.AppliedLSN, len(rows))
	if len(rows) != 10 {
		t.Errorf("compaction alone changed the row count: %d, want 10", len(rows))
	}
	// The replay of the same update, as pgoutput sends it when big is TOASTed
	// and unchanged: the column is absent.
	if _, err := t2.Apply([]changestream.Transaction{orphanTxn("0/20", changestream.Change{
		Op: changestream.OpUpdate, Row: map[string]any{"id": "5", "v": "new"}})}); err != nil {
		t.Fatal(err)
	}
	rows, _ = t2.Live()
	for _, r := range rows {
		if fmt.Sprint(r["id"]) == "5" {
			t.Logf("row 5 after replay: v=%v big-is-nil=%v", r["v"], r["big"] == nil)
			if r["big"] == nil {
				t.Errorf("row 5 lost its unchanged TOAST column: NULL in the mirror")
			}
		}
	}
}

// An array is stored as text whatever its element type: the prefix match in
// arrowType used to make numeric(10,2)[] a decimal, and {1.00,2.50} is not one.
func TestArrayColumnsAreText(t *testing.T) {
	for _, typ := range []string{"numeric(10,2)[]", "numeric[]", "double precision[]",
		"integer[]", "timestamptz[]", "text[]"} {
		if got := arrowType(typ); got.ID() != arrowType("text").ID() {
			t.Errorf("arrowType(%q) = %s, want the text type", typ, got)
		}
	}
	root := t.TempDir()
	cols := map[string]string{"id": "bigint", "a": "numeric(10,2)[]", "f": "double precision[]"}
	tbl, err := New(root, "public", "r", "id", cols, []string{"id", "a", "f"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Apply([]changestream.Transaction{orphanTxn("0/10", changestream.Change{
		Op: changestream.OpInsert, Row: map[string]any{"id": "1", "a": "{1.00,2.50}", "f": "{1.5,NaN}"}})}); err != nil {
		t.Fatalf("an array value halted the table: %v", err)
	}
	rows, err := tbl.Live()
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	if rows[0]["a"] != "{1.00,2.50}" || rows[0]["f"] != "{1.5,NaN}" {
		t.Errorf("array did not round-trip: %v", rows[0])
	}
}

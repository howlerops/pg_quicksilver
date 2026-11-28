package mirror

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// An abandoned rewrite's output is removed by a goroutine after the rewrite
// stops. Its name comes from State.Seq, and Reload moves Seq back to what the
// manifest on disk holds, so the next rewrite could take the same name while
// that goroutine is still removing it. The guard (abortPending) keeps a new
// rewrite from starting until the removal has run. Each round abandons a
// rewrite, reloads, and immediately runs a successful one; the base it publishes
// must exist.
func TestAbortedRewriteNameIsNotReusedBeforeItsCleanup(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	_, ord := openDomainTables(t, root, nil)
	var ops []changestream.Change
	for i := 0; i < 60; i++ {
		ops = append(ops, oiIns(i+1, dmUUID(i)))
	}
	applyTxn(t, ord, &lsn, ops...)
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}

	// abandon starts a rewrite and aborts it. BeginCompaction may refuse while
	// an earlier abandoned rewrite is being cleaned up; wait for it.
	abandon := func() {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if c := ord.BeginCompaction(); c != nil {
				ord.AbortCompaction()
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("BeginCompaction never accepted a rewrite after an abort")
			}
			time.Sleep(time.Millisecond)
		}
	}
	for round := 0; round < 200; round++ {
		abandon()
		if err := ord.Reload(); err != nil {
			t.Fatal(err)
		}
		if _, err := ord.Compact(); err != nil {
			t.Fatalf("round %d: rewrite after an abandoned one: %v", round, err)
		}
		for _, f := range ord.State.BaseFiles {
			if _, err := os.Stat(filepath.Join(ord.Dir, "base", f)); err != nil {
				t.Fatalf("round %d: manifest names %s but it is gone: %v", round, f, err)
			}
		}
	}
	rows, err := ord.Live()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 60 {
		t.Fatalf("%d rows after the aborts, want 60", len(rows))
	}
	ord.Close()
}

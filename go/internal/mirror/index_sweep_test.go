package mirror

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func indexEntries(t *testing.T, tbl *Table) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(tbl.Dir, "index"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func buckets(names []string) (out []string) {
	for _, n := range names {
		if strings.Contains(n, scratchBucket) {
			out = append(out, n)
		}
	}
	return out
}

func reopenCluster(t *testing.T, root string) *Table {
	t.Helper()
	cols := map[string]string{"id": "bigint", "ts": "timestamp without time zone", "v": "text", "n": "bigint"}
	tbl, err := New(root, "public", "c", "id", cols, []string{"id", "ts", "v", "n"})
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

// A sidecar killed while an order-preserving rewrite had its buckets on disk.
// Nothing removed them: the retention sweep covered base/, delta/ and dv/, and
// dropKidx only the names of the sequence number the rewrite was writing,
// which the replay has moved past. The writer's first manifest write now
// does; a process that only reads the mirror removes nothing.
func TestIndexScratchIsSweptAfterACrash(t *testing.T) {
	withRowGroup(t, 100)
	withClusterBy(t, "public.c=ts")
	m, _ := buildScattered(t, 3, 0)
	root := filepath.Dir(m.tbl.Dir)

	// The crash: the directory exactly as it stood with the buckets written.
	crashed := filepath.Join(t.TempDir(), "mirror")
	afterSpill = func(*clusterRewrite) {
		if err := os.CopyFS(crashed, os.DirFS(root)); err != nil {
			t.Error(err)
		}
	}
	defer func() { afterSpill = nil }()
	if c := m.rewrite(); c.clusterBy == "" {
		t.Fatalf("the rewrite was not clustered (%s); it spilled nothing", c.clusterWhy)
	}
	afterSpill = nil

	// A read-only user of the package (qs-query, qs-verify) opens it first.
	ro := reopenCluster(t, crashed)
	left := buckets(indexEntries(t, ro))
	if len(left) < 2 {
		t.Fatalf("the simulated crash left no buckets to sweep: %v", indexEntries(t, ro))
	}
	var bytes int64
	for _, n := range left {
		if fi, err := os.Stat(filepath.Join(ro.Dir, "index", n)); err == nil {
			bytes += fi.Size()
		}
	}
	t.Logf("a crash mid-rewrite left %d buckets, %d bytes", len(left), bytes)
	// And what the same crash left under the previous naming, and of an index
	// build, and the index of a base file no manifest names any more.
	for _, n := range []string{"000099.kidx.delta0", "000099.kidx.run0", "000099.kidx.tmp123", "000098.kidx"} {
		if err := os.WriteFile(filepath.Join(ro.Dir, "index", n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	before := indexEntries(t, ro)
	if _, err := ro.Live(); err != nil {
		t.Fatal(err)
	}
	if _, err := ro.ViewSQL(); err != nil {
		t.Fatal(err)
	}
	if _, err := ro.BuildIndex(); err != nil {
		t.Fatal(err)
	}
	ro.Close()
	if after := indexEntries(t, ro); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("reading the mirror changed index/:\n  %v\n  %v", before, after)
	}

	// The writer comes back and applies one change.
	w := reopenCluster(t, crashed)
	defer w.Close()
	manualGrace(w, time.Hour)
	row := map[string]any{"id": "900001", "ts": tsAt(1), "v": "after", "n": "1"}
	if _, err := w.Apply([]changestream.Transaction{txn("0/FFFF", changestream.Change{
		Op: changestream.OpInsert, Schema: "public", Table: "c", Row: row, Key: row})}); err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, b := range w.State.BaseFiles {
		want = append(want, trimParquet(b)+".kidx")
	}
	if got := indexEntries(t, w); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("after the writer's first manifest write index/ holds %v, expected only %v", got, want)
	}
	if rows, err := w.Live(); err != nil || len(rows) != len(m.want)+1 {
		t.Fatalf("the table reads %d rows (%v), expected %d", len(rows), err, len(m.want)+1)
	}
	checkInvariants(t, w, "after the sweep")
}

// The sweep runs while a rewrite is live, in a table that also holds a dead
// rewrite's buckets under the same base name. It takes the dead ones and none
// of the live ones, and the rewrite finishes.
func TestIndexSweepLeavesALiveRewriteAlone(t *testing.T) {
	withRowGroup(t, 100)
	withClusterBy(t, "public.c=ts")
	m, _ := buildScattered(t, 3, 0)
	root := filepath.Dir(m.tbl.Dir)
	m.tbl.Close()
	// A new Table: its first manifest write is still ahead of it.
	m.tbl = reopenCluster(t, root)
	defer m.tbl.Close()
	if _, err := m.tbl.BuildIndex(); err != nil {
		t.Fatal(err)
	}

	spilled, release := make(chan struct{}), make(chan struct{})
	afterSpill = func(*clusterRewrite) {
		close(spilled)
		<-release
	}
	freed := false
	free := func() {
		if !freed {
			freed = true
			close(release)
		}
	}
	defer func() { afterSpill = nil }()
	defer free() // a failure below must not leave Close waiting on the rewrite
	c := m.tbl.BeginCompaction()
	if c == nil {
		t.Fatal("BeginCompaction returned nil")
	}
	<-spilled
	live := buckets(indexEntries(t, m.tbl))
	if len(live) < 2 {
		t.Fatalf("the live rewrite has no buckets: %v", indexEntries(t, m.tbl))
	}
	// A dead rewrite of the SAME base name, by another process, and by this
	// one earlier (a prefix that is no longer registered).
	stem := trimParquet(c.name)
	dead := []string{
		stem + scratchMark + "1-1" + scratchBucket + "0",
		fmt.Sprintf("%s%s%d-0%s0", stem, scratchMark, os.Getpid(), scratchBucket),
		stem + ".kidx.delta0",
	}
	for _, n := range dead {
		if err := os.WriteFile(filepath.Join(m.tbl.Dir, "index", n), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// The first manifest write of this Table, with the rewrite in flight.
	m.apply([]changestream.Change{m.put(changestream.OpInsert, map[string]any{
		"id": "900001", "ts": tsAt(2), "v": "meanwhile", "n": "1"})})
	now := indexEntries(t, m.tbl)
	if got := buckets(now); fmt.Sprint(got) != fmt.Sprint(live) {
		t.Fatalf("the sweep changed a live rewrite's buckets:\n  before %v\n  after  %v", live, got)
	}
	for _, n := range dead[2:] {
		if contains(now, n) {
			t.Errorf("the sweep left %s", n)
		}
	}

	free()
	<-c.done
	if _, err := m.tbl.FinishCompaction(); err != nil {
		t.Fatalf("the rewrite failed after the sweep: %v", err)
	}
	if c.clusterBy == "" {
		t.Fatalf("the rewrite was not clustered: %s", c.clusterWhy)
	}
	m.check("ts", "v", "n")
	checkInvariants(t, m.tbl, "after the rewrite")
	if got := indexEntries(t, m.tbl); len(got) != 1 || got[0] != stem+".kidx" {
		t.Fatalf("after the rewrite index/ holds %v, expected only %s.kidx", got, stem)
	}
}

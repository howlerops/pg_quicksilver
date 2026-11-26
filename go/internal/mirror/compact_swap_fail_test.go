package mirror

// A swap that fails part-way.
//
// FinishCompaction writes a vector, pays the directory syncs the rewrite's
// files are owed, and replaces the manifest. Each of those is an fsync or a
// write that can fail, and before this test existed a failure at any of them
// returned with the rewrite's output still on disk under no manifest — or,
// past the point where memory had been swapped, with a Table describing files
// its manifest did not and the old ones never collected.
//
// So every one of them is failed here, one at a time, and after each the test
// requires ONE consistent set: either the table carries on exactly as it was,
// with nothing of the rewrite left on disk, or it says it must be reopened and
// the reopened table holds the right rows and, after its first write, exactly
// the files its manifest names.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

type swapScenario struct {
	tbl  *Table
	c    *Compaction
	want map[string]string
	lsn  int
}

func (s *swapScenario) apply(t *testing.T, tbl *Table, changes ...changestream.Change) error {
	t.Helper()
	s.lsn++
	_, err := tbl.Apply([]changestream.Transaction{txn(fmt.Sprintf("0/%X", s.lsn), changes...)})
	return err
}

func (s *swapScenario) put(id int, v string) changestream.Change {
	s.want[itoa(id)] = v
	return rChange(changestream.OpInsert, id, v, id)
}

func (s *swapScenario) del(id int) changestream.Change {
	delete(s.want, itoa(id))
	return rChange(changestream.OpDelete, id, "", 0)
}

// newSwapScenario builds a table with a base, a vector over it and deltas,
// runs a rewrite to completion, and applies changes behind its back so that
// the swap has rows to mark dead in the new base — which is what makes it
// write a vector. It stops just short of FinishCompaction.
func newSwapScenario(t *testing.T) *swapScenario {
	t.Helper()
	tbl := racyTable(t)
	manualGrace(tbl, 0)
	s := &swapScenario{tbl: tbl, want: map[string]string{}}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	var ch []changestream.Change
	for i := 0; i < 1000; i++ {
		ch = append(ch, s.put(i, "a"))
	}
	must(s.apply(t, tbl, ch...))
	_, err := tbl.Compact()
	must(err)
	ch = nil
	for i := 0; i < 200; i++ {
		ch = append(ch, s.put(i, "b"))
	}
	for i := 200; i < 250; i++ {
		ch = append(ch, s.del(i))
	}
	must(s.apply(t, tbl, ch...))
	s.c = tbl.BeginCompaction()
	if s.c == nil {
		t.Fatal("no rewrite started")
	}
	<-s.c.done
	ch = nil
	for i := 100; i < 150; i++ {
		ch = append(ch, s.put(i, "c"))
	}
	for i := 300; i < 310; i++ {
		ch = append(ch, s.del(i))
	}
	for i := 2000; i < 2010; i++ {
		ch = append(ch, s.put(i, "d"))
	}
	must(s.apply(t, tbl, ch...))
	return s
}

// diskFiles is every file under the table's directory but the two it always
// has, relative to it.
func diskFiles(t *testing.T, tbl *Table) []string {
	t.Helper()
	var out []string
	err := filepath.Walk(tbl.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(tbl.Dir, path)
		if info.IsDir() || rel == "state.json" || rel == PublishedFile {
			return nil
		}
		out = append(out, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}

// manifestSet is what should be on disk for the manifest in memory: the files
// it names and the index of its base file.
func manifestSet(tbl *Table) []string {
	out := append([]string(nil), tbl.manifestFiles()...)
	for _, b := range tbl.State.BaseFiles {
		if DiskIndex {
			out = append(out, filepath.Join("index", trimParquet(b)+".kidx"))
		}
	}
	sort.Strings(out)
	return out
}

func liveByID(t *testing.T, tbl *Table) map[string]string {
	t.Helper()
	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, r := range rows {
		id := Render(r["id"])
		if _, dup := out[id]; dup {
			t.Fatalf("key %s is live twice", id)
		}
		out[id] = Render(r["v"])
	}
	return out
}

func swapRows(t *testing.T, when string, got, want map[string]string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d live rows, want %d", when, len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: key %s is %q, want %q", when, k, got[k], v)
		}
	}
}

func swapFiles(t *testing.T, when string, tbl *Table) {
	t.Helper()
	got, want := diskFiles(t, tbl), manifestSet(tbl)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("%s: on disk\n  %s\nthe manifest names\n  %s", when,
			strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestSwapFailureLeavesOneSet(t *testing.T) {
	errInjected := errors.New("injected failure")

	// What a swap syncs, in order, when nothing fails.
	type call struct {
		path string
		dir  bool
	}
	var calls []call
	failAt := -1
	hook := func(path string, dir bool) error {
		calls = append(calls, call{path, dir})
		if len(calls)-1 == failAt {
			return errInjected
		}
		return nil
	}
	arm := func(at int) {
		calls, failAt = nil, at
		syncHook.Store(&hook)
	}
	disarm := func() { syncHook.Store(nil) }
	t.Cleanup(disarm)

	s := newSwapScenario(t)
	arm(-1)
	n, err := s.tbl.FinishCompaction()
	disarm()
	if err != nil || n == 0 {
		t.Fatalf("the swap with nothing failing: %d, %v", n, err)
	}
	swapFiles(t, "after a clean swap", s.tbl)
	swapRows(t, "after a clean swap", liveByID(t, s.tbl), s.want)
	if s.tbl.State.DVGen[dvStem(filepath.Join("base", s.c.name))] != 1 {
		t.Fatal("the scenario's swap wrote no vector for the new base, so its failure is not being tested")
	}
	clean := append([]call(nil), calls...)
	if len(clean) < 5 {
		t.Fatalf("the swap made only %d syncs: %v", len(clean), clean)
	}
	s.tbl.Close()

	type failure struct {
		name string
		// plant makes a write fail without the hook; it returns what to undo
		// before the disk is compared (the obstruction is the test's, not a leak).
		plant func(s *swapScenario) (undo func())
		at    int
	}
	var failures []failure
	for i, c := range clean {
		what := "file"
		if c.dir {
			what = "directory"
		}
		rel, _ := filepath.Rel(filepath.Dir(filepath.Dir(s.tbl.Dir)), c.path)
		failures = append(failures, failure{name: fmt.Sprintf("fsync %d of %d: %s %s", i+1, len(clean), what, filepath.Base(filepath.Dir(rel))+"/"+filepath.Base(rel)), at: i})
	}
	dvOf := func(s *swapScenario) string { return s.tbl.dvPathGen(filepath.Join("base", s.c.name), 1) }
	failures = append(failures,
		failure{name: "the vector's temporary cannot be created", at: -1, plant: func(s *swapScenario) func() {
			if err := os.Mkdir(dvOf(s)+".tmp", 0o755); err != nil {
				t.Fatal(err)
			}
			return func() {}
		}},
		failure{name: "the vector cannot be renamed into place", at: -1, plant: func(s *swapScenario) func() {
			p := dvOf(s)
			if err := os.MkdirAll(filepath.Join(p, "in-the-way"), 0o755); err != nil {
				t.Fatal(err)
			}
			return func() { _ = os.RemoveAll(p) }
		}},
		failure{name: "the manifest's temporary cannot be created", at: -1, plant: func(s *swapScenario) func() {
			if err := os.Mkdir(s.tbl.statePath()+".tmp", 0o755); err != nil {
				t.Fatal(err)
			}
			return func() {}
		}},
	)

	carriedOn, reopened := 0, 0
	for _, f := range failures {
		t.Run(f.name, func(t *testing.T) {
			s := newSwapScenario(t)
			tbl := s.tbl
			newStem := trimParquet(s.c.name)
			// What was there before the swap, less the rewrite's own output:
			// what must be there after a swap that did not happen. (It can
			// hold a file the last published snapshot still names, which the
			// next publish collects; that is not the swap's.)
			var asItWas []string
			for _, name := range diskFiles(t, tbl) {
				if !strings.HasPrefix(filepath.Base(name), newStem+".") {
					asItWas = append(asItWas, name)
				}
			}
			undo := func() {}
			if f.plant != nil {
				undo = f.plant(s)
			}
			arm(f.at)
			_, err := tbl.FinishCompaction()
			disarm()
			undo()
			if err == nil {
				t.Fatal("the swap reported success")
			}
			if f.at >= 0 && !errors.Is(err, errInjected) {
				t.Fatalf("the swap failed with something else: %v", err)
			}
			if tbl.Compacting() {
				t.Fatal("the failed rewrite is still in flight")
			}

			if !errors.Is(err, ErrNeedsReopen) {
				// The table carries on as it was. Nothing of the rewrite is left.
				carriedOn++
				for _, name := range diskFiles(t, tbl) {
					if strings.HasPrefix(filepath.Base(name), newStem+".") || strings.HasSuffix(name, ".tmp") {
						t.Fatalf("the failed swap left %s", name)
					}
				}
				if got := diskFiles(t, tbl); strings.Join(got, "\n") != strings.Join(asItWas, "\n") {
					t.Fatalf("after the failed swap the disk holds\n  %s\nand before it held\n  %s",
						strings.Join(got, "\n  "), strings.Join(asItWas, "\n  "))
				}
				swapRows(t, "after the failed swap", liveByID(t, tbl), s.want)
				if err := s.apply(t, tbl, s.put(3000, "e"), s.del(1)); err != nil {
					t.Fatalf("the table does not apply after a failed swap: %v", err)
				}
				swapRows(t, "after an apply", liveByID(t, tbl), s.want)
			} else {
				// The table says it cannot be trusted. Whoever opens it next
				// finds the right rows and, once it has written, one set.
				reopened++
				if err := s.apply(t, tbl, s.put(3000, "e")); !errors.Is(err, ErrNeedsReopen) {
					t.Fatalf("a table that needs reopening applied: %v", err)
				}
				delete(s.want, itoa(3000))
				if _, err := tbl.FinishCompaction(); err != nil {
					t.Fatalf("FinishCompaction with nothing in flight: %v", err)
				}
				tbl.Close()
				tbl, err = New(filepath.Dir(tbl.Dir), "public", "r", "id", tbl.Columns, tbl.Order)
				if err != nil {
					t.Fatal(err)
				}
				manualGrace(tbl, 0)
				swapRows(t, "reopened", liveByID(t, tbl), s.want)
				if err := s.apply(t, tbl, s.put(3000, "e"), s.del(1)); err != nil {
					t.Fatal(err)
				}
				swapRows(t, "reopened, after an apply", liveByID(t, tbl), s.want)
			}

			// And the next rewrite goes through, and leaves exactly its own
			// set: a file the failed swap had orphaned would still be here.
			// (Not compared before this: a superseded generation of a vector
			// outlives the publish that replaced it by one, by design.)
			c := tbl.BeginCompaction()
			if c == nil {
				t.Fatal("no rewrite started after the failure")
			}
			<-c.done
			if _, err := tbl.FinishCompaction(); err != nil {
				t.Fatalf("the rewrite after the failure: %v", err)
			}
			swapFiles(t, "after the next rewrite", tbl)
			swapRows(t, "after the next rewrite", liveByID(t, tbl), s.want)
			if idx := indexDump(t, tbl); len(idx) != len(s.want) {
				t.Fatalf("the index holds %d keys, want %d", len(idx), len(s.want))
			}
			tbl.Close()
		})
	}
	if carriedOn == 0 || reopened == 0 {
		t.Fatalf("%d failures left the table as it was and %d needed a reopen: both must be exercised", carriedOn, reopened)
	}
	t.Logf("%d injected failures: %d left the table as it was, %d needed a reopen", len(failures), carriedOn, reopened)
}

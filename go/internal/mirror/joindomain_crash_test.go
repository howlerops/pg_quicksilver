package mirror

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// The crash ordering for join dictionaries. An Apply that assigns new ids
// writes them to dict.log; the manifest that could name a file holding them is
// written only after dict.log is fsynced. If that ordering held, a crash at any
// point leaves every file the manifest names readable against the dictionary
// log that survived.
//
// The test fails each fsync of such an Apply in turn (through syncHook), then
// simulates the crash: the dictionary log is cut back to the last length that
// was durable, as a power loss would leave it. It reopens, and checks that every
// row of every file the manifest names has a companion equal to the dictionary
// id of its member value. A companion the surviving log cannot explain is the
// failure this exists to catch.

// crashRecorder tracks what the sync hook has seen and fails one call.
type crashRecorder struct {
	mu          sync.Mutex
	dictLog     string
	calls       int
	failAt      int // 1-based call to fail; 0 for none
	durableSize int64
	failedPath  string
}

func (c *crashRecorder) hook(path string, dir bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.calls == c.failAt {
		c.failedPath = path
		return errors.New("injected fsync failure")
	}
	if path == c.dictLog {
		// The sync that follows this call succeeds, so the log is durable now
		// at the length it has.
		if st, err := os.Stat(path); err == nil {
			c.durableSize = st.Size()
		}
	}
	return nil
}

func installCrashHook(t *testing.T, c *crashRecorder) {
	t.Helper()
	h := func(path string, dir bool) error { return c.hook(path, dir) }
	syncHook.Store(&h)
	t.Cleanup(func() { syncHook.Store(nil) })
}

// crashRows reads every row of every file the manifest names, dead or not,
// with its member and companion, and checks each companion against the
// dictionary. It returns the number of rows checked.
func crashRows(t *testing.T, tbl *Table, jd *JoinDomains) (int, error) {
	t.Helper()
	checked := 0
	rels := []string{}
	for _, f := range tbl.State.BaseFiles {
		rels = append(rels, filepath.Join("base", f))
	}
	for _, f := range tbl.State.DeltaFiles {
		rels = append(rels, filepath.Join("delta", f))
	}
	for _, dc := range tbl.DomainColumns() {
		d := jd.dicts[dc.Domain]
		for _, rel := range rels {
			path := filepath.Join(tbl.Dir, rel)
			_, order, err := schemaOf(path)
			if err != nil {
				return checked, fmt.Errorf("%s: %w", rel, err)
			}
			has := map[string]bool{}
			for _, c := range order {
				has[c] = true
			}
			if !has[dc.Column] {
				continue
			}
			if err := tbl.forEachRowsWant(path, nil, []string{dc.Column, dc.Companion}, false, 0,
				func(rs []map[string]any, _ int) error {
					for _, r := range rs {
						checked++
						m := r[dc.Column]
						comp := r[dc.Companion]
						if m == nil {
							if comp != nil {
								return fmt.Errorf("%s: NULL member with companion %v", rel, comp)
							}
							continue
						}
						key, err := joinValueKey(m)
						if err != nil {
							return err
						}
						id, ok := d.Lookup(key)
						if !ok {
							return fmt.Errorf("%s: a row names %v, which the surviving dictionary does not hold", rel, m)
						}
						if comp != id {
							return fmt.Errorf("%s: companion %v for %v, dictionary %d", rel, comp, m, id)
						}
					}
					return nil
				}); err != nil {
				return checked, err
			}
		}
	}
	return checked, nil
}

func TestJoinDomainCrashOrdering(t *testing.T) {
	var outcomes []string
	for k := 1; k <= 12; k++ {
		root := t.TempDir()
		var lsn lsnSeq
		specs, err := ParseJoinDomains(dmSpec)
		if err != nil {
			t.Fatal(err)
		}
		jd, err := OpenJoinDomains(root, specs, 0)
		if err != nil {
			t.Fatal(err)
		}
		cust, ord := openDomainTables(t, root, jd)
		rec := &crashRecorder{dictLog: filepath.Join(root, "_domains", "cust", joinDictLogName)}
		installCrashHook(t, rec)

		// A baseline that is fully durable: 20 customers, 40 orders.
		var base []changestream.Change
		for i := 0; i < 20; i++ {
			base = append(base, ciIns(dmUUID(i)))
		}
		applyTxn(t, cust, &lsn, base...)
		var ob []changestream.Change
		for i := 0; i < 40; i++ {
			ob = append(ob, oiIns(i+1, dmUUID(i%20)))
		}
		applyTxn(t, ord, &lsn, ob...)

		// The Apply under test assigns new ids: 10 customers never seen, and
		// orders pointing at them.
		rec.mu.Lock()
		rec.calls = 0
		rec.failAt = k
		rec.mu.Unlock()
		var nw []changestream.Change
		for i := 20; i < 30; i++ {
			nw = append(nw, ciIns(dmUUID(i)))
		}
		l := lsn.next()
		for i := range nw {
			nw[i].LSN = l
		}
		_, applyErr := cust.Apply([]changestream.Transaction{{CommitLSN: l, NextLSN: l, Changes: nw}})

		rec.mu.Lock()
		failed := rec.failedPath
		durable := rec.durableSize
		rec.failAt = 0
		rec.mu.Unlock()
		// Fewer than k fsyncs in this Apply: nothing failed, the Apply must have
		// succeeded, and its manifest names the new ids. That case is checked too:
		// it is the one where a missing dictionary sync would show.
		last := failed == ""
		if last && applyErr != nil {
			t.Fatalf("k=%d: no fsync failed but Apply returned %v", k, applyErr)
		}

		// The crash: the dictionary log keeps only what was durable.
		if err := os.Truncate(rec.dictLog, durable); err != nil {
			t.Fatal(err)
		}
		_ = jd.Close()
		syncHook.Store(nil)

		// Restart: reopen the dictionary and the tables.
		jd2 := domainsFor(t, root, dmSpec, 0)
		cust2, ord2 := openDomainTables(t, root, jd2)
		for _, tbl := range []*Table{cust2, ord2} {
			n, err := crashRows(t, tbl, jd2)
			if err != nil {
				t.Fatalf("k=%d (fsync of %s failed; apply err %v): %v", k, filepath.Base(failed), applyErr, err)
			}
			if n == 0 {
				t.Fatalf("k=%d: no rows checked in %s", k, tbl.Qualified)
			}
		}
		if last {
			outcomes = append(outcomes, fmt.Sprintf("clean apply (%d fsyncs in total): crash after it, manifest rows checked ok", k-1))
			break
		}
		outcomes = append(outcomes, fmt.Sprintf("fsync %d (%s): apply err=%v, manifest rows checked ok", k, shortPath(failed), applyErr != nil))
	}
	if len(outcomes) == 0 {
		t.Fatal("no fsync failure was exercised")
	}
	t.Logf("%s", strings.Join(outcomes, "\n"))
}

func shortPath(p string) string {
	parts := strings.Split(filepath.ToSlash(p), "/")
	if len(parts) >= 2 {
		return strings.Join(parts[len(parts)-2:], "/")
	}
	return p
}

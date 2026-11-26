package mirror

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// jk makes a distinct, non-zero 16-byte key from i.
func jk(i int) [joinDictRecordSize]byte {
	var k [joinDictRecordSize]byte
	binary.BigEndian.PutUint64(k[8:], uint64(i)+1)
	k[0] = 0xAB
	return k
}

// countSyncs installs a syncHook that counts fsyncs (and fails them while
// fail is set), and removes it when the test ends.
func countSyncs(t *testing.T) (*atomic.Int64, *atomic.Bool) {
	t.Helper()
	var n atomic.Int64
	var fail atomic.Bool
	h := func(path string, dir bool) error {
		n.Add(1)
		if fail.Load() {
			return errors.New("injected fsync failure")
		}
		return nil
	}
	syncHook.Store(&h)
	t.Cleanup(func() { syncHook.Store(nil) })
	return &n, &fail
}

func mustOpen(t *testing.T, dir string, max int64) *joinDict {
	t.Helper()
	d, err := openJoinDict(dir, max)
	if err != nil {
		t.Fatalf("openJoinDict: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestJoinDictIDsStableAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "_domains", "cust")
	d := mustOpen(t, dir, 0)
	const n = 1000
	for i := 0; i < n; i++ {
		id, err := d.Assign(jk(i))
		if err != nil {
			t.Fatal(err)
		}
		if id != int64(i+1) {
			t.Fatalf("value %d got id %d, want %d (dense, first-seen order)", i, id, i+1)
		}
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d2 := mustOpen(t, dir, 0)
	if d2.Len() != n {
		t.Fatalf("reopened with %d keys, want %d", d2.Len(), n)
	}
	for i := 0; i < n; i++ {
		id, ok := d2.Lookup(jk(i))
		if !ok || id != int64(i+1) {
			t.Fatalf("after restart value %d has id %d (ok=%v), want %d", i, id, ok, i+1)
		}
	}
	// A new value continues the sequence; the old ones are not reused.
	id, err := d2.Assign(jk(n))
	if err != nil || id != n+1 {
		t.Fatalf("new value after restart got id %d, err %v; want %d", id, err, n+1)
	}
}

func TestJoinDictTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, 0)
	for i := 0; i < 3; i++ {
		if _, err := d.Assign(jk(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = d.Close()

	path := filepath.Join(dir, joinDictLogName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	d2 := mustOpen(t, dir, 0)
	if d2.Len() != 3 {
		t.Fatalf("torn tail: %d keys, want 3", d2.Len())
	}
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != 3*joinDictRecordSize {
		t.Fatalf("torn tail not truncated: log is %d bytes, want %d", st.Size(), 3*joinDictRecordSize)
	}
	// The first new id is 4, and the log it lands in is whole records.
	id, err := d2.Assign(jk(99))
	if err != nil || id != 4 {
		t.Fatalf("id after torn tail = %d, err %v; want 4", id, err)
	}
	if err := d2.Sync(); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(path); st.Size() != 4*joinDictRecordSize {
		t.Fatalf("log is %d bytes after one append, want %d", st.Size(), 4*joinDictRecordSize)
	}
}

func TestJoinDictDuplicateRecordIsCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, joinDictLogName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	rec := jk(7)
	if err := os.WriteFile(path, append(append([]byte{}, rec[:]...), rec[:]...), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openJoinDict(dir, 0); err == nil {
		t.Fatal("a log that repeats a value opened without error; ids would be ambiguous")
	}
}

func TestJoinDictCapDisablesDurably(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, 3)
	for i := 0; i < 3; i++ {
		if _, err := d.Assign(jk(i)); err != nil {
			t.Fatalf("value %d under the cap: %v", i, err)
		}
	}
	if _, err := d.Assign(jk(3)); !errors.Is(err, ErrJoinDomainDisabled) {
		t.Fatalf("value over the cap: err %v, want ErrJoinDomainDisabled", err)
	}
	off, reason := d.Disabled()
	if !off || reason == "" {
		t.Fatalf("Disabled() = %v, %q; want true with a reason", off, reason)
	}
	if _, err := os.Stat(filepath.Join(dir, joinDictDisabled)); err != nil {
		t.Fatalf("no durable disabled marker: %v", err)
	}
	// Values already assigned still answer; new ones do not.
	if id, err := d.Assign(jk(1)); err != nil || id != 2 {
		t.Fatalf("existing value on a disabled domain: id %d err %v", id, err)
	}
	if _, err := d.Assign(jk(4)); !errors.Is(err, ErrJoinDomainDisabled) {
		t.Fatalf("new value on a disabled domain: err %v", err)
	}
	_ = d.Close()

	// It stays disabled across a restart, and the restart does not reassign.
	d2 := mustOpen(t, dir, 0) // a larger cap on restart does not re-enable it
	if off, _ := d2.Disabled(); !off {
		t.Fatal("domain re-enabled by restart")
	}
	if _, err := d2.Assign(jk(5)); !errors.Is(err, ErrJoinDomainDisabled) {
		t.Fatalf("new value after restart on disabled domain: err %v", err)
	}
}

func TestJoinDictAssignOfExistingValueDoesNotWrite(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, 0)
	if _, err := d.Assign(jk(1)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, joinDictLogName)
	before, _ := os.Stat(path)

	n, _ := countSyncs(t)
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	firstSyncs := n.Load()

	for i := 0; i < 100; i++ {
		if id, err := d.Assign(jk(1)); err != nil || id != 1 {
			t.Fatalf("re-assign: id %d err %v", id, err)
		}
	}
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Fatalf("re-assigning an existing value grew the log from %d to %d bytes", before.Size(), after.Size())
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	if n.Load() != firstSyncs {
		t.Fatalf("Sync after only re-assigns issued %d fsyncs, want 0", n.Load()-firstSyncs)
	}
}

func TestJoinDictSyncFailureIsRetried(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, 0)
	if _, err := d.Assign(jk(1)); err != nil {
		t.Fatal(err)
	}
	_, fail := countSyncs(t)
	fail.Store(true)
	if err := d.Sync(); err == nil {
		t.Fatal("Sync reported success though fsync failed")
	}
	fail.Store(false)
	// The failed sync left the records owed: the next Sync really syncs.
	n, _ := countSyncs(t)
	if err := d.Sync(); err != nil {
		t.Fatalf("retry Sync: %v", err)
	}
	if n.Load() != 1 {
		t.Fatalf("retry Sync issued %d fsyncs, want 1", n.Load())
	}
}

func TestJoinDictConcurrentAssignUnderRace(t *testing.T) {
	dir := t.TempDir()
	d := mustOpen(t, dir, 0)
	const (
		workers = 8
		keys    = 2000
	)
	ids := make([][]int64, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ids[w] = make([]int64, keys)
			// Each worker walks the keys in a different order, so the same
			// value is raced for by several goroutines.
			for j := 0; j < keys; j++ {
				i := (j + w*331) % keys
				id, err := d.Assign(jk(i))
				if err != nil {
					t.Errorf("worker %d key %d: %v", w, i, err)
					return
				}
				ids[w][i] = id
			}
		}(w)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	if d.Len() != keys {
		t.Fatalf("%d keys assigned, want %d", d.Len(), keys)
	}
	seen := make(map[int64]bool, keys)
	for i := 0; i < keys; i++ {
		first := ids[0][i]
		for w := 1; w < workers; w++ {
			if ids[w][i] != first {
				t.Fatalf("key %d has ids %d and %d in different goroutines", i, first, ids[w][i])
			}
		}
		if first < 1 || first > keys || seen[first] {
			t.Fatalf("key %d has id %d: not a dense permutation of 1..%d", i, first, keys)
		}
		seen[first] = true
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(filepath.Join(dir, joinDictLogName))
	if st.Size() != keys*joinDictRecordSize {
		t.Fatalf("log is %d bytes, want %d", st.Size(), keys*joinDictRecordSize)
	}
}

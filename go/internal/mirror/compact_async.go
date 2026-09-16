package mirror

// Background compaction.
//
// The p99 commit-to-visible latency measured 24.6s against a p50 of 206ms, and
// all of that gap is one thing: compaction rewrites every live row while the
// apply loop waits for it. The work is not wasteful — the base file has to be
// rebuilt eventually — it is just on the wrong goroutine.
//
// Concurrency here is subtle enough to be worth spelling out, because the naive
// version silently resurrects deleted rows. While a compaction is running, the
// apply loop keeps going: it writes new delta files, marks rows dead, and moves
// keys to new locations. The compactor is working from a snapshot of the file
// list taken before any of that.
//
// Two properties make it safe:
//
//   - Parquet files are IMMUTABLE once written. Only deletion vectors change,
//     and the compactor does not need them to be current.
//   - The in-memory index is the single source of truth for "where does this
//     key live NOW". So at swap time, a row in the new base file is live if and
//     only if the index still points at one of the files that were compacted.
//     Anything else — superseded by a newer delta, or deleted outright — is
//     marked dead in the new base's deletion vector.
//
// That means the compactor never has to see updates that raced it. It produces
// a candidate file; the swap, which runs on the apply goroutine and therefore
// needs no locking, decides which of its rows are actually live.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Compaction is a rewrite in progress. Begin and Finish both run on the apply
// goroutine; only the work between them runs elsewhere.
type Compaction struct {
	Started time.Time

	files map[string]bool // relative paths included in this rewrite
	name  string          // new base file name
	rows  []map[string]any
	err   error
	done  chan struct{}

	deltaRowsAtStart int
}

// Done reports whether the background work has finished, without blocking.
func (c *Compaction) Done() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// BeginCompaction snapshots the current file set and starts rewriting it on
// another goroutine. Returns nil if a compaction is already running.
func (t *Table) BeginCompaction() *Compaction {
	if t.compacting != nil {
		return nil
	}
	t.State.Seq++
	c := &Compaction{
		Started:          time.Now(),
		files:            map[string]bool{},
		name:             fmt.Sprintf("%06d.parquet", t.State.Seq),
		done:             make(chan struct{}),
		deltaRowsAtStart: t.State.DeltaRows,
	}
	for _, b := range t.State.BaseFiles {
		c.files[filepath.Join("base", b)] = true
	}
	for _, d := range t.State.DeltaFiles {
		c.files[filepath.Join("delta", d)] = true
	}
	if len(c.files) == 0 {
		return nil
	}

	// The file list is captured; everything below reads immutable files.
	snapshot := make([]string, 0, len(c.files))
	for f := range c.files {
		snapshot = append(snapshot, f)
	}
	sort.Strings(snapshot)

	t.compacting = c
	go func() {
		defer close(c.done)
		var rows []map[string]any
		for _, rel := range snapshot {
			got, err := t.readParquet(filepath.Join(t.Dir, rel), t.deadPositions(rel))
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				c.err = err
				return
			}
			rows = append(rows, got...)
		}
		if err := t.writeParquet(filepath.Join(t.Dir, "base", c.name), rows); err != nil {
			c.err = err
			return
		}
		c.rows = rows
	}()
	return c
}

// FinishCompaction swaps in the rewritten base. It runs on the apply goroutine,
// so it alone decides which of the rewritten rows are still live.
func (t *Table) FinishCompaction() (int, error) {
	c := t.compacting
	if c == nil || !c.Done() {
		return 0, nil
	}
	t.compacting = nil
	if c.err != nil {
		_ = os.Remove(filepath.Join(t.Dir, "base", c.name))
		return 0, c.err
	}
	if err := t.ensureIndex(); err != nil {
		return 0, err
	}

	// The same key can appear TWICE in a rewrite. The apply loop supersedes a
	// row by marking its old position dead and then writing the new row to a
	// delta; if the compactor read the old file in the window between those two
	// steps, it holds the stale copy as well as the fresh one. Files are read
	// base-first and then deltas in sequence order, so the LAST occurrence is
	// the newest — and taking the first would adopt the stale row and tombstone
	// the live one, which is how a 2.8M-row mirror came back short by one row
	// and a 500-row unit test came back with 162 rows reverted to their
	// pre-update values.
	newest := make(map[string]int, len(c.rows))
	for i, r := range c.rows {
		newest[keyString(r[t.Key])] = i
	}

	// Decide liveness per row against the CURRENT index, not against whatever
	// was true when the rewrite started.
	newRel := filepath.Join("base", c.name)
	var dead []int
	for i, r := range c.rows {
		k := keyString(r[t.Key])
		l, ok := t.index[k]
		switch {
		case newest[k] != i:
			// a staler copy of a key that appears again later in this rewrite
			dead = append(dead, i)
		case ok && c.files[l.File]:
			// still lives in a file we just folded in: it moves to the new base
			t.index[k] = loc{File: newRel, Pos: i}
		default:
			// deleted, or superseded by a delta written while we were rewriting
			dead = append(dead, i)
		}
	}
	sort.Ints(dead)
	if len(dead) > 0 {
		if err := writeJSON(t.dvPath(newRel), dead); err != nil {
			return 0, err
		}
	}

	// Keep every delta written after the rewrite began.
	var keptDeltas []string
	for _, d := range t.State.DeltaFiles {
		if !c.files[filepath.Join("delta", d)] {
			keptDeltas = append(keptDeltas, d)
		}
	}
	oldFiles := make([]string, 0, len(c.files))
	for f := range c.files {
		oldFiles = append(oldFiles, f)
	}

	t.State.BaseFiles = []string{c.name}
	t.State.DeltaFiles = keptDeltas
	t.State.BaseRows = len(c.rows) - len(dead)
	if t.State.DeltaRows -= c.deltaRowsAtStart; t.State.DeltaRows < 0 {
		t.State.DeltaRows = 0
	}

	// Persist the index for the new base so a restart does not have to rebuild
	// it by reading the file.
	idx := make(map[string]int, len(c.rows))
	for i, r := range c.rows {
		if l, ok := t.index[keyString(r[t.Key])]; ok && l.File == newRel && l.Pos == i {
			idx[keyString(r[t.Key])] = i
		}
	}
	if err := writeJSON(filepath.Join(t.Dir, "index",
		trimParquet(c.name)+".idx.json"), idx); err != nil {
		return 0, err
	}

	// State last, then the old files. A crash before this leaves the old set
	// intact and the new base as an orphan; a crash after leaves the new set.
	if err := t.saveState(); err != nil {
		return 0, err
	}
	for _, rel := range oldFiles {
		_ = os.Remove(filepath.Join(t.Dir, rel))
		_ = os.Remove(t.dvPath(rel))
	}
	return t.State.BaseRows, nil
}

// Compacting reports whether a background rewrite is in flight.
func (t *Table) Compacting() bool { return t.compacting != nil }

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
	err   error
	done  chan struct{}

	// Built by the background goroutine, consumed by the swap.
	index    map[string]loc // key -> position in the new base file
	dupDead  []int          // positions superseded within the rewrite itself
	rowCount int

	// touched is every key the apply loop superseded or deleted while this
	// rewrite was running. It is written only by the apply goroutine, and it is
	// what makes the swap O(changed) instead of O(table): the rewrite's own
	// index is correct for every key EXCEPT these.
	touched map[string]bool
}

// Touch records that a key moved or was deleted during a rewrite. Called from
// the apply goroutine, which also performs the swap, so no locking is needed.
func (t *Table) Touch(keys ...string) {
	if t.compacting == nil {
		return
	}
	for _, k := range keys {
		t.compacting.touched[k] = true
	}
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
		Started: time.Now(),
		files:   map[string]bool{},
		name:    fmt.Sprintf("%06d.parquet", t.State.Seq),
		done:    make(chan struct{}),
		touched: map[string]bool{},
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
	// The column shapes are captured too, and not read through t.State: the
	// apply goroutine adds an entry to that map on every batch that writes a
	// patch, and reading it from here would be a genuine data race.
	snapshot := make([]string, 0, len(c.files))
	pcols := map[string][]string{}
	for f := range c.files {
		snapshot = append(snapshot, f)
		if pc := t.partialColsOf(f); pc != nil {
			pcols[f] = pc
		}
	}
	// base/... sorts before delta/..., and delta names are a zero-padded
	// sequence, so this is base first and then deltas oldest to newest.
	sort.Strings(snapshot)

	t.compacting = c
	go func() {
		defer close(c.done)
		var rows []map[string]any
		var overlay map[string]map[string]any
		for _, rel := range snapshot {
			dead, derr := t.deadPositions(rel)
			if derr != nil {
				c.err = derr
				return
			}
			got, err := t.readParquetCols(filepath.Join(t.Dir, rel), dead, pcols[rel])
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				c.err = err
				return
			}
			if pcols[rel] != nil {
				// A patch file contributes no rows of its own; it amends rows
				// that came from the base file. A rewrite is where patches stop
				// being patches — the file it writes holds whole rows again.
				if overlay == nil {
					overlay = make(map[string]map[string]any, len(got))
				}
				for _, r := range got {
					overlay[keyString(r[t.Key])] = r
				}
				continue
			}
			rows = append(rows, got...)
		}
		for _, r := range rows {
			if p, ok := overlay[keyString(r[t.Key])]; ok {
				applyPatch(r, p)
			}
		}
		// Build the new index and find rows superseded within the rewrite
		// itself. The same key can appear twice: the apply loop supersedes a row
		// by marking its old position dead and THEN writing the new one, so a
		// compactor reading between those steps holds both copies. Files are
		// read base-first then deltas in sequence order, so the last occurrence
		// is the newest.
		newRel := filepath.Join("base", c.name)
		idx := make(map[string]loc, len(rows))
		var dup []int
		for i, r := range rows {
			k := keyString(r[t.Key])
			if prev, ok := idx[k]; ok {
				dup = append(dup, prev.Pos)
			}
			idx[k] = loc{File: newRel, Pos: i}
		}

		if err := t.writeParquet(filepath.Join(t.Dir, "base", c.name), rows); err != nil {
			c.err = err
			return
		}

		// Persist the index here rather than in the swap: marshalling a map of
		// every key is O(table) and belongs off the apply goroutine. It is
		// slightly stale for keys touched during the rewrite, which is safe —
		// the deletion vector, not the index, decides what is live, and
		// ensureIndex overlays the deltas on top when rebuilding.
		persist := make(map[string]int, len(idx))
		for k, l := range idx {
			persist[k] = l.Pos
		}
		if err := writeJSON(filepath.Join(t.Dir, "index",
			trimParquet(c.name)+".idx.json"), persist); err != nil {
			c.err = err
			return
		}

		c.index = idx
		c.dupDead = dup
		c.rowCount = len(rows)
	}()
	return c
}

// AbortCompaction discards a rewrite in flight and removes its partial output.
//
// Needed for DDL: the background goroutine reads t.Order and t.Columns while it
// works, and a schema change mutates both. Rather than lock the schema for the
// length of a rewrite, a barrier throws the rewrite away — it is recomputable,
// and the next one starts from the evolved schema.
func (t *Table) AbortCompaction() {
	c := t.compacting
	if c == nil {
		return
	}
	t.compacting = nil
	go func() {
		<-c.done
		_ = os.Remove(filepath.Join(t.Dir, "base", c.name))
		_ = os.Remove(filepath.Join(t.Dir, "index", trimParquet(c.name)+".idx.json"))
	}()
}

// FinishCompaction swaps in the rewritten base.
//
// The work here is proportional to what CHANGED during the rewrite, not to the
// size of the table. The compactor's index is already correct for every key it
// folded in; only the keys the apply loop touched meanwhile need fixing up, and
// installing the result is a pointer assignment.
//
// Walking every row here instead is what left p99 commit-to-visible at 6-9s on
// shapes that accumulate rows, against 217ms on shapes that do not (docs/19).
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

	newRel := filepath.Join("base", c.name)
	deadSet := make(map[int]bool, len(c.dupDead)+len(c.touched))
	for _, p := range c.dupDead {
		deadSet[p] = true
	}

	// Reconcile only the keys that moved while the rewrite ran.
	for k := range c.touched {
		inNew, wasFolded := c.index[k]
		cur, stillLive := t.index[k]
		if wasFolded && (!stillLive || cur.File != inNew.File || cur.Pos != inNew.Pos) {
			// the copy in the new base is not the live one any more
			deadSet[inNew.Pos] = true
		}
		if stillLive {
			c.index[k] = cur
		} else {
			delete(c.index, k)
		}
	}

	if os.Getenv("QS_COMPACT_DEBUG") != "" {
		orphan := 0
		var sample []string
		for k := range c.index {
			if _, live := t.index[k]; !live && !c.touched[k] {
				orphan++
				if len(sample) < 3 {
					sample = append(sample, k)
				}
			}
		}
		fmt.Fprintf(os.Stderr, "[swap] folded=%d touched=%d dup=%d "+
			"ORPHANS(in new base, not live, not touched)=%d %v\n",
			len(c.index), len(c.touched), len(c.dupDead), orphan, sample)
	}

	dead := make([]int, 0, len(deadSet))
	for p := range deadSet {
		dead = append(dead, p)
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
		} else {
			t.forgetDeltaFile(d)
		}
	}
	oldFiles := make([]string, 0, len(c.files))
	for f := range c.files {
		oldFiles = append(oldFiles, f)
	}

	// Every patch the rewrite folded in lived in a file it is about to delete,
	// and its value is now part of the whole row in the new base. No patches
	// can have been created since (canPatch refuses while a rewrite is in
	// flight), so this empties the map — but it is written as a filter, because
	// "it should be empty" is how the last three silent-corruption bugs in this
	// package were reasoned into existence.
	for k, p := range t.patch {
		if c.files[p.File] {
			delete(t.patch, k)
		}
	}

	t.index = c.index // O(1): the rewrite built it
	t.State.BaseFiles = []string{c.name}
	t.State.DeltaFiles = keptDeltas
	t.State.BaseRows = c.rowCount - len(dead)
	t.recountDeltaRows()

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

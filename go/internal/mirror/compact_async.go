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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"
)

// Compaction is a rewrite in progress. Begin and Finish both run on the apply
// goroutine; only the work between them runs elsewhere.
type Compaction struct {
	Started time.Time

	files map[fileID]bool // data files included in this rewrite
	name  string          // new base file name
	err   error
	done  chan struct{}

	// Built by the background goroutine, consumed by the swap.
	index    *keyIndex // key -> position in the new base file
	dupDead  []int     // positions superseded within the rewrite itself
	rowCount int
	// clusterBy is the column the rewrite kept the output clustered on, ""
	// if it wrote the base and then the deltas; clusterWhy is the reason.
	clusterBy, clusterWhy string
	// sorted says the rewrite sorted the whole table on clusterBy, because the
	// base had lost its clustering (compact_cluster.go, "re-clustering").
	sorted bool
	// halted is the reason the rewrite met a value the mirror cannot hold. The
	// rewrite works on a detached copy of the table and cannot halt the real
	// one; the swap does it, on the apply goroutine.
	halted string

	// touched is every key the apply loop superseded or deleted while this
	// rewrite was running. It is written only by the apply goroutine, and it is
	// what makes the swap O(changed) instead of O(table): the rewrite's own
	// index is correct for every key EXCEPT these.
	touched map[rowKey]bool
}

// Touch records that a key moved or was deleted during a rewrite. Called from
// the apply goroutine, which also performs the swap, so no locking is needed.
func (t *Table) Touch(keys ...rowKey) {
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
		files:   map[fileID]bool{},
		name:    fmt.Sprintf("%06d.parquet", t.State.Seq),
		done:    make(chan struct{}),
		touched: map[rowKey]bool{},
	}
	for _, b := range t.State.BaseFiles {
		c.files[baseID(b)] = true
	}
	for _, d := range t.State.DeltaFiles {
		c.files[deltaID(d)] = true
	}
	if len(c.files) == 0 {
		return nil
	}
	// Process-wide: a table that finds the rewrites in flight full waits for its
	// next tick, and keeps its deltas bounded meanwhile (rewrite_slots.go).
	if !tryAcquireRewriteSlot() {
		return nil
	}
	// Claimed here, on the apply goroutine: the rewrite writes through a
	// detached copy of the table, which has no publisher to claim from.
	t.claim(filepath.Join(t.Dir, "base", c.name))

	// The file list is captured; everything below reads immutable files.
	// The column shapes are captured too, and not read through t.State: the
	// apply goroutine adds an entry to that map on every batch that writes a
	// patch, and reading it from here would be a genuine data race.
	snapshot := make([]string, 0, len(c.files))
	pcols := map[string][]string{}
	for f := range c.files {
		rel := f.name()
		snapshot = append(snapshot, rel)
		if pc := t.partialCols[f]; pc != nil {
			pcols[rel] = pc
		}
	}
	// The new base file's id and the table's key form are resolved here, on the
	// apply goroutine, so the background goroutine never reads anything the
	// apply loop writes.
	// The DEAD SETS are captured here too, as the manifest names them, and not
	// read from disk by the rewrite. It used to take the newest vector on disk
	// (deadPositionsLatest), on the argument that seeing more rows dead than the
	// snapshot accounts for is safe because every key that moved is reconciled
	// at the swap. That holds only for vectors an Apply went on to commit. An
	// Apply that publishes its vector and then fails to write the manifest — or
	// a process that dies between the two — leaves a generation on disk that no
	// manifest names; a rewrite started before the replay folded that vector in,
	// dropped the row, and nothing was in `touched` to put it back. Idle
	// compaction made that reachable on the first tick after a restart.
	deadSnap := make(map[string]*deadSet, len(snapshot))
	for _, rel := range snapshot {
		d, err := t.deadSetFor(rel)
		if err != nil {
			releaseRewriteSlot()
			return nil
		}
		deadSnap[rel] = &deadSet{gen: d.gen, n: d.n, words: append([]uint64(nil), d.words...)}
	}
	// As the bitmaps they are. They used to be turned into a map[int]bool per
	// file read, which for a base with two million dead rows — what the churn
	// trigger means at ten million — was 74 MB to say what 1.25 MB says.
	deadOf := func(rel string) func(int) bool { return deadSnap[rel].has }
	deadMapOf := func(rel string) map[int]bool {
		d := deadSnap[rel]
		m := make(map[int]bool, d.n)
		for _, p := range d.positions() {
			m[p] = true
		}
		return m
	}
	numeric, hashing := t.keyMode(), t.elideWorked
	// base/... sorts before delta/..., and delta names are a zero-padded
	// sequence, so this is base first and then deltas oldest to newest.
	sort.Strings(snapshot)

	// And the schema. The rewrite reads the column order, the column types and
	// the missing-value map for every row group it decodes, and a DDL barrier
	// replaces all three without waiting for it (AbortCompaction returns at
	// once, by design: waiting would stall the apply loop for the length of a
	// rewrite). So the goroutine gets its own copy and never sees the Table the
	// apply loop owns — inside it, t IS the copy.
	snap, sizeHint := t.rewriteSnapshot(), t.State.BaseRows

	t.compacting = c
	go func(t *Table) {
		defer releaseRewriteSlot() // runs after close(c.done): the slot is the last thing given back
		defer close(c.done)
		defer func() { c.halted = t.State.Halted }()

		// Patches first, because they amend rows that come from other files.
		// They are bounded by the delta files, which the compaction policy keeps
		// small; the whole rows are not, and are never held.
		var overlay map[rowKey]map[string]any
		for _, rel := range snapshot {
			if pcols[rel] == nil {
				continue
			}
			// A patch file: bounded by the deltas, and read whole.
			got, err := t.readParquetCols(filepath.Join(t.Dir, rel), deadMapOf(rel), pcols[rel])
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				c.err = err
				return
			}
			if overlay == nil {
				overlay = make(map[rowKey]map[string]any, len(got))
			}
			// A rewrite is where patches stop being patches: the file it writes
			// holds whole rows again.
			for _, r := range got {
				overlay[keyFor(numeric, r[t.Key])] = r
			}
		}

		// Then the whole rows, ONE ROW GROUP AT A TIME, straight into the new
		// file. Reading them all into a slice first is four million Go maps on
		// the narrow shape, which is where this process's resident memory went
		// — and the garbage collector it implies pauses the apply goroutine too.
		//
		// The index is built as the rows stream past, which also finds rows
		// superseded within the rewrite itself: the same key can appear twice,
		// because the apply loop supersedes a row by marking its old position
		// dead and THEN writing the new one, so a compactor reading between
		// those steps holds both copies. Files are read base-first then deltas
		// in sequence order, so the last occurrence is the newest.
		//
		// The keys go to the builder of the new base file's index (kidx.go),
		// which sorts them in bounded runs instead of holding a second map of
		// the whole table next to the live one, and which reports the
		// duplicates when it merges: every position whose key appears again at
		// a higher one.
		kb := t.newKidxBuilder(c.name, sizeHint, true)
		defer func() {
			if c.index == nil {
				kb.abort()
			}
		}()
		out := 0

		w, err := t.newParquetWriter(filepath.Join(t.Dir, "base", c.name), nil, true)
		if err != nil {
			c.err = err
			return
		}
		// Where the delta rows go. With a column the base file is already
		// clustered on, they are merged into the base stream on it, so that the
		// new file is clustered as the old one was (compact_cluster.go). With
		// none, or with no delta rows to place, it is the loop below, which
		// puts them at the end.
		var baseRels, deltaRels []string
		for _, rel := range snapshot {
			switch {
			case pcols[rel] != nil:
			case strings.HasPrefix(rel, "base"):
				baseRels = append(baseRels, rel)
			default:
				deltaRels = append(deltaRels, rel)
			}
		}
		c.clusterBy, c.clusterWhy = t.clusterColumn(baseRels)
		// A base that has lost the clustering it had is not merged into: it is
		// sorted on the column, once, deltas or no deltas (compact_cluster.go).
		sortCol, sortWhy := t.reclusterColumn(baseRels)
		sortAll := sortCol != ""
		if sortAll {
			c.clusterBy, c.clusterWhy, c.sorted = sortCol, sortWhy, true
			slog.Info("rewrite re-clusters the base", "table", t.Qualified, "column", sortCol, "why", sortWhy)
		}
		if len(deltaRels) == 0 && !sortAll {
			c.clusterBy = ""
		}
		if c.clusterBy != "" {
			n, err := t.rewriteClustered(c.clusterBy, baseRels, deltaRels, deadOf, overlay,
				numeric, hashing, kb, w, sortAll)
			if err != nil {
				w.Close()
				c.err = err
				return
			}
			out = n
			snapshot = nil // written; nothing for the loop below
		}
		for _, rel := range snapshot {
			if pcols[rel] != nil {
				continue
			}
			err := t.forEachRows(filepath.Join(t.Dir, rel), deadOf(rel), nil, rowChunk,
				func(rows []map[string]any, _ int) error {
					for _, r := range rows {
						k := keyFor(numeric, r[t.Key])
						if p, ok := overlay[k]; ok {
							applyPatch(r, p)
						}
						var h uint64
						if hashing {
							// A rewrite sees every value, so it is where a
							// digest forgotten by a partial write comes back.
							h, _ = largeHash(r, t.Order)
						}
						if err := kb.add(k, int32(out), h, false); err != nil {
							return err
						}
						out++
					}
					return w.Append(rows)
				})
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				w.Close()
				c.err = err
				return
			}
		}
		if err := w.Close(); err != nil {
			c.err = err
			return
		}

		// The index is NOT persisted. This used to marshal a map of every key to
		// index/<name>.idx.json so that a later ensureIndex could load it
		// instead of reading the base file; measured, that file was 2.7x the
		// size of the Parquet file it indexed and SLOWER to load than rebuilding
		// from it (TestPersistedIndexCost, docs/32). Writing it also put an
		// O(table) JSON marshal on this goroutine for nothing.
		//
		// It was found by a measurement aimed at something else: on the purge
		// shape a rewritten mirror came out 2.4x larger on disk than an
		// un-rewritten one holding 100,000 MORE rows, and the base file had
		// shrunk exactly as expected. One file accounted for all of it.

		// After the base file is closed: the index binds to its final size.
		idx, recDups, extraDups, err := kb.finish()
		if err != nil {
			c.err = err
			return
		}
		// The copies superseded within the rewrite are not live in the new
		// base. The swap writes them to its deletion vector; the index is told
		// here, so that it answers for the newest copy from the start.
		idx.markGone(recDups)
		c.index = idx
		c.dupDead = append(recDups, extraDups...)
		c.rowCount = out
	}(snap)
	return c
}

// rewriteSnapshot is the immutable view of the table a background rewrite
// works from: where the files are, what the key is, and the schema as it stood
// when the rewrite began. It is small — one map entry per column — and it is
// everything the read and write paths consult (forEachRowGroup, fillMissing,
// newParquetWriter, arrowSchemaFor, checkTemporal).
//
// It is a *Table so that those paths need no second implementation, and it is
// detached so that the one thing they can do besides read — Halt, on a value
// Arrow cannot hold — does not write a manifest from a State that is mostly
// empty. The reason is carried back in Compaction.halted instead.
func (t *Table) rewriteSnapshot() *Table {
	s := &Table{
		Qualified:  t.Qualified,
		Key:        t.Key,
		Dir:        t.Dir,
		Columns:    maps.Clone(t.Columns),
		Order:      slices.Clone(t.Order),
		numericKey: t.numericKey,
		notNull:    maps.Clone(t.notNull),
		decimalKey: t.decimalKey,
		detached:   true,
	}
	s.State.MissingVals = maps.Clone(t.State.MissingVals)
	return s
}

// AbortCompaction discards a rewrite in flight and removes its partial output.
//
// Needed for DDL: a rewrite produces a file in the schema it started with, and
// a schema change makes that the wrong file. Rather than lock the schema for
// the length of a rewrite, a barrier throws the rewrite away — it is
// recomputable, and the next one starts from the evolved schema. Needed for
// TRUNCATE for the same reason with the rows in place of the columns.
//
// This does NOT wait for the goroutine, which goes on reading until it is
// done. That is safe only because it reads a private snapshot of the schema
// (rewriteSnapshot) and files that are immutable; nothing it touches is
// something the caller is about to change.
func (t *Table) AbortCompaction() {
	c := t.compacting
	if c == nil {
		return
	}
	t.compacting = nil
	// Counted, so that Close can wait: this unlinks a PATH, later, and the
	// Table that replaces a closed one may be writing that path by then.
	t.aborting.Add(1)
	go func() {
		defer t.aborting.Done()
		<-c.done
		_ = os.Remove(filepath.Join(t.Dir, "base", c.name))
		c.index.close()
		t.dropKidx(c.name)
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
		t.discardRewrite(c)
		if c.halted != "" {
			_ = t.Halt(c.halted)
		}
		return 0, c.err
	}
	if t.broken {
		// Nothing may be saved from this Table, so nothing may be swapped in.
		t.discardRewrite(c)
		return 0, ErrNeedsReopen
	}
	if err := t.ensureIndex(); err != nil {
		t.discardRewrite(c)
		return 0, err
	}

	newRel := filepath.Join("base", c.name)
	deadSet := make(map[int]bool, len(c.dupDead)+len(c.touched))
	for _, p := range c.dupDead {
		deadSet[p] = true
	}

	// Reconcile only the keys that moved while the rewrite ran.
	for k := range c.touched {
		inNew, wasFolded := c.index.get(k)
		cur, stillLive := t.index.get(k)
		if wasFolded && (!stillLive || !cur.sameRow(inNew)) {
			// the copy in the new base is not the live one any more
			deadSet[int(inNew.Pos)] = true
		}
		if stillLive {
			c.index.set(k, cur)
		} else {
			c.index.del(k)
		}
	}

	if os.Getenv("QS_COMPACT_DEBUG") != "" {
		orphan := 0
		var sample []string
		c.index.each(func(k rowKey, _ loc) {
			if _, live := t.index.get(k); !live && !c.touched[k] {
				orphan++
				if len(sample) < 3 {
					sample = append(sample, k.String())
				}
			}
		})
		fmt.Fprintf(os.Stderr, "[swap] folded=%d touched=%d dup=%d "+
			"ORPHANS(in new base, not live, not touched)=%d %v\n",
			c.index.len(), len(c.touched), len(c.dupDead), orphan, sample)
	}

	// THE SWAP, AND WHAT A FAILURE IN IT LEAVES.
	//
	// Three things are written here: the new base's vector, the directories
	// the rewrite's files were created in, and the manifest. Any of them can
	// fail, and each failure used to return with whatever had been done so far
	// still done: the new base and its index and vector on disk under no
	// manifest, or — once memory had been swapped — a Table describing files
	// its manifest did not, with the old ones never collected.
	//
	// So the order is: everything that can fail first, memory last.
	//
	//   - The vector fails: nothing of the table has changed. The rewrite's
	//     output is removed and the table carries on as it was.
	//   - The manifest fails and the one on disk is still the old one: State is
	//     put back, the rewrite's output is removed, the table carries on.
	//   - The manifest fails and the one on disk is the NEW one (it was renamed
	//     into place and the sync of its directory, or the publication, failed)
	//     or cannot be read: which set a crash would come back to is not
	//     knowable from here, so neither is removed. Memory is brought to the
	//     new set and the Table is marked as needing reopen; the process that
	//     opens it next reads whichever manifest survived, and its sweep
	//     (publish.go) collects the files that one does not name.
	dead := dvSorted(deadSet)
	if len(dead) > 0 {
		if err := t.writeDV(t.dvPathGen(newRel, 1), dead); err != nil {
			t.discardRewrite(c)
			return 0, err
		}
	}

	prev := t.State
	prev.DVGen = maps.Clone(t.State.DVGen)
	prev.PartialCols = maps.Clone(t.State.PartialCols)
	prev.FileRows = maps.Clone(t.State.FileRows)
	prevPartial := maps.Clone(t.partialCols)

	if len(dead) > 0 {
		if t.State.DVGen == nil {
			t.State.DVGen = map[string]int{}
		}
		t.State.DVGen[dvStem(newRel)] = 1
	}
	// Keep every delta written after the rewrite began.
	var keptDeltas []string
	for _, d := range t.State.DeltaFiles {
		if !c.files[deltaID(d)] {
			keptDeltas = append(keptDeltas, d)
		} else {
			t.forgetDeltaFile(d)
		}
	}
	oldFiles := make([]string, 0, len(c.files))
	for f := range c.files {
		oldFiles = append(oldFiles, f.name())
	}
	t.State.BaseFiles = []string{c.name}
	t.State.DeltaFiles = keptDeltas
	t.State.BaseRows = c.rowCount - len(dead)
	t.recountDeltaRows()
	// Whatever was cached for a file of the new base's name is not its vector.
	t.forgetDead(newRel)

	// State, then memory, then the old files. A crash before the manifest is
	// replaced leaves the old set intact and the new base as an orphan; a crash
	// after leaves the new set.
	//
	// The new base was fsynced by the goroutine that wrote it, and its vector
	// just above; both left their directories owed a sync, which saveState
	// pays before it writes the manifest.
	saveErr := t.saveState()
	if saveErr != nil && t.manifestOnDisk(c.name) == manifestOld {
		t.State, t.partialCols = prev, prevPartial
		t.forgetDead(newRel)
		_ = os.Remove(t.statePath() + ".tmp")
		t.discardRewrite(c)
		return 0, saveErr
	}

	// Every patch the rewrite folded in lived in a file it is about to delete,
	// and its value is now part of the whole row in the new base. Patches
	// written SINCE must survive: they landed in deltas outside the snapshot,
	// the rewrite never saw them, and the read path puts them back on the row it
	// folded (partial.go). So this is a filter and not a clear.
	//
	// It previously justified itself with "no patches can have been created
	// since (canPatch refuses while a rewrite is in flight)", which is simply not
	// true — canPatch has no such check, partial.go says in as many words that
	// patches ARE created during a rewrite, and the racy test fails outright if
	// none was. The filter was right; only its reason was wrong, which is the
	// more dangerous of the two to leave lying around.
	t.patch.deleteWhere(func(p loc) bool { return c.files[p.File] })

	t.noteCompacted(c)
	old := t.index
	t.index = c.index // O(1): the rewrite built it
	old.close()       // the old base file's index; its file goes with the base below

	if saveErr != nil {
		// The manifest on disk names the new set, or cannot be read. Both sets
		// stay where they are; see above.
		t.broken = true
		return 0, errors.Join(saveErr, ErrNeedsReopen)
	}
	for _, rel := range oldFiles {
		t.removeDataFile(rel)
	}
	if c.sorted {
		reclusterSorted.Store(t.Dir, c.clusterBy)
	}
	return t.State.BaseRows, nil
}

// discardRewrite removes everything a rewrite that will not be swapped in left
// on disk: the base file, its key index and anything a build of it left, and
// its deletion vector. No manifest has named any of it, so nothing waits out a
// grace.
func (t *Table) discardRewrite(c *Compaction) {
	rel := filepath.Join("base", c.name)
	_ = os.Remove(filepath.Join(t.Dir, rel))
	c.index.close()
	t.dropKidx(c.name)
	for _, dv := range t.dvGlob(rel) {
		_ = os.Remove(dv)
	}
	_ = os.Remove(t.dvPathGen(rel, 1) + ".tmp")
}

const (
	manifestUnknown = iota
	manifestOld
	manifestNew
)

// manifestOnDisk says which side of a swap the manifest on disk is on: the new
// one names the rewrite's base file and the old one does not. Unknown is a
// manifest that cannot be read, which a failed write must be assumed able to
// leave.
func (t *Table) manifestOnDisk(newBase string) int {
	b, err := os.ReadFile(t.statePath())
	if err != nil {
		return manifestUnknown
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return manifestUnknown
	}
	if contains(st.BaseFiles, newBase) {
		return manifestNew
	}
	return manifestOld
}

// Compacting reports whether a background rewrite is in flight.
func (t *Table) Compacting() bool { return t.compacting != nil }

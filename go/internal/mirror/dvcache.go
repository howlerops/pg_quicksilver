package mirror

// The dead set of each data file, held in memory.
//
// A deletion vector on disk is immutable: every batch that marks a row dead
// publishes a whole NEW generation (State.DVGen says why). What that rule does
// not require is that the writer learn the previous generation by reading it
// back, and it used to — markDead re-read the entire vector into a
// map[int]bool, added a batch's worth of positions, sorted the lot and wrote
// the lot. Per 1,000-delete Apply on a 3M-row base (TestMarkDeadCost):
//
//	dead in the base    re-read every batch    held in memory
//	           7,000                8.3 ms            4.6 ms
//	         107,000               62.2 ms           10.6 ms
//	       1,007,000              408.9 ms          100.4 ms
//	       2,007,000              788.1 ms          162.0 ms
//
// So the apply path slowed down in proportion to how much had ALREADY been
// deleted, which is the one quantity a delete should not care about. Roughly a
// third each was the read into a map, the sort and the write. The first two are
// what this file removes; the write is the on-disk contract and stays.
//
// WHY A BITMAP. A position is a row number in one file, so the set is dense in
// a small universe: one bit per row of the file is 375 KB for a 3M-row base,
// against 16 MB for the same two million positions as a sorted []int and
// several times that as a map. Adding is one OR, counting is free, and reading
// it back out in ascending order — which is what the file wants — is a walk
// over the words with no sort in it.
//
// WHAT IT IS NOT. It is not a second source of truth. The manifest names a
// generation, the cache remembers which generation it equals, and a cache whose
// generation is not the manifest's is thrown away and reloaded from the file
// the manifest names. Everything that reads State.DVGen from disk — the view,
// Reload, a restart, the background compactor — reads exactly what it read
// before. Only the apply goroutine touches this; the background compactor still
// goes to disk (deadPositionsLatest), because it may not read anything the
// apply loop writes.

import (
	"math/bits"
	"os"
	"time"
)

// deadSet is the positions of one data file that are no longer live.
type deadSet struct {
	// gen is the generation on disk this set is equal to. Zero means no vector
	// has been written for the file, and then the set is empty.
	gen   int
	words []uint64
	n     int
}

// add marks a position dead and reports whether it was live before.
func (d *deadSet) add(p int) bool {
	w := p >> 6
	if w >= len(d.words) {
		grown := make([]uint64, max(w+1, 2*len(d.words)))
		copy(grown, d.words)
		d.words = grown
	}
	bit := uint64(1) << (uint(p) & 63)
	if d.words[w]&bit != 0 {
		return false
	}
	d.words[w] |= bit
	d.n++
	return true
}

func (d *deadSet) has(p int) bool {
	w := p >> 6
	return w < len(d.words) && d.words[w]&(1<<(uint(p)&63)) != 0
}

// positions returns the set ascending, which is the order a vector is written
// in (dv.go) and comes out of a bitmap without sorting anything.
func (d *deadSet) positions() []int {
	out := make([]int, 0, d.n)
	for w, word := range d.words {
		for word != 0 {
			out = append(out, w<<6|bits.TrailingZeros64(word))
			word &= word - 1
		}
	}
	return out
}

// readDVPositions reads one generation of a vector, turning "the manifest names
// a file that is not there" into ErrStaleManifest. See deadPositions for why
// that is an error and not an empty set.
func readDVPositions(path string) ([]int, error) {
	if path == "" {
		return nil, ErrStaleManifest
	}
	pos, err := readDVFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrStaleManifest
		}
		return nil, err
	}
	return pos, nil
}

// deadSetFor returns the dead set of a file as THIS MANIFEST describes it,
// reading the vector from disk only if the cache does not already hold that
// generation.
//
// The generation check is what makes a missed invalidation a slow path instead
// of a wrong answer: Reload, a compaction swap or a truncate all change which
// generation the manifest names for a file (or that it names one at all), so a
// stale entry fails the comparison and is replaced. The explicit resets at
// those sites are there so the memory goes away with the file, not because
// correctness waits on them.
func (t *Table) deadSetFor(rel string) (*deadSet, error) {
	stem := dvStem(rel)
	gen := t.State.DVGen[stem]
	if d := t.dead[stem]; d != nil && d.gen == gen {
		return d, nil
	}
	d := &deadSet{gen: gen}
	if gen != 0 {
		pos, err := readDVPositions(t.dvPathGenAny(rel, gen))
		if err != nil {
			return nil, err
		}
		for _, p := range pos {
			d.add(p)
		}
	}
	if t.dead == nil {
		t.dead = map[string]*deadSet{}
	}
	t.dead[stem] = d
	return d, nil
}

// forgetDead drops the cached dead set of one file.
func (t *Table) forgetDead(rel string) { delete(t.dead, dvStem(rel)) }

// resetDead drops every cached dead set, for when the manifest itself has been
// replaced underneath them.
func (t *Table) resetDead() { t.dead = nil }

// baseDead is how many positions of the base are dead. After the first call it
// costs nothing, which is what lets the compaction policy ask on every tick —
// it used to read the whole base vector from disk to answer, and so could only
// afford to be asked when the knob that needed it was on.
func (t *Table) baseDead() (int, error) {
	dead := 0
	for _, b := range t.State.BaseFiles {
		d, err := t.deadSetFor("base/" + b)
		if err != nil {
			return 0, err
		}
		dead += d.n
	}
	return dead, nil
}

// now is the policy's clock, replaceable so that a test of "idle for thirty
// seconds" does not take thirty seconds.
func (t *Table) now() time.Time {
	if t.clock != nil {
		return t.clock()
	}
	return time.Now()
}

// noteCompacted records that a rewrite has just been swapped in: when, and how
// long it took. The second is the only honest estimate of what the NEXT rewrite
// of this table will cost, and the idle trigger spaces itself by it.
func (t *Table) noteCompacted(c *Compaction) {
	t.forgetDead("base/" + c.name)
	t.lastRewrite = time.Since(c.Started)
	t.lastCompacted = t.now()
}

// noteActivity keeps the time of this table's last write, for the idle trigger.
//
// A write is observed rather than reported: every delta file written bumps
// State.Seq, and a delete — which writes no delta — goes through markDead,
// which stamps the time itself. So the two together see every row change
// without the apply path having to tell anyone. A table seen for the first time
// counts as just written, so a restart does not start a rewrite before the
// stream has had a chance to say whether the source is actually quiet.
func (t *Table) noteActivity() {
	if t.lastChange.IsZero() || t.State.Seq != t.seenSeq {
		t.seenSeq = t.State.Seq
		t.lastChange = t.now()
	}
}

// LastRewrite is how long the most recent base rewrite took, start to swap.
func (t *Table) LastRewrite() time.Duration { return t.lastRewrite }

// compactRetryAfter is how long a failed rewrite keeps every trigger quiet.
const compactRetryAfter = 30 * time.Second

// CompactionFailed tells the policy that a rewrite did not land.
//
// A failed rewrite changes nothing the triggers look at, so whichever one
// pulled is still pulling on the next tick. While triggers were only evaluated
// on ticks that applied something, that retried at the pace of the workload;
// evaluated on every tick, it would retry an O(table) rewrite five times a
// second against a full disk. So a failure buys a pause before anything may
// start another.
func (t *Table) CompactionFailed() { t.retryAt = t.now().Add(compactRetryAfter) }

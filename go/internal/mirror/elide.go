package mirror

// Dropping large values the mirror can see are unchanged.
//
// Column-partial deltas (partial.go) exist because pgoutput does not resend a
// large value an UPDATE did not touch. That covers exactly one case: the value
// is TOASTed. PostgreSQL stores a value out of line only once the whole row
// exceeds about 2 KB, so a 1.5 KB JSON blob, a serialised state machine or a
// long description sits INLINE — and is resent, in full, on every update to any
// other column in the row. The mirror then writes it again, exactly the
// behaviour docs/19 round four measured as the dominant cost of the jsonb shape,
// only without the hint that would let it stop.
//
// So the mirror works the hint out for itself: it keeps, per key, a hash of the
// large columns of the row it holds. When a change arrives carrying large
// columns whose hash matches, those values did not change, and the columns are
// removed from the change before anything else looks at it. What is left is a
// short row, and the machinery from round four takes over unmodified.
//
// Note that the saving IS a column-partial delta: elision only makes the change
// shorter, and partial.go is what writes a short change as short bytes. With
// QS_PARTIAL_DELTAS=0 the elided columns are carried forward again and the row
// is written whole — correct, and worth nothing.
//
// This is the one place in this package that decides data equality from a hash
// rather than from the bytes. A collision would leave a stale value in the
// mirror silently and permanently, which is the failure mode everything else
// here is built to avoid — so it is worth being explicit that the odds are
// 2^-64 per comparison, that nothing is compared unless the KEY already matches,
// and that the alternative is reading the previous value back out of Parquet,
// which is the row-group decode per update that this whole line of work exists
// to remove. QS_ELIDE_UNCHANGED=0 turns it off.

import (
	"hash/maphash"
	"os"
	"strconv"
)

// One seed for the process. These digests never leave memory — they are not
// persisted, not compared across processes, and a restart simply relearns them
// — so a per-process seed costs nothing and makes the comparison unforgeable
// from outside.
//
// maphash, not FNV: Go's FNV writes a byte at a time, and this hashes every
// large value that arrives plus every one a rewrite touches. Measured on the
// inline shape, FNV made the optimisation cost 18% more CPU than it saved in
// bytes written.
var elideSeed = maphash.MakeSeed()

// ElideUnchanged can be turned off so the cost and the benefit are measurable
// against one binary rather than argued about.
var ElideUnchanged = os.Getenv("QS_ELIDE_UNCHANGED") != "0"

// elideMinBytes is how large a value has to be before it is worth hashing.
//
// Below it, hashing and a second file cost more than rewriting the value. The
// default is a quarter of PostgreSQL's TOAST threshold: above that the column
// is worth keeping out of a delta, and well above it PostgreSQL will have
// TOASTed it and partial.go handles the case without any of this.
var elideMinBytes = int(envInt("QS_ELIDE_MIN_BYTES", 256))

// largeHash summarises the large columns of a row: for each column, in the
// table's column order, whose rendered value is at least elideMinBytes, the
// column NAME, the length and the bytes.
//
// The name and length are in the hash on purpose. Without them, a row whose
// large columns are {a: X} and one whose are {b: X} hash the same, and so do
// two different splits of the same concatenated bytes — and a match is taken as
// proof that nothing changed.
//
// ok is false when the row has no large columns at all, which is the common
// case and means there is nothing here to do.
func largeHash(row map[string]any, order []string) (uint64, bool) {
	var h maphash.Hash
	h.SetSeed(elideSeed)
	found := false
	for _, c := range order {
		v, present := row[c]
		if !present {
			continue
		}
		s := renderValue(v)
		if len(s) < elideMinBytes {
			continue
		}
		found = true
		h.WriteString(c)
		h.WriteByte(0)
		h.WriteString(strconv.Itoa(len(s)))
		h.WriteByte(0)
		h.WriteString(s)
		h.WriteByte(0)
	}
	if !found {
		return 0, false
	}
	// Sum64 can legitimately be 0, and 0 is how a loc says "no digest". One
	// value in 2^64 losing the optimisation is not worth a second field.
	if sum := h.Sum64(); sum != 0 {
		return sum, true
	}
	return 1, true
}

// elideUnchanged removes, from each incoming row, the large columns whose
// values the mirror can already prove it holds.
//
// Returns the keys whose rows were shortened, so the caller knows which stored
// hashes stay valid. Everything downstream — canPatch, carry-forward, the
// writer — sees only a change that arrived carrying fewer columns, which is a
// case they all already handle.
func (t *Table) elideUnchanged(upserts map[rowKey]map[string]any) (map[rowKey]bool, error) {
	if !ElideUnchanged || !t.sawLarge || len(upserts) == 0 {
		return nil, nil
	}
	if err := t.ensureIndex(); err != nil {
		return nil, err
	}
	var elided map[rowKey]bool
	for k, row := range upserts {
		l, ok := t.index.get(k)
		if !ok || l.Hash == 0 {
			continue // nothing to compare against
		}
		h, any := largeHash(row, t.Order)
		if !any || h != l.Hash {
			continue
		}
		// Every large column in this change holds the value the mirror already
		// has. Drop them; what remains is a short row, and partial.go writes
		// short rows without expanding them.
		for _, c := range t.Order {
			if c == t.Key {
				continue
			}
			v, present := row[c]
			if !present {
				continue
			}
			if len(renderValue(v)) >= elideMinBytes {
				delete(row, c)
			}
		}
		if elided == nil {
			elided = map[rowKey]bool{}
		}
		elided[k] = true
		// Only now is it worth a compaction maintaining these digests.
		t.elideWorked = true
	}
	return elided, nil
}

// noteHashes records what the mirror now knows about each key's large columns,
// after a batch has been written.
//
//   - a whole row was written, so every value is known: store its hash;
//   - the change was elided, so the values are by definition the ones already
//     stored: leave the hash alone;
//   - a patch was written without eliding — the round-four case, where pgoutput
//     omitted a large column — so the row's large columns are partly unknown:
//     clear the hash, and the next whole-row write or compaction restores it.
//
// Clearing rather than guessing is the whole discipline of this package: an
// unknown hash costs an optimisation, a wrong one costs the data.
func (t *Table) noteHashes(upserts map[rowKey]map[string]any,
	asPatch, elided map[rowKey]bool,
) {
	if !ElideUnchanged {
		return
	}
	for k, row := range upserts {
		if elided[k] {
			continue // values unchanged, so the stored hash still describes them
		}
		l, ok := t.index.get(k)
		if !ok {
			continue
		}
		if asPatch[k] {
			if l.Hash != 0 {
				l.Hash = 0
				t.index.set(k, l)
			}
			continue
		}
		h, any := largeHash(row, t.Order)
		if any {
			t.sawLarge = true
		}
		if l.Hash != h {
			l.Hash = h
			t.index.set(k, l)
		}
	}
}

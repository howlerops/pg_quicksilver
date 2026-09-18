package mirror

// The invariant the racy tests check at the END, checked after EVERY step.
//
// The symptom is a mirror that returns one key twice, about once in a thousand
// runs. Checked only at the end, all that is known is the final manifest — which
// is several merges and a base rewrite away from whatever created the second
// copy, and reading backwards from it is what produced the wrong diagnosis in
// docs/31.
//
// So this states the two properties the whole package rests on, in a form that
// can be asserted between operations:
//
//   - a key is live in at most ONE data file. A deletion vector addresses
//     positions in one file, so two live copies is a state nothing can express a
//     fix for; the read path returns both.
//   - the index points AT that copy. Everything that supersedes a row —
//     markDead, carry-forward, the compaction swap — asks the index where the
//     row is, so an index pointing anywhere else silently tombstones the wrong
//     row and leaves the real one live.
//
// Run under QS_RACE_INVARIANTS=1, because it is O(mirror) per call and the hunt
// it serves runs the racy tests a thousand times over.

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// liveAt is one physical place a key's row is live.
type liveAt struct {
	rel string
	pos int
}

func (l liveAt) String() string { return fmt.Sprintf("%s#%d", l.rel, l.pos) }

// checkInvariants asserts the two properties above and returns false on the
// first violation, so the caller can stop at the step that broke them rather
// than reporting every consequence.
//
// `when` names the step just performed. That label is the point: it turns "the
// mirror is wrong at the end" into "MergeDeltas at round 37 did this".
func checkInvariants(t *testing.T, tbl *Table, when string) bool {
	t.Helper()
	if os.Getenv("QS_RACE_INVARIANTS") == "" {
		return true
	}

	live := map[rowKey][]liveAt{}
	for _, set := range [][2]any{
		{"base", tbl.State.BaseFiles}, {"delta", tbl.State.DeltaFiles},
	} {
		sub := set[0].(string)
		for _, f := range set[1].([]string) {
			if tbl.State.PartialCols[f] != nil {
				continue // patches amend a row, they are not rows
			}
			rel := filepath.Join(sub, f)
			dead, err := tbl.deadPositions(rel)
			if err != nil {
				t.Errorf("%s: deadPositions(%s): %v", when, rel, err)
				return false
			}
			// No vector passed to the reader: positions must come back PHYSICAL,
			// because that is what the index stores and what a vector addresses.
			//
			// The KEY COLUMN only. This runs after every apply, swap and merge of
			// a sixty-round test that the hunt repeats a thousand times, and the
			// whole-row version was slow enough that one batch of 25 did not
			// finish in ten minutes — a check that cannot be run often enough to
			// catch a one-in-a-thousand failure is not a check. The key and the
			// position are the entire content of both properties being asserted.
			rows, err := tbl.readParquetCols(filepath.Join(tbl.Dir, rel), nil,
				[]string{tbl.Key})
			if err != nil {
				t.Errorf("%s: read %s: %v", when, rel, err)
				return false
			}
			for i, r := range rows {
				if dead[i] {
					continue
				}
				k := tbl.keyOf(r[tbl.Key])
				live[k] = append(live[k], liveAt{rel, i})
			}
		}
	}

	ok := true
	var dupKeys []string
	for k, at := range live {
		if len(at) > 1 {
			dupKeys = append(dupKeys, k.String())
			if len(dupKeys) <= 3 {
				t.Errorf("%s: key %s is live in %d places: %v",
					when, k.String(), len(at), at)
			}
			ok = false
		}
	}
	if len(dupKeys) > 3 {
		sort.Strings(dupKeys)
		t.Errorf("%s: %d keys live in more than one file: %v",
			when, len(dupKeys), dupKeys[:3])
	}

	// The index must name the live copy — for every key that has one.
	//
	// The converse is NOT asserted: an index entry for a key with no live row is
	// checked separately below, because the two failures have different causes
	// and reporting them as one loses that.
	misdirected := 0
	for k, at := range live {
		l, has := tbl.index.get(k)
		if !has {
			if misdirected < 3 {
				t.Errorf("%s: key %s is live at %v but the index has no entry",
					when, k.String(), at)
			}
			misdirected++
			ok = false
			continue
		}
		want := liveAt{l.File.name(), int(l.Pos)}
		found := false
		for _, a := range at {
			if a == want {
				found = true
			}
		}
		if !found {
			if misdirected < 3 {
				t.Errorf("%s: index puts key %s at %v, but it is live at %v",
					when, k.String(), want, at)
			}
			misdirected++
			ok = false
		}
	}

	// An index entry pointing at a row that is dead, or at a file the manifest
	// no longer names, is how a later markDead tombstones nothing at all.
	stale := 0
	tbl.index.each(func(k rowKey, l loc) {
		if _, isLive := live[k]; isLive {
			return
		}
		if stale < 3 {
			t.Errorf("%s: index keeps key %s at %s#%d, where no row is live",
				when, k.String(), l.File.name(), l.Pos)
		}
		stale++
		ok = false
	})

	return ok
}

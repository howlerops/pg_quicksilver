package mirror

// Column-partial deltas.
//
// An OLTP update usually touches two or three columns out of twenty, and
// pgoutput tells us exactly which: an unchanged TOASTed value is not sent at
// all. Up to now the mirror expanded that back into a whole row — carry-forward
// read the missing columns out of the mirror and the writer wrote them again.
//
// For a table with a large document column that is the dominant cost of running
// the mirror at all. Measured on the jsonb shape in docs/19: 45% of the sidecar
// in writeParquet, and a mirror 1.6x LARGER than the PostgreSQL table it
// mirrors, because every UPDATE copies a 3.3 KB document it never touched into
// a new delta file.
//
// A partial delta stores the change as it arrived. The row it supersedes stays
// exactly where it is, alive, and the delta holds only the columns the change
// carried. The read path merges them.
//
// The invariants that keep this from becoming a linked list of patches — which
// is what would make reads unbounded — are deliberately narrow:
//
//   - A patch stands on a WHOLE row, never on another patch. So a key's value
//     is at most one row plus one patch, never a chain.
//   - A key has at most ONE live patch. A second partial update to the same key
//     supersedes the first, and is only allowed to if it carries at least the
//     same columns; otherwise the row is materialised the old way.
//
// Both fall back to the previous behaviour rather than to anything novel, so
// the worst case is the cost we were paying already.
//
// The first version of this required the row underneath to be in a BASE file,
// which sounded conservative and was in fact the optimisation not happening:
// measured on the jsonb shape, base_rows was 200 against 200,102 rows in
// deltas, because that shape rewrites its whole hot set several times between
// compactions. Almost every row lives in a delta there, so almost every update
// was refused. A whole row is a whole row wherever it lives.
//
// Patches ARE created during a background rewrite, and the swap handles them by
// doing nothing: the rewrite folds in the pre-patch row, the patch it never saw
// was written to a delta outside its snapshot, and the read path puts the two
// back together. That is why a patched key is deliberately NOT added to the
// rewrite's `touched` set — the whole row did not move.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// PartialDeltas turns the optimisation off, so that its cost and its benefit
// can be measured against the same binary rather than argued about. See docs/19.
var PartialDeltas = os.Getenv("QS_PARTIAL_DELTAS") != "0"

// maxPartialGroups bounds how many distinct column shapes one batch may write.
// Each shape is a separate file, and the read path pays per file — the 203-file
// measurement in docs/11 is what that costs. Two covers the real case (one
// shape for the updates, one for anything else); beyond that the batch's odd
// rows are written whole, which is merely the old behaviour.
const maxPartialGroups = 2

// partialColsOf returns the columns a delta file carries, or nil if it holds
// whole rows. Base files are always whole rows.
func (t *Table) partialColsOf(rel string) []string {
	if len(t.State.PartialCols) == 0 {
		return nil
	}
	return t.State.PartialCols[filepath.Base(rel)]
}

// rebuildPartialCols mirrors State.PartialCols into a map keyed by file id, so
// that the per-key question "does the file this patch lives in carry column c?"
// costs a lookup rather than rendering a file name.
func (t *Table) rebuildPartialCols() {
	t.partialCols = make(map[fileID][]string, len(t.State.PartialCols))
	for name, cols := range t.State.PartialCols {
		t.partialCols[deltaID(name)] = cols
	}
}

// colSignature names the shape of a change: which of the table's columns it
// carried, in the table's own column order.
func colSignature(row map[string]any, order []string) (string, []string) {
	cols := make([]string, 0, len(row))
	for _, c := range order {
		if _, ok := row[c]; ok {
			cols = append(cols, c)
		}
	}
	return strings.Join(cols, "\x00"), cols
}

func containsStr(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// canPatch decides whether a change can be stored as it arrived. Every "no"
// here means the row gets carried forward and written whole, which is correct
// by construction — it is what the mirror did before any of this existed.
func (t *Table) canPatch(k rowKey, row map[string]any) bool {
	if len(row) >= len(t.Order) {
		return false // carries everything; there is nothing to leave behind
	}
	if _, ok := row[t.Key]; !ok {
		return false // without the key it cannot be matched to a row
	}
	if _, ok := t.index.get(k); !ok {
		// No whole row to stand on. This is a genuine insert, or a key whose
		// row was deleted; either way the change is all there is.
		return false
	}
	if p, ok := t.patch.get(k); ok {
		// Superseding a patch means covering everything it carried; otherwise
		// the older patch's columns would have to survive underneath it, and
		// that is the chain this design exists to avoid.
		for _, c := range t.partialCols[p.File] {
			if _, has := row[c]; !has {
				return false
			}
		}
	}
	return true
}

// classifyPartial picks the keys in a batch that will be written as patches.
func (t *Table) classifyPartial(upserts map[rowKey]map[string]any) (map[rowKey]bool, error) {
	out := map[rowKey]bool{}
	if !PartialDeltas || len(upserts) == 0 {
		return out, nil
	}
	if err := t.ensureIndex(); err != nil {
		return nil, err
	}
	bySig := map[string][]rowKey{}
	for k, row := range upserts {
		if !t.canPatch(k, row) {
			continue
		}
		sig, _ := colSignature(row, t.Order)
		bySig[sig] = append(bySig[sig], k)
	}
	if len(bySig) == 0 {
		return out, nil
	}
	sigs := make([]string, 0, len(bySig))
	for s := range bySig {
		sigs = append(sigs, s)
	}
	// Busiest shapes first, ties broken by name so the choice is deterministic
	// and two identical runs produce identical files.
	sort.Slice(sigs, func(i, j int) bool {
		if len(bySig[sigs[i]]) != len(bySig[sigs[j]]) {
			return len(bySig[sigs[i]]) > len(bySig[sigs[j]])
		}
		return sigs[i] < sigs[j]
	})
	if len(sigs) > maxPartialGroups {
		sigs = sigs[:maxPartialGroups]
	}
	for _, s := range sigs {
		for _, k := range bySig[s] {
			out[k] = true
		}
	}
	return out, nil
}

// patchOverlay reads every live patch into memory, keyed by row key.
//
// Bounded by the rows in delta files, not by the table: patches live only in
// deltas, a compaction folds them into the base, and they carry only the
// columns their change carried. The read path reads them once per scan rather
// than seeking into them per row, because a patch stands on a base row and
// random access into Parquet costs a row group either way.
func (t *Table) patchOverlay() (map[rowKey]map[string]any, error) {
	if len(t.State.PartialCols) == 0 {
		return nil, nil
	}
	var out map[rowKey]map[string]any
	for _, d := range t.State.DeltaFiles {
		cols := t.State.PartialCols[d]
		if len(cols) == 0 {
			continue
		}
		rel := filepath.Join("delta", d)
		dead, err := t.deadPositions(rel)
		if err != nil {
			return nil, err
		}
		rows, err := t.readParquetCols(filepath.Join(t.Dir, rel), dead, cols)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, ErrStaleManifest
			}
			return nil, err
		}
		if out == nil {
			out = make(map[rowKey]map[string]any, len(rows))
		}
		// Files are listed oldest first, so a later patch for the same key wins.
		for _, r := range rows {
			out[t.keyOf(r[t.Key])] = r
		}
	}
	return out, nil
}

// applyPatch merges a patch into a whole row, in place.
func applyPatch(row, patch map[string]any) {
	for c, v := range patch {
		row[c] = v
	}
}

// forgetDeltaFile drops the bookkeeping for a delta file that no longer exists.
// Leaving it behind is not merely untidy: the file names are a sequence, and a
// stale PartialCols entry would make a future whole-row file read as a patch.
func (t *Table) forgetDeltaFile(name string) {
	delete(t.State.PartialCols, name)
	delete(t.State.FileRows, name)
	delete(t.partialCols, deltaID(name))
}

// recountDeltaRows recomputes the compaction trigger from the files that
// actually exist. It replaces a running total that drifted every time a merge
// or a swap removed files it had counted.
func (t *Table) recountDeltaRows() {
	n := 0
	for _, d := range t.State.DeltaFiles {
		n += t.State.FileRows[d]
	}
	t.State.DeltaRows = n
}

func (t *Table) noteDeltaFile(name string, rows int, cols []string) {
	if t.State.FileRows == nil {
		t.State.FileRows = map[string]int{}
	}
	t.State.FileRows[name] = rows
	if cols != nil {
		if t.State.PartialCols == nil {
			t.State.PartialCols = map[string][]string{}
		}
		t.State.PartialCols[name] = cols
		if t.partialCols == nil {
			t.partialCols = map[fileID][]string{}
		}
		t.partialCols[deltaID(name)] = cols
	}
}

package mirror

// One file that says what the manifests of untouched tables used to.
//
// Table.Apply advanced applied_lsn for every transaction in a batch whether or
// not it touched the table, and wrote the manifest to say so: temp file,
// fsync, rename, directory sync, and then the published snapshot the same way.
// It had to. On restart a table whose applied_lsn is more than one behind the
// slot's confirmed position is re-snapshotted (cmd/qs-mirror/resume.go), and a
// table nobody writes to would fall behind at once. So a mirror of a hundred
// tables with one of them busy paid for a hundred manifests and a hundred
// snapshots on every tick — four hundred syncs to record that ninety-nine
// tables had seen nothing.
//
// An Apply that changed nothing in its table now writes nothing. What it
// learned — "this table has applied through L" — is kept in memory, and ONE
// file per mirror root, watermark.json, records it for all of them:
//
//	{"applied_lsn": W, "tables": {"public.a": A, "public.b": B}}
//
// "Each table named here had applied every transaction through W when this
// was written, and its manifest then stood at the LSN beside its name."
//
// WHEN IT IS WRITTEN. Before the position is confirmed to the server, and
// only then: changestream.BeforeConfirm runs SyncWatermarks at the start of
// Streaming.Confirm, on the apply goroutine, after every table has applied the
// batch. It is written temp-fsync-rename-syncdir like a manifest, and a
// failure fails the Confirm. It is skipped when every table it would name has
// a manifest that already says as much, so a mirror whose every table changed
// in the tick writes no watermark at all.
//
// WHAT A TABLE'S APPLIED LSN IS, after a restart (effectiveLSN):
//
//	the watermark's W   if the watermark names the table, the LSN beside its
//	                    name is exactly the one the table's manifest holds,
//	                    and W is the later of the two;
//	its manifest's      otherwise.
//
// The equality is the phrase "untouched since". A manifest that has moved on
// since the watermark was written is newer than it (a table's LSN never goes
// back, and it was at least W in memory when the watermark named it), so the
// manifest is the answer. A manifest OLDER than the one the watermark saw — a
// table directory restored from a backup — does not match either, falls back
// to itself, and is re-snapshotted exactly as it was before this file existed.
// A table the watermark does not name (halted, added later, failed its Apply)
// is not vouched for.
//
// CRASH CONSISTENCY. The claim to keep is the one resume.go makes: the slot's
// confirmed position is at most one past every table's effective applied LSN.
// Walk a tick that ends at commit L, confirming C = L+1:
//
//  1. Tables the batch touched write their manifests (applied_lsn = L). A
//     crash here leaves the slot at the previous C' <= every effective LSN:
//     the server resends from C', the touched tables' floors discard what
//     they have, and the untouched ones never had anything to apply.
//  2. SyncWatermarks makes W = L durable, naming every table that has
//     applied through L. A crash after this and before 3 is the same as 1
//     with later effective LSNs, all still >= C'-1.
//  3. Confirm records C. From here a restart may find the slot at C, and
//     every table's effective LSN is L: from its manifest if it was touched,
//     from the watermark if not. C <= L+1 holds for each.
//
// Step 2 before step 3 is the only ordering this adds, and Streaming.Confirm
// enforces it by construction. A table that changes AFTER a watermark writes
// its own manifest, at an LSN at or above W, and stops matching the entry: it
// uses its own.
//
// New folds the watermark into State.AppliedLSN when it opens a table, so
// everything that reads the field — planResume, the starting position of the
// apply loop — sees the effective LSN without knowing this file exists.
// EffectiveAppliedLSN and ReadWatermark are there for a caller that would
// rather ask.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// WatermarkFile is the name of the watermark, in the mirror root.
const WatermarkFile = "watermark.json"

// Watermark is the contents of watermark.json. See the top of this file.
type Watermark struct {
	// AppliedLSN is the commit LSN every table in Tables had applied through.
	AppliedLSN string `json:"applied_lsn"`
	// Tables maps a qualified table name to the applied_lsn its MANIFEST held
	// when this was written. The watermark speaks for a table only while its
	// manifest still holds exactly that.
	Tables    map[string]string `json:"tables"`
	WrittenAt time.Time         `json:"written_at"`
}

// ReadWatermark reads a mirror root's watermark. A root without one — every
// mirror written before this file existed — returns nil and no error.
func ReadWatermark(root string) (*Watermark, error) {
	b, err := os.ReadFile(filepath.Join(root, WatermarkFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var w Watermark
	if err := json.Unmarshal(b, &w); err != nil {
		return nil, fmt.Errorf("%s: %w", WatermarkFile, err)
	}
	return &w, nil
}

// effectiveLSN is the rule at the top of this file, and the only place it is
// written down in code.
func effectiveLSN(qualified, manifestLSN string, w *Watermark) string {
	if w == nil || manifestLSN == "" {
		// A table with no applied_lsn has never been snapshotted. No
		// watermark can speak for it.
		return manifestLSN
	}
	seen, named := w.Tables[qualified]
	if !named || seen != manifestLSN {
		return manifestLSN
	}
	if changestream.ParseLSN(w.AppliedLSN) > changestream.ParseLSN(manifestLSN) {
		return w.AppliedLSN
	}
	return manifestLSN
}

// EffectiveAppliedLSN is the LSN a table has durably applied through: its
// manifest's applied_lsn, or the root watermark's when that speaks for it. It
// reads both files and opens nothing, so it can be asked of a table before its
// columns are known. A table with no manifest reports "".
//
// It is what a resume check compares with the slot. Table.State.AppliedLSN of
// a table opened with New already holds the same value.
func EffectiveAppliedLSN(root, schema, table string) (string, error) {
	q := schema + "." + table
	b, err := os.ReadFile(filepath.Join(root, q, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var st struct {
		AppliedLSN string `json:"applied_lsn"`
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return "", fmt.Errorf("%s state.json: %w", q, err)
	}
	w, err := ReadWatermark(root)
	if err != nil {
		return "", err
	}
	return effectiveLSN(q, st.AppliedLSN, w), nil
}

// EffectiveAppliedLSN is the LSN this table has applied through, in memory:
// its manifest's, the watermark's it was opened under, or that of an Apply
// which changed nothing in it and so wrote no manifest. It is durable once
// the Confirm that follows that Apply has returned.
func (t *Table) EffectiveAppliedLSN() string { return t.State.AppliedLSN }

// ManifestAppliedLSN is the applied_lsn of the manifest on disk, which lags
// EffectiveAppliedLSN on a table that has been quiet.
func (t *Table) ManifestAppliedLSN() string {
	t.wm.Lock()
	defer t.wm.Unlock()
	return t.wm.disk
}

// markState is what the watermark needs to know about a table. The apply
// goroutine writes it and SyncWatermarks may read it from another, hence the
// lock; nothing else of a Table is touched off the apply goroutine.
type markState struct {
	sync.Mutex
	mem    string // applied through, in memory
	disk   string // applied_lsn of the manifest on disk
	halted bool
	root   string // registered under this root, once it has applied
}

// applyAlwaysSaves restores a manifest write on every Apply, changed or not.
// It exists so the cost this file removes can still be measured
// (TestIdleTablesCostNothingPerTick).
var applyAlwaysSaves = false

var watermarks = struct {
	sync.Mutex
	roots map[string]map[*Table]bool
	hook  sync.Once
}{roots: map[string]map[*Table]bool{}}

// noteManifest records the manifest just made durable.
func (t *Table) noteManifest(lsn, halted string) {
	t.wm.Lock()
	t.wm.disk, t.wm.halted = lsn, halted != ""
	if changestream.ParseLSN(lsn) >= changestream.ParseLSN(t.wm.mem) {
		t.wm.mem = lsn
	}
	t.wm.Unlock()
}

// noteApplied records that an Apply has taken this table through lsn, and
// makes the table one the root's watermark speaks for.
func (t *Table) noteApplied(lsn string) {
	t.wm.Lock()
	t.wm.mem = lsn
	registered := t.wm.root != ""
	if !registered {
		t.wm.root = filepath.Dir(t.Dir)
	}
	root := t.wm.root
	t.wm.Unlock()
	if registered {
		return
	}
	watermarks.Lock()
	if watermarks.roots[root] == nil {
		watermarks.roots[root] = map[*Table]bool{}
	}
	watermarks.roots[root][t] = true
	watermarks.Unlock()
	watermarks.hook.Do(func() { changestream.BeforeConfirm(SyncWatermarks) })
}

// forgetWatermark takes a closed table out of its root's watermark.
func (t *Table) forgetWatermark() {
	t.wm.Lock()
	root := t.wm.root
	t.wm.root = ""
	t.wm.Unlock()
	if root == "" {
		return
	}
	watermarks.Lock()
	delete(watermarks.roots[root], t)
	if len(watermarks.roots[root]) == 0 {
		delete(watermarks.roots, root)
	}
	watermarks.Unlock()
}

// SyncWatermarks makes durable, for every mirror root this process writes,
// what its tables have applied and their manifests do not say, ahead of
// confirming through confirmLSN (a transaction's NextLSN: one past the last
// applied commit). Streaming.Confirm calls it; a caller with another kind of
// stream calls it itself, immediately before its own confirm.
func SyncWatermarks(confirmLSN string) error {
	watermarks.Lock()
	roots := make([]string, 0, len(watermarks.roots))
	for root := range watermarks.roots {
		roots = append(roots, root)
	}
	watermarks.Unlock()
	sort.Strings(roots)
	for _, root := range roots {
		if err := SyncWatermark(root, confirmLSN); err != nil {
			return err
		}
	}
	return nil
}

// SyncWatermark is SyncWatermarks for one root.
//
// It names every open, unhalted table under root that has applied through
// confirmLSN-1, and writes nothing if each of those has a manifest that
// already says so. A table that has not got that far — halted, failed, still
// below its snapshot's floor — is left out, and after a restart answers for
// itself.
func SyncWatermark(root, confirmLSN string) error {
	c := changestream.ParseLSN(confirmLSN)
	if c == 0 {
		return nil
	}
	watermarks.Lock()
	tables := make([]*Table, 0, len(watermarks.roots[root]))
	for t := range watermarks.roots[root] {
		tables = append(tables, t)
	}
	watermarks.Unlock()

	w := Watermark{
		AppliedLSN: lsnString(c - 1),
		Tables:     map[string]string{},
		WrittenAt:  time.Now().UTC(),
	}
	needed := false
	for _, t := range tables {
		t.wm.Lock()
		mem, disk, halted := t.wm.mem, t.wm.disk, t.wm.halted
		t.wm.Unlock()
		if halted || disk == "" || changestream.ParseLSN(mem)+1 < c {
			continue
		}
		w.Tables[t.Qualified] = disk
		if changestream.ParseLSN(disk)+1 < c {
			needed = true
		}
	}
	if !needed {
		return nil
	}
	b, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(root, WatermarkFile), b); err != nil {
		return fmt.Errorf("watermark: %w", err)
	}
	if err := syncDir(root); err != nil {
		return fmt.Errorf("watermark: %w", err)
	}
	return nil
}

func lsnString(v uint64) string { return fmt.Sprintf("%X/%X", uint32(v>>32), uint32(v)) }

// Package mirror is the columnar mirror: writer, deletion vectors, compaction
// and verification.
//
// Implements docs/04 as corrected by measurement: deletes and superseded rows
// resolve via DELETION VECTORS computed at write/compaction time, never a
// query-time _lsn window function. The original design measured 33x even at
// zero deltas (docs/11 Result 5); deletion vectors measured 0.8-1.1x.
//
// Layout per mirrored table:
//
//	<root>/<schema.table>/
//	  base/000001.parquet     compacted data
//	  delta/000007.parquet    recent micro-batches, append-only
//	  dv/000001.000000003.dv.json
//	                          dead row POSITIONS in the matching data file, as
//	                          of generation 3. Vectors are never modified in
//	                          place; state.json names the generation that
//	                          belongs to each file, so a manifest and the
//	                          vectors it names are one point in time.
//	  index/000001.idx.json   key -> row position, needed to build a dv
//	  state.json              applied_lsn + manifest
//
// Unlike the Python reference, nothing here needs a query engine: verification
// reads Parquet directly. The writer should not depend on DuckDB.
package mirror

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

type State struct {
	AppliedLSN string   `json:"applied_lsn"`
	BaseFiles  []string `json:"base_files"`
	DeltaFiles []string `json:"delta_files"`
	Seq        int      `json:"seq"`

	// MissingVals mirrors pg_attribute.attmissingval: the value a row written
	// before a column existed reads back as. ADD COLUMN ... DEFAULT emits no
	// row-level WAL, so without this the mirror serves NULL where the source
	// serves the default, with identical row counts and no error anywhere.
	MissingVals map[string]string `json:"missing_vals,omitempty"`

	// BaseRows and DeltaRows drive the compaction policy. Without them the only
	// available trigger is "every N batches", which is a count of the wrong
	// thing: compaction rewrites the WHOLE base file, so its cost scales with
	// the table while its trigger scaled with traffic. On a 5M-row table that
	// measured a 30x collapse in apply throughput and 6 GB of RSS, because a
	// full rewrite ran every 32 transactions regardless of how little had
	// actually changed.
	BaseRows  int `json:"base_rows"`
	DeltaRows int `json:"delta_rows"`

	// PartialCols names, per delta file, the columns that file's rows carry
	// when it holds PATCHES rather than whole rows. A file absent from this map
	// holds whole rows. See partial.go.
	PartialCols map[string][]string `json:"partial_cols,omitempty"`

	// FileRows is the row count of each delta file. DeltaRows is recomputed
	// from it rather than accumulated, because the running total drifted every
	// time a merge or a compaction swap removed files it had already counted —
	// and DeltaRows is the compaction trigger, so drift there is a mirror that
	// either rewrites constantly or never.
	FileRows map[string]int `json:"file_rows,omitempty"`

	// DVGen names WHICH deletion vector belongs to each data file.
	//
	// Deletion vectors used to be overwritten in place, at a fixed path per data
	// file, and that made a manifest and its vectors two different points in
	// time. A reader that loaded this file and then read a vector could see a
	// row marked dead by a write whose replacement row was not in the manifest
	// it holds — the row is then in neither place, and the reader is silently
	// short by exactly one row. It reproduced about once in seven benchmark
	// runs, and it is the failure mode a mirror can least afford: the reader is
	// wrong while every file on disk is right.
	//
	// Vectors are now written to a NEW generation and never modified, so a
	// manifest describes a complete, immutable snapshot. Old generations are
	// kept for one further write before being removed, and a reader that finds
	// the generation its manifest names already gone gets ErrStaleManifest and
	// retries — which is the difference between "I could not tell" and a wrong
	// answer, again.
	DVGen map[string]int `json:"dv_gen,omitempty"`

	// Halted, when set, is why this mirror stopped and must not be served.
	// It is durable on purpose: a halt that lives only in a running process is
	// undone by the next restart, which re-reads the catalog, sees the new
	// schema as if it had always been there, and serves the corruption it
	// stopped for.
	Halted string `json:"halted,omitempty"`
}

type Table struct {
	Qualified string
	Key       string
	Columns   map[string]string // name -> postgres type
	Order     []string          // stable column order
	Dir       string
	State     State

	// index maps key -> current row location, across base AND delta files. It
	// is the difference between an apply that touches only the files it has to
	// and one that rewrites everything.
	//
	// It used to live only on disk, as one JSON map per base file, re-read and
	// re-parsed on EVERY batch. On a 5M-row table that is a 5M-entry JSON parse
	// per batch, which is most of the 30x apply-throughput collapse measured in
	// docs/18. Held in memory it is built once per process and updated in place.
	index *keyIndex

	// patch maps key -> the newest PARTIAL row for that key, which stands on
	// top of the whole row index points at. Empty unless column-partial deltas
	// are in use; at most one entry per key, and only ever pointing into a
	// delta file. See partial.go.
	patch *keyIndex

	// numericKey is whether the primary key is an integer type, which decides
	// whether the index can hold keys as int64 rather than as strings. See
	// index.go: it is the difference between a map the garbage collector walks
	// entry by entry and one it never looks inside.
	numericKey bool

	// partialCols is State.PartialCols keyed by file id, so that the per-key
	// question "is the file this patch lives in a partial one?" does not have
	// to render a file name.
	partialCols map[fileID][]string

	// sawLarge is whether any row of this table has ever carried a column big
	// enough to be worth not rewriting. Until one does, elide.go costs nothing
	// and does nothing.
	sawLarge bool

	// elideWorked is whether an elision has ever actually fired. It gates the
	// only part of elide.go that is not free: a compaction hashing the large
	// columns of every row it rewrites.
	//
	// "Has large columns" is NOT the same question. The jsonb shape has a 6 KB
	// document in every row and can never elide anything, because pgoutput
	// omits that document on every update and an omitted column makes the
	// digest unknowable. Hashing on the first signal cost that shape 1.2 GB of
	// hashing per rewrite for nothing, and left it with 24 delta files and a
	// mirror 631 MB instead of 330 MB.
	elideWorked bool

	// compacting is the rewrite currently in flight, if any. Only the apply
	// goroutine touches this field.
	compacting *Compaction
}

func New(root, schema, table, key string, cols map[string]string, order []string) (*Table, error) {
	t := &Table{
		Qualified: schema + "." + table,
		Key:       key,
		Columns:   cols,
		Order:     order,
		Dir:       filepath.Join(root, schema+"."+table),
	}
	t.numericKey = arrowType(cols[key]) == arrow.PrimitiveTypes.Int64
	for _, sub := range []string{"base", "delta", "dv", "index"} {
		if err := os.MkdirAll(filepath.Join(t.Dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	if b, err := os.ReadFile(t.statePath()); err == nil {
		_ = json.Unmarshal(b, &t.State)
	}
	return t, nil
}

// ensureIndex builds the key -> location map if it is not loaded. The base
// file's index is persisted (written by Snapshot and Compact), so only the
// deltas have to be read back, and those are small by construction.
func (t *Table) ensureIndex() error {
	if t.index != nil {
		return nil
	}
	t.index = newKeyIndex(t.State.BaseRows)
	t.patch = newKeyIndex(0)
	t.rebuildPartialCols()

	for _, base := range t.State.BaseFiles {
		rel := filepath.Join("base", base)
		id := baseID(base)
		stem := trimParquet(base)
		raw, err := os.ReadFile(filepath.Join(t.Dir, "index", stem+".idx.json"))
		if err == nil {
			var m map[string]int
			if json.Unmarshal(raw, &m) == nil {
				for k, pos := range m {
					t.index.set(t.keyOfString(k), loc{File: id, Pos: int32(pos)})
				}
				continue
			}
		}
		// No persisted index: rebuild it from the file. This is not a degraded
		// path, it is the normal one — the snapshot stopped writing that JSON
		// because the file already holds everything it said.
		//
		// ONE COLUMN, not all of them. The key column and the row's position
		// are the entire content of the index, and this is a column store, so
		// asking for the key alone reads a fraction of the file. Passing nil
		// here meant "every column", which on the jsonb shape is a 6 KB
		// document per row decoded to learn its id.
		if err := t.forEachRowGroup(filepath.Join(t.Dir, rel), nil, []string{t.Key},
			func(rows []map[string]any, first int) error {
				for i, r := range rows {
					t.index.set(t.keyOf(r[t.Key]), loc{File: id, Pos: int32(first + i)})
				}
				return nil
			}); err != nil {
			return err
		}
	}

	for _, d := range t.State.DeltaFiles {
		rel := filepath.Join("delta", d)
		id := deltaID(d)
		partial := t.State.PartialCols[d]
		dead, derr := t.deadPositions(rel)
		if derr != nil {
			return derr
		}
		target := t.index
		if partial != nil {
			target = t.patch
		}
		n := 0
		err := t.forEachRowGroup(filepath.Join(t.Dir, rel), dead, partial,
			func(rows []map[string]any, first int) error {
				for i, r := range rows {
					target.set(t.keyOf(r[t.Key]), loc{File: id, Pos: int32(first + i)})
				}
				n = first + len(rows)
				return nil
			})
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		// A mirror written before FileRows existed has no count for this file,
		// and DeltaRows is derived from those counts. Filling it in from the
		// read we are doing anyway is the difference between a mirror that
		// compacts and one that never reaches the trigger again.
		if _, known := t.State.FileRows[d]; !known {
			t.noteDeltaFile(d, n, nil)
		}
	}
	t.recountDeltaRows()
	return nil
}

// deadPositions reads the deletion vector THIS MANIFEST names for a file. Every
// file gets one, base and delta alike: marking a superseded row dead is O(1),
// whereas rewriting the file that holds it is O(file) and was being done on
// every batch.
//
// Two ways of not getting an answer are errors rather than empty sets, and both
// distinctions were paid for:
//
//   - a vector that exists but cannot be parsed. "This file has no deleted
//     rows" and "I could not tell" are not the same statement, and spelling the
//     second as the first resurrects every delete in the file.
//   - a vector the manifest names that is no longer on disk. That means the
//     manifest is older than the mirror, and answering from a newer vector
//     would drop rows whose replacements this manifest cannot see.
func (t *Table) deadPositions(rel string) (map[int]bool, error) {
	gen := t.State.DVGen[dvStem(rel)]
	if gen == 0 {
		return map[int]bool{}, nil // genuinely nothing deleted in this file yet
	}
	return t.readDV(t.dvPathGenAny(rel, gen), rel)
}

// deadPositionsLatest reads the newest deletion vector on disk for a file,
// ignoring the manifest.
//
// This is for the background compactor, which cannot read t.State.DVGen — the
// apply goroutine writes it on every batch — and does not need to: seeing MORE
// rows as dead than its snapshot accounts for is safe there, because every key
// that moved while the rewrite ran is reconciled against the live index at swap
// time anyway. An external reader must never do this; see deadPositions.
func (t *Table) deadPositionsLatest(rel string) (map[int]bool, error) {
	matches := t.dvGlob(rel)
	if len(matches) == 0 {
		return map[int]bool{}, nil
	}
	return t.readDV(matches[len(matches)-1], rel)
}

func (t *Table) readDV(path, rel string) (map[int]bool, error) {
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
	dead := make(map[int]bool, len(pos))
	for _, p := range pos {
		dead[p] = true
	}
	return dead, nil
}

func dvStem(rel string) string { return trimParquet(filepath.Base(rel)) }

// dvPathGen names the file a NEW generation of a deletion vector is written to.
// Generations are zero-padded so that lexical order is numeric order, which is
// what lets the compactor find the newest with a glob.
func (t *Table) dvPathGen(rel string, gen int) string {
	return filepath.Join(t.Dir, "dv", fmt.Sprintf("%s.%09d.dv.parquet", dvStem(rel), gen))
}

// dvPathGenAny names a generation that already exists, in whichever encoding it
// was written in — see dv.go. A mirror written before deletion vectors became
// Parquet still has .dv.json on disk, and failing to find it does not read as
// an error anywhere: it reads as "nothing in that file is dead", which
// resurrects every row the vector retired.
//
// It returns "" when neither exists, which callers turn into ErrStaleManifest.
func (t *Table) dvPathGenAny(rel string, gen int) string {
	for _, ext := range dvExts {
		p := filepath.Join(t.Dir, "dv", fmt.Sprintf("%s.%09d.dv%s", dvStem(rel), gen, ext))
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// dvGlob matches every generation of one file's deletion vector, in both
// encodings, in an order where the newest sorts last.
func (t *Table) dvGlob(rel string) []string {
	var all []string
	for _, ext := range dvExts {
		m, _ := filepath.Glob(filepath.Join(t.Dir, "dv", dvStem(rel)+".*.dv"+ext))
		all = append(all, m...)
	}
	// Sort on the generation number rather than the whole name, because the
	// extension is part of the name and ".json" sorts after ".parquet" — so a
	// plain lexical sort would hand back a stale JSON vector as the newest
	// during the one changeover where both exist.
	sort.Slice(all, func(i, j int) bool { return dvGenOf(all[i]) < dvGenOf(all[j]) })
	return all
}

// dvExts is every encoding a deletion vector may be on disk, newest format
// first — dvPathGenAny prefers the earlier entry when both exist.
var dvExts = []string{".parquet", ".json"}

// dvGenOf pulls the generation out of a vector's filename, or -1.
func dvGenOf(path string) int {
	base := filepath.Base(path)
	i := strings.Index(base, ".dv.")
	if i < 0 {
		return -1
	}
	j := strings.LastIndex(base[:i], ".")
	if j < 0 {
		return -1
	}
	n, err := strconv.Atoi(base[j+1 : i])
	if err != nil {
		return -1
	}
	return n
}

// dropDV removes every generation of a file's deletion vector, for when the
// data file itself is going away.
func (t *Table) dropDV(rel string) {
	for _, m := range t.dvGlob(rel) {
		_ = os.Remove(m)
	}
	delete(t.State.DVGen, dvStem(rel))
}

// removeDataFile deletes a data file and the deletion vectors that described it.
func (t *Table) removeDataFile(rel string) {
	_ = os.Remove(filepath.Join(t.Dir, rel))
	t.dropDV(rel)
}

func (t *Table) statePath() string { return filepath.Join(t.Dir, "state.json") }

// saveState writes the durable watermark AFTER the data files, so a crash
// replays the last batch rather than skipping it.
func (t *Table) saveState() error {
	b, err := json.MarshalIndent(t.State, "", "  ")
	if err != nil {
		return err
	}
	tmp := t.statePath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	_ = f.Sync()
	_ = f.Close()
	return os.Rename(tmp, t.statePath())
}

// Evolve adopts a new column list. Existing files are untouched — the read path
// fills in what they lack — so there is no rewrite and no downtime.
//
// missing carries the values that rows predating an added column must read back
// as (PostgreSQL's attmissingval). A column absent from the map fills with NULL,
// which is correct only when ADD COLUMN had no default.
func (t *Table) Evolve(cols map[string]string, order []string, missing map[string]string) error {
	t.Columns, t.Order = cols, order
	for k, v := range missing {
		if t.State.MissingVals == nil {
			t.State.MissingVals = map[string]string{}
		}
		t.State.MissingVals[k] = v
	}
	return t.saveState()
}

// Halt records, durably, that this mirror cannot be trusted and why. Recovery
// is deliberately manual: the reasons a mirror halts are ones where guessing
// produced the problem in the first place.
func (t *Table) Halt(reason string) error {
	t.State.Halted = reason
	return t.saveState()
}

// Halted reports the reason this mirror is out of service, or "".
func (t *Table) Halted() string { return t.State.Halted }

// ---- arrow schema ---------------------------------------------------------

// ArrowSchema maps declared Postgres types to a FIXED Arrow schema. Inferring
// per batch makes an all-NULL column null-typed, so schemas drift between delta
// files and the union read path breaks (found by the Python reference).
func (t *Table) ArrowSchema() *arrow.Schema { return t.arrowSchemaFor(t.Order) }

func (t *Table) arrowSchemaFor(cols []string) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(cols))
	for _, name := range cols {
		fields = append(fields, arrow.Field{
			Name: name, Type: arrowType(t.Columns[name]), Nullable: true,
		})
	}
	return arrow.NewSchema(fields, nil)
}

func arrowType(pgType string) arrow.DataType {
	// Temporal types first: see temporal.go for why they were text until
	// docs/22 made parsing them a fact rather than a guess.
	if k := temporalOf(pgType); k != notTemporal {
		return temporalArrowType(k)
	}
	s := strings.ToLower(strings.TrimSpace(pgType))
	switch {
	case strings.HasPrefix(s, "numeric"), strings.HasPrefix(s, "decimal"):
		prec, scale := int32(38), int32(9)
		if i := strings.Index(s, "("); i >= 0 {
			if j := strings.Index(s, ")"); j > i {
				parts := strings.SplitN(s[i+1:j], ",", 2)
				if p, err := strconv.Atoi(strings.TrimSpace(parts[0])); err == nil {
					prec = int32(p)
				}
				if len(parts) == 2 {
					if sc, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
						scale = int32(sc)
					}
				}
			}
		}
		return &arrow.Decimal128Type{Precision: prec, Scale: scale}
	case s == "smallint", s == "int", s == "integer", s == "bigint",
		s == "int2", s == "int4", s == "int8":
		return arrow.PrimitiveTypes.Int64
	case s == "bool", s == "boolean":
		return arrow.FixedWidthTypes.Boolean
	case strings.HasPrefix(s, "double"), s == "real":
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.BinaryTypes.String
	}
}

// ---- write path -----------------------------------------------------------

type ApplyStats struct {
	Upserts    int
	Deletes    int
	AppliedLSN string
}

// Apply lands a run of whole transactions atomically: every change in every
// transaction, then applied_lsn advances. There is no state in which half a
// transaction is visible.
func (t *Table) Apply(txns []changestream.Transaction) (ApplyStats, error) {
	upserts := map[rowKey]map[string]any{}
	order := []rowKey{}
	deletes := map[rowKey]bool{}
	last := t.State.AppliedLSN

	// Per-table LSN floor. A transaction this table has already applied is
	// skipped, and the reason is not idempotence — the mirror is upsert-by-key,
	// so replaying the same change twice converges anyway.
	//
	// It is for a table that joined a slot the others were already using. That
	// table is snapshotted at the CURRENT LSN while the stream resumes from the
	// slot's older confirmed position, so without a floor it would replay
	// changes from BEFORE its snapshot on top of it — writing values the source
	// had moved past, with correct row counts and no error anywhere. A table
	// that has never been bootstrapped has no floor and takes everything.
	floor := changestream.ParseLSN(t.State.AppliedLSN)

	for _, txn := range txns {
		if floor > 0 && changestream.ParseLSN(txn.CommitLSN) < floor {
			continue
		}
		for _, c := range txn.Changes {
			if c.Qualified() != t.Qualified {
				continue
			}
			switch c.Op {
			case changestream.OpInsert, changestream.OpUpdate:
				k := t.keyOf(c.Row[t.Key])
				if _, seen := upserts[k]; !seen {
					order = append(order, k)
				}
				// An earlier change to the same key in this batch is the better
				// base than anything on disk, so merge onto it. A column absent
				// from BOTH is filled from the mirror afterwards.
				if prev, seen := upserts[k]; seen {
					merged := make(map[string]any, len(t.Order))
					for name, v := range prev {
						merged[name] = v
					}
					for name, v := range c.Row {
						merged[name] = v
					}
					upserts[k] = merged
				} else {
					upserts[k] = c.Row
				}
				delete(deletes, k)
			case changestream.OpDelete:
				k := t.keyOf(c.Key[t.Key])
				deletes[k] = true
				delete(upserts, k)
			case changestream.OpTruncate:
				if err := t.truncate(); err != nil {
					return ApplyStats{}, err
				}
				upserts, order, deletes = map[rowKey]map[string]any{}, nil, map[rowKey]bool{}
			}
		}
		last = txn.CommitLSN
	}

	// Drop large values the mirror can prove it already holds. pgoutput omits an
	// unchanged TOASTed value but resends an unchanged inline one, so this is
	// the same saving as a column-partial delta for the columns PostgreSQL does
	// not hint about. It runs FIRST, because everything after it simply sees a
	// change that arrived carrying fewer columns.
	elided, err := t.elideUnchanged(upserts)
	if err != nil {
		return ApplyStats{}, err
	}

	// Which of these changes can be stored as they arrived, as a patch on the
	// row already in the base file, and which have to be expanded into whole
	// rows. Decided before carry-forward, because carry-forward is the expansion.
	asPatch, err := t.classifyPartial(upserts)
	if err != nil {
		return ApplyStats{}, err
	}

	// Carry forward anything pgoutput did not resend. A large value that an
	// update did not change arrives as "unchanged TOAST" — the column is simply
	// absent — and writing NULL for it destroys the data with no error and no
	// change in row count. Absent means "keep what is there".
	if err := t.carryForward(upserts, asPatch); err != nil {
		return ApplyStats{}, err
	}

	// Tombstones, in two kinds. A whole-row write supersedes both the row the
	// mirror holds and any patch standing on it. A PATCH supersedes only the
	// previous patch — the row underneath stays alive, exactly where it is,
	// which is the entire point: the large column nobody touched is not
	// rewritten.
	superseded := make(map[rowKey]bool, len(upserts)+len(deletes))
	patchDead := make(map[rowKey]bool, len(upserts)+len(deletes))
	for k := range upserts {
		patchDead[k] = true
		if !asPatch[k] {
			superseded[k] = true
		}
	}
	for k := range deletes {
		superseded[k] = true
		patchDead[k] = true
	}

	// Tell an in-flight rewrite which keys moved out from under it. Its own
	// index is correct for everything else, which is what lets the swap be
	// O(changed) rather than O(table).
	//
	// A PATCH is deliberately not reported. The whole row did not move: the
	// rewrite folds in the pre-patch version, the patch lands in a delta
	// outside the rewrite's snapshot and therefore survives the swap, and the
	// read path puts the two back together. Reporting it would make the swap
	// mark the rewrite's own copy dead and then point the index at a file it
	// is about to delete.
	if t.compacting != nil {
		for k := range upserts {
			if !asPatch[k] {
				t.compacting.touched[k] = true
			}
		}
		for k := range deletes {
			t.compacting.touched[k] = true
		}
	}

	if len(superseded) > 0 || len(patchDead) > 0 {
		if err := t.ensureIndex(); err != nil {
			return ApplyStats{}, err
		}
		byFile := map[fileID][]int{}
		for k := range superseded {
			if l, ok := t.index.get(k); ok {
				byFile[l.File] = append(byFile[l.File], int(l.Pos))
			}
		}
		for k := range patchDead {
			if l, ok := t.patch.get(k); ok {
				byFile[l.File] = append(byFile[l.File], int(l.Pos))
			}
		}
		if err := t.markDead(byFile); err != nil {
			return ApplyStats{}, err
		}
		// A deleted key has no current location. Leaving it in the index would
		// make a later re-insert of the same key tombstone the wrong row.
		for k := range deletes {
			t.index.del(k)
			t.patch.del(k)
		}
		// A whole row replaces whatever patch stood on the old one.
		for k := range upserts {
			if !asPatch[k] {
				t.patch.del(k)
			}
		}
	}
	if len(upserts) > 0 {
		if err := t.writeUpserts(order, upserts, asPatch); err != nil {
			return ApplyStats{}, err
		}
		// After the writes, so the locations the hashes attach to are current.
		t.noteHashes(upserts, asPatch, elided)
	}

	t.State.AppliedLSN = last
	if err := t.saveState(); err != nil {
		return ApplyStats{}, err
	}
	return ApplyStats{Upserts: len(upserts), Deletes: len(deletes), AppliedLSN: last}, nil
}

// carryForward fills columns that a change did not carry, from the row the
// mirror already holds.
//
// skip names the keys that will be written as PATCHES, and therefore must not
// be expanded at all: leaving their columns absent is exactly what keeps the
// large value they never touched out of the file. Everything else is filled in,
// because a whole-row write that left a column absent would write NULL over it.
func (t *Table) carryForward(upserts map[rowKey]map[string]any, skip map[rowKey]bool) error {
	// which keys are short, and which columns each one needs
	needCols := map[rowKey][]string{}
	for k, row := range upserts {
		if skip[k] || len(row) == len(t.Order) {
			continue
		}
		var missing []string
		for _, name := range t.Order {
			if _, ok := row[name]; !ok {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			needCols[k] = missing
		}
	}
	if len(needCols) == 0 {
		return nil
	}
	if err := t.ensureIndex(); err != nil {
		return err
	}

	// A column's newest value may be in a PATCH rather than in the row the
	// index points at, and the row underneath still holds the stale one. So
	// patches are read first, and only what they do not supply is read from
	// the whole row.
	fromPatch := map[rowKey][]string{}
	fromRow := map[rowKey][]string{}
	for k, missing := range needCols {
		rest := missing
		if p, ok := t.patch.get(k); ok {
			pc := t.partialCols[p.File]
			var want, left []string
			for _, c := range missing {
				if containsStr(pc, c) {
					want = append(want, c)
				} else {
					left = append(left, c)
				}
			}
			if len(want) > 0 {
				fromPatch[k] = want
			}
			rest = left
		}
		if len(rest) > 0 {
			fromRow[k] = rest
		}
	}

	if err := t.fetchInto(upserts, fromPatch, t.patch); err != nil {
		return err
	}
	return t.fetchInto(upserts, fromRow, t.index)
}

// fetchInto reads the named columns for the named keys out of wherever `where`
// says they live, batched by file and projected to the columns actually wanted.
//
// Per row it would mean a Parquet decode per update; without the projection it
// would mean decoding a whole row group for a column nobody asked about. Either
// way it would reintroduce the per-batch table scan that docs/18 removed.
func (t *Table) fetchInto(upserts map[rowKey]map[string]any,
	need map[rowKey][]string, where *keyIndex,
) error {
	if len(need) == 0 {
		return nil
	}
	type req struct {
		positions []int
		cols      map[string]bool
		keyAt     map[int]rowKey
	}
	byFile := map[fileID]*req{}
	for k := range need {
		l, ok := where.get(k)
		if !ok {
			// No prior row: this is a genuine insert whose columns really are
			// absent, so NULL is the right answer.
			continue
		}
		r := byFile[l.File]
		if r == nil {
			r = &req{cols: map[string]bool{}, keyAt: map[int]rowKey{}}
			byFile[l.File] = r
		}
		r.positions = append(r.positions, int(l.Pos))
		r.keyAt[int(l.Pos)] = k
		for _, c := range need[k] {
			r.cols[c] = true
		}
	}

	for rel, r := range byFile {
		cols := make([]string, 0, len(r.cols))
		for c := range r.cols {
			cols = append(cols, c)
		}
		sort.Strings(cols)
		got, err := t.readRowsAt(rel.name(), r.positions, cols)
		if err != nil {
			return fmt.Errorf("carry-forward read of %s: %w", rel.name(), err)
		}
		for pos, prior := range got {
			k := r.keyAt[pos]
			row := upserts[k]
			for _, c := range need[k] {
				if v, ok := prior[c]; ok {
					row[c] = v
				}
			}
		}
	}
	return nil
}

func (t *Table) writeUpserts(order []rowKey, upserts map[rowKey]map[string]any,
	asPatch map[rowKey]bool,
) error {
	var full []map[string]any
	groups := map[string][]map[string]any{}
	groupCols := map[string][]string{}
	var groupOrder []string
	for _, k := range order {
		r, ok := upserts[k]
		if !ok {
			continue
		}
		if !asPatch[k] {
			full = append(full, r)
			continue
		}
		sig, cols := colSignature(r, t.Order)
		if _, seen := groups[sig]; !seen {
			groupOrder = append(groupOrder, sig)
			groupCols[sig] = cols
		}
		groups[sig] = append(groups[sig], r)
	}

	if len(full) > 0 {
		if err := t.writeDelta(full, nil); err != nil {
			return err
		}
	}
	for _, sig := range groupOrder {
		if err := t.writeDelta(groups[sig], groupCols[sig]); err != nil {
			return err
		}
	}
	return nil
}

// writeDelta appends one delta file. cols == nil means whole rows; otherwise
// the file holds patches carrying exactly those columns.
func (t *Table) writeDelta(rows []map[string]any, cols []string) error {
	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	if err := t.writeParquetCols(filepath.Join(t.Dir, "delta", name), rows, cols, false); err != nil {
		return err
	}
	t.State.DeltaFiles = append(t.State.DeltaFiles, name)
	t.noteDeltaFile(name, len(rows), cols)
	t.recountDeltaRows()

	// The new rows are now the current location for their keys. Updating the
	// index here rather than rebuilding it is what keeps apply O(change).
	if err := t.ensureIndex(); err != nil {
		return err
	}
	id := deltaID(name)
	target := t.index
	if cols != nil {
		target = t.patch
	}
	for i, r := range rows {
		target.set(t.keyOf(r[t.Key]), loc{File: id, Pos: int32(i)})
	}
	return nil
}

func (t *Table) writeParquetCodec(path string, rows []map[string]any, durable bool) error {
	return t.writeParquetCols(path, rows, nil, durable)
}

// writeParquetCols writes a whole slice at once. cols == nil writes every
// column; otherwise the file carries only those, which is how a column-partial
// delta stores a change as it arrived.
//
// Row groups are bounded regardless of how much is handed over, because a row
// group is the smallest unit Parquet can decode and therefore the upper bound
// on the cost of fetching one row for carry-forward. See parquetWriter.
func (t *Table) writeParquetCols(path string, rows []map[string]any,
	cols []string, durable bool,
) error {
	// cols == nil means "every column", and it has to be resolved HERE rather
	// than left to the writer: checkTemporal walks the list it is given, and a
	// nil list checks nothing at all. That is not hypothetical — a whole-row
	// write passes nil, which is most writes, so the check silently covered
	// only the column-partial ones until a test caught it.
	if cols == nil {
		cols = t.Order
	}
	// A value Arrow cannot hold is caught BEFORE anything is written, so the
	// mirror halts with the file untouched rather than half a batch on disk.
	if err := t.checkTemporal(rows, cols); err != nil {
		return err
	}
	w, err := t.newParquetWriter(path, cols, durable)
	if err != nil {
		return err
	}
	if err := w.Append(rows); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (t *Table) writeParquet(path string, rows []map[string]any) error {
	return t.writeParquetCodec(path, rows, true)
}

func appendValue(b array.Builder, dt arrow.DataType, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch bb := b.(type) {
	case *array.TimestampBuilder:
		k := kindTimestamp
		if tt, ok := dt.(*arrow.TimestampType); ok && tt.TimeZone != "" {
			k = kindTimestampTZ
		}
		appendTemporal(bb.Append, arrow.Timestamp(0), k, v, bb.AppendNull)
	case *array.Date32Builder:
		appendTemporal(bb.Append, arrow.Date32(0), kindDate, v, bb.AppendNull)
	case *array.Time64Builder:
		appendTemporal(bb.Append, arrow.Time64(0), kindTime, v, bb.AppendNull)
	case *array.MonthDayNanoIntervalBuilder:
		// Not appendTemporal: that helper is generic over one integer, and an
		// interval is three. See interval.go.
		s, ok := v.(string)
		if !ok {
			s = fmt.Sprint(v)
		}
		if iv, ok := parseInterval(s); ok {
			bb.Append(iv)
		} else {
			bb.AppendNull()
		}
	case *array.Int64Builder:
		if str, ok := v.(string); ok {
			if n, err := strconv.ParseInt(strings.TrimSpace(str), 10, 64); err == nil {
				bb.Append(n)
			} else {
				bb.AppendNull()
			}
			return
		}
		n, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(v)), 10, 64)
		if err != nil {
			bb.AppendNull()
			return
		}
		bb.Append(n)
	case *array.Float64Builder:
		f, err := strconv.ParseFloat(fmt.Sprint(v), 64)
		if err != nil {
			bb.AppendNull()
			return
		}
		bb.Append(f)
	case *array.BooleanBuilder:
		switch x := v.(type) {
		case bool:
			bb.Append(x)
		default:
			s := strings.ToLower(fmt.Sprint(v))
			bb.Append(s == "t" || s == "true" || s == "1")
		}
	case *array.Decimal128Builder:
		d := dt.(*arrow.Decimal128Type)
		sv, ok := v.(string)
		if !ok {
			sv = fmt.Sprint(v)
		}
		dec, err := decimal128.FromString(sv, d.Precision, d.Scale)
		if err != nil {
			bb.AppendNull()
			return
		}
		bb.Append(dec)
	case *array.StringBuilder:
		// pgoutput delivers every value as text, so this is the hot path and
		// fmt.Sprint on an interface holding a string is pure overhead.
		if str, ok := v.(string); ok {
			bb.Append(str)
			return
		}
		bb.Append(fmt.Sprint(v))
	default:
		b.AppendNull()
	}
}

// markDead marks the previous rows for the given keys dead, wherever they
// physically live. This is the work query time does NOT do — the whole point of
// the docs/11 correction.
//
// It replaces two O(table) costs that ran on every single batch: a full JSON
// parse of the base index, and a read-and-rewrite of every delta file. Both
// scaled with the mirror rather than with the change, which is why apply
// throughput collapsed as the table grew (docs/18). Marking a position dead is
// O(1) per key, and the file it lives in is never rewritten.
//
// Each call publishes a NEW deletion-vector generation rather than overwriting
// the one a reader may be holding; see State.DVGen. The generation two back is
// removed, which leaves any reader a full write cycle of grace and turns the
// remaining case into a retry rather than a wrong answer.
func (t *Table) markDead(byFile map[fileID][]int) error {
	for id, positions := range byFile {
		rel := id.name()
		existing, err := t.deadPositions(rel)
		if err != nil {
			return err
		}
		for _, p := range positions {
			existing[p] = true
		}
		merged := dvSorted(existing)

		stem := dvStem(rel)
		gen := t.State.DVGen[stem] + 1
		// writeDV, not a plain write: a new generation is still a file another
		// goroutine may open the instant it appears, and a truncate-then-write
		// leaves a window where it parses as nothing — which reads as "nothing
		// is dead" and undoes every delete in the file.
		if err := writeDV(t.dvPathGen(rel, gen), merged); err != nil {
			return err
		}
		if t.State.DVGen == nil {
			t.State.DVGen = map[string]int{}
		}
		t.State.DVGen[stem] = gen
		if gen > 2 {
			// Both encodings, because the generation being retired here may
			// predate the format change.
			for _, ext := range dvExts {
				_ = os.Remove(filepath.Join(t.Dir, "dv",
					fmt.Sprintf("%s.%09d.dv%s", stem, gen-2, ext)))
			}
		}
	}
	return nil
}

func (t *Table) truncate() error {
	for _, sub := range []string{"base", "delta", "dv", "index"} {
		entries, _ := os.ReadDir(filepath.Join(t.Dir, sub))
		for _, e := range entries {
			_ = os.Remove(filepath.Join(t.Dir, sub, e.Name()))
		}
	}
	t.State.BaseFiles, t.State.DeltaFiles = nil, nil
	t.State.BaseRows, t.State.DeltaRows = 0, 0
	t.State.PartialCols, t.State.FileRows, t.State.DVGen = nil, nil, nil
	t.index, t.patch, t.partialCols = nil, nil, nil
	return nil
}

// Compact folds base + deltas into one base file with deletion vectors applied,
// and rebuilds the key->position index. Query cost is a function of how far
// behind this runs, so compaction backlog is a first-class metric.
func (t *Table) Compact() (int, error) {
	if len(t.State.BaseFiles) == 0 && len(t.State.DeltaFiles) == 0 {
		return 0, nil
	}
	rows, err := t.Live()
	if err != nil {
		return 0, err
	}
	if err := t.truncate(); err != nil {
		return 0, err
	}
	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	if err := t.writeParquet(filepath.Join(t.Dir, "base", name), rows); err != nil {
		return 0, err
	}
	t.State.BaseFiles = []string{name}
	t.State.DeltaFiles = nil
	t.State.BaseRows = len(rows)
	t.State.DeltaRows = 0

	index := make(map[string]int, len(rows))
	t.index = newKeyIndex(len(rows))
	// Every patch has just been folded into the whole rows above.
	t.patch = newKeyIndex(0)
	t.rebuildPartialCols()
	id := baseID(name)
	for i, r := range rows {
		k := t.keyOf(r[t.Key])
		index[k.String()] = i
		l := loc{File: id, Pos: int32(i)}
		if t.elideWorked {
			l.Hash, _ = largeHash(r, t.Order)
		}
		t.index.set(k, l)
	}
	stem := strings.TrimSuffix(name, ".parquet")
	if err := writeJSON(filepath.Join(t.Dir, "index", stem+".idx.json"), index); err != nil {
		return 0, err
	}
	return len(rows), t.saveState()
}

func (t *Table) CompactionBacklog() int { return len(t.State.DeltaFiles) }

// mergeableDeltas is the deltas a merge may touch right now: everything except
// the ones a background rewrite is currently reading.
//
// Those are always the OLDEST deltas — a rewrite snapshots the file set at its
// start — so the mergeable ones are a contiguous newest run, and folding them
// into one file at the end preserves the ordering that makes "last occurrence
// wins" correct.
func (t *Table) mergeableDeltas() []string {
	if t.compacting == nil {
		return t.State.DeltaFiles
	}
	var out []string
	for _, d := range t.State.DeltaFiles {
		if !t.compacting.files[deltaID(d)] {
			out = append(out, d)
		}
	}
	return out
}

// ShouldMergeDeltas reports whether there are enough delta files that opening
// them is costing more than merging them would.
//
// This must remain possible WHILE a rewrite is in flight. Blocking it meant
// that under heavy churn — where a rewrite takes long enough that every key
// moves before it lands — the file count grew unchecked behind a rewrite that
// was already stale, which is most of why the jsonb shape never converged.
func (t *Table) ShouldMergeDeltas() bool {
	return len(t.mergeableDeltas()) >= compactMaxFiles
}

// MergeDeltas rewrites all delta files into one, honouring their deletion
// vectors. Cost is proportional to the rows in the deltas, not to the table, so
// it can run often — which is what keeps the read path's file count bounded
// without paying for a full base rewrite.
// Files of DIFFERENT column shapes are merged separately: a whole-row delta and
// a patch delta cannot be folded into one file, because that would mean
// inventing a value for a column the patch never carried, and absent is not
// NULL. In practice a table has one or two shapes, so this is one or two merges.
func (t *Table) MergeDeltas() (int, error) {
	mergeable := t.mergeableDeltas()
	if len(mergeable) < 2 {
		return 0, nil
	}
	if err := t.ensureIndex(); err != nil {
		return 0, err
	}
	groups := map[string][]string{}
	var groupOrder []string
	for _, d := range mergeable {
		sig := strings.Join(t.State.PartialCols[d], "\x00")
		if _, seen := groups[sig]; !seen {
			groupOrder = append(groupOrder, sig)
		}
		groups[sig] = append(groups[sig], d)
	}
	total := 0
	for _, sig := range groupOrder {
		n, err := t.mergeGroup(groups[sig], t.State.PartialCols[groups[sig][0]])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// mergeGroup folds delta files that share a column shape into one.
//
// Cost is proportional to the rows in those files, not to the table, so it can
// run often — which is what keeps the read path's file count bounded without
// paying for a full base rewrite.
func (t *Table) mergeGroup(old []string, cols []string) (int, error) {
	if len(old) < 2 {
		return 0, nil
	}
	// Bound the batch, for the same reason the apply tick is bounded: this runs
	// on the apply goroutine and nothing else in that loop runs while it does.
	// Measured at merge_during_compaction_ms=5842 on the wide shape, which was
	// the largest remaining stall once apply was capped (docs/26).
	//
	// The OLDEST are taken rather than the newest, so the files that have been
	// read through most often are the ones consolidated, and so the count
	// actually falls instead of leaving an un-mergeable prefix behind forever.
	if MaxMergeFiles > 0 && len(old) > MaxMergeFiles {
		old = old[:MaxMergeFiles]
	}

	// Everything not being merged stays exactly where it is, and the merged
	// file takes the POSITION of the run it replaces rather than going on the
	// end. Appending was correct only while `old` was always the newest run;
	// with a bounded batch it is a run from the middle, and moving older
	// content after newer content would invert "last occurrence wins" for any
	// key that somehow appears live in two files.
	kept := make([]string, 0, len(t.State.DeltaFiles))
	inserted := false
	for _, d := range t.State.DeltaFiles {
		if contains(old, d) {
			if !inserted {
				kept = append(kept, "") // placeholder, filled in below
				inserted = true
			}
			continue
		}
		kept = append(kept, d)
	}

	var rows []map[string]any
	for _, d := range old {
		rel := filepath.Join("delta", d)
		dead, derr := t.deadPositions(rel)
		if derr != nil {
			return 0, derr
		}
		got, err := t.readParquetCols(filepath.Join(t.Dir, rel), dead, cols)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		rows = append(rows, got...)
	}
	// DEDUPE. A merge concatenates the live rows of several files, and if a key
	// is live in two of them both copies land in the merged file — where
	// nothing can tombstone the older one, because a deletion vector addresses
	// positions in ONE file and both positions are in this one. The result is a
	// row the mirror returns twice: count(*) and every aggregate disagree with
	// the source while each file on disk is individually valid.
	//
	// The comment above already names the precondition — "any key that somehow
	// appears live in two files" — and handles the ORDERING it implies. This is
	// the other half. It reproduced about once in twenty runs of
	// TestCompactionRacesPartialRows, and the instrumentation that found it
	// showed both copies in the same file with the index pointing at the second:
	//
	//   index says key 398 lives at delta/000130.parquet pos 2
	//   LIVE COPY in delta/000130.parquet pos 1
	//   LIVE COPY in delta/000130.parquet pos 2
	//
	// Last occurrence wins, which is what the index below already records and
	// what the read path assumes. Keeping the last rather than the first is not
	// arbitrary: files are merged oldest-first, so the later row is the newer
	// value.
	//
	// This is deliberately a fix at the MERGE rather than at whatever let a key
	// be live in two files. A merge that cannot express "this row is dead"
	// must not create the situation that needs it, however the input arose.
	lastAt := make(map[rowKey]int, len(rows))
	for i, r := range rows {
		lastAt[t.keyOf(r[t.Key])] = i
	}
	if len(lastAt) != len(rows) {
		deduped := make([]map[string]any, 0, len(lastAt))
		for i, r := range rows {
			if lastAt[t.keyOf(r[t.Key])] == i {
				deduped = append(deduped, r)
			}
		}
		rows = deduped
	}

	if len(rows) == 0 {
		// everything in them was superseded; just drop the files, and the
		// placeholder with them since no file takes their place
		for _, d := range old {
			t.removeDataFile(filepath.Join("delta", d))
			t.forgetDeltaFile(d)
		}
		out := kept[:0]
		for _, d := range kept {
			if d != "" {
				out = append(out, d)
			}
		}
		t.State.DeltaFiles = out
		t.recountDeltaRows()
		return 0, t.saveState()
	}

	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	rel := filepath.Join("delta", name)
	if err := t.writeParquetCols(filepath.Join(t.Dir, rel), rows, cols, false); err != nil {
		return 0, err
	}
	// The new file is written before the old ones are removed and before the
	// state names it, so a crash at any point leaves a readable mirror.
	id := deltaID(name)
	target := t.index
	if cols != nil {
		target = t.patch
	}
	for i, r := range rows {
		k := t.keyOf(r[t.Key])
		target.set(k, loc{File: id, Pos: int32(i)})
		// A merge moves keys too, and a rewrite in flight has to know. Patches
		// never move during a rewrite (they only exist inside its snapshot,
		// which mergeableDeltas excludes), so this is a whole-row question.
		if t.compacting != nil && cols == nil {
			t.compacting.touched[k] = true
		}
	}
	for _, d := range old {
		t.forgetDeltaFile(d)
	}
	merged := append([]string(nil), kept...)
	for i, d := range merged {
		if d == "" {
			merged[i] = name
			break
		}
	}
	t.State.DeltaFiles = merged
	t.noteDeltaFile(name, len(rows), cols)
	t.recountDeltaRows()
	if err := t.saveState(); err != nil {
		return 0, err
	}
	for _, d := range old {
		t.removeDataFile(filepath.Join("delta", d))
	}
	return len(rows), nil
}

// Compaction policy constants. Both bounds exist because the two costs they
// bound are different: rewriting the base is proportional to the TABLE, while
// reading through many small deltas is proportional to the FILE COUNT.
const (
	// Rewrite once the accumulated deltas are worth a fifth of the base. Any
	// smaller fraction spends more on rewriting than it saves on reading.
	compactChurnRatio = 5
	// ...but never rewrite for trivial churn on a small table either.
	compactMinRows = 25_000
	// A ceiling on read amplification, independent of churn. This is not a
	// safety valve, it is a primary constraint: measured at 3.2M rows, letting
	// 203 small delta files accumulate dropped the analytical speedup from
	// single-digit multiples to 2.3x, with a plain count(*) coming out SLOWER
	// than PostgreSQL. Opening files dominates once they are small enough.
	//
	// Hitting it triggers a delta MERGE, not a base rewrite: merging is
	// O(delta rows) and bounds the file count, whereas rewriting the base is
	// O(table) and is only worth it when enough of the table has actually
	// changed. Trying to solve both with one knob is what produced first a 30x
	// throughput collapse and then a 203-file read path.
	compactMaxFiles = 16
)

// ShouldCompact reports whether a full rewrite is currently worth its cost.
//
// The trigger must scale with the TABLE, not with traffic. Compaction reads
// every live row and writes a new base file, so triggering it on a batch count
// makes a busy 5M-row mirror rewrite 5M rows every few seconds — which is both
// the throughput collapse and the memory spike measured in docs/18.
func (t *Table) ShouldCompact() bool {
	if len(t.State.DeltaFiles) == 0 {
		return false
	}
	threshold := t.State.BaseRows / compactChurnRatio
	if threshold < compactMinRows {
		threshold = compactMinRows
	}
	if t.State.DeltaRows >= threshold {
		return true
	}
	return t.baseVectorTooExpensive()
}

// baseVectorTooExpensive is the READ side of the compaction trade, which the
// churn trigger above does not represent at all.
//
// The churn trigger is sized entirely by the cost of the rewrite: compaction is
// O(table), so triggering proportionally to the table keeps amortised write
// cost constant. Correct, and one side of a two-sided ledger. The other side is
// that every query pays for the deletion vector the rewrite has not folded
// away, and that cost tracks the ABSOLUTE number of dead positions in the BASE
// file — so holding the ratio constant lets the read penalty grow without bound
// as the table grows.
//
// Measured on a 20.5M-row narrow mirror (bench/scripts/dv_cost_by_shape.py),
// count(*) against the base file:
//
//	dead in the base    count(*)    vs a column scan
//	               0      2.6 ms                   —   answered from the footer
//	          20,458     64.1 ms               21.2x
//	         204,582    112.9 ms               38.0x
//	       2,045,826    220.3 ms               75.0x
//
// The step from NO vector to ANY vector is 24x; the step from 20,458 dead to
// 2,045,826 — a hundred times more — is only 3.5x. So this is not a dead-row
// ceiling in any useful sense. What matters is how long the base is allowed to
// carry a vector at all, and the only thing that removes one is a rewrite.
//
// WHY IT IS OFF BY DEFAULT. The same measurement says the penalty is a property
// of NARROW rows, because the anti-join is per-row while the scan it competes
// with is per-byte:
//
//	shape       bytes/row   column scan   penalty at 1% dead
//	narrow              6        1.0 ms       13.9 ms  14.5x
//	wide               27        1.1 ms        7.8 ms   6.9x
//	jsonb            1692      238.3 ms        2.0 ms  <0.01x
//
// On jsonb the vector is a rounding error, so compacting sooner would buy that
// shape nothing and cost it a full rewrite. Turning this on by default would
// also change write throughput, which docs/19 measures and this has not — and
// an unmeasured throughput change is exactly what the 30x collapse in docs/18
// was. So it ships as a flag with the evidence attached, to be defaulted on
// when an A/B says what it costs the write path.
var CompactDeadFraction = envFloat("QS_COMPACT_DEAD_FRACTION", 0)

func (t *Table) baseVectorTooExpensive() bool {
	if CompactDeadFraction <= 0 || t.State.BaseRows < compactMinRows {
		return false
	}
	dead := 0
	for _, b := range t.State.BaseFiles {
		if gen := t.State.DVGen[dvStem(b)]; gen != 0 {
			d, err := t.readDV(t.dvPathGenAny(filepath.Join("base", b), gen), b)
			if err != nil {
				// A vector we cannot read is not a reason to rewrite the table.
				// The read path will raise ErrStaleManifest and retry, which is
				// a better answer than a compaction started on a guess.
				return false
			}
			dead += len(d)
		}
	}
	return float64(dead) >= float64(t.State.BaseRows)*CompactDeadFraction
}

// writeJSON writes atomically: temp file, fsync, rename.
//
// os.WriteFile truncates and then writes, so a concurrent reader can observe an
// empty or half-written file. That matters enormously here because the files
// this writes are DELETION VECTORS, and a deletion vector that fails to parse
// used to read as "nothing is dead" — every deleted row in that file came back
// to life. Background compaction reads these vectors on another goroutine while
// the apply loop writes them, so the window is not theoretical: it resurrected
// rows deleted 7 rounds before the rewrite even started.
func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	_ = f.Sync()
	_ = f.Close()
	return os.Rename(tmp, path)
}

func trimParquet(name string) string { return strings.TrimSuffix(name, ".parquet") }

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

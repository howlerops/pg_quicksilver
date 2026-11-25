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
//	  state.json              applied_lsn + manifest
//
// There is no persisted key index. Compaction used to write
// index/<stem>.idx.json — every key as JSON — and it was measured at 2.7x the
// size of the Parquet file it indexed and slower to load than rebuilding from
// it, because the key column is stored compressed and columnar while JSON is
// text with punctuation. ensureIndex reads the key column back instead. The
// directory is still removed on upgrade; see dropIndexJSON.
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
	"time"

	"github.com/apache/arrow-go/v18/arrow"

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

	// detached marks the private copy a background rewrite works from
	// (rewriteSnapshot). It shares a directory with the real table and nothing
	// else, and must never write the manifest: its State is a fragment.
	detached bool

	// dead is each data file's dead set, by dvStem, as the manifest in State
	// describes it. See dvcache.go. Only the apply goroutine touches it.
	dead map[string]*deadSet

	// What the idle compaction trigger needs to know and the manifest does not
	// say: when this table was last written, when its base was last rewritten
	// and how long that took. None of it is durable, deliberately — after a
	// restart "just written" is the safe assumption. See CompactIdleSeconds.
	lastChange    time.Time
	lastCompacted time.Time
	lastRewrite   time.Duration
	seenSeq       int
	retryAt       time.Time // no rewrite before this; see CompactionFailed
	clock         func() time.Time
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
	// The directories are named by nothing but their parents, so those are
	// what has to be durable for a file written under them to be findable.
	for _, dir := range []string{t.Dir, root} {
		if err := syncDir(dir); err != nil {
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
// BuildIndex builds the key index now, and reports how many keys it holds.
//
// It exists so that readiness can stop lying. The index is built lazily, on the
// first change — neither Snapshot nor a resume builds one — so a restarted
// sidecar reported ready with nothing in memory and then paid for the whole
// index on its first write, behind a probe that had already said yes. Measured
// at 4M rows (bench/results/restart_cost_4m.txt):
//
//	ready in 0.04s, RSS 25 MB          <- Kubernetes routes traffic here
//	first change after restart: 2.3s, RSS peaked at 507 MB
//	a second, identical write:  0.5s
//
// So ~1.8s of stall and a twentyfold jump in resident memory, both invisible to
// the probe and to an operator sizing a limit from what RSS looks like at
// startup. Calling this before readiness moves that cost to where both are
// visible: the Pod takes longer to return to service and is able to serve when
// it says it can.
//
// Idempotent, and free when the index is already there.
func (t *Table) BuildIndex() (int, error) {
	if err := t.ensureIndex(); err != nil {
		return 0, err
	}
	return t.index.len(), nil
}

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
		// ALWAYS from the file. There used to be a persisted index —
		// index/<stem>.idx.json, every key as JSON, written by both compaction
		// paths — that was read here in preference to the file.
		//
		// It was measured (TestPersistedIndexCost) and it lost on both axes:
		//
		//	base/000002.parquet     1,112,975 bytes
		//	index/000002.idx.json   2,977,786 bytes   2.7x the file it indexes
		//
		//	ensureIndex from the persisted JSON   61-71 ms
		//	ensureIndex from the base file        52-60 ms
		//
		// Slower AND larger, consistently, which is what a column store should
		// do to a text format: Parquet holds the key column compressed and
		// columnar, while JSON holds every key as text with punctuation and
		// allocates a map entry per key to parse it. The snapshot path had
		// already stopped writing one and said as much in this comment; the two
		// rewrite paths had simply never been revisited.
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
		// The dead rows are skipped HERE, not by forEachRowGroup. Handing it
		// the vector makes it drop them before fn sees the group, and then
		// first+i is a position among the SURVIVORS: every row after a dead
		// one was indexed one place too early. Nothing reads wrong at that
		// point. The next delete or update of such a row marks its
		// neighbour's position dead instead of its own — the row that should
		// have gone stays, and a row nobody touched disappears — and it took
		// only a restart or a Reload with one delete in a delta file to get
		// there (TestIndexRebuildKeepsDeltaPositions).
		err := t.forEachRowGroup(filepath.Join(t.Dir, rel), nil, partial,
			func(rows []map[string]any, first int) error {
				for i, r := range rows {
					if dead[first+i] {
						continue
					}
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
	pos, err := readDVPositions(path)
	if err != nil {
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
	t.forgetDead(rel)
}

// removeDataFile deletes a data file and the deletion vectors that described it.
func (t *Table) removeDataFile(rel string) {
	_ = os.Remove(filepath.Join(t.Dir, rel))
	t.dropDV(rel)
	t.dropIndexJSON(filepath.Base(rel))
}

// dropIndexJSON removes the persisted key index a previous version wrote for a
// base file, if one is there.
//
// Nothing writes these any more — they were 2.7x the size of the Parquet file
// they indexed and slower to load than rebuilding from it (ensureIndex, and
// TestPersistedIndexCost). Nothing READS them either, so one left behind is
// inert; it is removed anyway because a mirror that upgrades into this version
// would otherwise carry a file larger than its own base file forever, and
// "harmless" is not a good reason to keep several megabytes per table on a
// volume sized from a formula that does not know about it.
func (t *Table) dropIndexJSON(name string) {
	_ = os.Remove(filepath.Join(t.Dir, "index", trimParquet(filepath.Base(name))+".idx.json"))
}

func (t *Table) statePath() string { return filepath.Join(t.Dir, "state.json") }

// saveState writes the durable watermark AFTER the data files, so a crash
// replays the last batch rather than skipping it.
//
// "After" means after they are DURABLE, not after they are written. Each data
// file was fsynced when it was closed; the directories they were created in
// are synced here (syncOwedDirs, stream.go), and only then is the manifest replaced — temp file, fsync,
// rename — and the table directory synced so that the rename itself survives.
// Every one of those can fail, and a failure is returned: the caller must not
// confirm an LSN on the strength of a manifest that may not exist.
func (t *Table) saveState() error {
	if t.detached {
		return nil
	}
	b, err := json.MarshalIndent(t.State, "", "  ")
	if err != nil {
		return err
	}
	if err := syncOwedDirs(t.Dir); err != nil {
		return err
	}
	if err := writeFileAtomic(t.statePath(), b); err != nil {
		return err
	}
	return syncDir(t.Dir)
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
	// queued is membership of `order`, kept SEPARATELY from upserts.
	//
	// It used to be answered by looking the key up in upserts, which is wrong the
	// moment a delete removes it: a later insert of the same key in the same
	// batch then found upserts empty, read that as "not queued yet", and appended
	// the key to order a second time. writeUpserts walks order and writes
	// upserts[k] per occurrence, so the delta file got two identical copies of
	// the row and the index — written last — named only the second.
	//
	// Nothing downstream can repair that: a deletion vector addresses positions
	// in ONE file and both positions are in this one, so every later delete or
	// update tombstones the copy the index names and leaves the other live
	// forever. See apply_order_test.go.
	queued := map[rowKey]bool{}
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
				if !queued[k] {
					queued[k] = true
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
				upserts, order, queued, deletes = map[rowKey]map[string]any{}, nil,
					map[rowKey]bool{}, map[rowKey]bool{}
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
	// than left to the writer: the check walks the list it is given, and a
	// nil list checks nothing at all. That is not hypothetical — a whole-row
	// write passes nil, which is most writes, so the check silently covered
	// only the column-partial ones until a test caught it.
	if cols == nil {
		cols = t.Order
	}
	// A value the column cannot hold is caught BEFORE anything is written, so
	// the mirror halts with the file untouched rather than half a batch on
	// disk. The check is the conversion (values.go), so its result is what the
	// writer is handed and nothing is parsed a second time.
	vals, err := t.storableRows(t.arrowSchemaFor(cols), cols, rows)
	if err != nil {
		return err
	}
	w, err := t.newParquetWriter(path, cols, durable)
	if err != nil {
		return err
	}
	if err := w.appendStorable(vals); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

func (t *Table) writeParquet(path string, rows []map[string]any) error {
	return t.writeParquetCodec(path, rows, true)
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
// The VECTOR is rewritten, though, and that sentence used to hide a third cost
// of the same kind: the previous generation was read back from disk into a map
// and sorted on every call, so a thousand deletes cost 3.5 ms with 7,000 rows
// already dead in the file and 590 ms with two million. The dead set is now
// held in memory (dvcache.go) and only the write remains.
//
// Each call publishes a NEW deletion-vector generation rather than overwriting
// the one a reader may be holding; see State.DVGen. The generation two back is
// removed, which leaves any reader a full write cycle of grace and turns the
// remaining case into a retry rather than a wrong answer.
func (t *Table) markDead(byFile map[fileID][]int) error {
	for id, positions := range byFile {
		rel := id.name()
		set, err := t.deadSetFor(rel)
		if err != nil {
			return err
		}
		added := 0
		for _, p := range positions {
			if set.add(p) {
				added++
			}
		}
		if added == 0 {
			// Nothing this generation does not already say. A new one would be
			// the same file under a new name, and would retire a generation
			// some reader is holding in exchange for nothing.
			continue
		}
		t.lastChange = t.now()
		merged := set.positions()

		stem := dvStem(rel)
		gen := t.State.DVGen[stem] + 1
		// writeDV, not a plain write: a new generation is still a file another
		// goroutine may open the instant it appears, and a truncate-then-write
		// leaves a window where it parses as nothing — which reads as "nothing
		// is dead" and undoes every delete in the file.
		if err := writeDV(t.dvPathGen(rel, gen), merged); err != nil {
			// The set in memory is now ahead of anything on disk. Drop it, so
			// the next caller reloads what the manifest actually names.
			t.forgetDead(rel)
			return err
		}
		if t.State.DVGen == nil {
			t.State.DVGen = map[string]int{}
		}
		t.State.DVGen[stem] = gen
		set.gen = gen
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

// truncate empties the mirror, for a TRUNCATE on the source.
//
// Two things here are about ordering.
//
// A background rewrite in flight is ABORTED first. Its output lives under
// base/, which is about to be emptied, and it folds in rows the source no
// longer has; left alone, the next FinishCompaction installed a manifest naming
// a file this function had deleted, over a table that had since moved on, and
// the mirror was unreadable from then until someone rebuilt it. The same holds
// for a rewrite that has finished but not been swapped in.
//
// And the EMPTY MANIFEST is written before the files are removed, not after.
// The old manifest names those files; deleting them first leaves a window in
// which a crash produces a manifest pointing at nothing. Written first, a
// crash leaves an empty table at the previous watermark, and replay from there
// arrives at the TRUNCATE again.
func (t *Table) truncate() error {
	t.forgetFiles()
	if err := t.saveState(); err != nil {
		return err
	}
	t.removeAllFiles()
	return nil
}

// forgetFiles abandons any rewrite in flight and clears every in-memory record
// of the files the mirror holds. It removes nothing from disk.
func (t *Table) forgetFiles() {
	t.AbortCompaction()
	t.State.BaseFiles, t.State.DeltaFiles = nil, nil
	t.State.BaseRows, t.State.DeltaRows = 0, 0
	t.State.PartialCols, t.State.FileRows, t.State.DVGen = nil, nil, nil
	t.index, t.patch, t.partialCols = nil, nil, nil
	t.resetDead()
}

// removeAllFiles deletes every data file, vector and index on disk. An aborted
// rewrite may still be writing its output under base/; unlinking it is
// harmless, and AbortCompaction removes it again when the goroutine ends in
// case it was created after this ran.
func (t *Table) removeAllFiles() {
	for _, sub := range []string{"base", "delta", "dv", "index"} {
		entries, _ := os.ReadDir(filepath.Join(t.Dir, sub))
		for _, e := range entries {
			_ = os.Remove(filepath.Join(t.Dir, sub, e.Name()))
		}
	}
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
	// Not truncate(): that persists an empty manifest, and these rows are only
	// in memory until the write below.
	t.forgetFiles()
	t.removeAllFiles()
	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	if err := t.writeParquet(filepath.Join(t.Dir, "base", name), rows); err != nil {
		return 0, err
	}
	t.State.BaseFiles = []string{name}
	t.State.DeltaFiles = nil
	t.State.BaseRows = len(rows)
	t.State.DeltaRows = 0

	t.index = newKeyIndex(len(rows))
	// Every patch has just been folded into the whole rows above.
	t.patch = newKeyIndex(0)
	t.rebuildPartialCols()
	id := baseID(name)
	for i, r := range rows {
		k := t.keyOf(r[t.Key])
		l := loc{File: id, Pos: int32(i)}
		if t.elideWorked {
			l.Hash, _ = largeHash(r, t.Order)
		}
		t.index.set(k, l)
	}
	// No index/<stem>.idx.json. See dropIndexJSON.
	t.dropIndexJSON(name)
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
func (t *Table) ShouldCompact() bool { return t.CompactReason() != "" }

// CompactReason is ShouldCompact with the answer attached: "" for no, otherwise
// which of the three triggers pulled — "churn", "dead-fraction" or "idle".
//
// It exists because an A/B of QS_COMPACT_DEAD_FRACTION measured nothing and did
// not say so. Six runs across three shapes came back with identical compaction
// counts in both arms — deletes 5/5, churn 0/0, jsonb 3/3 — and a throughput
// comparison between two runs of the SAME behaviour reads exactly like a knob
// that is free. Which trigger fired was not recoverable from any log, so the
// only thing separating "no cost" from "no effect" was a count somebody thought
// to take afterwards.
//
// In production it answers the same question for an operator staring at a
// rewrite: a base rewrite is the most expensive thing this process does, and
// "why now" should not require reading this function.
//
// It is meant to be asked on EVERY tick, idle ones included, and is cheap
// enough to be: the only thing it ever read from disk was the base's deletion
// vector, and that is now a count held in memory (dvcache.go).
//
// The three triggers, in the order they are tried:
//
//   - churn: the deltas hold a fifth of the base. Sized by what the rewrite
//     costs; see compactChurnRatio.
//   - dead-fraction: a fifth of the base is dead. The same bound for a workload
//     that deletes without writing, which churn cannot see at all.
//   - idle: the base carries a vector and nothing has been written for a while.
//     Sized by what the vector costs READERS; see CompactIdleSeconds.
//
// The first two bound how much a busy table carries. Only the third bounds how
// LONG a quiet one carries it, and it is the one that was missing.
func (t *Table) CompactReason() string {
	t.noteActivity()
	if !t.retryAt.IsZero() && t.now().Before(t.retryAt) {
		return ""
	}
	if len(t.State.DeltaFiles) > 0 {
		threshold := t.State.BaseRows / compactChurnRatio
		if threshold < compactMinRows {
			threshold = compactMinRows
		}
		if t.State.DeltaRows >= threshold {
			return "churn"
		}
	}
	// Everything below is about the base's deletion vector, and does not need a
	// delta file to exist: a workload of pure deletes writes none, and used to
	// leave here on the first line with a vector that could only ever grow.
	dead, err := t.baseDead()
	if err != nil || dead == 0 {
		// A vector we cannot read is not a reason to rewrite the table. The
		// read path will raise ErrStaleManifest and retry, which is a better
		// answer than a compaction started on a guess.
		return ""
	}
	if t.baseVectorTooExpensive(dead) {
		return "dead-fraction"
	}
	if t.idleWithBaseVector() {
		return "idle"
	}
	return ""
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
// That is why the fraction is NOT the lever for read cost, and why it is no
// longer asked to be. The same measurement says the penalty is a property of
// NARROW rows, because the anti-join is per-row while the scan it competes with
// is per-byte:
//
//	shape       bytes/row   column scan   penalty at 1% dead
//	narrow              6        1.0 ms       13.9 ms  14.5x
//	wide               27        1.1 ms        7.8 ms   6.9x
//	jsonb            1692      238.3 ms        2.0 ms  <0.01x
//
// so a small fraction would buy the narrow shape a factor of two or three at
// most and cost every shape a rewrite per few percent of the table — at 1%,
// twenty times the rewriting the churn trigger settled on. The A/B at 0.01
// (three shapes, six paired runs) then found the trigger barely fired anyway,
// because churn was tested first and any workload deleting enough to fill a
// vector was usually writing enough to clear that bar:
//
//	shape      frac=0            frac=0.01
//	deletes    6 churn           5 churn + 1 dead-fraction
//	jsonb      3 churn           3 churn
//	churn      no compaction     no compaction
//
// WHAT IT DEFAULTS TO NOW, AND WHY THAT IS NOT A SCHEDULE CHANGE. It used to be
// off, with the gap named honestly: the case it was designed for is a base that
// fills with dead rows while its deltas stay small — deletes without
// accompanying writes — and no shape in bench/scripts/workload_matrix.sh
// produced that. The purge shape was added to produce it, and did
// (bench/results/workload_matrix_purge.txt): a 400,000-row base with 100,800
// dead and zero compactions, count(*) at 15.1 ms against 5.7 ms rewritten.
// Off, that workload has NO trigger at all: no delta rows means no churn, and
// CompactReason used to return before even looking when there were no delta
// files, so a table that only ever deleted kept every row it had ever held and
// a vector naming most of them.
//
// The default is now 1/compactChurnRatio, the SAME fifth the churn trigger
// uses, above the same compactMinRows floor. That number was chosen for what a rewrite costs — five rows rewritten
// per row changed, amortised — and a dead base row is a changed row whether or
// not a replacement was written for it. For any workload that updates, the two
// reach a fifth together (each update is one dead position and one delta row)
// and churn is tested first, so nothing about its schedule moves; that is the
// A/B above, read the other way. Only the delete-only workload gets a rewrite
// it did not have, at the price already accepted for everything else.
//
// A rewrite also cannot retrigger this on its own: the new base holds only live
// rows, so the fraction restarts from whatever died during the rewrite.
//
// Set QS_COMPACT_DEAD_FRACTION=0 to turn it off, or lower to trade rewrites for
// a shorter vector under sustained deletes.
var CompactDeadFraction = envFloat("QS_COMPACT_DEAD_FRACTION", 1.0/compactChurnRatio)

func (t *Table) baseVectorTooExpensive(dead int) bool {
	if CompactDeadFraction <= 0 {
		return false
	}
	// The same floor as churn, for the same reason, and so that the two stay
	// the same bound: without it a 30,000-row table was rewritten by this
	// trigger at 6,000 updates when churn would have waited for 25,000.
	threshold := float64(t.State.BaseRows) * CompactDeadFraction
	if threshold < compactMinRows {
		threshold = compactMinRows
	}
	return float64(dead) >= threshold
}

// CompactIdleSeconds is how long a table must go unwritten before a base that
// carries a deletion vector is rewritten to get rid of it. Zero turns it off.
//
// This is the trigger the measurements above were asking for. If the cost is
// the step from no vector to any vector, the quantity to bound is not how many
// rows are dead but HOW LONG the base carries a vector — and under the two
// triggers above that was unbounded. A 10M-row mirror that had 8% of its rows
// updated or deleted and then went quiet sat at 700,000 dead and four deltas,
// under both thresholds, with
//
//	count(*)              3 ms  ->  332 ms
//	count by status      19 ms  ->  112 ms
//	sum                  37 ms  ->  127 ms
//
// for as long as nobody wrote another 12% of the table. And it could not have
// fired even with a threshold to cross, because the triggers were only
// evaluated on a tick that applied something: the moment a mirror has spare
// capacity to rewrite was exactly the moment it stopped asking whether to.
//
// WHY IDLENESS AND NOT A TIMER. A rewrite is O(table), and the comments at the
// top of this block are about what happens when its trigger scales with
// anything else. "The vector is N seconds old" scales with the clock: under
// steady writes it rewrites the table every N seconds forever, each rewrite
// landing with a fresh vector for the rows that changed while it ran. Idleness
// cannot do that. A write to the table resets the clock, so under steady writes
// this never fires and the schedule is exactly the churn trigger's; when writes
// stop it fires once, the swap lands a base with no vector, and there is
// nothing left for it to fire on.
//
// The price is that a table written continuously and slowly keeps its vector
// until a fifth of it has changed. That is the existing behaviour, not a new
// one, and it is the workload where a rewrite would be stale on arrival.
//
// Thirty seconds is long enough that a pause between two batches of one job is
// not mistaken for the end of it, and short against how long a quiet mirror
// would otherwise serve the slow answer, which was indefinitely.
var CompactIdleSeconds = envFloat("QS_COMPACT_IDLE_SECONDS", 30)

// CompactIdleBackoff spaces idle rewrites by what a rewrite costs: after one
// lands, the idle trigger stays quiet for this many times as long as that
// rewrite took.
//
// It is for the workload idleness alone gets wrong — bursts with gaps just
// longer than CompactIdleSeconds. Each gap looks idle, so each burst would buy
// a full rewrite, and on a table whose rewrite takes longer than the gap that
// is a mirror rewriting continuously for a handful of changed rows: the storm
// again, arrived at by a different road. Thirty seconds says nothing about the
// table's size; how long the last rewrite took says exactly that. At 10 the
// idle trigger can keep the background rewriter busy for at most one part in
// eleven, whatever the table and whatever the bursts.
var CompactIdleBackoff = envFloat("QS_COMPACT_IDLE_BACKOFF", 10)

// idleWithBaseVector reports whether the idle trigger should pull. The caller
// has already established that the base carries a vector.
func (t *Table) idleWithBaseVector() bool {
	if CompactIdleSeconds <= 0 {
		return false
	}
	now := t.now()
	if now.Sub(t.lastChange) < time.Duration(CompactIdleSeconds*float64(time.Second)) {
		return false
	}
	if t.lastCompacted.IsZero() {
		return true
	}
	wait := time.Duration(CompactIdleBackoff * float64(t.lastRewrite))
	return now.Sub(t.lastCompacted) >= wait
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
//
// The fsync error is returned, and the directory is synced after the rename.
func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := writeFileAtomic(path, b); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
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

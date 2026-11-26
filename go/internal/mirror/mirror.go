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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	// FileFormat is the writer format version of each data file, by file
	// name. A file with no entry is legacy: written before versions were
	// recorded. See format.go.
	FileFormat map[string]int `json:"file_format,omitempty"`

	// NumericScales records, per unconstrained numeric column, the scale at
	// which QS_NUMERIC_SCALE was ACCEPTED for it: every value stored was
	// checked to fit DECIMAL(38,s) exactly, or there was none to check. A
	// column named by the option and absent here is still served as text. See
	// numeric_optin.go.
	NumericScales map[string]int32 `json:"numeric_scales,omitempty"`
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

	// aborting counts rewrites that were abandoned and have not yet finished
	// and removed their output. See AbortCompaction and Close.
	aborting sync.WaitGroup

	// detached marks the private copy a background rewrite works from
	// (rewriteSnapshot). It shares a directory with the real table and nothing
	// else, and must never write the manifest: its State is a fragment.
	detached bool

	// dead is each data file's dead set, by dvStem, as the manifest in State
	// describes it. See dvcache.go. Only the apply goroutine touches it.
	dead map[string]*deadSet

	// deferDead is non-nil while an Apply is in progress, and holds the
	// positions it has superseded so far, per file, not yet published as
	// deletion-vector generations. See Apply and markDead.
	deferDead map[fileID]*deadSet

	// sweptOrphans is whether this process has looked for delta files a
	// crashed Apply left behind. See removeOrphanDeltas.
	sweptOrphans bool

	// afterChunk, when set, is called each time Apply has landed a chunk and
	// published nothing. Tests use it to look at the mirror as a reader would
	// in the middle of a transaction.
	afterChunk func()

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

	// pub is the published snapshot and the files waiting out their grace
	// after leaving the manifest. See publish.go.
	pub *publisher

	// ahead is set while State describes something no manifest has committed:
	// from the start of an Apply, or of a Snapshot that keeps the previous
	// contents readable, until its manifest is written. A Halt raised meanwhile
	// patches the manifest on disk rather than saving this one.
	//
	// broken is what ahead becomes when that Apply or Snapshot fails: memory
	// is now ahead of the disk for good. The Table refuses to write a manifest
	// or apply again, and the caller reopens it with New — which a process
	// that stops on the error does anyway. Halt still works, on the disk.
	ahead  bool
	broken bool

	// rebuilding, when set, is why the published snapshot refuses while a
	// re-snapshot runs. It is never written to the manifest. See Snapshot.
	rebuilding string

	// wm is what the root's watermark needs to know about this table, and
	// savedOnce is whether this process has written its manifest yet: the
	// first Apply always does, changed or not. See watermark.go.
	wm        markState
	savedOnce bool

	// fmtPrev is the writer format version of each file the last manifest
	// named, and freshContents says the files the next one adds were read
	// from the source rather than made from those. See format.go.
	fmtPrev       map[string]int
	freshContents bool
}

// ErrNeedsReopen is returned by a Table whose Apply or Snapshot failed part-way.
// Its in-memory manifest describes files and positions no manifest on disk
// names, so nothing may be saved from it; open the table again with New.
var ErrNeedsReopen = errors.New("mirror table is ahead of its manifest after a failed write and must be reopened")

// Close releases what a Table holds outside the heap: the timer that unlinks
// retired files, a rewrite in flight and the mapped key index. The Table must
// not be used afterwards. Files still inside their grace are collected by the
// next process to open the table.
//
// A rewrite in flight is WAITED for, here and nowhere else — one this Table
// was still running, and any it abandoned earlier whose goroutine has yet to
// remove its output. The Table that replaces this one starts from the same
// manifest and therefore the same sequence number, so its first rewrite
// writes the very path this one is writing; two writers on one file, or this
// one's cleanup unlinking the other's output, is the path-reuse hazard of
// publish.go from a different direction.
func (t *Table) Close() {
	if t == nil {
		return
	}
	t.AbortCompaction()
	t.aborting.Wait()
	if t.pub != nil {
		t.pub.stop()
	}
	t.forgetWatermark()
	t.index.close()
	t.index, t.patch = nil, nil
	t.resetDead()
	t.broken = true
}

func New(root, schema, table, key string, cols map[string]string, order []string) (*Table, error) {
	t := &Table{
		Qualified: schema + "." + table,
		Key:       key,
		Columns:   cols,
		Order:     order,
		Dir:       filepath.Join(root, schema+"."+table),
	}
	t.numericKey = isIntegerType(arrowType(cols[key]))
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
	// The manifest of a table that has been quiet is behind what the table
	// has applied, by design; the root's watermark says how far (watermark.go).
	// A watermark that cannot be read speaks for nobody, which errs towards a
	// re-snapshot.
	t.wm.disk, t.wm.mem, t.wm.halted = t.State.AppliedLSN, t.State.AppliedLSN, t.State.Halted != ""
	if w, err := ReadWatermark(root); err == nil {
		t.State.AppliedLSN = effectiveLSN(t.Qualified, t.State.AppliedLSN, w)
		t.wm.mem = t.State.AppliedLSN
	}
	t.noteFormats()
	// Decided now, from the manifest just read, and before anything can ask
	// through a rewrite's detached copy. See numeric_optin.go.
	t.forgetOptIns()
	t.primeOptIns()
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

	// The base file's keys come from its index file, or from the file itself if
	// that is missing or is not this base file's (kidx.go). Either way they are
	// not held as map entries: this map is for the deltas, read back below.
	//
	// More than one base file is something nothing writes any more, and keeps
	// the one map it always had.
	if len(t.State.BaseFiles) == 1 {
		x, err := t.baseIndex(t.State.BaseFiles[0])
		if err != nil {
			t.index, t.patch = nil, nil
			return err
		}
		t.index = x
	}
	for _, base := range t.State.BaseFiles {
		if len(t.State.BaseFiles) == 1 {
			break // indexed just above
		}
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
// Nothing on the write path uses it any more. The background compactor did,
// and it was wrong to: the newest vector on disk can belong to an Apply that
// never committed its manifest. See BeginCompaction, which now captures the
// dead sets the manifest names. An external reader must never use this either;
// see deadPositions.
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
		t.retire(m)
	}
	delete(t.State.DVGen, dvStem(rel))
	t.forgetDead(rel)
}

// removeDataFile retires a data file and the deletion vectors that described
// it: they go after the grace period, not now. See retire, publish.go.
func (t *Table) removeDataFile(rel string) {
	t.retire(filepath.Join(t.Dir, rel))
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
	// And the index this version does write for a base file (kidx.go), which
	// goes when its base file goes.
	t.dropKidx(name)
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
	if t.broken {
		return ErrNeedsReopen
	}
	// Two things the manifest records about its own files, brought up to
	// date here because this is the one place every manifest passes through:
	// which writer wrote each (format.go), and which opted-in numeric columns
	// have been shown to fit their decimal (numeric_optin.go; the scan that
	// shows it runs once, at the first manifest write of a process).
	t.stampFormats()
	if !t.savedOnce {
		t.resolveOptIns()
	} else {
		t.primeOptIns()
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
	if err := syncDir(t.Dir); err != nil {
		return err
	}
	t.noteManifest(t.State.AppliedLSN, t.State.Halted)
	t.savedOnce = true
	// Only now, with the manifest durable, is there something to publish.
	return t.publish()
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
//
// A halt raised from INSIDE an Apply — a value the column cannot hold, found in
// the fourth chunk of a large transaction — must not write the manifest in
// memory: that one already names the delta files of the chunks before it, and
// saving it would publish a third of a transaction on the way to refusing the
// rest. So the halt is added to the manifest that is on disk, which still
// describes the mirror as it was before the transaction began.
//
// The same holds AFTER that Apply has returned its error, which is when the
// sidecar records the halt a second time to be sure it is durable: the memory
// of a failed Apply is no more committed for the Apply being over. `ahead`
// covers both, and a Snapshot that has not yet replaced the manifest.
//
// Either way the halt is published, and the files the manifest names are left
// exactly where they are; see publishManifest.
func (t *Table) Halt(reason string) error {
	t.State.Halted = reason
	if t.detached || !t.ahead {
		return t.saveState()
	}
	var onDisk State
	if b, err := os.ReadFile(t.statePath()); err == nil {
		if err := json.Unmarshal(b, &onDisk); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	onDisk.Halted = reason
	b, err := json.MarshalIndent(onDisk, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(t.statePath(), b); err != nil {
		return err
	}
	if err := syncDir(t.Dir); err != nil {
		return err
	}
	t.noteManifest(onDisk.AppliedLSN, reason)
	return t.publishManifest(&onDisk, reason)
}

// Halted reports the reason this mirror is out of service, or "".
func (t *Table) Halted() string { return t.State.Halted }

// ClearHalt puts a halted table back in service, durably. The one caller is a
// rebuild an operator asked for, and it calls this only AFTER Snapshot has
// replaced the table's contents: Snapshot itself leaves the halt in the
// manifest, so a crash anywhere before this line restarts with a table that is
// still halted rather than one that is empty and trusted.
func (t *Table) ClearHalt() error {
	t.State.Halted = ""
	return t.saveState()
}

// HaltedOnDisk reads a table's halt reason straight from its manifest, without
// opening the table. Opening one needs the column list, which needs the source;
// the readiness probe has to be able to say "halted" before the source answers.
// A table with no manifest, or one that cannot be read, reports "".
func HaltedOnDisk(root, schema, table string) string {
	b, err := os.ReadFile(filepath.Join(root, schema+"."+table, "state.json"))
	if err != nil {
		return ""
	}
	var st struct {
		Halted string `json:"halted"`
	}
	if json.Unmarshal(b, &st) != nil {
		return ""
	}
	return st.Halted
}

// ---- arrow schema ---------------------------------------------------------

// ArrowSchema maps declared Postgres types to a FIXED Arrow schema. Inferring
// per batch makes an all-NULL column null-typed, so schemas drift between delta
// files and the union read path breaks (found by the Python reference).
func (t *Table) ArrowSchema() *arrow.Schema { return t.arrowSchemaFor(t.Order) }

func (t *Table) arrowSchemaFor(cols []string) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(cols))
	for _, name := range cols {
		fields = append(fields, arrow.Field{
			Name: name, Type: arrowType(t.typeOf(name)), Nullable: true,
		})
	}
	return arrow.NewSchema(fields, nil)
}

// ---- write path -----------------------------------------------------------

type ApplyStats struct {
	Upserts    int
	Deletes    int
	AppliedLSN string
}

// ApplyChunkChanges is how many row-changes of a LARGE transaction Apply holds
// in memory at once. A transaction with more than this is landed in chunks:
// each chunk is carried forward, written to its own delta file(s) and dropped
// before the next is read, and the manifest names none of them until the last
// is down. A batch of ordinary transactions is never split by it — those are
// already in memory, and one delta file per batch is what the drain-rate
// numbers in cmd/qs-mirror/tick.go were measured with.
//
// So the memory Apply needs for one transaction is
//
//	ApplyChunkChanges x (decoded row + its carry-forward) + 1 bit per row of
//	every file the transaction supersedes rows in
//
// whatever the transaction's size. Zero disables chunking.
var ApplyChunkChanges = 100_000

// applyRun is the state of one Apply call: the chunk being accumulated, and
// what must happen at the end for all of it to become visible at once.
type applyRun struct {
	t *Table

	upserts map[rowKey]map[string]any
	order   []rowKey
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
	//
	// ACROSS CHUNKS the same rule is kept by different means. A key written by
	// an earlier chunk is no longer in these maps; it is in the index, pointing
	// into that chunk's delta file, exactly as if an earlier batch had written
	// it. The later change finds it there, marks that position dead and — for
	// an update — carries its columns forward from that file.
	queued  map[rowKey]bool
	deletes map[rowKey]bool
	n       int // changes accumulated since the last flush
	// any is whether a single change in the batch was for this table. When
	// none was there is nothing to write, the manifest included.
	any bool

	stats ApplyStats
	// doomed are files a TRUNCATE inside this batch orphaned. They are removed
	// only after the manifest that no longer names them is durable.
	doomed []string
}

func (a *applyRun) reset() {
	a.upserts, a.order = map[rowKey]map[string]any{}, nil
	a.queued, a.deletes = map[rowKey]bool{}, map[rowKey]bool{}
	a.n = 0
}

// Apply lands a run of whole transactions atomically: every change in every
// transaction, then applied_lsn advances. There is no state in which half a
// transaction is visible.
//
// "Visible" means named by the manifest, and that is what makes it possible to
// apply a transaction larger than memory. Delta files are written as the
// transaction is read and superseded positions are collected, but the manifest
// on disk goes on naming the files and the deletion-vector generations it
// named before. Only when everything is down are the new vector generations
// published and the manifest replaced, once. A crash before that leaves the
// previous manifest describing a mirror that is entirely still there; the
// delta files written so far are named by nothing, and removeOrphanDeltas
// collects them on the first Apply after the restart.
func (t *Table) Apply(txns []changestream.Transaction) (stats ApplyStats, err error) {
	if t.broken {
		return ApplyStats{}, ErrNeedsReopen
	}
	t.ahead = true
	ok := false
	defer func() {
		t.deferDead = nil
		if ok {
			t.ahead = false
			return
		}
		// Memory is ahead of the manifest in ways that are not worth unwinding
		// one by one: the caller stops on an Apply error (or keeps the table
		// only as a halted one), and what it restarts from is the disk. So the
		// Table says so, rather than trusting every later caller to remember:
		// see broken. It stays `ahead`, so a Halt recorded now patches the
		// manifest on disk.
		t.resetDead()
		t.broken = true
	}()
	stats, err = t.apply(txns)
	ok = err == nil
	return stats, err
}

func (t *Table) apply(txns []changestream.Transaction) (ApplyStats, error) {
	t.removeOrphanDeltas()
	a := &applyRun{t: t}
	a.reset()
	last := t.State.AppliedLSN

	// Deletion vectors are published once, at the end. markDead publishes a new
	// generation per call and retires the one two back; called per chunk it
	// would retire, on the third chunk, the generation the manifest on disk
	// still names — and a reader would find its vector gone mid-transaction.
	t.deferDead = map[fileID]*deadSet{}

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

	for i := range txns {
		txn := &txns[i]
		if floor > 0 && changestream.ParseLSN(txn.CommitLSN) < floor {
			continue
		}
		// A spooled transaction is read from disk once per table. One that
		// never mentions this table need not be read at all.
		if !txn.Spooled() || txn.Touches(t.Qualified) {
			chunked := ApplyChunkChanges > 0 && txn.Len() > ApplyChunkChanges
			err := txn.Each(func(c changestream.Change) error {
				if c.Qualified() != t.Qualified {
					return nil
				}
				if err := a.add(c); err != nil {
					return err
				}
				if chunked && a.n >= ApplyChunkChanges {
					return a.flush()
				}
				return nil
			})
			if err != nil {
				return ApplyStats{}, err
			}
		}
		last = txn.CommitLSN
	}
	if err := a.flush(); err != nil {
		return ApplyStats{}, err
	}

	// Everything is written. Publish: one new generation per vector that
	// changed, then the manifest that names them and the new delta files.
	pending := t.deferDead
	t.deferDead = nil
	if !a.any && len(pending) == 0 && len(a.doomed) == 0 && t.savedOnce && !applyAlwaysSaves {
		// Nothing in the batch was for this table, so the manifest on disk
		// describes it exactly as it is, bar the LSN — and the LSN of a table
		// nothing happened to is what the root's watermark records, once, for
		// every such table, before the batch is confirmed (watermark.go). A
		// manifest and a published snapshot per idle table per tick was 4 syncs
		// each to say nothing.
		//
		// The first Apply of a process still writes: its publish is what
		// collects the files a previous process retired (publisher.sweep).
		t.State.AppliedLSN = last
		t.noteApplied(last)
		a.stats.AppliedLSN = last
		return a.stats, nil
	}
	if len(pending) > 0 {
		byFile := make(map[fileID][]int, len(pending))
		for id, set := range pending {
			byFile[id] = set.positions()
		}
		if err := t.markDead(byFile); err != nil {
			return ApplyStats{}, err
		}
	}
	t.State.AppliedLSN = last
	if err := t.saveState(); err != nil {
		return ApplyStats{}, err
	}
	t.noteApplied(last)
	// Through the grace, not unlinked: the snapshot published a moment ago
	// names these, and a reader that started on it is entitled to finish.
	t.discard(a.doomed)
	a.stats.AppliedLSN = last
	return a.stats, nil
}

// add folds one change into the chunk being accumulated.
func (a *applyRun) add(c changestream.Change) error {
	t := a.t
	a.any = true
	switch c.Op {
	case changestream.OpInsert, changestream.OpUpdate:
		k := t.keyOf(c.Row[t.Key])
		if !a.queued[k] {
			a.queued[k] = true
			a.order = append(a.order, k)
		}
		// An earlier change to the same key in this batch is the better
		// base than anything on disk, so merge onto it. A column absent
		// from BOTH is filled from the mirror afterwards.
		if prev, seen := a.upserts[k]; seen {
			merged := make(map[string]any, len(t.Order))
			for name, v := range prev {
				merged[name] = v
			}
			for name, v := range c.Row {
				merged[name] = v
			}
			a.upserts[k] = merged
		} else {
			a.upserts[k] = c.Row
		}
		delete(a.deletes, k)
	case changestream.OpDelete:
		k := t.keyOf(c.Key[t.Key])
		a.deletes[k] = true
		delete(a.upserts, k)
	case changestream.OpTruncate:
		a.truncate()
	}
	a.n++
	return nil
}

// truncate empties the mirror for a TRUNCATE on the source, in memory only.
//
// Table.truncate writes the empty manifest at once, which is right for a
// snapshot and wrong here: the rows the transaction goes on to insert would
// then appear some time after the table had visibly emptied, which is a state
// the source never had. So the files are forgotten, not removed, and the
// manifest is left alone; the one written at the end of Apply names whatever
// the transaction left, and only then are the old files deleted — the same
// order Table.truncate keeps, for the same reason.
//
// Everything on disk at this moment is doomed, including delta files written
// by earlier chunks of this same transaction: TRUNCATE supersedes those too.
func (a *applyRun) truncate() {
	t := a.t
	a.doomed = append(a.doomed, t.allFiles()...)
	t.forgetFiles()
	t.deferDead = map[fileID]*deadSet{}
	a.stats = ApplyStats{}
	a.reset()
}

// flush lands the accumulated chunk: delta files written, the index moved, and
// the positions it supersedes collected. It publishes nothing.
func (a *applyRun) flush() error {
	t := a.t
	upserts, order, deletes := a.upserts, a.order, a.deletes
	a.reset()
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	// Drop large values the mirror can prove it already holds. pgoutput omits an
	// unchanged TOASTed value but resends an unchanged inline one, so this is
	// the same saving as a column-partial delta for the columns PostgreSQL does
	// not hint about. It runs FIRST, because everything after it simply sees a
	// change that arrived carrying fewer columns.
	elided, err := t.elideUnchanged(upserts)
	if err != nil {
		return err
	}

	// Which of these changes can be stored as they arrived, as a patch on the
	// row already in the base file, and which have to be expanded into whole
	// rows. Decided before carry-forward, because carry-forward is the expansion.
	asPatch, err := t.classifyPartial(upserts)
	if err != nil {
		return err
	}

	// Carry forward anything pgoutput did not resend. A large value that an
	// update did not change arrives as "unchanged TOAST" — the column is simply
	// absent — and writing NULL for it destroys the data with no error and no
	// change in row count. Absent means "keep what is there".
	//
	// "What is there" is wherever the index says, and for a key an earlier
	// chunk of this transaction wrote, that is the earlier chunk's delta file:
	// closed, on disk, and not yet in any manifest, which does not matter to a
	// read by position.
	if err := t.carryForward(upserts, asPatch); err != nil {
		return err
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
			return err
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
			return err
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
			return err
		}
		// After the writes, so the locations the hashes attach to are current.
		t.noteHashes(upserts, asPatch, elided)
	}
	a.stats.Upserts += len(upserts)
	a.stats.Deletes += len(deletes)
	if t.afterChunk != nil {
		t.afterChunk()
	}
	return nil
}

// removeOrphanDeltas deletes delta files a crashed Apply left behind, once per
// process, before this process writes any.
//
// A large transaction writes its delta files chunk by chunk and names them in
// the manifest only at the end, so a crash in between leaves files nothing
// refers to. They are recognisable without guessing: a delta file's name is a
// sequence number, the manifest holds the highest one ever committed, and
// nothing with a higher number can have been written by anything but an Apply
// (or a delta merge) that did not reach its manifest. The replay usually
// overwrites them name for name; this is for the ones it does not.
func (t *Table) removeOrphanDeltas() {
	if t.sweptOrphans || t.detached {
		return
	}
	t.sweptOrphans = true
	entries, _ := os.ReadDir(filepath.Join(t.Dir, "delta"))
	for _, e := range entries {
		n, err := strconv.Atoi(trimParquet(e.Name()))
		if err != nil || n <= t.State.Seq || contains(t.State.DeltaFiles, e.Name()) {
			continue
		}
		_ = os.Remove(filepath.Join(t.Dir, "delta", e.Name()))
		for _, m := range t.dvGlob("delta/" + e.Name()) {
			_ = os.Remove(m)
		}
	}
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
	w, err := t.newParquetWriterFor(path, cols, durable, len(vals))
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
	if t.deferDead != nil {
		// Inside Apply: collect, and publish once when the whole batch is
		// written (see Apply). A generation per chunk would retire one the
		// manifest on disk still names.
		for id, positions := range byFile {
			set := t.deferDead[id]
			if set == nil {
				set = &deadSet{}
				t.deferDead[id] = set
			}
			for _, p := range positions {
				set.add(p)
			}
		}
		return nil
	}
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
		if err := t.writeDV(t.dvPathGen(rel, gen), merged); err != nil {
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
				t.retire(filepath.Join(t.Dir, "dv",
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
	t.index.close()
	t.index, t.patch, t.partialCols = nil, nil, nil
	t.resetDead()
}

// removeAllFiles deletes every data file, vector and index on disk. An aborted
// rewrite may still be writing its output under base/; unlinking it is
// harmless, and AbortCompaction removes it again when the goroutine ends in
// case it was created after this ran.
func (t *Table) removeAllFiles() {
	for _, path := range t.allFiles() {
		t.discardOne(path)
	}
}

// allFiles lists every data file, vector and index on disk right now.
func (t *Table) allFiles() []string {
	var out []string
	for _, sub := range []string{"base", "delta", "dv", "index"} {
		entries, _ := os.ReadDir(filepath.Join(t.Dir, sub))
		for _, e := range entries {
			out = append(out, filepath.Join(t.Dir, sub, e.Name()))
		}
	}
	return out
}

// discard removes files listed EARLIER (allFiles) now that a manifest which
// replaces them is durable. Data files and vectors are retired, so they
// outlive the snapshot that named them by the grace; an index is read by
// nobody but this process and is unlinked.
//
// Anything the manifest names NOW is skipped. The list was taken before the
// replacement was written, and a name on it can have been written again since
// (a crash's leftover at the next sequence number is the usual one).
func (t *Table) discard(paths []string) {
	keep := map[string]bool{}
	for _, rel := range t.manifestFiles() {
		keep[filepath.Join(t.Dir, rel)] = true
	}
	for _, b := range t.State.BaseFiles {
		keep[t.kidxPath(b)] = true
	}
	for _, path := range paths {
		if !keep[path] {
			t.discardOne(path)
		}
	}
}

func (t *Table) discardOne(path string) {
	sub := filepath.Base(filepath.Dir(path))
	// An index is read by nobody but this process. And a file no manifest can
	// have named — a failed Apply's chunks, swept up here by a rebuild — has no
	// reader to wait for and a name the next write will take: it is never
	// handed to the publisher. See the invariant in publish.go.
	if sub == "index" || t.neverNamed(sub, filepath.Base(path)) {
		_ = os.Remove(path)
		return
	}
	t.retire(path)
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

	// Every patch has just been folded into the whole rows above.
	t.patch = newKeyIndex(0)
	t.rebuildPartialCols()
	// The base file's keys go to its index file (kidx.go), not into a map.
	// Live() returned one row per key, so there are no duplicates to mark.
	kb := t.newKidxBuilder(name, len(rows), true)
	for i, r := range rows {
		var h uint64
		if t.elideWorked {
			h, _ = largeHash(r, t.Order)
		}
		if err := kb.add(t.keyOf(r[t.Key]), int32(i), h, false); err != nil {
			kb.abort()
			return 0, err
		}
	}
	x, _, _, err := kb.finish()
	if err != nil {
		return 0, err
	}
	t.index = x
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

	// THE ROWS ARE NOT HELD. This used to read every row of every file into one
	// slice of maps, drop the superseded ones, and write the slice: memory in
	// proportion to the deltas, which at the churn trigger is a fifth of the
	// TABLE. On a three-million-row table that was a 500,000-row merge and
	// 780 MB resident, in a sidecar whose default limit is 256Mi and whose
	// steady state fits in it.
	//
	// So it is two passes over files that do not change. The first reads the
	// key column alone and records, for each key, which live row is its last.
	// The second streams whole rows a few thousand at a time straight into the
	// merged file, keeping exactly those. What is held is the keys — the same
	// map the dedupe below always built — and one piece of one row group.
	type source struct {
		path string
		dead map[int]bool
	}
	var sources []source
	for _, d := range old {
		rel := filepath.Join("delta", d)
		dead, derr := t.deadPositions(rel)
		if derr != nil {
			return 0, derr
		}
		sources = append(sources, source{filepath.Join(t.Dir, rel), dead})
	}
	// lastAt is, per key, the ordinal of its last live row among the files in
	// order; in the second pass it becomes the row's position in the merged
	// file, stored as -1-position so the two cannot be confused.
	lastAt := map[rowKey]int{}
	nRows := 0
	for _, s := range sources {
		err := t.forEachRowGroup(s.path, s.dead, []string{t.Key}, func(keys []map[string]any, _ int) error {
			for _, r := range keys {
				lastAt[t.keyOf(r[t.Key])] = nRows
				nRows++
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			return 0, err
		}
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
	if len(lastAt) == 0 {
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
	w, err := t.newParquetWriterFor(filepath.Join(t.Dir, rel), cols, false, len(lastAt))
	if err != nil {
		return 0, err
	}
	ord, written := 0, 0
	keep := make([]map[string]any, 0, rowChunk)
	for _, s := range sources {
		isDead := func(p int) bool { return s.dead[p] }
		err = t.forEachRows(s.path, isDead, cols, rowChunk, func(rows []map[string]any, _ int) error {
			keep = keep[:0]
			for _, r := range rows {
				k := t.keyOf(r[t.Key])
				if lastAt[k] == ord {
					keep = append(keep, r)
					lastAt[k] = -1 - written
					written++
				}
				ord++
			}
			return w.Append(keep)
		})
		if err != nil && !os.IsNotExist(err) {
			break
		}
		err = nil
	}
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err == nil && (ord != nRows || written != len(lastAt)) {
		err = fmt.Errorf("delta merge: the files changed between passes (%d rows then %d, %d of %d keys written)",
			nRows, ord, written, len(lastAt))
	}
	if err != nil {
		// No manifest names it, and none will.
		_ = os.Remove(filepath.Join(t.Dir, rel))
		return 0, err
	}
	// The new file is written before the old ones are removed and before the
	// state names it, so a crash at any point leaves a readable mirror.
	id := deltaID(name)
	target := t.index
	if cols != nil {
		target = t.patch
	}
	for k, at := range lastAt {
		target.set(k, loc{File: id, Pos: int32(-1 - at)})
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
	t.noteDeltaFile(name, written, cols)
	t.recountDeltaRows()
	if err := t.saveState(); err != nil {
		return 0, err
	}
	for _, d := range old {
		t.removeDataFile(filepath.Join("delta", d))
	}
	return written, nil
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
//
// This is the three triggers; CompactReason itself (compact_cluster.go) asks
// them first and then whether the base wants re-clustering.
func (t *Table) compactReason() string {
	t.noteActivity()
	if !t.retryAt.IsZero() && t.now().Before(t.retryAt) {
		return ""
	}
	// A column QS_NUMERIC_SCALE was accepted for is still text in files
	// written before it was; only a rewrite converts them (numeric_optin.go).
	if t.numericRewriteDue() {
		return "numeric-opt-in"
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

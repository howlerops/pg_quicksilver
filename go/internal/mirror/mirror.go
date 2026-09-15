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
//	  dv/000001.dv.json       dead row POSITIONS in the matching base file
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
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

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

	// Halted, when set, is why this mirror stopped and must not be served.
	// It is durable on purpose: a halt that lives only in a running process is
	// undone by the next restart, which re-reads the catalog, sees the new
	// schema as if it had always been there, and serves the corruption it
	// stopped for.
	Halted string `json:"halted,omitempty"`
}

// loc is where a key's current row physically lives: a file, relative to the
// table directory, and a row position inside it.
type loc struct {
	File string
	Pos  int
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
	index map[string]loc
}

func New(root, schema, table, key string, cols map[string]string, order []string) (*Table, error) {
	t := &Table{
		Qualified: schema + "." + table,
		Key:       key,
		Columns:   cols,
		Order:     order,
		Dir:       filepath.Join(root, schema+"."+table),
	}
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
	t.index = make(map[string]loc)

	for _, base := range t.State.BaseFiles {
		rel := filepath.Join("base", base)
		stem := trimParquet(base)
		raw, err := os.ReadFile(filepath.Join(t.Dir, "index", stem+".idx.json"))
		if err == nil {
			var m map[string]int
			if json.Unmarshal(raw, &m) == nil {
				for k, pos := range m {
					t.index[k] = loc{File: rel, Pos: pos}
				}
				continue
			}
		}
		// No persisted index (an older mirror, or a truncated write): rebuild
		// it from the file rather than silently indexing nothing, which would
		// leave superseded rows visible forever.
		rows, err := t.readParquet(filepath.Join(t.Dir, rel), nil)
		if err != nil {
			return err
		}
		for i, r := range rows {
			t.index[keyString(r[t.Key])] = loc{File: rel, Pos: i}
		}
	}

	for _, d := range t.State.DeltaFiles {
		rel := filepath.Join("delta", d)
		rows, err := t.readParquet(filepath.Join(t.Dir, rel), nil)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		dead := t.deadPositions(rel)
		for i, r := range rows {
			if dead[i] {
				continue
			}
			t.index[keyString(r[t.Key])] = loc{File: rel, Pos: i}
		}
	}
	return nil
}

// deadPositions reads a file's deletion vector. Every file gets one now, base
// and delta alike: marking a superseded row dead is O(1), whereas rewriting the
// file that holds it is O(file) and was being done on every batch.
func (t *Table) deadPositions(rel string) map[int]bool {
	dead := map[int]bool{}
	b, err := os.ReadFile(t.dvPath(rel))
	if err != nil {
		return dead
	}
	var pos []int
	_ = json.Unmarshal(b, &pos)
	for _, p := range pos {
		dead[p] = true
	}
	return dead
}

func (t *Table) dvPath(rel string) string {
	return filepath.Join(t.Dir, "dv", trimParquet(filepath.Base(rel))+".dv.json")
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
func (t *Table) ArrowSchema() *arrow.Schema {
	fields := make([]arrow.Field, 0, len(t.Order))
	for _, name := range t.Order {
		fields = append(fields, arrow.Field{
			Name: name, Type: arrowType(t.Columns[name]), Nullable: true,
		})
	}
	return arrow.NewSchema(fields, nil)
}

func arrowType(pgType string) arrow.DataType {
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
	upserts := map[string]map[string]any{}
	order := []string{}
	deletes := map[string]bool{}
	last := t.State.AppliedLSN

	for _, txn := range txns {
		for _, c := range txn.Changes {
			if c.Qualified() != t.Qualified {
				continue
			}
			switch c.Op {
			case changestream.OpInsert, changestream.OpUpdate:
				k := keyString(c.Row[t.Key])
				if _, seen := upserts[k]; !seen {
					order = append(order, k)
				}
				upserts[k] = c.Row
				delete(deletes, k)
			case changestream.OpDelete:
				k := keyString(c.Key[t.Key])
				deletes[k] = true
				delete(upserts, k)
			case changestream.OpTruncate:
				if err := t.truncate(); err != nil {
					return ApplyStats{}, err
				}
				upserts, order, deletes = map[string]map[string]any{}, nil, map[string]bool{}
			}
		}
		last = txn.CommitLSN
	}

	// an update supersedes the old row: tombstone in base, re-insert in delta
	superseded := make(map[string]bool, len(upserts)+len(deletes))
	for k := range upserts {
		superseded[k] = true
	}
	for k := range deletes {
		superseded[k] = true
	}

	if len(superseded) > 0 {
		if err := t.extendDeletionVectors(superseded); err != nil {
			return ApplyStats{}, err
		}
		// A deleted key has no current location. Leaving it in the index would
		// make a later re-insert of the same key tombstone the wrong row.
		for k := range deletes {
			delete(t.index, k)
		}
	}
	if len(upserts) > 0 {
		rows := make([]map[string]any, 0, len(upserts))
		for _, k := range order {
			if r, ok := upserts[k]; ok {
				rows = append(rows, r)
			}
		}
		if err := t.writeDelta(rows); err != nil {
			return ApplyStats{}, err
		}
	}

	t.State.AppliedLSN = last
	if err := t.saveState(); err != nil {
		return ApplyStats{}, err
	}
	return ApplyStats{Upserts: len(upserts), Deletes: len(deletes), AppliedLSN: last}, nil
}

func keyString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	default:
		return fmt.Sprint(x)
	}
}

func (t *Table) writeDelta(rows []map[string]any) error {
	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	if err := t.writeParquet(filepath.Join(t.Dir, "delta", name), rows); err != nil {
		return err
	}
	t.State.DeltaFiles = append(t.State.DeltaFiles, name)
	t.State.DeltaRows += len(rows)

	// The new rows are now the current location for their keys. Updating the
	// index here rather than rebuilding it is what keeps apply O(change).
	if err := t.ensureIndex(); err != nil {
		return err
	}
	rel := filepath.Join("delta", name)
	for i, r := range rows {
		t.index[keyString(r[t.Key])] = loc{File: rel, Pos: i}
	}
	return nil
}

func (t *Table) writeParquet(path string, rows []map[string]any) error {
	schema := t.ArrowSchema()
	bld := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bld.Release()

	for _, r := range rows {
		for i, name := range t.Order {
			appendValue(bld.Field(i), schema.Field(i).Type, r[name])
		}
	}
	rec := bld.NewRecord()
	defer rec.Release()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd))
	w, err := pqarrow.NewFileWriter(schema, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		return err
	}
	if err := w.Write(rec); err != nil {
		return err
	}
	return w.Close()
}

func appendValue(b array.Builder, dt arrow.DataType, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch bb := b.(type) {
	case *array.Int64Builder:
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
		dec, err := decimal128.FromString(fmt.Sprint(v), d.Precision, d.Scale)
		if err != nil {
			bb.AppendNull()
			return
		}
		bb.Append(dec)
	case *array.StringBuilder:
		bb.Append(fmt.Sprint(v))
	default:
		b.AppendNull()
	}
}

// extendDeletionVectors marks row positions dead in each base file. This is the
// work query time does NOT do — the whole point of the docs/11 correction.
// extendDeletionVectors marks the previous rows for the given keys dead,
// wherever they physically live, using the in-memory index.
//
// This replaces two O(table) costs that ran on every single batch: a full JSON
// parse of the base index, and a read-and-rewrite of every delta file. Both
// scaled with the mirror rather than with the change, which is why apply
// throughput collapsed as the table grew (docs/18). Marking a position dead is
// O(1) per key, and the file it lives in is never rewritten.
func (t *Table) extendDeletionVectors(keys map[string]bool) error {
	if err := t.ensureIndex(); err != nil {
		return err
	}
	byFile := map[string][]int{}
	for k := range keys {
		if l, ok := t.index[k]; ok {
			byFile[l.File] = append(byFile[l.File], l.Pos)
		}
	}
	for rel, positions := range byFile {
		existing := t.deadPositions(rel)
		for _, p := range positions {
			existing[p] = true
		}
		merged := make([]int, 0, len(existing))
		for p := range existing {
			merged = append(merged, p)
		}
		sort.Ints(merged)
		b, err := json.Marshal(merged)
		if err != nil {
			return err
		}
		if err := os.WriteFile(t.dvPath(rel), b, 0o644); err != nil {
			return err
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
	t.index = nil
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
	t.index = make(map[string]loc, len(rows))
	rel := filepath.Join("base", name)
	for i, r := range rows {
		k := keyString(r[t.Key])
		index[k] = i
		t.index[k] = loc{File: rel, Pos: i}
	}
	b, _ := json.Marshal(index)
	stem := strings.TrimSuffix(name, ".parquet")
	if err := os.WriteFile(filepath.Join(t.Dir, "index", stem+".idx.json"), b, 0o644); err != nil {
		return 0, err
	}
	return len(rows), t.saveState()
}

func (t *Table) CompactionBacklog() int { return len(t.State.DeltaFiles) }

// ShouldMergeDeltas reports whether there are enough delta files that opening
// them is costing more than merging them would.
func (t *Table) ShouldMergeDeltas() bool {
	return len(t.State.DeltaFiles) >= compactMaxFiles
}

// MergeDeltas rewrites all delta files into one, honouring their deletion
// vectors. Cost is proportional to the rows in the deltas, not to the table, so
// it can run often — which is what keeps the read path's file count bounded
// without paying for a full base rewrite.
func (t *Table) MergeDeltas() (int, error) {
	if len(t.State.DeltaFiles) < 2 {
		return 0, nil
	}
	if err := t.ensureIndex(); err != nil {
		return 0, err
	}
	old := append([]string(nil), t.State.DeltaFiles...)

	var rows []map[string]any
	for _, d := range old {
		rel := filepath.Join("delta", d)
		got, err := t.readParquet(filepath.Join(t.Dir, rel), t.deadPositions(rel))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, err
		}
		rows = append(rows, got...)
	}
	if len(rows) == 0 {
		// everything in them was superseded; just drop the files
		for _, d := range old {
			_ = os.Remove(filepath.Join(t.Dir, "delta", d))
			_ = os.Remove(t.dvPath(filepath.Join("delta", d)))
		}
		t.State.DeltaFiles = nil
		t.State.DeltaRows = 0
		return 0, t.saveState()
	}

	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	rel := filepath.Join("delta", name)
	if err := t.writeParquet(filepath.Join(t.Dir, rel), rows); err != nil {
		return 0, err
	}
	// The new file is written before the old ones are removed and before the
	// state names it, so a crash at any point leaves a readable mirror.
	for i, r := range rows {
		t.index[keyString(r[t.Key])] = loc{File: rel, Pos: i}
	}
	t.State.DeltaFiles = []string{name}
	t.State.DeltaRows = len(rows)
	if err := t.saveState(); err != nil {
		return 0, err
	}
	for _, d := range old {
		_ = os.Remove(filepath.Join(t.Dir, "delta", d))
		_ = os.Remove(t.dvPath(filepath.Join("delta", d)))
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
	return t.State.DeltaRows >= threshold
}

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func trimParquet(name string) string { return strings.TrimSuffix(name, ".parquet") }

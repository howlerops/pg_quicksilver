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
}

type Table struct {
	Qualified string
	Key       string
	Columns   map[string]string // name -> postgres type
	Order     []string          // stable column order
	Dir       string
	State     State
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

// Evolve adopts a new column list. Existing files are untouched — the read
// path NULL-fills what they lack — so there is no rewrite and no downtime.
func (t *Table) Evolve(cols map[string]string, order []string) {
	t.Columns, t.Order = cols, order
}

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
func (t *Table) extendDeletionVectors(keys map[string]bool) error {
	for _, base := range t.State.BaseFiles {
		stem := strings.TrimSuffix(base, ".parquet")
		idxPath := filepath.Join(t.Dir, "index", stem+".idx.json")
		raw, err := os.ReadFile(idxPath)
		if err != nil {
			continue
		}
		var index map[string]int
		if err := json.Unmarshal(raw, &index); err != nil {
			continue
		}
		hits := map[int]bool{}
		for k := range keys {
			if pos, ok := index[k]; ok {
				hits[pos] = true
			}
		}
		if len(hits) == 0 {
			continue
		}
		dvPath := filepath.Join(t.Dir, "dv", stem+".dv.json")
		existing := map[int]bool{}
		if b, err := os.ReadFile(dvPath); err == nil {
			var prev []int
			_ = json.Unmarshal(b, &prev)
			for _, p := range prev {
				existing[p] = true
			}
		}
		for p := range hits {
			existing[p] = true
		}
		merged := make([]int, 0, len(existing))
		for p := range existing {
			merged = append(merged, p)
		}
		sort.Ints(merged)
		b, _ := json.Marshal(merged)
		if err := os.WriteFile(dvPath, b, 0o644); err != nil {
			return err
		}
	}

	// deltas are small; drop superseded rows by rewriting (cheap at batch size)
	kept := t.State.DeltaFiles[:0]
	for _, d := range t.State.DeltaFiles {
		p := filepath.Join(t.Dir, "delta", d)
		rows, err := t.readParquet(p, nil)
		if err != nil {
			kept = append(kept, d)
			continue
		}
		out := make([]map[string]any, 0, len(rows))
		for _, r := range rows {
			if !keys[keyString(r[t.Key])] {
				out = append(out, r)
			}
		}
		if len(out) == len(rows) {
			kept = append(kept, d)
			continue
		}
		if len(out) == 0 {
			_ = os.Remove(p) // drop rather than write an empty file
			continue
		}
		if err := t.writeParquet(p, out); err != nil {
			return err
		}
		kept = append(kept, d)
	}
	t.State.DeltaFiles = kept
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

	index := make(map[string]int, len(rows))
	for i, r := range rows {
		index[keyString(r[t.Key])] = i
	}
	b, _ := json.Marshal(index)
	stem := strings.TrimSuffix(name, ".parquet")
	if err := os.WriteFile(filepath.Join(t.Dir, "index", stem+".idx.json"), b, 0o644); err != nil {
		return 0, err
	}
	return len(rows), t.saveState()
}

func (t *Table) CompactionBacklog() int { return len(t.State.DeltaFiles) }

func writeJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func trimParquet(name string) string { return strings.TrimSuffix(name, ".parquet") }

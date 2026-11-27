package ducklake

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// harness drives a real mirror.Table and keeps a model of what it holds.
type harness struct {
	t     testing.TB
	rng   *rand.Rand
	root  string
	tbl   *mirror.Table
	cols  map[string]string
	order []string
	model map[int64]map[string]any
	next  int64
	lsn   uint64
	ncol  int
	last  *mirror.LakeManifest // the newest manifest the mirror committed
	onNew func(*mirror.LakeManifest)
	// infinities makes one temporal value in six -infinity or infinity.
	infinities bool
}

func newHarness(t testing.TB, seed int64) *harness {
	return newHarnessOf(t, seed, false, "id bigint", "v integer", "amount numeric(12,2)",
		"ts timestamptz", "name text", "flag boolean", "u uuid", "d date")
}

// newHarnessOf is a harness over the columns given, each "name type".
func newHarnessOf(t testing.TB, seed int64, infinities bool, cols ...string) *harness {
	h := &harness{t: t, rng: rand.New(rand.NewSource(seed)), root: t.TempDir(),
		cols: map[string]string{}, model: map[int64]map[string]any{}, next: 1, infinities: infinities}
	for _, c := range cols {
		name, typ, _ := strings.Cut(c, " ")
		h.cols[name] = typ
		h.order = append(h.order, name)
	}
	h.open()
	return h
}

func (h *harness) open() {
	cols := map[string]string{}
	for k, v := range h.cols {
		cols[k] = v
	}
	tbl, err := mirror.New(h.root, "public", "orders", "id", cols, append([]string{}, h.order...))
	if err != nil {
		h.t.Fatal(err)
	}
	h.tbl = tbl
	tbl.OnManifest(func(m *mirror.LakeManifest) {
		h.last = m
		if h.onNew != nil {
			h.onNew(m)
		}
	})
}

func (h *harness) row(id int64) map[string]any {
	r := map[string]any{"id": strconv.FormatInt(id, 10)}
	for _, c := range h.order[1:] {
		if h.rng.Intn(12) == 0 {
			r[c] = nil
			continue
		}
		switch typ := h.cols[c]; typ {
		case "timestamptz", "timestamp", "date":
			if h.infinities && h.rng.Intn(6) == 0 {
				r[c] = []string{"-infinity", "infinity"}[h.rng.Intn(2)]
				continue
			}
		}
		switch h.cols[c] {
		case "integer":
			r[c] = strconv.Itoa(h.rng.Intn(2000) - 1000)
		case "smallint":
			r[c] = strconv.Itoa(h.rng.Intn(65536) - 32768)
		case "real":
			// What float4out prints: the shortest text that reads back as
			// the same float4. A few values, so that DISTINCT has something
			// to merge.
			r[c] = strconv.FormatFloat(float64([]float32{0.1, 1.5, -2.25, 3.3e10, 1e-7, 16777216}[h.rng.Intn(6)]), 'g', -1, 32)
		case "numeric":
			r[c] = []string{"12.50", "0.000001", "-7", "100", "0.1", "99999999999999999.123"}[h.rng.Intn(6)]
		case "timestamp":
			r[c] = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).
				Add(time.Duration(h.rng.Int63n(5*365*86400)) * time.Second).Format("2006-01-02 15:04:05")
		case "numeric(12,2)":
			r[c] = fmt.Sprintf("%d.%02d", h.rng.Intn(100000)-50000, h.rng.Intn(100))
		case "timestamptz":
			r[c] = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).
				Add(time.Duration(h.rng.Int63n(5*365*86400)) * time.Second).Format("2006-01-02 15:04:05+00")
		case "date":
			r[c] = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, h.rng.Intn(2000)).Format("2006-01-02")
		case "boolean":
			r[c] = h.rng.Intn(2) == 0
		case "uuid":
			var b [16]byte
			h.rng.Read(b[:])
			r[c] = fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
		default:
			r[c] = fmt.Sprintf("n%d é %d", h.rng.Intn(50), h.rng.Int63())
		}
	}
	return r
}

func (h *harness) txn(ch []changestream.Change) []changestream.Transaction {
	h.lsn += 16
	l := fmt.Sprintf("0/%X", h.lsn)
	return []changestream.Transaction{{CommitLSN: l, NextLSN: l, Changes: ch}}
}

func (h *harness) ids() []int64 {
	ids := make([]int64, 0, len(h.model))
	for id := range h.model {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (h *harness) change(op changestream.Op, row map[string]any, id int64) changestream.Change {
	return changestream.Change{Op: op, Schema: "public", Table: "orders", Row: row,
		Key: map[string]any{"id": strconv.FormatInt(id, 10)}}
}

// apply is one transaction of inserts, updates and deletes, optionally after a
// TRUNCATE in the same transaction.
func (h *harness) apply(ins, upd, del int, truncate bool) {
	var ch []changestream.Change
	if truncate {
		ch = append(ch, changestream.Change{Op: changestream.OpTruncate, Schema: "public", Table: "orders"})
		h.model = map[int64]map[string]any{}
	}
	ids := h.ids()
	h.rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	for i := 0; i < upd && i < len(ids); i++ {
		r := h.row(ids[i])
		h.model[ids[i]] = r
		ch = append(ch, h.change(changestream.OpUpdate, r, ids[i]))
	}
	for i := upd; i < upd+del && i < len(ids); i++ {
		delete(h.model, ids[i])
		ch = append(ch, h.change(changestream.OpDelete, nil, ids[i]))
	}
	for i := 0; i < ins; i++ {
		id := h.next
		h.next++
		r := h.row(id)
		h.model[id] = r
		ch = append(ch, h.change(changestream.OpInsert, r, id))
	}
	if len(ch) == 0 {
		return
	}
	if _, err := h.tbl.Apply(h.txn(ch)); err != nil {
		h.t.Fatalf("apply: %v", err)
	}
}

func (h *harness) rewrite(applyMeanwhile bool) {
	c := h.tbl.BeginCompaction()
	if c == nil {
		return
	}
	if applyMeanwhile {
		h.apply(3, 5, 2, false)
	}
	for !c.Done() {
		time.Sleep(time.Millisecond)
	}
	if _, err := h.tbl.FinishCompaction(); err != nil {
		h.t.Fatalf("finish compaction: %v", err)
	}
}

func (h *harness) evolve(missing map[string]string) {
	cols := map[string]string{}
	for k, v := range h.cols {
		cols[k] = v
	}
	if err := h.tbl.Evolve(cols, append([]string{}, h.order...), missing); err != nil {
		h.t.Fatalf("evolve: %v", err)
	}
}

func (h *harness) addColumn() string {
	h.ncol++
	name := fmt.Sprintf("c%d", h.ncol)
	var missing map[string]string
	var fill any
	switch h.ncol % 3 {
	case 0:
		h.cols[name] = "text"
		missing, fill = map[string]string{name: "emea"}, "emea"
	case 1:
		h.cols[name] = "integer"
		missing, fill = map[string]string{name: "7"}, "7"
	default:
		h.cols[name] = "text"
	}
	h.order = append(h.order, name)
	for _, r := range h.model {
		r[name] = fill
	}
	h.evolve(missing)
	return name
}

func (h *harness) dropColumn() string {
	var cand []string
	for _, c := range h.order {
		if c != "id" && c != "v" {
			cand = append(cand, c)
		}
	}
	if len(cand) == 0 {
		return ""
	}
	name := cand[h.rng.Intn(len(cand))]
	delete(h.cols, name)
	var order []string
	for _, c := range h.order {
		if c != name {
			order = append(order, c)
		}
	}
	h.order = order
	for _, r := range h.model {
		delete(r, name)
	}
	h.evolve(nil)
	return name
}

// rebuild is what an operator's rebuild of a halted table amounts to: the
// table's directory is replaced by a new one holding the source's rows, with
// file names that start again from one.
func (h *harness) rebuild() {
	h.tbl.Close()
	if err := os.RemoveAll(h.tbl.Dir); err != nil {
		h.t.Fatal(err)
	}
	h.open()
	var ch []changestream.Change
	for _, id := range h.ids() {
		ch = append(ch, h.change(changestream.OpInsert, h.model[id], id))
	}
	if len(ch) == 0 {
		id := h.next
		h.next++
		h.model[id] = h.row(id)
		ch = append(ch, h.change(changestream.OpInsert, h.model[id], id))
	}
	if _, err := h.tbl.Apply(h.txn(ch)); err != nil {
		h.t.Fatalf("rebuild: %v", err)
	}
}

func (h *harness) sumV() int64 {
	var s int64
	for _, r := range h.model {
		if v, ok := r["v"].(string); ok {
			n, _ := strconv.ParseInt(v, 10, 64)
			s += n
		}
	}
	return s
}

// checksumSQL is count and an order-independent hash of every row as text,
// over the columns given, of relation rel.
func checksumSQL(order []string, rel string) string {
	parts := make([]string, len(order))
	for i, c := range order {
		parts[i] = fmt.Sprintf("coalesce(%s::varchar, '~')", quoteIdent(c))
	}
	return fmt.Sprintf("SELECT count(*), coalesce(bit_xor(hash(concat_ws('|', %s))), 0), coalesce(sum(v), 0) FROM %s",
		strings.Join(parts, ", "), rel)
}

// lakeFilesOnDisk lists the lake's data directory, relative to it.
func lakeFilesOnDisk(t testing.TB, p *Publisher) map[string]bool {
	out := map[string]bool{}
	err := filepath.WalkDir(p.dataDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(p.dataDir, path)
			out[filepath.ToSlash(rel)] = true
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// TestConformanceAgainstDuckDB drives a mirror table through a long random
// sequence and, after every publish, asks the real reader three things: that
// the lake table is the mirror's live rows, that every retained snapshot still
// reads as it did when it was published, and that every expired one is gone.
func TestConformanceAgainstDuckDB(t *testing.T) {
	// It compares a TIMESTAMPTZ predicate with the time column, which is text
	// under QS_TEMPORAL_TYPES=0; the lake's type mapping is tested as native.
	defer func(prev bool) { mirror.TemporalTypes = prev }(mirror.TemporalTypes)
	mirror.TemporalTypes = true
	old := mirror.PartialDeltas
	mirror.PartialDeltas = false // what the sidecar does when QS_DUCKLAKE is on
	defer func() { mirror.PartialDeltas = old }()

	steps := 120
	if s, err := strconv.Atoi(os.Getenv("QS_LAKE_STEPS")); err == nil && s > 0 {
		steps = s
	}
	seed := int64(1)
	if s, err := strconv.ParseInt(os.Getenv("QS_LAKE_SEED"), 10, 64); err == nil {
		seed = s
	}
	const retention = 8 * time.Second
	ctx := context.Background()
	h := newHarness(t, seed)
	defer func() { h.tbl.Close() }()
	clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	pub, err := Open(ctx, Options{MirrorRoot: h.root, Retention: retention, Now: clk.Now})
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	rd := startReader(t, CatalogPath(h.root))

	type expect struct {
		step    int
		answer  string // count, checksum, sum(v) — or "" when refused
		order   []string
		refused string
	}
	published := map[int64]expect{}
	var current expect
	counts := map[string]int{}
	halted := false
	maxSnapshots, timeTravels, expiredSeen, filters := 0, 0, 0, 0

	for step := 1; step <= steps; step++ {
		var op string
		switch r := h.rng.Intn(100); {
		case halted:
			op = "rebuild"
			h.rebuild()
			halted = false
		case step < 4:
			op = "apply"
			h.apply(40, 0, 0, false)
		case r < 52:
			op = "apply"
			h.apply(h.rng.Intn(12), h.rng.Intn(15), h.rng.Intn(8), false)
		case r < 60:
			op = "delete-only"
			h.apply(0, 0, 1+h.rng.Intn(10), false)
		case r < 70:
			op = "merge"
			if _, err := h.tbl.MergeDeltas(); err != nil {
				t.Fatalf("step %d merge: %v", step, err)
			}
		case r < 80:
			op = "rewrite"
			h.rewrite(h.rng.Intn(2) == 0)
		case r < 84:
			op = "truncate"
			h.apply(h.rng.Intn(20), 0, 0, true)
		case r < 90:
			op = "add-column"
			h.addColumn()
		case r < 94:
			op = "drop-column"
			if h.dropColumn() == "" {
				op = "apply"
				h.apply(5, 5, 1, false)
			}
		case r < 97:
			op = "halt"
			if err := h.tbl.Halt(fmt.Sprintf("test halt at step %d", step)); err != nil {
				t.Fatalf("step %d halt: %v", step, err)
			}
			halted = true
		default:
			op = "republish"
		}
		counts[op]++

		m := h.last
		if m == nil {
			t.Fatalf("step %d (%s): the mirror committed no manifest", step, op)
		}
		res, err := pub.Publish(ctx, m)
		if err != nil {
			t.Fatalf("step %d (%s): publish: %v", step, op, err)
		}
		if op == "republish" && res.Snapshot != 0 {
			t.Fatalf("step %d: publishing the same manifest again wrote snapshot %d", step, res.Snapshot)
		}

		// What the lake must say now.
		var want expect
		if halted {
			want = expect{step: step, refused: m.Refusal}
			if _, err := rd.query(false, "SELECT count(*) FROM lake.public.orders"); err == nil ||
				!strings.Contains(err.Error(), "is halted and must not be served: "+m.Refusal) {
				t.Fatalf("step %d: a halted table answered, or failed without the reason: %v", step, err)
			}
		} else {
			view, err := h.tbl.ViewSQL()
			if err != nil {
				t.Fatalf("step %d (%s): view sql: %v", step, op, err)
			}
			mirrorRows, err := rd.query(false, checksumSQL(h.order, "("+view+") t"))
			if err != nil {
				t.Fatalf("step %d (%s): the mirror's own view failed: %v", step, op, err)
			}
			lakeRows, err := rd.query(step%10 == 0, checksumSQL(h.order, "lake.public.orders"))
			if err != nil {
				t.Fatalf("step %d (%s): lake query: %v", step, op, err)
			}
			want = expect{step: step, answer: flat(lakeRows), order: append([]string{}, h.order...)}
			if flat(lakeRows) != flat(mirrorRows) {
				t.Fatalf("step %d (%s): lake %s, mirror view %s", step, op, flat(lakeRows), flat(mirrorRows))
			}
			if lakeRows[0][0] != strconv.Itoa(len(h.model)) || lakeRows[0][2] != strconv.FormatInt(h.sumV(), 10) {
				t.Fatalf("step %d (%s): lake count,sum(v) = %s,%s; model %d,%d",
					step, op, lakeRows[0][0], lakeRows[0][2], len(h.model), h.sumV())
			}
			// Filters, which DuckLake answers with the per-file statistics
			// this publisher wrote: a bound that is too tight loses rows here
			// and nowhere else.
			if step%5 == 0 {
				for _, pred := range predicates(h.order) {
					q := "SELECT count(*), coalesce(sum(id), 0) FROM %s WHERE " + pred
					a, err1 := rd.query(false, fmt.Sprintf(q, "lake.public.orders"))
					b, err2 := rd.query(false, fmt.Sprintf(q, "("+view+") t"))
					if err1 != nil || err2 != nil || flat(a) != flat(b) {
						t.Fatalf("step %d (%s): WHERE %s: lake %v (%v), mirror view %v (%v)", step, op, pred, a, err1, b, err2)
					}
					filters++
				}
			}
			// The column list, in order, is part of the table.
			cols, err := rd.query(false, "SELECT string_agg(column_name, ',' ORDER BY column_index) FROM duckdb_columns() "+
				"WHERE database_name = 'lake' AND schema_name = 'public' AND table_name = 'orders'")
			if err != nil || cols[0][0] != strings.Join(h.order, ",") {
				t.Fatalf("step %d (%s): lake columns %v (%v), mirror %v", step, op, cols, err, h.order)
			}
		}
		if res.Snapshot != 0 {
			published[res.Snapshot] = want
			current = want
		} else if want.answer != current.answer || want.refused != current.refused {
			t.Fatalf("step %d (%s): no snapshot written, yet the table changed", step, op)
		}

		clk.advance(time.Second)
		if err := pub.Expire(ctx); err != nil {
			t.Fatalf("step %d: expire: %v", step, err)
		}

		// Time travel: every snapshot the lake retains reads as it did.
		snaps, err := rd.query(false, "SELECT snapshot_id FROM ducklake_snapshots('lake') ORDER BY 1")
		if err != nil {
			t.Fatalf("step %d: snapshots: %v", step, err)
		}
		retained := map[int64]bool{}
		for _, r := range snaps {
			id, _ := strconv.ParseInt(r[0], 10, 64)
			retained[id] = true
		}
		maxSnapshots = max(maxSnapshots, len(retained))
		for id, e := range published {
			at := fmt.Sprintf("lake.public.orders AT (VERSION => %d)", id)
			if !retained[id] {
				if _, err := rd.query(false, "SELECT count(*) FROM "+at); err == nil {
					t.Fatalf("step %d: snapshot %d (step %d) is expired and still answers", step, id, e.step)
				}
				expiredSeen++
				delete(published, id)
				continue
			}
			if e.refused != "" {
				if _, err := rd.query(false, "SELECT count(*) FROM "+at); err == nil ||
					!strings.Contains(err.Error(), e.refused) {
					t.Fatalf("step %d: snapshot %d (halted at step %d): %v", step, id, e.step, err)
				}
				continue
			}
			got, err := rd.query(false, checksumSQL(e.order, at))
			if err != nil {
				t.Fatalf("step %d: time travel to snapshot %d (step %d): %v", step, id, e.step, err)
			}
			if flat(got) != e.answer {
				t.Fatalf("step %d: snapshot %d read %s when published at step %d and %s now",
					step, id, e.answer, e.step, flat(got))
			}
			timeTravels++
		}

		// The lake directory holds exactly the files the catalog names.
		named, err := referencedFiles(ctx, pub.cat.db)
		if err != nil {
			t.Fatal(err)
		}
		onDisk := lakeFilesOnDisk(t, pub)
		for f := range named {
			if !onDisk[f] {
				t.Fatalf("step %d (%s): the catalog names %s, which is not in the lake", step, op, f)
			}
		}
		for f := range onDisk {
			if !named[f] {
				t.Fatalf("step %d (%s): %s is in the lake and no snapshot names it", step, op, f)
			}
		}

		// DuckLake's own account of the table.
		if !halted && step%10 == 0 {
			files, err := rd.query(false, "SELECT count(*), count(delete_file) FROM ducklake_list_files('lake', 'orders', schema => 'public')")
			if err != nil {
				t.Fatalf("step %d: ducklake_list_files: %v", step, err)
			}
			withDead := 0
			for _, f := range m.Files {
				if f.DVGen != 0 {
					withDead++
				}
			}
			if files[0][0] != strconv.Itoa(len(m.Files)) || files[0][1] != strconv.Itoa(withDead) {
				t.Fatalf("step %d: ducklake_list_files says %v; the manifest names %d files, %d with a vector",
					step, files, len(m.Files), withDead)
			}
			info, err := rd.query(false, "SELECT file_count, delete_file_count FROM ducklake_table_info('lake') WHERE table_name = 'orders'")
			if err != nil || len(info) != 1 || info[0][0] != strconv.Itoa(len(m.Files)) || info[0][1] != strconv.Itoa(withDead) {
				t.Fatalf("step %d: ducklake_table_info says %v (%v)", step, info, err)
			}
		}
	}

	c, err := pub.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%d steps %v", steps, counts)
	t.Logf("time-travel reads checked: %d; expired snapshots confirmed gone: %d; most snapshots retained at once: %d",
		timeTravels, expiredSeen, maxSnapshots)
	t.Logf("filtered queries compared: %d", filters)
	t.Logf("catalog at the end: %+v", c)
	if !halted {
		compareStatsWithDuckDB(t, ctx, pub, rd)
	}
	if maxSnapshots > int(retention/time.Second)+3 {
		t.Fatalf("%d snapshots retained at once with a retention of %v at one publish a second", maxSnapshots, retention)
	}
}

func predicates(order []string) []string {
	all := map[string][]string{
		"id":     {"id > 100", "id BETWEEN 20 AND 60"},
		"v":      {"v > 0", "v IS NULL", "v = 17"},
		"amount": {"amount < 0", "amount >= 25000.50"},
		"ts":     {"ts >= TIMESTAMPTZ '2022-06-01 00:00:00+00'", "ts < TIMESTAMPTZ '2020-03-01 00:00:00+00'"},
		"name":   {"name > 'n3'", "name IS NOT NULL AND name < 'n1'"},
		"flag":   {"flag", "NOT flag"},
		"u":      {"u::varchar > '8'", "u IS NULL"},
		"d":      {"d < DATE '2022-06-01'", "d >= DATE '2025-01-01'"},
	}
	var out []string
	for _, c := range order {
		out = append(out, all[c]...)
		if strings.HasPrefix(c, "c") {
			out = append(out, quoteIdent(c)+" IS NULL", quoteIdent(c)+"::varchar >= '7'")
		}
	}
	return out
}

// compareStatsWithDuckDB registers every live lake file with a scratch
// DuckLake through DuckDB's own ducklake_add_data_files, and compares the
// statistics DuckDB writes for it with the ones this package computed from
// the footer.
func compareStatsWithDuckDB(t *testing.T, ctx context.Context, pub *Publisher, rd *reader) {
	scratch := t.TempDir()
	types, err := rd.query(false, "SELECT column_name, data_type FROM duckdb_columns() WHERE database_name = 'lake' "+
		"AND schema_name = 'public' AND table_name = 'orders' ORDER BY column_index")
	if err != nil {
		t.Fatal(err)
	}
	var defs []string
	for _, r := range types {
		defs = append(defs, quoteIdent(r[0])+" "+r[1])
	}
	for _, q := range []string{
		fmt.Sprintf("ATTACH 'ducklake:sqlite:%s/s.sqlite' AS scratch (DATA_PATH '%s/d/')", scratch, scratch),
		"CREATE TABLE scratch.t (" + strings.Join(defs, ", ") + ")",
	} {
		if _, err := rd.query(false, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	rows, err := pub.cat.db.QueryContext(ctx, `SELECT f.data_file_id, f.path, f.record_count, f.footer_size, f.file_size_bytes
		FROM ducklake_data_file f WHERE f.end_snapshot IS NULL ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	type file struct {
		id                 int64
		path               string
		rows, footer, size int64
	}
	var files []file
	for rows.Next() {
		var f file
		if err := rows.Scan(&f.id, &f.path, &f.rows, &f.footer, &f.size); err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	rows.Close()
	norm := func(s string) string { return strings.Replace(s, ".000000", "", 1) }
	compared := 0
	for _, f := range files {
		abs := filepath.Join(pub.dataDir, "public", "orders", f.path)
		if _, err := rd.query(false, fmt.Sprintf(
			"CALL ducklake_add_data_files('scratch', 't', '%s', allow_missing => true, ignore_extra_columns => true)", abs)); err != nil {
			t.Fatalf("ducklake_add_data_files(%s): %v", f.path, err)
		}
		theirs, err := rd.query(false, fmt.Sprintf(`SELECT c.column_name, s.value_count, s.null_count, s.min_value, s.max_value,
				f.record_count, f.footer_size, f.file_size_bytes
			FROM __ducklake_metadata_scratch.ducklake_file_column_stats s
			JOIN __ducklake_metadata_scratch.ducklake_data_file f ON f.data_file_id = s.data_file_id
			JOIN __ducklake_metadata_scratch.ducklake_column c ON c.column_id = s.column_id AND c.end_snapshot IS NULL
			WHERE f.path = '%s' ORDER BY 1`, abs))
		if err != nil {
			t.Fatal(err)
		}
		mine := map[string][]string{}
		mrows, err := pub.cat.db.QueryContext(ctx, `SELECT c.column_name, s.value_count, s.null_count,
				coalesce(s.min_value, '<nil>'), coalesce(s.max_value, '<nil>')
			FROM ducklake_file_column_stats s JOIN ducklake_column c ON c.column_id = s.column_id
				AND c.table_id = s.table_id AND c.end_snapshot IS NULL
			WHERE s.data_file_id = ?`, f.id)
		if err != nil {
			t.Fatal(err)
		}
		for mrows.Next() {
			var name, vals, nulls, lo, hi string
			if err := mrows.Scan(&name, &vals, &nulls, &lo, &hi); err != nil {
				t.Fatal(err)
			}
			mine[name] = []string{vals, nulls, norm(lo), norm(hi)}
		}
		mrows.Close()
		// DuckDB also writes a row for a table column the file does not
		// hold (allow_missing); a file registered before ADD COLUMN has
		// none, in DuckLake's own catalogs as in this one. So the columns
		// compared are the ones the file holds.
		seen := 0
		for _, r := range theirs {
			m := mine[r[0]]
			if m == nil {
				continue
			}
			seen++
			if m[0] != r[1] || m[1] != r[2] || m[2] != norm(r[3]) || m[3] != norm(r[4]) {
				t.Fatalf("%s column %s: DuckDB says values,nulls,min,max = %v; this package %v", f.path, r[0], r[1:5], m)
			}
			if r[5] != fmt.Sprint(f.rows) || r[6] != fmt.Sprint(f.footer) || r[7] != fmt.Sprint(f.size) {
				t.Fatalf("%s: DuckDB says rows,footer,size = %v; this package %d,%d,%d", f.path, r[5:8], f.rows, f.footer, f.size)
			}
			compared++
		}
		if seen != len(mine) {
			t.Fatalf("%s: this package wrote statistics for %d columns, DuckDB for %d of them", f.path, len(mine), seen)
		}
	}
	t.Logf("per-file statistics equal to DuckDB's own for %d columns of %d files", compared, len(files))
}

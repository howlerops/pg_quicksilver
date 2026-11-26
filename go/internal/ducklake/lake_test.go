package ducklake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

func openLake(t testing.TB, root string, retention time.Duration, now func() time.Time) *Publisher {
	t.Helper()
	pub, err := Open(context.Background(), Options{MirrorRoot: root, Retention: retention, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	return pub
}

func withoutPatches(t testing.TB) {
	old := mirror.PartialDeltas
	mirror.PartialDeltas = false
	t.Cleanup(func() { mirror.PartialDeltas = old })
}

// A table that holds patch files is not published as a table: it is refused,
// flagged for a rewrite, and published once the rewrite has removed them.
func TestPatchFilesAreNotPublished(t *testing.T) {
	old := mirror.PartialDeltas
	mirror.PartialDeltas = true
	defer func() { mirror.PartialDeltas = old }()
	ctx := context.Background()
	h := newHarness(t, 7)
	defer func() { h.tbl.Close() }()
	h.apply(200, 0, 0, false)
	if _, err := h.tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	pub := openLake(t, h.root, time.Minute, nil)
	if res, err := pub.Publish(ctx, h.last); err != nil || res.Refused != "" {
		t.Fatalf("publish of a table of whole rows: %+v, %v", res, err)
	}

	// An update that carries the key and one column is stored as a patch.
	ids := h.ids()
	var ch = h.txn(nil)
	for _, id := range ids[:20] {
		h.model[id]["v"] = "424242"
		ch[0].Changes = append(ch[0].Changes, h.change("update", map[string]any{"id": strconv.FormatInt(id, 10), "v": "424242"}, id))
	}
	if _, err := h.tbl.Apply(ch); err != nil {
		t.Fatal(err)
	}
	partial := 0
	for _, f := range h.last.Files {
		if f.Partial != nil {
			partial++
		}
	}
	if partial == 0 {
		t.Skip("the mirror wrote no patch file for a two-column update; nothing to test")
	}
	res, err := pub.Publish(ctx, h.last)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Refused, "patch files") || !pub.NeedsRewrite("public.orders") {
		t.Fatalf("a manifest with %d patch files was published: %+v", partial, res)
	}
	rd := startReader(t, CatalogPath(h.root))
	if _, err := rd.query(false, "SELECT count(*) FROM lake.public.orders"); err == nil ||
		!strings.Contains(err.Error(), "patch files") {
		t.Fatalf("the refused table answered, or failed without saying why: %v", err)
	}

	// What the sidecar does about it: partial deltas off, and a base rewrite.
	mirror.PartialDeltas = false
	h.rewrite(false)
	res, err = pub.Publish(ctx, h.last)
	if err != nil || res.Refused != "" || pub.NeedsRewrite("public.orders") {
		t.Fatalf("after the rewrite: %+v, %v", res, err)
	}
	got, err := rd.query(false, "SELECT count(*), sum(v) FROM lake.public.orders")
	if err != nil || got[0][0] != strconv.Itoa(len(h.model)) || got[0][1] != strconv.FormatInt(h.sumV(), 10) {
		t.Fatalf("after the rewrite the lake reads %v (%v); the model has %d rows, sum %d", got, err, len(h.model), h.sumV())
	}
}

func writeParquet(t testing.TB, path string, schema *arrow.Schema, cols ...arrow.Array) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatal(err)
	}
	rec := array.NewRecord(schema, cols, int64(cols[0].Len()))
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func int64s(v ...int64) arrow.Array {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues(v, nil)
	return b.NewArray()
}

func int32s(v ...int32) arrow.Array {
	b := array.NewInt32Builder(memory.DefaultAllocator)
	b.AppendValues(v, nil)
	return b.NewArray()
}

func strs(v ...string) arrow.Array {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.AppendValues(v, nil)
	return b.NewArray()
}

// The mirror's physical types are changing under this package (integers at
// their declared width, uuid as 16 bytes), so a table can hold older files of
// one type and newer files of another. Integers of different widths are one
// DuckLake column of the wider type. A string and a uuid are not one column:
// the table is refused until a base rewrite has left one type.
func TestFilesOfDifferentPhysicalTypes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "public.mix")
	const u1, u2, u3 = "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333"

	// Older files: every integer an int64, uuid as text.
	oldSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "n", Type: arrow.PrimitiveTypes.Int64}, {Name: "u", Type: arrow.BinaryTypes.String}}, nil)
	writeParquet(t, filepath.Join(dir, "base", "000001.parquet"), oldSchema, int64s(1, 2), int64s(10, 20), strs(u1, u2))
	// Newer files: n at its declared width.
	narrow := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "n", Type: arrow.PrimitiveTypes.Int32}, {Name: "u", Type: arrow.BinaryTypes.String}}, nil)
	writeParquet(t, filepath.Join(dir, "delta", "000002.parquet"), narrow, int64s(3), int32s(30), strs(u3))
	// Newer still: uuid as 16 bytes.
	ub := extensions.NewUUIDBuilder(memory.DefaultAllocator)
	ub.Append(uuid.MustParse(u3))
	uuidSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "n", Type: arrow.PrimitiveTypes.Int32}, {Name: "u", Type: extensions.NewUUIDType()}}, nil)
	writeParquet(t, filepath.Join(dir, "delta", "000003.parquet"), uuidSchema, int64s(4), int32s(40), ub.NewArray())

	manifest := func(seq int, uuidType arrow.DataType, files ...string) *mirror.LakeManifest {
		m := &mirror.LakeManifest{Schema: "public", Table: "mix", Dir: dir, Seq: seq, AppliedLSN: fmt.Sprintf("0/%X", seq),
			Columns: []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: arrow.PrimitiveTypes.Int64},
				{Name: "n", PGType: "integer", Type: arrow.PrimitiveTypes.Int32},
				{Name: "u", PGType: "uuid", Type: uuidType}}}
		for _, f := range files {
			m.Files = append(m.Files, mirror.LakeFile{Rel: f})
		}
		return m
	}
	pub := openLake(t, root, time.Minute, nil)
	rd := startReader(t, CatalogPath(root))
	types := func() string {
		rows, err := rd.query(false, "SELECT string_agg(column_name || ' ' || data_type, ', ' ORDER BY column_index) "+
			"FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'mix'")
		if err != nil {
			t.Fatal(err)
		}
		return rows[0][0]
	}

	// int64 and int32 files of one column: published, as the wider type.
	res, err := pub.Publish(ctx, manifest(1, arrow.BinaryTypes.String, "base/000001.parquet", "delta/000002.parquet"))
	if err != nil || res.Refused != "" {
		t.Fatalf("int64 + int32 files: %+v, %v", res, err)
	}
	got, err := rd.query(false, "SELECT count(*), sum(n), count(DISTINCT u) FROM lake.public.mix WHERE n >= 20")
	if err != nil || flat(got) != "2,50,2" {
		t.Fatalf("int64 + int32 files read %v (%v)", got, err)
	}
	if ty := types(); ty != "id BIGINT, n BIGINT, u VARCHAR" {
		t.Fatalf("column types with an int64 file live: %s", ty)
	}

	// Once the int64 file is gone the column is the declared width.
	if res, err = pub.Publish(ctx, manifest(2, arrow.BinaryTypes.String, "delta/000002.parquet")); err != nil || res.Refused != "" {
		t.Fatalf("int32 file alone: %+v, %v", res, err)
	}
	if ty := types(); ty != "id BIGINT, n INTEGER, u VARCHAR" {
		t.Fatalf("column types with only the int32 file: %s", ty)
	}

	// A text uuid file and a 16-byte uuid file: one UUID column. DuckLake
	// casts the text on the way in, filters included.
	mixed := manifest(3, extensions.NewUUIDType(), "delta/000002.parquet", "delta/000003.parquet")
	res, err = pub.Publish(ctx, mixed)
	if err != nil || res.Refused != "" {
		t.Fatalf("varchar + uuid files: %+v, %v", res, err)
	}
	if ty := types(); ty != "id BIGINT, n INTEGER, u UUID" {
		t.Fatalf("column types with a text-uuid file and a uuid file: %s", ty)
	}
	for q, want := range map[string]string{
		"SELECT id, u FROM lake.public.mix ORDER BY id":                     "3," + u3 + ";4," + u3,
		"SELECT id FROM lake.public.mix WHERE u = '" + u3 + "' ORDER BY id": "3;4",
		"SELECT id FROM lake.public.mix WHERE u > '" + u2 + "' ORDER BY id": "3;4",
		"SELECT count(*) FROM lake.public.mix WHERE u < '" + u2 + "'":       "0",
		"SELECT count(*), min(u), max(u) FROM lake.public.mix":              "2," + u3 + "," + u3,
	} {
		if got, err = rd.query(true, q); err != nil || flat(got) != want {
			t.Fatalf("%s: %v (%v), want %s", q, got, err, want)
		}
	}

	// A column that is a number in one file and text in another is not one
	// column: refused, and a rewrite is wanted.
	text := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "n", Type: arrow.BinaryTypes.String}, {Name: "u", Type: arrow.BinaryTypes.String}}, nil)
	writeParquet(t, filepath.Join(dir, "delta", "000004.parquet"), text, int64s(5), strs("fifty"), strs(u1))
	bad := manifest(4, extensions.NewUUIDType(), "delta/000003.parquet", "delta/000004.parquet")
	res, err = pub.Publish(ctx, bad)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Refused, `column "n" as two types`) || !pub.NeedsRewrite("public.mix") {
		t.Fatalf("int32 + varchar files were published: %+v", res)
	}
	if _, err := rd.query(false, "SELECT count(*) FROM lake.public.mix"); err == nil || !strings.Contains(err.Error(), "two types") {
		t.Fatalf("the refused table answered, or failed without the reason: %v", err)
	}

	// The rewrite leaves one type, and the table is published.
	res, err = pub.Publish(ctx, manifest(5, extensions.NewUUIDType(), "delta/000003.parquet"))
	if err != nil || res.Refused != "" || pub.NeedsRewrite("public.mix") {
		t.Fatalf("uuid file alone: %+v, %v", res, err)
	}
	got, err = rd.query(false, "SELECT id, n, u FROM lake.public.mix WHERE u = '"+u3+"'")
	if err != nil || flat(got) != "4,40,"+u3 {
		t.Fatalf("uuid file read %v (%v)", got, err)
	}
	if ty := types(); ty != "id BIGINT, n INTEGER, u UUID" {
		t.Fatalf("column types with the uuid file: %s", ty)
	}
}

// A publish that dies with its files already in the lake leaves the previous
// snapshot exactly as it was; the next start removes the files and publishes
// the manifest the mirror is at.
func TestCrashMidPublish(t *testing.T) {
	withoutPatches(t)
	ctx := context.Background()
	h := newHarness(t, 3)
	defer func() { h.tbl.Close() }()
	h.apply(300, 0, 0, false)
	pub, err := Open(ctx, Options{MirrorRoot: h.root, Retention: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pub.Publish(ctx, h.last); err != nil {
		t.Fatal(err)
	}
	rd := startReader(t, CatalogPath(h.root))
	before, err := rd.query(false, checksumSQL(h.order, "lake.public.orders"))
	if err != nil {
		t.Fatal(err)
	}
	filesBefore := lakeFilesOnDisk(t, pub)

	h.apply(40, 60, 30, false)
	var left map[string]bool
	pub.beforeCommit = func() error {
		left = lakeFilesOnDisk(t, pub)
		return errors.New("killed")
	}
	if _, err := pub.Publish(ctx, h.last); err == nil {
		t.Fatal("the publish was supposed to die")
	}
	if len(left) <= len(filesBefore) {
		t.Fatalf("the dying publish had made no files: %d before, %d at the moment of death", len(filesBefore), len(left))
	}
	// kill -9: nothing is cleaned up by the process. Put the files back as
	// they were at the moment of death, then abandon the publisher.
	for f := range left {
		if !filesBefore[f] {
			if err := os.WriteFile(filepath.Join(pub.dataDir, f), []byte("half-written"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	pub.Close()

	after, err := rd.query(true, checksumSQL(h.order, "lake.public.orders"))
	if err != nil || flat(after) != flat(before) {
		t.Fatalf("after the crash the lake reads %v (%v); before it, %v", after, err, before)
	}

	pub = openLake(t, h.root, time.Minute, nil)
	if n := len(lakeFilesOnDisk(t, pub)); n != len(filesBefore) {
		t.Fatalf("after reopening, the lake holds %d files; the last snapshot names %d", n, len(filesBefore))
	}
	// Reconciliation: the mirror says where it is, the lake goes there.
	if _, err := pub.Publish(ctx, h.tbl.LakeManifest()); err != nil {
		t.Fatal(err)
	}
	view, _ := h.tbl.ViewSQL()
	want, err := rd.query(false, checksumSQL(h.order, "("+view+") t"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := rd.query(true, checksumSQL(h.order, "lake.public.orders"))
	if err != nil || flat(got) != flat(want) || got[0][0] != strconv.Itoa(len(h.model)) {
		t.Fatalf("after reconciliation the lake reads %v (%v); the mirror %v, the model %d rows", got, err, want, len(h.model))
	}
}

// Submit does not wait for the lake, and a publisher that falls behind skips
// to the newest manifest. Whatever it publishes is a manifest the mirror
// committed.
func TestSubmitCoalesces(t *testing.T) {
	withoutPatches(t)
	ctx := context.Background()
	h := newHarness(t, 5)
	defer func() { h.tbl.Close() }()
	pub := openLake(t, h.root, time.Minute, nil)
	var slowest time.Duration
	committed := map[int]bool{}
	h.onNew = func(m *mirror.LakeManifest) {
		committed[m.Seq] = true
		t0 := time.Now()
		pub.Submit(m)
		slowest = max(slowest, time.Since(t0))
	}
	for i := 0; i < 200; i++ {
		h.apply(5, 5, 2, false)
		if i%40 == 39 {
			if _, err := h.tbl.MergeDeltas(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := pub.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tables, _ := pub.Stats()
	st := tables["public.orders"]
	if st.PublishedSeq != int64(h.last.Seq) || st.PublishedLSN != h.last.AppliedLSN || st.LagSeconds != 0 {
		t.Fatalf("after Flush the lake is at manifest %d (%s); the mirror at %d (%s)",
			st.PublishedSeq, st.PublishedLSN, h.last.Seq, h.last.AppliedLSN)
	}
	if st.Errors != 0 {
		t.Fatalf("%d publishes failed: %s", st.Errors, st.LastError)
	}
	t.Logf("200 applies: %d snapshots, %d manifests coalesced away, slowest Submit %v", st.Publishes, st.Coalesced, slowest)
	if slowest > 50*time.Millisecond {
		t.Fatalf("Submit took %v; it is on the apply path", slowest)
	}
	rows, err := pub.cat.db.QueryContext(ctx, `SELECT commit_extra_info FROM ducklake_snapshot_changes WHERE commit_extra_info IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var extra string
		var info struct {
			Seq int `json:"manifest_seq"`
		}
		if err := rows.Scan(&extra); err != nil || json.Unmarshal([]byte(extra), &info) != nil || !committed[info.Seq] {
			t.Fatalf("snapshot published from %q, which is not a manifest the mirror committed (%v)", extra, err)
		}
	}
	rd := startReader(t, CatalogPath(h.root))
	got, err := rd.query(false, "SELECT count(*), sum(v) FROM lake.public.orders")
	if err != nil || got[0][0] != strconv.Itoa(len(h.model)) || got[0][1] != strconv.FormatInt(h.sumV(), 10) {
		t.Fatalf("lake reads %v (%v); model %d rows, sum %d", got, err, len(h.model), h.sumV())
	}
	fresh, err := rd.query(false, FreshnessSQL("lake", "public", "orders"))
	if err != nil || fresh[0][0] != h.last.AppliedLSN {
		t.Fatalf("freshness query says %v (%v); the mirror is at %s", fresh, err, h.last.AppliedLSN)
	}
}

// The catalog does not grow with the number of publishes: expiry removes the
// rows of what it expires, and SQLite reuses the pages.
func TestCatalogSizeIsStable(t *testing.T) {
	withoutPatches(t)
	publishes := 800
	if s, err := strconv.Atoi(os.Getenv("QS_LAKE_PUBLISHES")); err == nil && s > 0 {
		publishes = s
	}
	ctx := context.Background()
	h := newHarness(t, 11)
	defer func() { h.tbl.Close() }()
	clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	pub := openLake(t, h.root, 20*time.Second, clk.Now)
	h.apply(500, 0, 0, false)
	type sample struct {
		n            int
		catalog, wal int64
		lake         int
		c            Counts
	}
	var samples []sample
	for i := 1; i <= publishes; i++ {
		h.apply(3, 6, 2, false)
		switch {
		case i%400 == 0:
			h.rewrite(false)
		case len(h.last.Files) > 12:
			if _, err := h.tbl.MergeDeltas(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pub.Publish(ctx, h.last); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		clk.advance(100 * time.Millisecond)
		if err := pub.Expire(ctx); err != nil {
			t.Fatal(err)
		}
		if i%(publishes/5) == 0 {
			_, g := pub.Stats()
			c, _ := pub.Counts(ctx)
			samples = append(samples, sample{i, g.CatalogBytes, g.WALBytes, len(lakeFilesOnDisk(t, pub)), c})
		}
	}
	for _, s := range samples {
		t.Logf("after %5d publishes: catalog %d bytes, wal %d bytes, lake files %d, rows %+v", s.n, s.catalog, s.wal, s.lake, s.c)
	}
	first, last := samples[0], samples[len(samples)-1]
	if last.catalog > first.catalog*3/2 || last.c.Snapshots > first.c.Snapshots*3/2 || last.lake > first.lake*2 {
		t.Fatalf("the lake grew between publish %d and %d", first.n, last.n)
	}
	_, g := pub.Stats()
	t.Logf("expired %d snapshots, unlinked %d files (%d bytes)", g.ExpiredSnapshots, g.UnlinkedFiles, g.UnlinkedBytes)
}

func TestRefusesAnotherFormatVersion(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	pub, err := Open(ctx, Options{MirrorRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pub.cat.db.Exec(`UPDATE ducklake_metadata SET "value" = '1.2' WHERE "key" = 'version'`); err != nil {
		t.Fatal(err)
	}
	// A publisher that is already running notices at its next publish.
	_, err = pub.Publish(ctx, &mirror.LakeManifest{Schema: "public", Table: "t", Dir: root, Seq: 1, AppliedLSN: "0/1"})
	if err == nil || !strings.Contains(err.Error(), "format 1.2") {
		t.Fatalf("published into a catalog of another format version: %v", err)
	}
	pub.Close()
	if _, err := Open(ctx, Options{MirrorRoot: root}); err == nil || !strings.Contains(err.Error(), "format 1.2") {
		t.Fatalf("opened a catalog of another format version: %v", err)
	}
}

func TestRefusesAnotherFilesystem(t *testing.T) {
	root := t.TempDir()
	other, err := os.MkdirTemp("/dev/shm", "qs-lake-")
	if err != nil {
		t.Skip("no second filesystem to try (/dev/shm): ", err)
	}
	defer os.RemoveAll(other)
	_, err = Open(context.Background(), Options{MirrorRoot: root, Dir: other})
	if err == nil {
		t.Skip("/dev/shm is on the same filesystem as the test directory")
	}
	if !strings.Contains(err.Error(), "same filesystem") {
		t.Fatalf("a lake on another filesystem failed without saying why: %v", err)
	}
	t.Log(err)
}

func TestOneWriterPerLake(t *testing.T) {
	root := t.TempDir()
	openLake(t, root, time.Minute, nil)
	if _, err := Open(context.Background(), Options{MirrorRoot: root}); err == nil ||
		!strings.Contains(err.Error(), "another process") {
		t.Fatalf("a second publisher opened the same lake: %v", err)
	}
}

// Readers in other processes query in a loop while the publisher publishes
// and expires. Every answer has to be a state the mirror committed, and no
// query may fail.
func TestConcurrentReaders(t *testing.T) {
	withoutPatches(t)
	seconds := 10.0
	if s, err := strconv.ParseFloat(os.Getenv("QS_LAKE_CONCURRENT_SECONDS"), 64); err == nil && s > 0 {
		seconds = s
	}
	ctx := context.Background()
	h := newHarness(t, 13)
	defer func() { h.tbl.Close() }()
	pub := openLake(t, h.root, 2*time.Second, nil)

	// The answer a reader gives is (count, sum(v), marker): row 1's v is the
	// number of the apply, so no two committed states give the same answer
	// and a mixture of two is, almost surely, not the answer of any.
	var mu sync.Mutex
	states := map[string]bool{}
	apply := 0
	h.onNew = func(m *mirror.LakeManifest) {
		mu.Lock()
		states[fmt.Sprintf("%d,%d,%d", len(h.model), h.sumV(), apply)] = true
		mu.Unlock()
		pub.Submit(m)
	}
	step := func() {
		apply++
		// The marker row is updated in the same transaction as the rest.
		ch := h.txn(nil)
		r := h.row(1)
		r["v"] = strconv.Itoa(apply)
		h.model[1] = r
		ch[0].Changes = append(ch[0].Changes, h.change("update", r, 1))
		ids := h.ids()
		for i := 0; i < 6 && len(ids) > 20; i++ {
			id := ids[1+h.rng.Intn(len(ids)-1)]
			if _, ok := h.model[id]; !ok || id == 1 {
				continue
			}
			if i < 2 {
				delete(h.model, id)
				ch[0].Changes = append(ch[0].Changes, h.change("delete", nil, id))
			} else {
				nr := h.row(id)
				h.model[id] = nr
				ch[0].Changes = append(ch[0].Changes, h.change("update", nr, id))
			}
		}
		for i := 0; i < 3; i++ {
			id := h.next
			h.next++
			h.model[id] = h.row(id)
			ch[0].Changes = append(ch[0].Changes, h.change("insert", h.model[id], id))
		}
		if _, err := h.tbl.Apply(ch); err != nil {
			t.Fatalf("apply: %v", err)
		}
		switch {
		case apply%150 == 0:
			h.rewrite(false)
		case len(h.last.Files) > 10:
			if _, err := h.tbl.MergeDeltas(); err != nil {
				t.Fatalf("merge: %v", err)
			}
		}
	}
	h.apply(2000, 0, 0, false)
	step()
	if err := pub.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	const readers = 3
	rds := make([]*reader, readers)
	for i := range rds {
		rds[i] = startReader(t, CatalogPath(h.root))
	}
	const q = "SELECT count(*), coalesce(sum(v), 0), (SELECT v FROM lake.public.orders WHERE id = 1) FROM lake.public.orders"
	replies := make([]*reply, readers)
	errs := make([]error, readers)
	var wg sync.WaitGroup
	for i, rd := range rds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// One reader attaches afresh for its loop; the others keep the
			// connection they attached before anything was published since.
			replies[i], errs[i] = rd.call(map[string]any{"fresh": i == 0,
				"loop": map[string]any{"sql": q, "seconds": seconds}})
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	start := time.Now()
writing:
	for {
		select {
		case <-done:
			break writing
		default:
			step()
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := pub.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	tables, g := pub.Stats()
	st := tables["public.orders"]
	queries, distinct := 0, 0
	for i, rep := range replies {
		if errs[i] != nil || rep.Error != "" {
			t.Fatalf("reader %d: %v %s", i, errs[i], rep.Error)
		}
		if len(rep.Errors) > 0 {
			t.Fatalf("reader %d: %d queries failed, the first with: %s", i, len(rep.Errors), rep.Errors[0])
		}
		queries += rep.Queries
		for _, raw := range rep.RawAnswers {
			var parts []json.RawMessage
			var rows [][]*string
			if json.Unmarshal(raw, &parts) != nil || json.Unmarshal(parts[0], &rows) != nil {
				t.Fatalf("reader %d: unreadable answer %s", i, raw)
			}
			distinct++
			mu.Lock()
			ok := states[flat(textRows(rows))]
			mu.Unlock()
			if !ok {
				t.Fatalf("reader %d saw %s, which is no state the mirror committed (a torn read)", i, flat(textRows(rows)))
			}
		}
	}
	t.Logf("%.0fs: %d applies, %d snapshots published (%d manifests coalesced), %d expired, %d files unlinked",
		time.Since(start).Seconds(), apply, st.Publishes, st.Coalesced, g.ExpiredSnapshots, g.UnlinkedFiles)
	t.Logf("%d readers: %d queries, %d distinct answers, 0 errors, 0 torn", readers, queries, distinct)
	t.Logf("publish p50 %.1f ms, p95 %.1f ms; catalog %d bytes, wal %d bytes", g.P50Seconds*1000, g.P95Seconds*1000, g.CatalogBytes, g.WALBytes)
	if st.Errors != 0 {
		t.Fatalf("%d publishes failed: %s", st.Errors, st.LastError)
	}
	if distinct < 3 || g.ExpiredSnapshots == 0 {
		t.Fatalf("the readers saw %d states and %d snapshots expired: nothing was tested", distinct, g.ExpiredSnapshots)
	}
}

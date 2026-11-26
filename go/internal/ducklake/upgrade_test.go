package ducklake

// A mirror the previous release wrote, published by this one. Its files hold
// every integer as INT64, real as DOUBLE, uuid as text, unconstrained numeric
// as DECIMAL(38,9) and -infinity as the type's minimum, next to files in the
// current form, until a base rewrite. The lake is those files: it either
// reads exactly what the mirror's own view reads over them, or it refuses the
// table and asks for the rewrite. It is never a third thing.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

func float64s(v ...float64) arrow.Array {
	b := array.NewFloat64Builder(memory.DefaultAllocator)
	b.AppendValues(v, nil)
	return b.NewArray()
}

func float32s(v ...float32) arrow.Array {
	b := array.NewFloat32Builder(memory.DefaultAllocator)
	b.AppendValues(v, nil)
	return b.NewArray()
}

func int16s(v ...int16) arrow.Array {
	b := array.NewInt16Builder(memory.DefaultAllocator)
	b.AppendValues(v, nil)
	return b.NewArray()
}

func uuids(v ...string) arrow.Array {
	b := extensions.NewUUIDBuilder(memory.DefaultAllocator)
	for _, s := range v {
		b.Append(uuid.MustParse(s))
	}
	return b.NewArray()
}

func lakeFiles(rels ...string) []mirror.LakeFile {
	var out []mirror.LakeFile
	for _, r := range rels {
		out = append(out, mirror.LakeFile{Rel: r})
	}
	return out
}

// mustRefuse checks that a publish refused the table for the reason given,
// asked for a rewrite, and that a reader gets the reason and not rows.
func mustRefuse(t *testing.T, pub *Publisher, rd *reader, table string, res Result, err error, reason string) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Refused, reason) {
		t.Fatalf("published instead of refused for %q: %+v", reason, res)
	}
	if st, _ := pub.Stats(); !st["public."+table].NeedsRewrite {
		t.Fatalf("refused, but no base rewrite is wanted: %+v", res)
	}
	if got, err := rd.query(true, "SELECT count(*) FROM lake.public."+table); err == nil || !strings.Contains(err.Error(), reason) {
		t.Fatalf("the refused table answered %v, or failed without the reason: %v", got, err)
	}
}

// -infinity as the previous release stored it is the type's minimum, which
// DuckDB reads as a finite date in 290309 BC. Linked as it is, `WHERE ts =
// '-infinity'` found one of two rows. Such a file is not published.
func TestLegacyNegInfinityIsNotPublished(t *testing.T) {
	ctx := context.Background()
	tsT := &arrow.TimestampType{Unit: arrow.Microsecond}
	tzT := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	for _, tc := range []struct {
		name, pg, sql  string
		typ            arrow.DataType
		old, neg, pos  int64
		finite, column string
	}{
		{"timestamp", "timestamp without time zone", "TIMESTAMP", tsT, math.MinInt64, -math.MaxInt64, math.MaxInt64, "2024-02-29 00:00:00", "x"},
		{"timestamptz", "timestamp with time zone", "TIMESTAMPTZ", tzT, math.MinInt64, -math.MaxInt64, math.MaxInt64, "2024-02-29 00:00:00+00", "x"},
		{"date", "date", "DATE", arrow.FixedWidthTypes.Date32, math.MinInt32, -math.MaxInt32, math.MaxInt32, "2024-02-29", "x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "public.inf")
			sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "x", Type: tc.typ}}, nil)
			col := func(v ...int64) arrow.Array {
				if tc.name == "date" {
					b := array.NewDate32Builder(memory.DefaultAllocator)
					for _, x := range v {
						b.Append(arrow.Date32(x))
					}
					return b.NewArray()
				}
				b := array.NewTimestampBuilder(memory.DefaultAllocator, tc.typ.(*arrow.TimestampType))
				for _, x := range v {
					b.Append(arrow.Timestamp(x))
				}
				return b.NewArray()
			}
			const day = 19782 // 2024-02-29
			fin := int64(day)
			if tc.name != "date" {
				fin = day * 86400 * 1e6
			}
			// The previous release: -infinity as the minimum, +infinity as now.
			writeParquet(t, filepath.Join(dir, "base", "000001.parquet"), sc, int64s(1, 2, 3), col(tc.old, tc.pos, fin))
			// This release.
			writeParquet(t, filepath.Join(dir, "delta", "000002.parquet"), sc, int64s(4, 5, 6), col(tc.neg, tc.pos, fin))
			// The previous release, a file that holds no -infinity at all.
			writeParquet(t, filepath.Join(dir, "delta", "000003.parquet"), sc, int64s(7, 8), col(tc.pos, fin))
			// The rewrite of all three.
			writeParquet(t, filepath.Join(dir, "base", "000004.parquet"), sc,
				int64s(1, 2, 3, 4, 5, 6, 7, 8), col(tc.neg, tc.pos, fin, tc.neg, tc.pos, fin, tc.pos, fin))
			seq := 0
			manifest := func(rels ...string) *mirror.LakeManifest {
				seq++
				return &mirror.LakeManifest{Schema: "public", Table: "inf", Dir: dir, Seq: seq, AppliedLSN: fmt.Sprintf("0/%X", seq),
					Columns: []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: arrow.PrimitiveTypes.Int64},
						{Name: "x", PGType: tc.pg, Type: tc.typ}},
					Files: lakeFiles(rels...)}
			}
			pub := openLake(t, root, time.Minute, nil)
			rd := startReader(t, CatalogPath(root))
			reads := func(step, want string) {
				t.Helper()
				q := fmt.Sprintf("SELECT count(*) FILTER (x = '-infinity'::%[1]s), count(*) FILTER (x = 'infinity'::%[1]s), "+
					"count(*) FILTER (isfinite(x)), count(DISTINCT x), min(x)::VARCHAR, max(x)::VARCHAR FROM lake.public.inf", tc.sql)
				got, err := rd.query(true, q)
				if err != nil || flat(got) != want {
					t.Fatalf("%s: the lake reads %v (%v), expected %s", step, got, err, want)
				}
				// And through a filter DuckLake answers from the statistics.
				for pred, n := range map[string]string{
					"x = '-infinity'::" + tc.sql:  strings.Split(want, ",")[0],
					"x = 'infinity'::" + tc.sql:   strings.Split(want, ",")[1],
					"x < '0001-01-01'::" + tc.sql: strings.Split(want, ",")[0],
				} {
					if got, err := rd.query(true, "SELECT count(*) FROM lake.public.inf WHERE "+pred); err != nil || flat(got) != n {
						t.Fatalf("%s: WHERE %s counts %v (%v), expected %s", step, pred, got, err, n)
					}
				}
			}

			// Files of this release, and an old one whose statistics say it
			// holds no -infinity: +infinity is the same in both, so published.
			res, err := pub.Publish(ctx, manifest("delta/000002.parquet", "delta/000003.parquet"))
			if err != nil || res.Refused != "" {
				t.Fatalf("no file holds the old -infinity, yet: %+v, %v", res, err)
			}
			reads("without the old file", "1,2,2,3,-infinity,infinity")

			// With the old file that does.
			res, err = pub.Publish(ctx, manifest("base/000001.parquet", "delta/000002.parquet", "delta/000003.parquet"))
			mustRefuse(t, pub, rd, "inf", res, err, "may hold -infinity")
			// The old file ALONE is as wrong as the mix.
			res, err = pub.Publish(ctx, manifest("base/000001.parquet"))
			mustRefuse(t, pub, rd, "inf", res, err, "may hold -infinity")

			// The rewrite.
			res, err = pub.Publish(ctx, manifest("base/000004.parquet"))
			if err != nil || res.Refused != "" || pub.NeedsRewrite("public.inf") {
				t.Fatalf("after the rewrite: %+v, %v", res, err)
			}
			reads("after the rewrite", "2,3,3,3,-infinity,infinity")
		})
	}
}

// The previous release stored `real` by parsing its text as a float64, so
// 0.1::real is 0.1 in its files and 0.10000000149011612 (the float4, widened)
// from this release's under a DOUBLE column: one source value, two values in
// the lake. Float widths are not widened; the table waits for the rewrite.
func TestRealWidthMixIsNotPublished(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "public.fm")
	old := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "r", Type: arrow.PrimitiveTypes.Float64}}, nil)
	nw := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "r", Type: arrow.PrimitiveTypes.Float32}}, nil)
	writeParquet(t, filepath.Join(dir, "base", "000001.parquet"), old, int64s(1, 2), float64s(0.1, 0.1))
	writeParquet(t, filepath.Join(dir, "delta", "000002.parquet"), nw, int64s(3), float32s(0.1))
	writeParquet(t, filepath.Join(dir, "base", "000003.parquet"), nw, int64s(1, 2, 3), float32s(0.1, 0.1, 0.1))
	manifest := func(seq int, rels ...string) *mirror.LakeManifest {
		return &mirror.LakeManifest{Schema: "public", Table: "fm", Dir: dir, Seq: seq, AppliedLSN: fmt.Sprintf("0/%X", seq),
			Columns: []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: arrow.PrimitiveTypes.Int64},
				{Name: "r", PGType: "real", Type: arrow.PrimitiveTypes.Float32}},
			Files: lakeFiles(rels...)}
	}
	pub := openLake(t, root, time.Minute, nil)
	rd := startReader(t, CatalogPath(root))
	res, err := pub.Publish(ctx, manifest(1, "base/000001.parquet", "delta/000002.parquet"))
	mustRefuse(t, pub, rd, "fm", res, err, `column "r" as two types`)
	// The old file alone is no mix, and is as wrong: 0.1 where the source
	// value is the float4, which `r = 0.1::real` does not find.
	res, err = pub.Publish(ctx, manifest(2, "base/000001.parquet"))
	mustRefuse(t, pub, rd, "fm", res, err, `column "r" as two types`)

	res, err = pub.Publish(ctx, manifest(3, "base/000003.parquet"))
	if err != nil || res.Refused != "" || pub.NeedsRewrite("public.fm") {
		t.Fatalf("after the rewrite: %+v, %v", res, err)
	}
	got, err := rd.query(true, "SELECT count(*), count(DISTINCT r), count(*) FILTER (r = 0.1::FLOAT), typeof(any_value(r)) FROM lake.public.fm")
	if err != nil || flat(got) != "3,1,3,FLOAT" {
		t.Fatalf("one source value (real 0.1) in three rows reads %v (%v)", got, err)
	}
}

// The proof behind every pair widen still allows: the same source values in
// both physical forms, in one lake table, read as the same values by every
// operator that compares them. A pair that fails here comes out of widen.
func TestWidenedColumnsReadAsOneValue(t *testing.T) {
	ctx := context.Background()
	id := arrow.PrimitiveTypes.Int64
	us := []string{
		"00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000001",
		"0a0eebc9-9c0b-4ef8-bb6d-6bb9bd380a11", "7fffffff-ffff-ffff-ffff-ffffffffffff",
		"80000000-0000-0000-0000-000000000000", "9e107d9d-372b-4826-bd81-d3542a419d6f",
		"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "abcdef01-2345-6789-abcd-ef0123456789",
		"f47ac10b-58cc-4372-a567-0e02b2c3d479", "ffffffff-ffff-ffff-ffff-ffffffffffff",
	}
	quoted := func(v []string) []string {
		out := make([]string, len(v))
		for i, s := range v {
			out[i] = "'" + s + "'"
		}
		return out
	}
	ints := []string{"-32768", "-1", "0", "1", "7", "255", "256", "1000", "32766", "32767"}
	for _, tc := range []struct {
		name, pg, lake string
		declared       arrow.DataType
		// The forms, oldest first; each holds every value once.
		forms  []arrow.DataType
		cols   []arrow.Array
		values []string // as SQL literals, in the column's order
	}{
		{"int64+int32", "integer", "BIGINT", arrow.PrimitiveTypes.Int32,
			[]arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int32},
			[]arrow.Array{int64s(-32768, -1, 0, 1, 7, 255, 256, 1000, 32766, 32767), int32s(-32768, -1, 0, 1, 7, 255, 256, 1000, 32766, 32767)}, ints},
		{"int64+int16", "smallint", "BIGINT", arrow.PrimitiveTypes.Int16,
			[]arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int16},
			[]arrow.Array{int64s(-32768, -1, 0, 1, 7, 255, 256, 1000, 32766, 32767), int16s(-32768, -1, 0, 1, 7, 255, 256, 1000, 32766, 32767)}, ints},
		{"int64+int32+int16", "smallint", "BIGINT", arrow.PrimitiveTypes.Int16,
			[]arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int16},
			[]arrow.Array{int64s(-32768, -1, 0, 1, 7, 255, 256, 1000, 32766, 32767), int32s(-32768, -1, 0, 1, 7, 255, 256, 1000, 32766, 32767),
				int16s(-32768, -1, 0, 1, 7, 255, 256, 1000, 32766, 32767)}, ints},
		{"text uuid+uuid", "uuid", "UUID", extensions.NewUUIDType(),
			[]arrow.DataType{arrow.BinaryTypes.String, extensions.NewUUIDType()},
			[]arrow.Array{strs(us...), uuids(us...)}, quoted(us)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "public.w")
			n, forms := len(tc.values), len(tc.forms)
			var rels []string
			for f, typ := range tc.forms {
				ids := make([]int64, n)
				for i := range ids {
					ids[i] = int64(f*1000 + i)
				}
				rel := fmt.Sprintf("delta/%06d.parquet", f+1)
				if f == 0 {
					rel = "base/000001.parquet"
				}
				writeParquet(t, filepath.Join(dir, rel), arrow.NewSchema([]arrow.Field{{Name: "id", Type: id}, {Name: "x", Type: typ}}, nil),
					int64s(ids...), tc.cols[f])
				rels = append(rels, rel)
			}
			pub := openLake(t, root, time.Minute, nil)
			res, err := pub.Publish(ctx, &mirror.LakeManifest{Schema: "public", Table: "w", Dir: dir, Seq: 1, AppliedLSN: "0/1",
				Columns: []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: id}, {Name: "x", PGType: tc.pg, Type: tc.declared}},
				Files:   lakeFiles(rels...)})
			if err != nil || res.Refused != "" {
				t.Fatalf("widen allows this mix, yet: %+v, %v", res, err)
			}
			rd := startReader(t, CatalogPath(root))
			const w = "lake.public.w"
			check := func(q, want string) {
				t.Helper()
				if got, err := rd.query(false, q); err != nil || flat(got) != want {
					t.Fatalf("%s\n  = %v (%v), expected %s", q, got, err, want)
				}
			}
			lit := func(i int) string { return fmt.Sprintf("CAST(%s AS %s)", tc.values[i], tc.lake) }
			check("SELECT data_type FROM duckdb_columns() WHERE database_name = 'lake' AND table_name = 'w' AND column_name = 'x'", tc.lake)
			// One value per source value, whichever file it is in.
			check("SELECT count(*), count(DISTINCT x) FROM "+w, fmt.Sprintf("%d,%d", n*forms, n))
			check("SELECT count(*), min(c), max(c) FROM (SELECT x, count(*) c FROM "+w+" GROUP BY x)", fmt.Sprintf("%d,%d,%d", n, forms, forms))
			// Equality, with the value as a literal of the column's type and
			// as the untyped literal a person writes.
			for i := range tc.values {
				check(fmt.Sprintf("SELECT count(*), count(DISTINCT id %% 1000) FROM %s WHERE x = %s", w, lit(i)), fmt.Sprintf("%d,1", forms))
				check(fmt.Sprintf("SELECT count(*) FROM %s WHERE x = %s", w, tc.values[i]), fmt.Sprint(forms))
				// Ranges: the values are in the column's order, so i of them
				// are below value i in each form.
				check(fmt.Sprintf("SELECT count(*) FROM %s WHERE x < %s", w, lit(i)), fmt.Sprint(i*forms))
				check(fmt.Sprintf("SELECT count(*) FROM %s WHERE x >= %s", w, lit(i)), fmt.Sprint((n-i)*forms))
			}
			check(fmt.Sprintf("SELECT count(*) FROM %s WHERE x BETWEEN %s AND %s", w, lit(2), lit(6)), fmt.Sprint(5*forms))
			check(fmt.Sprintf("SELECT count(*) FROM %s WHERE x IN (%s, %s, %s)", w, lit(0), lit(4), lit(n-1)), fmt.Sprint(3*forms))
			check(fmt.Sprintf("SELECT count(*) FROM %s WHERE x <> %s", w, lit(3)), fmt.Sprint((n-1)*forms))
			check(fmt.Sprintf("SELECT min(x) = %s, max(x) = %s FROM %s", lit(0), lit(n-1), w), "True,True")
			// Ordering: by value, the forms interleaved, in the order given.
			var order []string
			for i := 0; i < n; i++ {
				for f := 0; f < forms; f++ {
					order = append(order, fmt.Sprint(f*1000+i))
				}
			}
			check("SELECT string_agg(id::VARCHAR, ',' ORDER BY x, id) FROM "+w, strings.Join(order, ","))
			check("SELECT string_agg(id::VARCHAR, ',') FROM (SELECT id FROM "+w+" ORDER BY x, id LIMIT 3)", strings.Join(order[:3], ","))
			// Joins: each row of one form finds its own value in every other,
			// and nothing else, hashed and merged.
			for f := 1; f < forms; f++ {
				check(fmt.Sprintf("SELECT count(*), count(*) FILTER (a.id %% 1000 = b.id %% 1000) FROM %[1]s a JOIN %[1]s b ON a.x = b.x "+
					"WHERE a.id < 1000 AND b.id >= %d AND b.id < %d", w, f*1000, (f+1)*1000), fmt.Sprintf("%d,%d", n, n))
			}
			check(fmt.Sprintf("SELECT count(*) FROM %[1]s a JOIN %[1]s b ON a.x = b.x", w), fmt.Sprint(n*forms*forms))
			check(fmt.Sprintf("SELECT count(*) FROM %[1]s a JOIN %[1]s b ON a.x < b.x WHERE a.id < 1000 AND b.id >= 1000 AND b.id < 2000", w),
				fmt.Sprint(n*(n-1)/2))
			check(fmt.Sprintf("SELECT count(*) FROM %[1]s a WHERE a.id >= 1000 AND a.x IN (SELECT x FROM %[1]s WHERE id < 5)", w), fmt.Sprint(5*(forms-1)))
			check(fmt.Sprintf("SELECT count(*) FROM %[1]s a WHERE a.id < 1000 AND NOT EXISTS (SELECT 1 FROM %[1]s b WHERE b.x = a.x AND b.id >= 1000)", w), "0")
			// As text, the two forms spell a value the same way.
			check("SELECT count(DISTINCT x::VARCHAR) FROM "+w, fmt.Sprint(n))
		})
	}
}

// The sidecar starts a base rewrite each time NeedsRewrite says yes. While the
// publish of the rewrite's own manifest was failing it said yes on every
// tick: the verdict it answered from was the previous manifest's.
func TestNoRewriteIsAskedForWhileAPublishIsFailing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "public.p")
	sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	writeParquet(t, filepath.Join(dir, "base", "000001.parquet"), sc, int64s(1, 2))
	writeParquet(t, filepath.Join(dir, "delta", "000002.parquet"), sc, int64s(2))
	writeParquet(t, filepath.Join(dir, "base", "000003.parquet"), sc, int64s(1, 2))
	cols := []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: arrow.PrimitiveTypes.Int64}}
	clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	pub := openLake(t, root, time.Hour, clk.Now)
	submit := func(seq int, files ...mirror.LakeFile) {
		t.Helper()
		pub.Submit(&mirror.LakeManifest{Schema: "public", Table: "p", Dir: dir, Seq: seq,
			AppliedLSN: fmt.Sprintf("0/%X", seq), Columns: cols, Files: files})
		if err := pub.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	patched := []mirror.LakeFile{{Rel: "base/000001.parquet"}, {Rel: "delta/000002.parquet", Partial: []string{"id"}}}
	submit(2, patched...)
	if !pub.NeedsRewrite("public.p") {
		t.Fatal("a patch file should want a rewrite")
	}
	// Asked once. The sidecar's next tick, and the one after, ask again
	// before the rewrite has produced anything: not a second rewrite.
	for i := 0; i < 3; i++ {
		if pub.NeedsRewrite("public.p") {
			t.Fatal("a second rewrite was asked for, for the same reason, before the first had a result")
		}
	}
	// A request the sidecar dropped (or a rewrite that failed) is not lost.
	clk.advance(rewriteBackoff - time.Second)
	if pub.NeedsRewrite("public.p") {
		t.Fatal("asked again before the backoff had passed")
	}
	clk.advance(time.Second)
	if !pub.NeedsRewrite("public.p") {
		t.Fatal("never asked again: a dropped request would leave the table refused for good")
	}
	clk.advance(2*rewriteBackoff - time.Second)
	if pub.NeedsRewrite("public.p") {
		t.Fatal("the backoff did not grow")
	}

	// The rewrite finished; its manifest is whole rows. The catalog is failing.
	pub.work.Lock()
	pub.beforeCommit = func() error { return errors.New("disk full") }
	pub.work.Unlock()
	rewrites := 0
	for i := 0; i < 5; i++ {
		submit(3+i, mirror.LakeFile{Rel: "base/000003.parquet"})
		clk.advance(time.Hour) // no backoff stands in the way: only the failing publish does
		if pub.NeedsRewrite("public.p") {
			rewrites++ // the sidecar would call BeginCompaction here
		}
	}
	if rewrites > 0 {
		t.Fatalf("a table whose manifest no longer needs a rewrite asked for one %d times out of 5 while its publish was failing", rewrites)
	}
	if st, _ := pub.Stats(); st["public.p"].Errors == 0 {
		t.Fatal("the publishes did not fail; this proved nothing")
	}

	// The catalog recovers: the manifest is published and nothing is asked.
	pub.work.Lock()
	pub.beforeCommit = nil
	pub.work.Unlock()
	submit(9, mirror.LakeFile{Rel: "base/000003.parquet"})
	if st, _ := pub.Stats(); st["public.p"].Refused != "" || st["public.p"].PublishedSeq != 9 || pub.NeedsRewrite("public.p") {
		t.Fatalf("after the catalog recovered: %+v", st["public.p"])
	}
	// A NEW reason is asked for at once, whatever the backoff had grown to.
	submit(10, patched...)
	if !pub.NeedsRewrite("public.p") {
		t.Fatal("new patch files were not given a rewrite")
	}
}

// The upgrade, end to end, against DuckDB: a table built by the previous
// release's writer, then applied to, merged and rewritten by this one, and
// published at every step. At every step the lake reads exactly what the
// mirror's own view reads, or refuses; and it refuses only until the rewrite
// it asked for.
func TestConformanceUpgradeFromThePreviousWriter(t *testing.T) {
	// It compares a TIMESTAMPTZ predicate with the time column, which is text
	// under QS_TEMPORAL_TYPES=0; the lake's type mapping is tested as native.
	defer func(prev bool) { mirror.TemporalTypes = prev }(mirror.TemporalTypes)
	mirror.TemporalTypes = true
	old := mirror.PartialDeltas
	mirror.PartialDeltas = false // what the sidecar does when QS_DUCKLAKE is on
	defer func() { mirror.PartialDeltas = old }()
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		cols []string
		inf  bool
		// refusedFor is a part of each reason the table may be refused for
		// before its rewrite; empty when the mix is one the lake reads.
		refusedFor []string
		probes     []string
	}{
		// Integers at two widths and a uuid in two forms: read as one column.
		{name: "widened", cols: []string{"id bigint", "v integer", "s smallint", "u uuid", "name text",
			"flag boolean", "amount numeric(12,2)", "ts timestamptz", "d date"},
			probes: []string{"v >= 0", "s < 0", "u < '80000000-0000-0000-0000-000000000000'",
				"u >= 'c0000000-0000-0000-0000-000000000000'", "ts >= '2022-06-01'", "d < '2023-01-01'", "v IS NULL", "u IS NULL"}},
		// real: a DOUBLE and a FLOAT are two values for one.
		{name: "real", cols: []string{"id bigint", "v integer", "r real"},
			refusedFor: []string{`column "r" as two types`}, probes: []string{"r = 0.1::FLOAT", "r > 1", "r IS NULL"}},
		// Unconstrained numeric: DECIMAL(38,9) then, the text now.
		{name: "numeric", cols: []string{"id bigint", "v integer", "n numeric"},
			refusedFor: []string{`column "n" as two types`}, probes: []string{"n = '12.50'", "n IS NULL"}},
		// -infinity as the type's minimum.
		{name: "infinity", cols: []string{"id bigint", "v integer", "ts timestamptz", "t2 timestamp", "d date"}, inf: true,
			refusedFor: []string{"may hold -infinity"},
			probes: []string{"ts = '-infinity'", "t2 = '-infinity'", "d = '-infinity'", "ts = 'infinity'", "d = 'infinity'",
				"NOT isfinite(t2)", "d < '0001-01-01'", "ts IS NULL"}},
		// All of it at once.
		{name: "everything", cols: []string{"id bigint", "v integer", "s smallint", "r real", "n numeric", "u uuid",
			"amount numeric(12,2)", "ts timestamptz", "t2 timestamp", "d date", "name text", "flag boolean"}, inf: true,
			refusedFor: []string{"as two types", "may hold -infinity"},
			probes:     []string{"v >= 0", "r = 0.1::FLOAT", "n = '12.50'", "ts = '-infinity'", "d = '-infinity'", "u < '80000000-0000-0000-0000-000000000000'"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarnessOf(t, 7, tc.inf, tc.cols...)
			defer func() { h.tbl.Close() }()

			// The previous release: a base, and deltas on it.
			h.tbl.Close()
			mirror.LegacyWriterForTest(true)
			func() {
				defer mirror.LegacyWriterForTest(false)
				h.open()
				h.apply(120, 0, 0, false)
				h.rewrite(false)
				h.apply(15, 20, 5, false)
				h.apply(5, 10, 0, false)
				h.tbl.Close()
			}()
			// This one.
			h.open()
			if _, err := h.tbl.BuildIndex(); err != nil {
				t.Fatal(err)
			}
			clk := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
			pub, err := Open(ctx, Options{MirrorRoot: h.root, Retention: time.Hour, Now: clk.Now})
			if err != nil {
				t.Fatal(err)
			}
			defer pub.Close()
			rd := startReader(t, CatalogPath(h.root))

			published, refused, rewrites, mixed := 0, 0, 0, 0
			step := func(op string, m *mirror.LakeManifest) {
				t.Helper()
				clk.advance(time.Hour)
				res, err := pub.Publish(ctx, m)
				if err != nil {
					t.Fatalf("%s: publish: %v", op, err)
				}
				if res.Refused != "" {
					refused++
					ok := false
					for _, why := range tc.refusedFor {
						ok = ok || strings.Contains(res.Refused, why)
					}
					if !ok || rewrites > 0 {
						t.Fatalf("%s: refused after %d rewrites, for %q", op, rewrites, res.Refused)
					}
					if got, err := rd.query(true, "SELECT count(*) FROM lake.public.orders"); err == nil ||
						!strings.Contains(err.Error(), "a base rewrite") {
						t.Fatalf("%s: the refused table answered %v, or failed without the reason: %v", op, got, err)
					}
					return
				}
				published++
				if rewrites == 0 {
					mixed++
				}
				view, err := h.tbl.ViewSQL()
				if err != nil {
					t.Fatalf("%s: view sql: %v", op, err)
				}
				mirrorRows, err := rd.query(false, checksumSQL(h.order, "("+view+") t"))
				if err != nil {
					t.Fatalf("%s: the mirror's own view failed: %v", op, err)
				}
				lakeRows, err := rd.query(true, checksumSQL(h.order, "lake.public.orders"))
				if err != nil {
					t.Fatalf("%s: lake query: %v", op, err)
				}
				if flat(lakeRows) != flat(mirrorRows) {
					t.Fatalf("%s: lake %s, mirror view %s", op, flat(lakeRows), flat(mirrorRows))
				}
				if lakeRows[0][0] != fmt.Sprint(len(h.model)) || lakeRows[0][2] != fmt.Sprint(h.sumV()) {
					t.Fatalf("%s: lake count,sum(v) = %s,%s; model %d,%d", op, lakeRows[0][0], lakeRows[0][2], len(h.model), h.sumV())
				}
				var distinct []string
				for _, c := range h.order[1:] {
					distinct = append(distinct, "count(DISTINCT "+quoteIdent(c)+")")
				}
				qs := []string{"SELECT " + strings.Join(distinct, ", ") + " FROM %s"}
				for _, p := range append(predicates(h.order), tc.probes...) {
					qs = append(qs, "SELECT count(*), coalesce(sum(id), 0) FROM %s WHERE "+p)
				}
				for _, q := range qs {
					a, err1 := rd.query(false, fmt.Sprintf(q, "lake.public.orders"))
					b, err2 := rd.query(false, fmt.Sprintf(q, "("+view+") t"))
					if err1 != nil || err2 != nil || flat(a) != flat(b) {
						t.Fatalf("%s: %s: lake %v (%v), mirror view %v (%v)", op, q, a, err1, b, err2)
					}
				}
			}
			maybeRewrite := func() {
				t.Helper()
				if !pub.NeedsRewrite("public.orders") {
					return
				}
				h.rewrite(h.rng.Intn(2) == 0)
				rewrites++
				step("the rewrite the lake asked for", h.last)
			}

			// The mirror as the previous release left it.
			step("old files only", h.tbl.LakeManifest())
			if len(tc.refusedFor) > 0 {
				// The first applies of the new writer, before any rewrite:
				// both forms side by side, as on the day of the upgrade.
				for i := 0; i < 3; i++ {
					h.apply(8, 10, 3, false)
					step(fmt.Sprintf("apply %d before the rewrite", i), h.last)
				}
				if refused == 0 || published > 0 && tc.name != "infinity" {
					// (infinity: the old files may happen to be publishable
					// until one that holds the sentinel is among them.)
					t.Fatalf("published %d and refused %d times before the rewrite", published, refused)
				}
				maybeRewrite()
				if rewrites != 1 {
					t.Fatal("the refused table did not ask for a rewrite")
				}
			}
			for i := 0; i < 14; i++ {
				switch r := h.rng.Intn(10); {
				case r < 6:
					h.apply(h.rng.Intn(12), h.rng.Intn(15), h.rng.Intn(6), false)
					step(fmt.Sprintf("apply %d", i), h.last)
				case r < 8:
					if _, err := h.tbl.MergeDeltas(); err != nil {
						t.Fatal(err)
					}
					step(fmt.Sprintf("merge %d", i), h.last)
				default:
					if len(tc.refusedFor) == 0 && i < 9 {
						continue // keep the old base under the widened mix a while longer
					}
					h.rewrite(h.rng.Intn(2) == 0)
					rewrites++
					step(fmt.Sprintf("rewrite %d", i), h.last)
				}
				maybeRewrite()
			}
			if len(tc.refusedFor) == 0 && (refused != 0 || mixed < 5) {
				t.Fatalf("a mix the lake reads was refused %d times, published %d times before a rewrite", refused, mixed)
			}
			// The last word: a rewrite, and the declared types.
			h.rewrite(false)
			rewrites++
			step("final rewrite", h.last)
			if pub.NeedsRewrite("public.orders") {
				t.Fatal("a rewritten table still asks for a rewrite")
			}
			t.Logf("published %d times (%d over old and new files), refused %d, %d rewrites", published, mixed, refused, rewrites)
		})
	}
}

// A file is held against the type the mirror says it writes now, so the two
// have to be the same for everything the current writer writes: a type they
// disagreed on would be refused for good, rewrite after rewrite. Every kind
// of column, written by this release, is published and reads as the view.
func TestEveryCurrentTypeIsPublished(t *testing.T) {
	ctx := context.Background()
	defer func(prev string) { mirror.NumericScale = prev }(mirror.NumericScale)
	cols := [][3]string{
		{"id", "bigint", "1"}, {"v", "integer", "-7"}, {"si", "smallint", "-32768"}, {"bi", "bigint", "9223372036854775807"},
		{"r", "real", "0.1"}, {"dp", "double precision", "0.1"}, {"b", "boolean", "t"},
		{"u", "uuid", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}, {"raw", "bytea", `\xdeadbeef00ff`},
		{"n2", "numeric(12,2)", "1.50"}, {"n", "numeric", "12.50"}, {"nn", "numeric", "0.25"},
		{"n50", "numeric(50,2)", "123456789012345678901234567890123456789012345678.25"},
		{"d", "date", "-infinity"}, {"tm", "time without time zone", "12:34:56.5"},
		{"ts", "timestamp without time zone", "-infinity"}, {"tz", "timestamp with time zone", "2026-01-02 03:04:05.123456+00"},
		{"tx", "text", "é"}, {"ch", "character(8)", "ab      "}, {"vc", "character varying(10)", "abc"},
		{"j", "jsonb", `{"a": 1}`}, {"iv", "interval", "1 day"}, {"ip", "inet", "192.168.1.7"},
		{"arr", "integer[]", "{1,2}"}, {"ttz", "time with time zone", "12:00:00+02"}, {"en", "mood", "happy"},
	}
	for _, scale := range []string{"", "public.orders.nn=4"} {
		t.Run("QS_NUMERIC_SCALE="+scale, func(t *testing.T) {
			mirror.NumericScale = scale
			root := t.TempDir()
			types, row, null := map[string]string{}, map[string]any{}, map[string]any{}
			var order []string
			for _, c := range cols {
				types[c[0]], row[c[0]], null[c[0]] = c[1], c[2], nil
				order = append(order, c[0])
			}
			null["id"] = "2"
			tbl, err := mirror.New(root, "public", "orders", "id", types, order)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			ch := func(r map[string]any) changestream.Change {
				return changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "orders", Row: r,
					Key: map[string]any{"id": r["id"]}}
			}
			if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: "0/10", NextLSN: "0/10",
				Changes: []changestream.Change{ch(row), ch(null)}}}); err != nil {
				t.Fatal(err)
			}
			if tbl.Halted() != "" {
				t.Fatal(tbl.Halted())
			}
			pub := openLake(t, root, time.Minute, nil)
			publish := func(step string) {
				t.Helper()
				res, err := pub.Publish(ctx, tbl.LakeManifest())
				if err != nil || res.Refused != "" {
					t.Fatalf("%s: files this release wrote were not published: %+v, %v", step, res, err)
				}
				if st, _ := pub.Stats(); st["public.orders"].NeedsRewrite {
					t.Fatalf("%s: a rewrite is wanted of files this release wrote", step)
				}
			}
			publish("a delta")
			rd := startReader(t, CatalogPath(root))
			same := func(step string) {
				t.Helper()
				view, err := tbl.ViewSQL()
				if err != nil {
					t.Fatal(err)
				}
				for _, q := range []string{
					checksumSQL(order, "%s"),
					"SELECT string_agg(typeof(COLUMNS(*)), ',') FROM (SELECT * FROM %s WHERE id = 1)",
				} {
					a, err1 := rd.query(true, strings.Replace(q, "%s", "lake.public.orders", 1))
					b, err2 := rd.query(false, strings.Replace(q, "%s", "("+view+") t", 1))
					if err1 != nil || err2 != nil || flat(a) != flat(b) || flat(a) == "" {
						t.Fatalf("%s: %s:\n  lake        %v (%v)\n  mirror view %v (%v)", step, q, a, err1, b, err2)
					}
				}
			}
			same("a delta")
			if _, err := tbl.Compact(); err != nil {
				t.Fatal(err)
			}
			publish("a base")
			same("a base")
			want := "VARCHAR"
			if scale != "" {
				want = "DECIMAL(38,4)"
			}
			got, err := rd.query(false, "SELECT typeof(nn), typeof(n), typeof(n50), nn::VARCHAR, n, n50 FROM lake.public.orders WHERE id = 1")
			if err != nil || !strings.HasPrefix(flat(got), want+",VARCHAR,VARCHAR,") ||
				!strings.HasSuffix(flat(got), ",12.50,123456789012345678901234567890123456789012345678.25") {
				t.Fatalf("the numerics read %v (%v)", got, err)
			}
		})
	}
}

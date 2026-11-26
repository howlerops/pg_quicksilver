package ducklake

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// One lake holds every table of the mirror: tables of one schema, tables of
// another, each published on its own, with the ids of all of them drawn from
// the same counters. Halting one leaves the others readable.
func TestSeveralTablesInOneLake(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String}}, nil)
	type tbl struct {
		schema, name string
		seq          int
		files        []string
		rows         int64
		sum          int64
	}
	tables := []*tbl{{schema: "public", name: "a"}, {schema: "public", name: "b"}, {schema: "sales", name: "a"}}
	next := int64(1)
	manifest := func(tb *tbl, dead map[string][]uint64) *mirror.LakeManifest {
		tb.seq++
		m := &mirror.LakeManifest{Schema: tb.schema, Table: tb.name,
			Dir: filepath.Join(root, tb.schema+"."+tb.name), Seq: tb.seq, AppliedLSN: fmt.Sprintf("0/%X", next),
			Columns: []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", PGType: "text", Type: arrow.BinaryTypes.String}}}
		for _, f := range tb.files {
			lf := mirror.LakeFile{Rel: f}
			if d := dead[f]; d != nil {
				lf.DVGen, lf.Dead, lf.DeadCount = tb.seq, d, 1
			}
			m.Files = append(m.Files, lf)
		}
		return m
	}
	add := func(tb *tbl) {
		rel := fmt.Sprintf("delta/%06d.parquet", len(tb.files)+1)
		writeParquet(t, filepath.Join(root, tb.schema+"."+tb.name, rel), schema,
			int64s(next, next+1, next+2), strs(tb.schema, tb.name, "x"))
		tb.files = append(tb.files, rel)
		tb.rows += 3
		tb.sum += 3*next + 3
		next += 3
	}
	pub := openLake(t, root, time.Minute, nil)
	for round := 0; round < 4; round++ {
		for _, tb := range tables {
			add(tb)
			if res, err := pub.Publish(ctx, manifest(tb, nil)); err != nil || res.Refused != "" || res.Added != 1 {
				t.Fatalf("round %d %s.%s: %+v, %v", round, tb.schema, tb.name, res, err)
			}
		}
	}
	// One row of public.b's first file dies: position 0, whose id is 4.
	b := tables[1]
	if _, err := pub.Publish(ctx, manifest(b, map[string][]uint64{b.files[0]: {1}})); err != nil {
		t.Fatal(err)
	}
	b.rows, b.sum = b.rows-1, b.sum-4

	rd := startReader(t, CatalogPath(root))
	check := func(tb *tbl) {
		t.Helper()
		got, err := rd.query(false, fmt.Sprintf("SELECT count(*), sum(id), count(DISTINCT name) FROM lake.%s.%s", tb.schema, tb.name))
		if err != nil || flat(got) != fmt.Sprintf("%d,%d,3", tb.rows, tb.sum) {
			t.Fatalf("%s.%s reads %v (%v); it holds %d rows, sum %d", tb.schema, tb.name, got, err, tb.rows, tb.sum)
		}
	}
	for _, tb := range tables {
		check(tb)
	}
	// Halt one; the other two are untouched, and it comes back.
	a := tables[0]
	halt := manifest(a, nil)
	halt.Refusal, halt.Columns, halt.Files = "a value the column cannot hold", nil, nil
	if _, err := pub.Publish(ctx, halt); err != nil {
		t.Fatal(err)
	}
	if _, err := rd.query(false, "SELECT count(*) FROM lake.public.a"); err == nil {
		t.Fatal("the halted table answered")
	}
	check(tables[1])
	check(tables[2])
	if res, err := pub.Publish(ctx, manifest(a, nil)); err != nil || res.Added != len(a.files) {
		t.Fatalf("back in service: %+v, %v", res, err)
	}
	for _, tb := range tables {
		check(tb)
	}
	lsn, err := rd.query(false, "SELECT string_agg(table_name || '=' || applied_lsn, ' ' ORDER BY table_name) FROM __ducklake_metadata_lake.qs_mirror_table")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("three tables in one lake: %s", lsn[0][0])
}

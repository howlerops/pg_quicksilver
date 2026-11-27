package ducklake

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

func putWatermark(t *testing.T, root, lsn string, tables map[string]string) {
	t.Helper()
	b, _ := json.Marshal(mirror.Watermark{AppliedLSN: lsn, Tables: tables, WrittenAt: time.Now().UTC()})
	tmp := filepath.Join(root, mirror.WatermarkFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(root, mirror.WatermarkFile)); err != nil {
		t.Fatal(err)
	}
}

// A table nothing writes to keeps its manifest, so nothing is published for
// it, and the catalog row stays at its last change. The mirror's watermark
// says how far it has really applied; the catalog carries that beside the
// snapshots, in a transaction that publishes none, and both freshness readers
// (ReadTable, which is `qs-query -ducklake -lsn`, and the printed freshness
// query in DuckDB) report it. It applies only to a lake table standing at the
// very manifest the watermark saw.
func TestQuietTableFreshnessComesFromTheWatermarkWithoutASnapshot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String}}, nil)
	seq := map[string]int{}
	files := map[string][]string{}
	publish := func(pub *Publisher, table, lsn string) {
		t.Helper()
		seq[table]++
		rel := fmt.Sprintf("delta/%06d.parquet", seq[table])
		writeParquet(t, filepath.Join(root, "public."+table, rel), schema, int64s(int64(seq[table])), strs(table))
		files[table] = append(files[table], rel)
		m := &mirror.LakeManifest{Schema: "public", Table: table, Dir: filepath.Join(root, "public."+table),
			Seq: seq[table], AppliedLSN: lsn,
			Columns: []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: arrow.PrimitiveTypes.Int64},
				{Name: "name", PGType: "text", Type: arrow.BinaryTypes.String}}}
		for _, f := range files[table] {
			m.Files = append(m.Files, mirror.LakeFile{Rel: f})
		}
		if res, err := pub.Publish(ctx, m); err != nil || res.Refused != "" {
			t.Fatalf("publish %s at %s: %+v %v", table, lsn, res, err)
		}
	}
	// A clock that never reaches the worker's expiry, and a worker whose tick
	// this test does not wait for except where it says so.
	pub := openLake(t, root, time.Hour, nil)
	catalog := CatalogPath(root)
	state := func(table string) TableState {
		t.Helper()
		st, err := ReadTable(ctx, catalog, "public", table)
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	snapshots := func() (n, last int64) {
		t.Helper()
		db, err := sql.Open("sqlite", "file:"+catalog+"?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := db.QueryRow(`SELECT count(*), max(snapshot_id) FROM ducklake_snapshot`).Scan(&n, &last); err != nil {
			t.Fatal(err)
		}
		return n, last
	}
	sync := func() {
		t.Helper()
		if err := pub.SyncWatermark(ctx); err != nil {
			t.Fatal(err)
		}
	}
	syncs := func() int64 { _, g := pub.Stats(); return g.WatermarkSyncs }

	publish(pub, "quiet", "0/10")
	publish(pub, "busy", "0/10")
	if st := state("quiet"); st.AppliedLSN != "0/10" || st.ManifestLSN != "0/10" {
		t.Fatalf("with no watermark: %+v", st)
	}
	n0, last0 := snapshots()

	// The source moves on in some other table; these two are untouched.
	putWatermark(t, root, "0/50", map[string]string{"public.quiet": "0/10", "public.busy": "0/10"})
	sync()
	for _, table := range []string{"quiet", "busy"} {
		if st := state(table); st.AppliedLSN != "0/50" || st.ManifestLSN != "0/10" {
			t.Fatalf("%s under a watermark at 0/50: %+v", table, st)
		}
	}
	if n, last := snapshots(); n != n0 || last != last0 {
		t.Fatalf("carrying the watermark published a snapshot: %d snapshots to %d, was %d to %d", n, last, n0, last0)
	}
	// The same watermark again costs nothing.
	before := syncs()
	sync()
	if got := syncs(); got != before || before == 0 {
		t.Fatalf("an unchanged watermark was written again: %d copies, then %d", before, got)
	}

	// `busy` changes and is published. The watermark still names its OLD
	// manifest: it must not drag the table back to 0/50, nor forward.
	publish(pub, "busy", "0/60")
	if st := state("busy"); st.AppliedLSN != "0/60" {
		t.Fatalf("a table published past the watermark reads %+v", st)
	}
	// The mirror is ahead of the lake: the watermark speaks for a manifest of
	// `busy` at 0/70 that has not been published. The lake table is at 0/60
	// and that is all it may claim; `quiet` goes to 0/90.
	putWatermark(t, root, "0/90", map[string]string{"public.quiet": "0/10", "public.busy": "0/70"})
	sync()
	if st := state("busy"); st.AppliedLSN != "0/60" {
		t.Fatalf("a watermark for a manifest the lake does not hold was applied to it: %+v", st)
	}
	if st := state("quiet"); st.AppliedLSN != "0/90" || st.ManifestLSN != "0/10" {
		t.Fatalf("quiet: %+v", st)
	}
	if n, last := snapshots(); n != n0+1 || last != last0+1 {
		t.Fatalf("%d snapshots to %d; one publish since %d to %d", n, last, n0, last0)
	}

	// What a DuckDB reader sees through the query qs-query prints.
	rd := startReader(t, catalog)
	for table, want := range map[string]string{"quiet": "0/90,1,", "busy": "0/60,2,"} {
		got, err := rd.query(false, FreshnessSQL("lake", "public", table))
		if err != nil || len(got) != 1 || len(got[0]) != 5 {
			t.Fatalf("freshness of %s: %v (%v)", table, got, err)
		}
		manifest := map[string]string{"quiet": "0/10", "busy": "0/60"}[table]
		if got[0][0]+","+got[0][1]+"," != want || got[0][3] != "<nil>" && got[0][3] != "" || got[0][4] != manifest {
			t.Fatalf("freshness of %s through DuckDB: %v", table, got[0])
		}
	}
	if got, err := rd.query(false, "SELECT count(*) FROM lake.public.quiet"); err != nil || got[0][0] != "1" {
		t.Fatalf("the lake table itself: %v (%v)", got, err)
	}

	// Nobody has to call it: the worker copies a new watermark on its tick.
	putWatermark(t, root, "0/A0", map[string]string{"public.quiet": "0/10", "public.busy": "0/60"})
	deadline := time.Now().Add(10 * time.Second)
	for state("quiet").AppliedLSN != "0/A0" || state("busy").AppliedLSN != "0/A0" {
		if time.Now().After(deadline) {
			t.Fatalf("the worker did not copy the watermark: %+v %+v", state("quiet"), state("busy"))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A catalog from before the table existed: a reader falls back to the
	// manifest's LSN, and the next publisher to open it adds the table.
	pub.Close()
	db, err := sql.Open("sqlite", "file:"+catalog)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE qs_mirror_watermark`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if st := state("quiet"); st.AppliedLSN != "0/10" {
		t.Fatalf("a catalog without the watermark table: %+v", st)
	}
	pub = openLake(t, root, time.Hour, nil)
	if err := pub.SyncWatermark(ctx); err != nil {
		t.Fatal(err)
	}
	if st := state("quiet"); st.AppliedLSN != "0/A0" {
		t.Fatalf("after a publisher reopened the old catalog: %+v", st)
	}
}

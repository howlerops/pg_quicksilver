package ducklake

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	_ "modernc.org/sqlite"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// ducklake_table_column_stats is restated on every publish. Once the smallest
// row is deleted the stored minimum is a bound and not a value (the file the
// row came from still holds it, behind a deletion vector), and DuckDB must
// not answer min() from it: it has to scan, and find the new minimum.
func TestTableColumnStatsAfterDeletingTheMinimum(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dir := filepath.Join(root, "public.s")
	id, v := arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: id}, {Name: "v", Type: v}}, nil)
	writeParquet(t, filepath.Join(dir, "base", "000001.parquet"), schema,
		int64s(1, 2, 3, 4, 5), int64s(50, 30, 90, 10, 70))
	cols := []mirror.LakeColumn{{Name: "id", PGType: "bigint", Type: id}, {Name: "v", PGType: "bigint", Type: v}}
	pub := openLake(t, root, time.Minute, nil)
	rd := startReader(t, CatalogPath(root))

	res, err := pub.Publish(ctx, &mirror.LakeManifest{Schema: "public", Table: "s", Dir: dir, Seq: 1, AppliedLSN: "0/1",
		Columns: cols, Files: lakeFiles("base/000001.parquet")})
	if err != nil || res.Refused != "" {
		t.Fatalf("publish: %+v, %v", res, err)
	}
	// The row with v = 10 is at position 3: delete it through the deletion
	// vector the mirror keeps (bit p of word p/64).
	res, err = pub.Publish(ctx, &mirror.LakeManifest{Schema: "public", Table: "s", Dir: dir, Seq: 2, AppliedLSN: "0/2",
		Columns: cols, Files: []mirror.LakeFile{{Rel: "base/000001.parquet", DVGen: 1, Dead: []uint64{1 << 3}, DeadCount: 1}}})
	if err != nil || res.Refused != "" {
		t.Fatalf("publish after the delete: %+v, %v", res, err)
	}
	// The catalog row: the bounds are restated, and not exact any more.
	db, err := sql.Open("sqlite", CatalogPath(root))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const stats = `SELECT s.min_value, s.max_value, s.min_is_exact, s.max_is_exact FROM ducklake_table_column_stats s
		JOIN ducklake_column c ON c.column_id = s.column_id AND c.table_id = s.table_id
		WHERE c.column_name = 'v' AND c.end_snapshot IS NULL`
	var lo, hi string
	var loExact, hiExact int64
	if err := db.QueryRow(stats).Scan(&lo, &hi, &loExact, &hiExact); err != nil {
		t.Fatalf("table column stats: %v", err)
	}
	if lo != "10" || hi != "90" || loExact != 0 || hiExact != 0 {
		t.Fatalf("table column stats after the delete: min %q (exact %d), max %q (exact %d); want the old bounds 10 and 90, not exact",
			lo, loExact, hi, hiExact)
	}

	got, err := rd.query(true, "SELECT min(v), max(v), count(*) FROM lake.public.s")
	if err != nil || flat(got) != "30,90,4" {
		t.Fatalf("after deleting the minimum: min/max/count = %v (%v), want 30,90,4", got, err)
	}
	// A stored bound is still a bound: the table's own rows say the minimum is
	// no larger than the live one, and the row count is right.
	got, err = rd.query(true, "SELECT count(*) FROM lake.public.s WHERE v < 30")
	if err != nil || flat(got) != "0" {
		t.Fatalf("rows below the new minimum: %v (%v)", got, err)
	}
}

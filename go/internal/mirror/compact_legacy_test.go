package mirror

import (
	"fmt"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// An order-preserving rewrite over a mirror the previous release wrote: a
// base holding a text uuid key, every integer as BIGINT and real as DOUBLE, a
// delta in the same form and deltas in the current one. The rewrite merges on
// values it reads in two physical forms, so it is run clustered on a column
// of each kind, with applies before it, during it and after a restart.
func TestClusteredRewriteOverLegacyFiles(t *testing.T) {
	for _, by := range []string{"bi", "ref", "r", "ts", "n2"} {
		t.Run("cluster="+by, func(t *testing.T) {
			withRowGroup(t, 50)
			withClusterBy(t, "public.every="+by)
			dir := t.TempDir()
			cols := map[string]string{}
			var order []string
			for _, c := range allTypes {
				if c.name == "n50" {
					continue
				}
				cols[c.name] = c.pg
				order = append(order, c.name)
			}
			open := func() *Table {
				tbl, err := New(dir, "public", "every", "id", cols, order)
				if err != nil {
					t.Fatal(err)
				}
				return tbl
			}
			row := func(i, ver int) map[string]any {
				r := everyRow(i, i%2 == 1)
				delete(r, "n50")
				r["n"] = "12.5"
				r["bi"] = fmt.Sprint(int64(i)*1000 + int64(ver)*777777)
				r["ref"] = fmt.Sprintf("%08x-0000-4000-8000-%012x", i*4096+ver*999999, i)
				r["r"] = fmt.Sprint(float32(i) + float32(ver)*300.5)
				r["ts"] = fmt.Sprintf("2024-01-01 00:%02d:%02d", (i+ver*400)/60%60, i%60)
				r["n2"] = fmt.Sprintf("%d.25", i+ver*400)
				r["tx"] = fmt.Sprintf("v%d", ver)
				return r
			}
			want := map[string]map[string]string{}
			put := func(r map[string]any) changestream.Change {
				want[r["id"].(string)] = expectRow(r)
				return typedChange(changestream.OpInsert, "every", r)
			}
			del := func(i int) changestream.Change {
				r := row(i, 0)
				delete(want, r["id"].(string))
				return typedChange(changestream.OpDelete, "every", r)
			}
			var tbl *Table
			withLegacyWriter(func() {
				tbl = open()
				var batch []changestream.Change
				for i := 0; i < 600; i++ {
					batch = append(batch, put(row(i, 0)))
				}
				tapply(t, tbl, 1, batch...)
				if _, err := tbl.Compact(); err != nil {
					t.Fatal(err)
				}
				batch = nil
				for i := 10; i < 20; i++ {
					batch = append(batch, put(row(i, 1)))
				}
				tapply(t, tbl, 2, batch...)
				tbl.Close()
			})
			tbl = open()
			if _, err := tbl.BuildIndex(); err != nil {
				t.Fatal(err)
			}
			old := PartialDeltas
			PartialDeltas = false
			defer func() { PartialDeltas = old }()
			var batch []changestream.Change
			for i := 20; i < 40; i++ {
				batch = append(batch, put(row(i, 2)))
			}
			for i := 10; i < 15; i++ {
				batch = append(batch, put(row(i, 3)))
			}
			for i := 40; i < 45; i++ {
				batch = append(batch, del(i))
			}
			for i := 600; i < 620; i++ {
				batch = append(batch, put(row(i, 0)))
			}
			tapply(t, tbl, 3, batch...)
			tapply(t, tbl, 4, put(row(25, 4)), put(row(601, 4)), del(602))
			sameContents(t, "before the rewrite", liveText(t, tbl), want)

			before := clusteredRewrites.Load()
			c := tbl.BeginCompaction()
			if c == nil {
				t.Fatal("no compaction")
			}
			// while it runs
			tapply(t, tbl, 5, put(row(30, 5)), put(row(300, 5)), del(301), put(row(700, 0)))
			<-c.done
			if _, err := tbl.FinishCompaction(); err != nil {
				t.Fatal(err)
			}
			if clusteredRewrites.Load() == before {
				t.Fatalf("not clustered: %s", c.clusterWhy)
			}
			sameContents(t, "after the rewrite", liveText(t, tbl), want)
			checkInvariants(t, tbl, "after the rewrite")
			tapply(t, tbl, 6, put(row(0, 6)), put(row(12, 6)), put(row(25, 6)), put(row(610, 6)), del(599), put(row(700, 6)))
			sameContents(t, "after the rewrite and an apply", liveText(t, tbl), want)
			checkInvariants(t, tbl, "after the rewrite and an apply")
			tbl.Close()
			tbl = open()
			if _, err := tbl.BuildIndex(); err != nil {
				t.Fatal(err)
			}
			tapply(t, tbl, 7, put(row(1, 7)), put(row(13, 7)), del(26))
			sameContents(t, "after a restart", liveText(t, tbl), want)
			checkInvariants(t, tbl, "after a restart")
			tbl.Close()
		})
	}
}

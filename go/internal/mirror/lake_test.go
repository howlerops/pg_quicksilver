package mirror

import (
	"fmt"
	"math/bits"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// What a publisher is handed describes the manifest that was just committed,
// and goes on describing it after the table has moved on: the dead set of a
// later apply must not show through an earlier manifest.
func TestLakeManifestIsACopy(t *testing.T) {
	tbl, err := New(t.TempDir(), "public", "t", "id",
		map[string]string{"id": "bigint", "v": "text"}, []string{"id", "v"})
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	var got []*LakeManifest
	tbl.OnManifest(func(m *LakeManifest) { got = append(got, m) })

	lsn := 0
	apply := func(ch ...changestream.Change) {
		t.Helper()
		lsn += 16
		l := fmt.Sprintf("0/%X", lsn)
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: l, NextLSN: l, Changes: ch}}); err != nil {
			t.Fatal(err)
		}
	}
	row := func(op changestream.Op, id int) changestream.Change {
		k := map[string]any{"id": fmt.Sprint(id)}
		c := changestream.Change{Op: op, Schema: "public", Table: "t", Key: k}
		if op != changestream.OpDelete {
			c.Row = map[string]any{"id": fmt.Sprint(id), "v": "x"}
		}
		return c
	}
	var ins []changestream.Change
	for i := 1; i <= 200; i++ {
		ins = append(ins, row(changestream.OpInsert, i))
	}
	apply(ins...)
	apply(row(changestream.OpDelete, 3), row(changestream.OpDelete, 5))
	afterTwo := got[len(got)-1]
	apply(row(changestream.OpDelete, 7), row(changestream.OpDelete, 9), row(changestream.OpDelete, 11))
	afterFive := got[len(got)-1]

	count := func(m *LakeManifest) (files, dead, bitsSet int) {
		for _, f := range m.Files {
			files++
			dead += f.DeadCount
			for _, w := range f.Dead {
				bitsSet += bits.OnesCount64(w)
			}
		}
		return
	}
	if _, d, b := count(afterTwo); d != 2 || b != 2 {
		t.Fatalf("the manifest after two deletes now describes %d dead rows (%d bits set); it was handed over with 2", d, b)
	}
	if _, d, b := count(afterFive); d != 5 || b != 5 {
		t.Fatalf("the manifest after five deletes describes %d dead rows (%d bits set)", d, b)
	}
	if afterTwo.AppliedLSN == afterFive.AppliedLSN || afterTwo.Files[0].DVGen == afterFive.Files[0].DVGen {
		t.Fatalf("two manifests, one description: %+v / %+v", afterTwo.Files[0].DVGen, afterFive.Files[0].DVGen)
	}
	// And the one a restarted publisher asks for is the current manifest.
	now := tbl.LakeManifest()
	if _, d, _ := count(now); d != 5 || now.AppliedLSN != afterFive.AppliedLSN || len(now.Columns) != 2 {
		t.Fatalf("LakeManifest() = %+v", now)
	}

	// A halt is handed over as a refusal, with nothing to read.
	if err := tbl.Halt("a value the column cannot hold"); err != nil {
		t.Fatal(err)
	}
	if last := got[len(got)-1]; last.Refusal != "a value the column cannot hold" || len(last.Files) != 0 {
		t.Fatalf("after Halt the publisher was handed %+v", last)
	}
}

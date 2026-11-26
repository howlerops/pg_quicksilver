package mirror

import (
	"path/filepath"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// allTableRels is every data file the table's manifest names, by relative path.
func allTableRels(tbl *Table) []string {
	var out []string
	for _, f := range tbl.State.BaseFiles {
		out = append(out, filepath.Join("base", f))
	}
	for _, f := range tbl.State.DeltaFiles {
		out = append(out, filepath.Join("delta", f))
	}
	return out
}

// assertNoNullCompanions reads every data file of tbl, hidden companion
// included, and fails on any row whose member is non-NULL while its companion
// is NULL. A file that carries no companion column at all is not read as a
// NULL companion: it is the sanctioned state once a domain has tripped.
func assertNoNullCompanions(t *testing.T, tbl *Table) (withComp, withoutComp int) {
	t.Helper()
	for _, rel := range allTableRels(tbl) {
		cols, err := tbl.colsOfFile(rel)
		if err != nil {
			t.Fatalf("%s: %v", rel, err)
		}
		for _, member := range tbl.joinColumns() {
			comp := companionName(member)
			if !cols[comp] {
				if cols[member] {
					withoutComp++
				}
				continue
			}
			withComp++
			want := append([]string{}, tbl.Order...)
			want = append(want, comp)
			err := tbl.forEachRowsDead(filepath.Join(tbl.Dir, rel), nil, want, true,
				func(rows []map[string]any, _ int) error {
					for _, r := range rows {
						if r[member] != nil && r[comp] == nil {
							t.Errorf("%s: row %v has %s set and %s NULL", rel, r[tbl.Key], member, comp)
						}
					}
					return nil
				})
			if err != nil {
				t.Fatalf("%s: %v", rel, err)
			}
		}
	}
	return withComp, withoutComp
}

// TestJoinDomainCapMidBatch: the cap trips on the sixth new key of one
// eight-row batch. The batch's file is written without the companion column;
// no file anywhere carries a NULL companion for a non-NULL member.
func TestJoinDomainCapMidBatch(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	jd := domainsFor(t, root, dmSpec, 5)
	cust, _ := openDomainTables(t, root, jd)

	var ops []changestream.Change
	for i := 0; i < 8; i++ {
		ops = append(ops, ciIns(dmUUID(i)))
	}
	applyTxn(t, cust, &lsn, ops...)

	if st := jd.Status(); !st[0].Disabled {
		t.Fatalf("status = %+v, want the cap to have tripped", st)
	}
	_, without := assertNoNullCompanions(t, cust)
	if without == 0 {
		t.Fatal("the batch that tripped the cap wrote its file with a companion column")
	}
	// A batch written after the trip, for a key that was already assigned, is
	// also written with no companion: the domain is off for the whole file.
	before := len(allTableRels(cust))
	applyTxn(t, cust, &lsn, ciIns(dmUUID(0)), ciIns(dmUUID(99)))
	rels := allTableRels(cust)
	if len(rels) != before+1 {
		t.Fatalf("%d files after the second batch, want %d", len(rels), before+1)
	}
	cols, err := cust.colsOfFile(rels[len(rels)-1])
	if err != nil {
		t.Fatal(err)
	}
	if cols[companionName("id")] {
		t.Fatal("a batch written after the domain tripped carries its companion column")
	}
	assertNoNullCompanions(t, cust)
	if completeOf(t, cust, "id") {
		t.Fatal("a table with a companion-less file reports itself complete")
	}
	n := 0
	if err := cust.forEachLive(false, func(map[string]any) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 9 {
		t.Fatalf("%d customers live, want 9", n)
	}
}

// TestJoinDomainCapMidRewrite: a table written before the domain existed is
// attached to it with a cap that trips inside the rewrite that assigns its ids.
// The rewrite is abandoned or finishes without companions; either way no file
// carries a NULL companion, and every row is still live.
func TestJoinDomainCapMidRewrite(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq

	plain, _ := openDomainTables(t, root, nil)
	var ops []changestream.Change
	for i := 0; i < 8; i++ {
		ops = append(ops, ciIns(dmUUID(i)))
	}
	applyTxn(t, plain, &lsn, ops...)

	jd := domainsFor(t, root, dmSpec, 5)
	cust, _ := openDomainTables(t, root, jd)
	_, _ = cust.Compact() // an error here is the trip inside the rewrite
	if st := jd.Status(); !st[0].Disabled {
		t.Fatalf("status = %+v, want the cap to have tripped in the rewrite", st)
	}
	assertNoNullCompanions(t, cust)
	n := 0
	if err := cust.forEachLive(false, func(map[string]any) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Fatalf("%d customers live after the rewrite, want 8", n)
	}
}

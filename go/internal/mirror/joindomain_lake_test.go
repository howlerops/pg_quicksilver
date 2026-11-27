package mirror

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
)

// lakeDeclared is the column list the lake would declare for the table now.
func lakeDeclared(t *testing.T, tbl *Table) []LakeColumn {
	t.Helper()
	m := tbl.describe(&tbl.State, "")
	if m.Err != nil {
		t.Fatal(m.Err)
	}
	return m.Columns
}

// TestJoinDomainLakeDeclaresCompanions: a table whose files all carry the
// companion declares it after the logical columns, as int64.
func TestJoinDomainLakeDeclaresCompanions(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	jd := domainsFor(t, root, dmSpec, 0)
	_, ord := openDomainTables(t, root, jd)
	applyTxn(t, ord, &lsn, oiIns(1, dmUUID(1)), oiIns(2, ""))

	cols := lakeDeclared(t, ord)
	if len(cols) != len(dmOrdOrder)+1 {
		t.Fatalf("declared %d columns, want %d", len(cols), len(dmOrdOrder)+1)
	}
	for i, c := range dmOrdOrder {
		if cols[i].Name != c {
			t.Fatalf("column %d is %s, want %s", i, cols[i].Name, c)
		}
	}
	comp := cols[len(cols)-1]
	if comp.Name != "__qs_d_customer_id" || comp.Type.ID() != arrow.INT64 {
		t.Fatalf("companion declared as %s %v, want __qs_d_customer_id int64", comp.Name, comp.Type)
	}
}

// TestJoinDomainLakeWaitsForCompleteness: a companion is declared only when every
// file that carries its column carries it. Here an added uuid column with a
// non-NULL default: the old files lack it and read back the default, which their
// companion cannot hold, so the domain is incomplete and nothing is declared.
func TestJoinDomainLakeWaitsForCompleteness(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	specs, err := ParseJoinDomains("d2=public.things.ref")
	if err != nil {
		t.Fatal(err)
	}
	jd, err := OpenJoinDomains(root, specs, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer jd.Close()
	things, err := New(root, "public", "things", "id", map[string]string{"id": "bigint", "name": "text"}, []string{"id", "name"})
	if err != nil {
		t.Fatal(err)
	}
	applyTxn(t, things, &lsn, dmChange("insert", "things", map[string]any{"id": "1", "name": "a"}))
	if _, err := things.Compact(); err != nil {
		t.Fatal(err)
	}

	// The column is added with a default; the file written above predates it.
	ref := dmUUID(9)
	if err := things.Evolve(
		map[string]string{"id": "bigint", "name": "text", "ref": "uuid"},
		[]string{"id", "name", "ref"},
		map[string]string{"ref": ref},
	); err != nil {
		t.Fatal(err)
	}
	if err := things.AttachJoinDomains(jd); err != nil {
		t.Fatal(err)
	}
	if things.DomainComplete("ref") {
		t.Fatal("an old file without the column, under a non-NULL default, counted as complete")
	}
	// The companion is declared whenever the domain is configured (the mirror
	// relation keeps one definition on every node), but the manifest does not
	// call the column complete: that is what the sidecar reports.
	m := things.LakeManifest()
	declared := false
	for _, c := range m.Columns {
		if c.Name == "__qs_d_ref" {
			declared = true
		}
	}
	if !declared {
		t.Fatal("a configured companion is not declared to the lake")
	}
	if len(m.JoinComplete) != 0 {
		t.Fatalf("JoinComplete = %v for an incomplete table, want none", m.JoinComplete)
	}
	if len(m.JoinMembers) != 1 || m.JoinMembers[0] != "ref" {
		t.Fatalf("JoinMembers = %v, want [ref]", m.JoinMembers)
	}
}

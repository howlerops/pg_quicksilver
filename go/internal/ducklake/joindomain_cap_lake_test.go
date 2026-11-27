package ducklake

import (
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// declaredCompanion reports whether a manifest declares the companion column.
func declaredCompanion(m *mirror.LakeManifest, col string) bool {
	want := "__qs_d_" + col
	for _, c := range m.Columns {
		if c.Name == want {
			return true
		}
	}
	return false
}

// TestJoinDomainCapTripKeepsCompanionDeclared: once the cap trips, the file
// written without the companion is read as NULL for it, and the companion stays
// declared on every manifest (the view on the primary does not change with
// completeness). The manifest no longer calls the column complete, which is what
// stops the join rewrite on this node.
func TestJoinDomainCapTripKeepsCompanionDeclared(t *testing.T) {
	root := t.TempDir()
	h := jdOpenMax(t, root, true, 5)

	// Four customers: the domain is complete and under the cap.
	h.addCustomers(4)
	h.addOrders(10)
	pub := openLake(t, root, time.Minute, nil)
	h.publish(pub)
	if !declaredCompanion(h.lastCust, "id") {
		t.Fatal("a configured domain does not declare its companion")
	}
	if !containsStr(h.lastCust.JoinComplete, "id") {
		t.Fatalf("JoinComplete = %v for a complete domain, want id", h.lastCust.JoinComplete)
	}

	// Four more: the sixth key trips the cap inside this batch, so the batch's
	// file is written without the companion.
	h.addCustomers(4)
	if st := h.jd.Status(); !st[0].Disabled {
		t.Fatalf("status = %+v, want the cap to have tripped", st)
	}
	if !declaredCompanion(h.lastCust, "id") {
		t.Fatal("the companion stopped being declared when the cap tripped; the view would change with completeness")
	}
	if containsStr(h.lastCust.JoinComplete, "id") {
		t.Fatal("a manifest naming a companion-less file calls the column complete")
	}
	h.publish(pub)

	rd := startReader(t, CatalogPath(root))
	n, err := rd.query(false, "SELECT count(*) FROM lake.public.customers")
	if err != nil || n[0][0] != "8" {
		t.Fatalf("customers through the lake: %v (%v), want 8 rows", n, err)
	}
	nulls, err := rd.query(false, `SELECT count(*) FROM lake.public.customers WHERE "__qs_d_id" IS NULL`)
	if err != nil || nulls[0][0] != "4" {
		t.Fatalf("companion NULLs: %v (%v), want the 4 rows of the companion-less file", nulls, err)
	}
	set, err := rd.query(false, `SELECT count(*) FROM lake.public.customers WHERE "__qs_d_id" IS NOT NULL`)
	if err != nil || set[0][0] != "4" {
		t.Fatalf("companions set: %v (%v), want 4", set, err)
	}
}

func containsStr(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

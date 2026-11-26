package ducklake

import (
	"strings"
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

// TestJoinDomainCapTripPublishesNoCompanion: the first manifest that names a
// file written after the cap tripped does not declare the companion, and DuckDB
// reading the companion through the lake fails with a binder error. It must not
// read NULLs for it.
func TestJoinDomainCapTripPublishesNoCompanion(t *testing.T) {
	root := t.TempDir()
	h := jdOpenMax(t, root, true, 5)

	// Four customers: the domain is complete and under the cap.
	h.addCustomers(4)
	h.addOrders(10)
	pub := openLake(t, root, time.Minute, nil)
	h.publish(pub)
	if !declaredCompanion(h.lastCust, "id") {
		t.Fatal("a complete, enabled domain does not declare its companion")
	}

	// Four more: the sixth key trips the cap inside this batch, so the batch's
	// file is written without the companion.
	h.addCustomers(4)
	if st := h.jd.Status(); !st[0].Disabled {
		t.Fatalf("status = %+v, want the cap to have tripped", st)
	}
	if declaredCompanion(h.lastCust, "id") {
		t.Fatal("the manifest that first names a companion-less file declares the companion")
	}
	h.publish(pub)

	rd := startReader(t, CatalogPath(root))
	n, err := rd.query(false, "SELECT count(*) FROM lake.public.customers")
	if err != nil || n[0][0] != "8" {
		t.Fatalf("customers through the lake: %v (%v), want 8 rows", n, err)
	}
	_, err = rd.query(false, `SELECT count(*) FROM lake.public.customers WHERE "__qs_d_id" IS NULL`)
	if err == nil {
		t.Fatal("the lake answers a read of an undeclared companion; it must be a binder error, not NULLs")
	}
	if !strings.Contains(err.Error(), "__qs_d_id") {
		t.Fatalf("unexpected error: %v", err)
	}
}

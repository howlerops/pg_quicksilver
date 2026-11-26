package ducklake

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const jdSpec = "cust=public.customers.id,public.orders.customer_id"

// jdTables is the customers and orders pair of a join domain, each with its
// manifest captured for the publisher.
type jdTables struct {
	t          *testing.T
	root       string
	jd         *mirror.JoinDomains
	cust, ord  *mirror.Table
	lastCust   *mirror.LakeManifest
	lastOrd    *mirror.LakeManifest
	rng        *rand.Rand
	lsn        int
	custIDs    []string
	orderCount int
}

func jdOpen(t *testing.T, root string, withDomain bool) *jdTables {
	t.Helper()
	return jdOpenMax(t, root, withDomain, 0)
}

// jdOpenMax is jdOpen with a per-domain key cap (<= 0 means none).
func jdOpenMax(t *testing.T, root string, withDomain bool, maxKeys int64) *jdTables {
	t.Helper()
	h := &jdTables{t: t, root: root, rng: rand.New(rand.NewSource(11))}
	if withDomain {
		specs, err := mirror.ParseJoinDomains(jdSpec)
		if err != nil {
			t.Fatal(err)
		}
		h.jd, err = mirror.OpenJoinDomains(root, specs, maxKeys)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.jd.Close() })
	}
	var err error
	h.cust, err = mirror.New(root, "public", "customers", "id",
		map[string]string{"id": "uuid", "name": "text"}, []string{"id", "name"})
	if err != nil {
		t.Fatal(err)
	}
	h.ord, err = mirror.New(root, "public", "orders", "id",
		map[string]string{"id": "bigint", "customer_id": "uuid", "amount": "bigint"},
		[]string{"id", "customer_id", "amount"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.cust.AttachJoinDomains(h.jd); err != nil {
		t.Fatal(err)
	}
	if err := h.ord.AttachJoinDomains(h.jd); err != nil {
		t.Fatal(err)
	}
	h.cust.OnManifest(func(m *mirror.LakeManifest) { h.lastCust = m })
	h.ord.OnManifest(func(m *mirror.LakeManifest) { h.lastOrd = m })
	return h
}

func (h *jdTables) apply(tbl *mirror.Table, table string, changes []changestream.Change) {
	h.t.Helper()
	h.lsn++
	lsn := fmt.Sprintf("0/%X", h.lsn*16)
	for i := range changes {
		changes[i].Schema, changes[i].Table, changes[i].LSN = "public", table, lsn
	}
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: lsn, NextLSN: lsn, Changes: changes}}); err != nil {
		h.t.Fatalf("apply: %v", err)
	}
}

func (h *jdTables) uuid() string {
	var b [16]byte
	h.rng.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (h *jdTables) addCustomers(n int) {
	var ch []changestream.Change
	for i := 0; i < n; i++ {
		id := h.uuid()
		h.custIDs = append(h.custIDs, id)
		ch = append(ch, changestream.Change{Op: changestream.OpInsert, Row: map[string]any{"id": id, "name": "c"}})
	}
	h.apply(h.cust, "customers", ch)
}

// addOrders adds n orders, every fifth with a NULL member.
func (h *jdTables) addOrders(n int) {
	var ch []changestream.Change
	for i := 0; i < n; i++ {
		h.orderCount++
		var cid any
		if h.orderCount%5 != 0 {
			cid = h.custIDs[h.rng.Intn(len(h.custIDs))]
		}
		ch = append(ch, changestream.Change{Op: changestream.OpInsert, Row: map[string]any{
			"id": fmt.Sprint(h.orderCount), "customer_id": cid, "amount": fmt.Sprint(h.orderCount % 97),
		}})
	}
	h.apply(h.ord, "orders", ch)
}

// publish hands both tables' current manifests to the lake and fails on refusal.
func (h *jdTables) publish(pub *Publisher) {
	h.t.Helper()
	for _, m := range []*mirror.LakeManifest{h.lastCust, h.lastOrd} {
		res, err := pub.Publish(context.Background(), m)
		if err != nil || res.Refused != "" {
			h.t.Fatalf("publish %s.%s: %+v, %v", m.Schema, m.Table, res, err)
		}
	}
}

// joinSums is the join on the uuid and on the companion, both read by DuckDB
// through the lake: they must agree, and both must match the plain count.
func joinSums(t *testing.T, rd *reader) (string, string) {
	t.Helper()
	uuidJoin, err := rd.query(false, "SELECT count(*), sum(o.amount) FROM lake.public.orders o JOIN lake.public.customers c ON o.customer_id = c.id")
	if err != nil {
		t.Fatalf("join on uuid: %v", err)
	}
	compJoin, err := rd.query(false, `SELECT count(*), sum(o.amount) FROM lake.public.orders o JOIN lake.public.customers c ON o."__qs_d_customer_id" = c."__qs_d_id"`)
	if err != nil {
		t.Fatalf("join on companion: %v", err)
	}
	return fmt.Sprint(uuidJoin), fmt.Sprint(compJoin)
}

// TestJoinDomainCompanionsReadThroughDuckLake: a complete domain publishes its
// companions, and DuckDB joins on them to the same answer as on the uuids.
func TestJoinDomainCompanionsReadThroughDuckLake(t *testing.T) {
	root := t.TempDir()
	h := jdOpen(t, root, true)
	h.addCustomers(200)
	h.addOrders(1500)
	if _, err := h.cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ord.Compact(); err != nil {
		t.Fatal(err)
	}
	pub := openLake(t, root, time.Minute, nil)
	h.publish(pub)

	rd := startReader(t, CatalogPath(root))
	uj, cj := joinSums(t, rd)
	if uj != cj {
		t.Fatalf("join on uuid %s, join on companion %s", uj, cj)
	}
	got, err := rd.query(false, `SELECT count(*) FROM lake.public.orders WHERE (customer_id IS NULL) <> ("__qs_d_customer_id" IS NULL)`)
	if err != nil || got[0][0] != "0" {
		t.Fatalf("rows where the member and its companion disagree on NULL: %v (%v)", got, err)
	}
	t.Logf("join on uuid and on companion agree: %s", uj)
}

// TestJoinDomainIncompleteStillReads: a table whose new delta carries the
// companion while an older base does not is incomplete. The lake must still
// publish and read it correctly, and must not declare the companion until a
// rewrite makes the domain complete.
func TestJoinDomainIncompleteStillReads(t *testing.T) {
	root := t.TempDir()

	// Written with the domain off.
	off := jdOpen(t, root, false)
	off.addCustomers(100)
	off.addOrders(400)
	if _, err := off.cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := off.ord.Compact(); err != nil {
		t.Fatal(err)
	}

	// Switched on: a new delta, with companions, over the base without them.
	on := jdOpen(t, root, true)
	// The customer ids the orders refer to must be the ones already in the
	// mirror, so the id list is rebuilt from the table.
	rows, err := on.cust.Live()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		on.custIDs = append(on.custIDs, mirror.Render(r["id"]))
	}
	on.orderCount = 400
	// One customer in the new delta, so that the customers table also has a
	// manifest from this run; it stays incomplete like the orders.
	on.addCustomers(1)
	on.addOrders(300)
	pub := openLake(t, root, time.Minute, nil)
	on.publish(pub)

	if on.ord.DomainComplete("customer_id") {
		t.Fatal("a base without the companion counts as complete")
	}
	rd := startReader(t, CatalogPath(root))
	if _, err := rd.query(false, "SELECT count(*) FROM lake.public.orders"); err != nil {
		t.Fatalf("an incomplete domain broke the read: %v", err)
	}
	cnt, err := rd.query(false, "SELECT count(*), sum(amount) FROM lake.public.orders")
	if err != nil || cnt[0][0] != "700" {
		t.Fatalf("lake orders: %v (%v), want 700 rows", cnt, err)
	}
	if _, err := rd.query(false, `SELECT count(*) FROM lake.public.orders WHERE "__qs_d_customer_id" IS NOT NULL`); err == nil {
		t.Fatal("the lake exposes a companion that is not declared while incomplete")
	} else if !strings.Contains(err.Error(), "__qs_d_customer_id") {
		t.Fatalf("unexpected error reading the undeclared companion: %v", err)
	}

	// A rewrite completes the domain, and the companion is then declared and read.
	if _, err := on.cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := on.ord.Compact(); err != nil {
		t.Fatal(err)
	}
	if !on.ord.DomainComplete("customer_id") {
		t.Fatal("not complete after the rewrite")
	}
	on.publish(pub)
	uj, cj := joinSums(t, rd)
	if uj != cj {
		t.Fatalf("after the rewrite: join on uuid %s, join on companion %s", uj, cj)
	}
}

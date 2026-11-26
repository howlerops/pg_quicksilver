package mirror

// The join domain model test. Two tables, customers(id uuid) and
// orders(customer_id uuid), share one domain. A seeded random workload drives
// both through Apply with inserts, full and partial updates (including moving a
// member to another value and to NULL), deletes, merges, synchronous and
// background rewrites, restarts and TRUNCATE. After EVERY step it reads the
// live rows straight from the Parquet files, hidden companions included, and
// asserts:
//
//   - the logical rows are what the model says they are;
//   - each member's companion is the dictionary id of its value, or NULL with it;
//   - across both tables, two companions are equal if and only if the values are.
//
// The same workload with the domain off must give the same logical checksums at
// every step, and must write no companion column into any file.

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

const (
	dmSpec    = "cust=public.customers.id,public.orders.customer_id"
	dmPoolLen = 120
)

var (
	dmCustCols  = map[string]string{"id": "uuid", "name": "text"}
	dmCustOrder = []string{"id", "name"}
	dmOrdCols   = map[string]string{"id": "bigint", "customer_id": "uuid", "amount": "bigint"}
	dmOrdOrder  = []string{"id", "customer_id", "amount"}
)

// dmModel is what the source holds: each change applied in order.
type dmModel struct {
	cust map[string]string // customer id -> name
	ord  map[string]dmOrd  // order id -> row
}

type dmOrd struct {
	cid string // "" is NULL
	amt string
}

func newDMModel() *dmModel {
	return &dmModel{cust: map[string]string{}, ord: map[string]dmOrd{}}
}

// dmHarness owns the tables and the domain, and can restart them.
type dmHarness struct {
	t    *testing.T
	root string
	on   bool
	jd   *JoinDomains
	cust *Table
	ord  *Table
	lsn  int
}

func (h *dmHarness) open() {
	h.t.Helper()
	if h.on {
		specs, err := ParseJoinDomains(dmSpec)
		if err != nil {
			h.t.Fatal(err)
		}
		h.jd, err = OpenJoinDomains(h.root, specs, 0)
		if err != nil {
			h.t.Fatal(err)
		}
	}
	var err error
	h.cust, err = New(h.root, "public", "customers", "id", dmCustCols, dmCustOrder)
	if err != nil {
		h.t.Fatal(err)
	}
	h.ord, err = New(h.root, "public", "orders", "id", dmOrdCols, dmOrdOrder)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.cust.AttachJoinDomains(h.jd); err != nil {
		h.t.Fatal(err)
	}
	if err := h.ord.AttachJoinDomains(h.jd); err != nil {
		h.t.Fatal(err)
	}
}

func (h *dmHarness) close() {
	if h.jd != nil {
		_ = h.jd.Close()
		h.jd = nil
	}
}

func (h *dmHarness) restart() {
	h.close()
	h.open()
}

// dmDelete is a delete: the identity goes in Key, as pgoutput sends it.
func dmDelete(table, col, id string) changestream.Change {
	return changestream.Change{Op: changestream.OpDelete, Schema: "public", Table: table, Key: map[string]any{col: id}}
}

// dmUUID is the i'th pool uuid, lowercase and canonical.
func dmUUID(i int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", i+1, i+1)
}

func dmChange(op changestream.Op, table string, row map[string]any) changestream.Change {
	return changestream.Change{Op: op, Schema: "public", Table: table, Row: row}
}

func dmStr(v string, null bool) any {
	if null {
		return nil
	}
	return v
}

// dmWorkload applies one random step to both the model and the tables.
func dmWorkload(h *dmHarness, m *dmModel, rng *rand.Rand) {
	pick := func() string { return dmUUID(rng.Intn(dmPoolLen)) }
	ordIDs := func() []string {
		out := make([]string, 0, len(m.ord))
		for k := range m.ord {
			out = append(out, k)
		}
		sortStrings(out)
		return out
	}
	custIDs := func() []string {
		out := make([]string, 0, len(m.cust))
		for k := range m.cust {
			out = append(out, k)
		}
		sortStrings(out)
		return out
	}

	var changes []changestream.Change
	n := 1 + rng.Intn(3)
	for k := 0; k < n; k++ {
		switch r := rng.Intn(100); {
		case r < 14: // new customer
			id := pick()
			if _, ok := m.cust[id]; ok {
				continue
			}
			name := fmt.Sprintf("c%d", rng.Intn(1000))
			m.cust[id] = name
			changes = append(changes, dmChange(changestream.OpInsert, "customers", map[string]any{"id": id, "name": name}))
		case r < 22: // customer update, full row
			ids := custIDs()
			if len(ids) == 0 {
				continue
			}
			id := ids[rng.Intn(len(ids))]
			name := fmt.Sprintf("n%d", rng.Intn(1000))
			m.cust[id] = name
			changes = append(changes, dmChange(changestream.OpUpdate, "customers", map[string]any{"id": id, "name": name}))
		case r < 25: // customer delete
			ids := custIDs()
			if len(ids) == 0 {
				continue
			}
			id := ids[rng.Intn(len(ids))]
			delete(m.cust, id)
			changes = append(changes, dmDelete("customers", "id", id))
		case r < 45: // new order, member possibly NULL
			oid := strconv.Itoa(1 + rng.Intn(400))
			if _, ok := m.ord[oid]; ok {
				continue
			}
			cid := ""
			if rng.Intn(6) != 0 {
				cid = pick()
			}
			amt := strconv.Itoa(rng.Intn(1000))
			m.ord[oid] = dmOrd{cid: cid, amt: amt}
			changes = append(changes, dmChange(changestream.OpInsert, "orders", map[string]any{
				"id": oid, "customer_id": dmStr(cid, cid == ""), "amount": amt,
			}))
		case r < 60: // order update, full row, member may change or go NULL
			ids := ordIDs()
			if len(ids) == 0 {
				continue
			}
			oid := ids[rng.Intn(len(ids))]
			cid := ""
			if rng.Intn(5) != 0 {
				cid = pick()
			}
			amt := strconv.Itoa(rng.Intn(1000))
			m.ord[oid] = dmOrd{cid: cid, amt: amt}
			changes = append(changes, dmChange(changestream.OpUpdate, "orders", map[string]any{
				"id": oid, "customer_id": dmStr(cid, cid == ""), "amount": amt,
			}))
		case r < 72: // order update, PARTIAL: the member alone, as a patch
			ids := ordIDs()
			if len(ids) == 0 {
				continue
			}
			oid := ids[rng.Intn(len(ids))]
			o := m.ord[oid]
			cid := ""
			if rng.Intn(4) != 0 {
				cid = pick()
			}
			o.cid = cid
			m.ord[oid] = o
			changes = append(changes, dmChange(changestream.OpUpdate, "orders", map[string]any{
				"id": oid, "customer_id": dmStr(cid, cid == ""),
			}))
		case r < 80: // order update, PARTIAL: the amount alone
			ids := ordIDs()
			if len(ids) == 0 {
				continue
			}
			oid := ids[rng.Intn(len(ids))]
			amt := strconv.Itoa(rng.Intn(1000))
			o := m.ord[oid]
			o.amt = amt
			m.ord[oid] = o
			changes = append(changes, dmChange(changestream.OpUpdate, "orders", map[string]any{"id": oid, "amount": amt}))
		case r < 90: // order delete
			ids := ordIDs()
			if len(ids) == 0 {
				continue
			}
			oid := ids[rng.Intn(len(ids))]
			delete(m.ord, oid)
			changes = append(changes, dmDelete("orders", "id", oid))
		default:
			// nothing this slot
		}
	}
	if len(changes) > 0 {
		h.txnFor(changes)
	}
}

// txnFor applies changes, each routed to its own table, in order.
func (h *dmHarness) txnFor(changes []changestream.Change) {
	h.t.Helper()
	h.lsn++
	lsn := fmt.Sprintf("0/%X", h.lsn*16)
	for i := range changes {
		changes[i].LSN = lsn
		tbl := h.cust
		if changes[i].Table == "orders" {
			tbl = h.ord
		}
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: lsn, NextLSN: lsn, Changes: []changestream.Change{changes[i]}}}); err != nil {
			h.t.Fatalf("apply %s: %v", changes[i].Op, err)
		}
	}
}

func sortStrings(xs []string) {
	for i := 1; i < len(xs); i++ {
		for j := i; j > 0 && xs[j] < xs[j-1]; j-- {
			xs[j], xs[j-1] = xs[j-1], xs[j]
		}
	}
}

// dmCheck asserts the model against what the files say. It returns the logical
// checksums of both tables for the caller to compare across runs.
func dmCheck(t *testing.T, h *dmHarness, m *dmModel, when string) (uint64, uint64) {
	t.Helper()
	jd := h.jd
	dictOf := func(col string) *joinDict {
		if jd == nil {
			return nil
		}
		for _, s := range jd.Specs() {
			for _, mem := range s.Members {
				if mem.Column == col {
					return jd.dicts[s.Name]
				}
			}
		}
		return nil
	}
	// companion -> value and value -> companion, across both tables.
	compToVal := map[string]string{}
	valToComp := map[string]string{}
	noteRel := func(comp any, val string) {
		cs := fmt.Sprint(comp)
		if prev, ok := compToVal[cs]; ok && prev != val {
			t.Fatalf("%s: companion %s is both %s and %s", when, cs, prev, val)
		}
		if prev, ok := valToComp[val]; ok && prev != cs {
			t.Fatalf("%s: value %s has companions %s and %s", when, val, prev, cs)
		}
		compToVal[cs] = val
		valToComp[val] = cs
	}

	seenCust := map[string]string{}
	if err := h.cust.forEachLive(true, func(r map[string]any) error {
		id := Render(r["id"])
		seenCust[id] = Render(r["name"])
		return nil
	}); err != nil {
		t.Fatalf("%s: customers: %v", when, err)
	}
	if len(seenCust) != len(m.cust) {
		t.Fatalf("%s: customers: %d live, model has %d", when, len(seenCust), len(m.cust))
	}
	for id, name := range m.cust {
		if seenCust[id] != name {
			t.Fatalf("%s: customer %s is %q, model %q", when, id, seenCust[id], name)
		}
	}
	if err := h.cust.forEachLive(true, func(r map[string]any) error {
		id := Render(r["id"])
		if d := dictOf("id"); d != nil {
			key, _ := parseUUIDValue(id)
			want, ok := d.Lookup([joinDictRecordSize]byte(key))
			if !ok {
				return fmt.Errorf("customer %s has no dictionary id", id)
			}
			if got := r[companionName("id")]; got != want {
				return fmt.Errorf("customer %s companion %v, dictionary %d", id, got, want)
			}
			noteRel(want, id)
		} else if _, has := r[companionName("id")]; has {
			return fmt.Errorf("feature off: companion present")
		}
		return nil
	}); err != nil {
		t.Fatalf("%s: %v", when, err)
	}

	seenOrd := map[string]dmOrd{}
	if err := h.ord.forEachLive(true, func(r map[string]any) error {
		o := dmOrd{amt: Render(r["amount"])}
		if r["customer_id"] != nil {
			o.cid = Render(r["customer_id"])
		}
		seenOrd[Render(r["id"])] = o
		return nil
	}); err != nil {
		t.Fatalf("%s: orders: %v", when, err)
	}
	if len(seenOrd) != len(m.ord) {
		t.Fatalf("%s: orders: %d live, model has %d", when, len(seenOrd), len(m.ord))
	}
	for id, o := range m.ord {
		got := seenOrd[id]
		if got.amt != o.amt || got.cid != o.cid {
			t.Fatalf("%s: order %s is %+v, model %+v", when, id, got, o)
		}
	}
	if err := h.ord.forEachLive(true, func(r map[string]any) error {
		comp, has := r[companionName("customer_id")]
		d := dictOf("customer_id")
		if d == nil {
			if has {
				return fmt.Errorf("feature off: companion present")
			}
			return nil
		}
		if r["customer_id"] == nil {
			if comp != nil {
				return fmt.Errorf("order %s: NULL member but companion %v", Render(r["id"]), comp)
			}
			return nil
		}
		v := Render(r["customer_id"])
		key, _ := parseUUIDValue(v)
		want, ok := d.Lookup([joinDictRecordSize]byte(key))
		if !ok {
			return fmt.Errorf("order %s member %s has no dictionary id", Render(r["id"]), v)
		}
		if comp != want {
			return fmt.Errorf("order %s companion %v, dictionary %d for %s", Render(r["id"]), comp, want, v)
		}
		noteRel(want, v)
		return nil
	}); err != nil {
		t.Fatalf("%s: %v", when, err)
	}

	ck1 := dmChecksum(t, h.cust, dmCustOrder)
	ck2 := dmChecksum(t, h.ord, dmOrdOrder)
	return ck1, ck2
}

func dmRows(t *testing.T, tbl *Table) []map[string]any {
	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func dmChecksum(t *testing.T, tbl *Table, order []string) uint64 {
	_, sum := Checksum(dmRows(t, tbl), order)
	return sum
}

// dmNoCompanionOnDisk asserts no file of the table carries a companion column.
func dmNoCompanionOnDisk(t *testing.T, tbl *Table) {
	t.Helper()
	for _, rel := range append(append([]string{}, tbl.State.BaseFiles...), tbl.State.DeltaFiles...) {
		_, order, err := schemaOf(filepath.Join(tbl.Dir, rel))
		if err != nil {
			continue
		}
		for _, c := range order {
			if isCompanion(c) {
				t.Fatalf("feature off: %s carries %s", rel, c)
			}
		}
	}
}

func dmSteps() int {
	if testing.Short() {
		return 250
	}
	return 2000
}

// dmRun drives one workload and returns the logical checksums after each step.
func dmRun(t *testing.T, on bool, seed int64) []uint64 {
	t.Helper()
	root := t.TempDir()
	h := &dmHarness{t: t, root: root, on: on}
	h.open()
	defer h.close()
	m := newDMModel()
	rng := rand.New(rand.NewSource(seed))
	var sums []uint64
	steps := dmSteps()
	var patchFilesSeen, compactingSeen, restarts, began int
	defer func() {
		if on {
			t.Logf("domain on: %d steps; rewrites begun %d; steps with a rewrite in flight %d; max patch files named at once %d; restarts %d",
				steps, began, compactingSeen, patchFilesSeen, restarts)
		}
	}()
	for step := 0; step < steps; step++ {
		what := "workload"
		switch r := rng.Intn(1000); {
		case r < 8:
			what = "merge"
			if _, err := h.ord.MergeDeltas(); err != nil {
				t.Fatalf("step %d merge: %v", step, err)
			}
			if _, err := h.cust.MergeDeltas(); err != nil {
				t.Fatalf("step %d merge: %v", step, err)
			}
		case r < 14:
			what = "compact"
			if _, err := h.ord.Compact(); err != nil {
				t.Fatalf("step %d compact: %v", step, err)
			}
			if _, err := h.cust.Compact(); err != nil {
				t.Fatalf("step %d compact: %v", step, err)
			}
		case r < 30:
			what = "restart"
			restarts++
			// A rewrite in flight belongs to the tables being discarded: finish
			// it, as the apply loop would, before they go.
			for _, tbl := range []*Table{h.cust, h.ord} {
				if tbl.Compacting() {
					if _, err := tbl.FinishCompaction(); err != nil {
						t.Fatalf("step %d finish before restart: %v", step, err)
					}
				}
			}
			h.restart()
		case r < 32:
			what = "truncate orders"
			h.txnFor([]changestream.Change{{Op: changestream.OpTruncate, Schema: "public", Table: "orders"}})
			m.ord = map[string]dmOrd{}
		case r < 60:
			// Start a background rewrite; the workload below applies while it runs.
			what = "begin rewrite"
			for _, tbl := range []*Table{h.cust, h.ord} {
				if !tbl.Compacting() && tbl.BeginCompaction() != nil {
					began++
				}
			}
		case r < 90:
			what = "finish rewrite"
			for _, tbl := range []*Table{h.cust, h.ord} {
				if tbl.Compacting() {
					if _, err := tbl.FinishCompaction(); err != nil {
						t.Fatalf("step %d finish: %v", step, err)
					}
				}
			}
		default:
			dmWorkload(h, m, rng)
		}
		c1, c2 := dmCheck(t, h, m, fmt.Sprintf("step %d (%s)", step, what))
		sums = append(sums, c1, c2)
		if n := len(h.ord.State.PartialCols); n > patchFilesSeen {
			patchFilesSeen = n
		}
		if h.ord.Compacting() {
			compactingSeen++
		}

		if !on {
			dmNoCompanionOnDisk(t, h.cust)
			dmNoCompanionOnDisk(t, h.ord)
		}
	}
	return sums
}

func TestJoinDomainModel(t *testing.T) {
	const seed = 20260723
	on := dmRun(t, true, seed)
	off := dmRun(t, false, seed)
	if len(on) != len(off) {
		t.Fatalf("%d checksums with the domain, %d without", len(on), len(off))
	}
	for i := range on {
		if on[i] != off[i] {
			t.Fatalf("logical checksum %d differs with the domain on (%x) and off (%x)", i, on[i], off[i])
		}
	}
	t.Logf("%d steps: logical checksums identical with the domain on and off", len(on)/2)
}

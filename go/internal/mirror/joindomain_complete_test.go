package mirror

import (
	"fmt"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// lsnSeq hands out increasing commit LSNs for a test's transactions.
type lsnSeq int

func (s *lsnSeq) next() string {
	*s++
	return fmt.Sprintf("0/%X", int(*s)*16)
}

// applyTxn applies one transaction of changes to tbl.
func applyTxn(t *testing.T, tbl *Table, lsn *lsnSeq, ops ...changestream.Change) {
	t.Helper()
	l := lsn.next()
	for i := range ops {
		ops[i].LSN = l
	}
	if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: l, NextLSN: l, Changes: ops}}); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func ciIns(id string) changestream.Change {
	return dmChange(changestream.OpInsert, "customers", map[string]any{"id": id, "name": "n" + id[:4]})
}

func oiIns(id int, cid string) changestream.Change {
	var c any
	if cid != "" {
		c = cid
	}
	return dmChange(changestream.OpInsert, "orders", map[string]any{
		"id": fmt.Sprint(id), "customer_id": c, "amount": fmt.Sprint(id),
	})
}

// openDomainTables opens the two model tables, attaching jd when it is not nil.
func openDomainTables(t *testing.T, root string, jd *JoinDomains) (*Table, *Table) {
	t.Helper()
	cust, err := New(root, "public", "customers", "id", dmCustCols, dmCustOrder)
	if err != nil {
		t.Fatal(err)
	}
	ord, err := New(root, "public", "orders", "id", dmOrdCols, dmOrdOrder)
	if err != nil {
		t.Fatal(err)
	}
	if err := cust.AttachJoinDomains(jd); err != nil {
		t.Fatal(err)
	}
	if err := ord.AttachJoinDomains(jd); err != nil {
		t.Fatal(err)
	}
	return cust, ord
}

func domainsFor(t *testing.T, root, spec string, maxKeys int64) *JoinDomains {
	t.Helper()
	specs, err := ParseJoinDomains(spec)
	if err != nil {
		t.Fatal(err)
	}
	jd, err := OpenJoinDomains(root, specs, maxKeys)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = jd.Close() })
	return jd
}

func completeOf(t *testing.T, tbl *Table, col string) bool {
	t.Helper()
	for _, dc := range tbl.DomainColumns() {
		if dc.Column == col {
			return dc.Complete
		}
	}
	t.Fatalf("%s: no domain column %s", tbl.Qualified, col)
	return false
}

// TestJoinDomainCompletenessTransitions: a mirror written without the domain,
// the domain switched on, a base rewrite that brings every file up to date.
func TestJoinDomainCompletenessTransitions(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq

	// The feature is off: the mirror is written as it always was.
	cust, ord := openDomainTables(t, root, nil)
	var ops []changestream.Change
	ids := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		ids = append(ids, dmUUID(i))
		ops = append(ops, ciIns(dmUUID(i)))
	}
	applyTxn(t, cust, &lsn, ops...)
	var oops []changestream.Change
	for i := 0; i < 200; i++ {
		oops = append(oops, oiIns(i+1, ids[i%len(ids)]))
	}
	applyTxn(t, ord, &lsn, oops...)
	if _, err := cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}
	dmNoCompanionOnDisk(t, cust)
	dmNoCompanionOnDisk(t, ord)

	// Switch the domain on for the existing mirror.
	jd := domainsFor(t, root, dmSpec, 0)
	cust, ord = openDomainTables(t, root, jd)
	if completeOf(t, cust, "id") || completeOf(t, ord, "customer_id") {
		t.Fatal("a mirror whose files predate the domain reports itself complete")
	}

	// A delta written with the domain on carries companions, but the base does
	// not, so the table is still incomplete.
	applyTxn(t, ord, &lsn, oiIns(1001, ids[0]))
	if completeOf(t, ord, "customer_id") {
		t.Fatal("an old base without the companion still counts as complete")
	}

	// A base rewrite with no older deltas: every file now carries the companion.
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if len(ord.State.DeltaFiles) != 0 {
		t.Fatalf("%d delta files after a rewrite, want none", len(ord.State.DeltaFiles))
	}
	if !completeOf(t, ord, "customer_id") || !completeOf(t, cust, "id") {
		t.Fatal("tables are not complete after a base rewrite with no older deltas")
	}

	// And they stay complete as new deltas arrive.
	applyTxn(t, cust, &lsn, ciIns(dmUUID(500)))
	if !completeOf(t, cust, "id") {
		t.Fatal("a new delta with the companion made the table incomplete")
	}
	dc := cust.DomainColumns()
	if len(dc) != 1 || dc[0].Domain != "cust" || dc[0].Companion != "__qs_d_id" || !dc[0].Complete {
		t.Fatalf("DomainColumns = %+v", dc)
	}
}

// TestJoinDomainCapIsForever: a domain past its cap is disabled, every table
// reports it incomplete, and a rewrite and a restart do not change that. The
// write itself never fails.
func TestJoinDomainCapIsForever(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	jd := domainsFor(t, root, dmSpec, 5)
	cust, ord := openDomainTables(t, root, jd)

	var ops []changestream.Change
	for i := 0; i < 12; i++ {
		ops = append(ops, ciIns(dmUUID(i)))
	}
	applyTxn(t, cust, &lsn, ops...)
	var oops []changestream.Change
	for i := 0; i < 20; i++ {
		oops = append(oops, oiIns(i+1, dmUUID(i%12)))
	}
	applyTxn(t, ord, &lsn, oops...) // a shared domain: this table's values count too

	st := jd.Status()
	if len(st) != 1 || !st[0].Disabled || st[0].Keys != 5 {
		t.Fatalf("status after the cap = %+v, want disabled with 5 keys", st)
	}
	if completeOf(t, cust, "id") || completeOf(t, ord, "customer_id") {
		t.Fatal("a disabled domain reports itself complete")
	}

	// Every row is still there, and a row whose value never got an id has a
	// NULL companion rather than a wrong one.
	n := 0
	if err := cust.forEachLive(true, func(r map[string]any) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n != 12 {
		t.Fatalf("%d customers live, want 12", n)
	}

	if _, err := cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}
	if completeOf(t, cust, "id") || completeOf(t, ord, "customer_id") {
		t.Fatal("a rewrite made a disabled domain complete")
	}

	// A restart with a larger cap does not re-enable it.
	_ = jd.Close()
	jd2 := domainsFor(t, root, dmSpec, 0)
	cust, ord = openDomainTables(t, root, jd2)
	if completeOf(t, cust, "id") || completeOf(t, ord, "customer_id") {
		t.Fatal("a restart re-enabled a disabled domain")
	}
	if st := jd2.Status(); !st[0].Disabled || st[0].Reason == "" {
		t.Fatalf("status after restart = %+v", st)
	}
}

// TestJoinDomainOffAgainAndOn: a mirror with companions read with the domain off
// (the column is ignored), written while off (and so incomplete when on again).
func TestJoinDomainOffAgainAndOn(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq

	jd := domainsFor(t, root, dmSpec, 0)
	cust, ord := openDomainTables(t, root, jd)
	var ops []changestream.Change
	for i := 0; i < 30; i++ {
		ops = append(ops, ciIns(dmUUID(i)))
	}
	applyTxn(t, cust, &lsn, ops...)
	if _, err := cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}
	_ = jd.Close()

	// Off: the companion is on disk and invisible.
	cust, ord = openDomainTables(t, root, nil)
	if len(cust.DomainColumns()) != 0 {
		t.Fatal("feature off: DomainColumns not empty")
	}
	rows, err := cust.Live()
	if err != nil {
		t.Fatalf("Live with companion files and the domain off: %v", err)
	}
	if len(rows) != 30 {
		t.Fatalf("%d rows live, want 30", len(rows))
	}
	for _, r := range rows {
		for k := range r {
			if isCompanion(k) {
				t.Fatalf("Live exposed %s with the domain off", k)
			}
		}
	}
	cols, order, err := ColumnsFromFiles(root, "public", "customers")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != len(dmCustCols) || len(order) != len(dmCustOrder) {
		t.Fatalf("ColumnsFromFiles = %v %v: companions are not the table's columns", cols, order)
	}
	for _, c := range order {
		if isCompanion(c) {
			t.Fatalf("ColumnsFromFiles lists %s", c)
		}
	}

	// A delta written while off has no companion.
	applyTxn(t, cust, &lsn, ciIns(dmUUID(900)))

	// On again: the delta written while off makes the table incomplete.
	jd2 := domainsFor(t, root, dmSpec, 0)
	cust, _ = openDomainTables(t, root, jd2)
	if completeOf(t, cust, "id") {
		t.Fatal("a delta written with the domain off counts as complete")
	}
	// Reads are still correct.
	rows, err = cust.Live()
	if err != nil || len(rows) != 31 {
		t.Fatalf("Live after switching back on: %d rows, %v", len(rows), err)
	}
}

// TestJoinDomainConfigErrors: the configuration is refused at start when it is
// malformed, and a member that is not uuid is refused when the table attaches.
func TestJoinDomainConfigErrors(t *testing.T) {
	for _, bad := range []string{
		"cust",
		"Cust=public.customers.id",
		"cust=public.customers",
		"cust=customers.id",
		"cust=public.customers.id;cust=public.orders.customer_id",
		"a=public.customers.id;b=public.customers.id",
		"cust=",
	} {
		if _, err := ParseJoinDomains(bad); err == nil {
			t.Errorf("%q parsed without error", bad)
		}
	}
	if specs, err := ParseJoinDomains(""); err != nil || specs != nil {
		t.Fatalf("empty env: %v %v, want no domains and no error", specs, err)
	}

	root := t.TempDir()
	specs, err := ParseJoinDomains("bad=public.orders.amount")
	if err != nil {
		t.Fatal(err)
	}
	jd, err := OpenJoinDomains(root, specs, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer jd.Close()
	ord, err := New(root, "public", "orders", "id", dmOrdCols, dmOrdOrder)
	if err != nil {
		t.Fatal(err)
	}
	if err := ord.AttachJoinDomains(jd); err == nil {
		t.Fatal("a bigint member attached without error")
	}

	miss, err := ParseJoinDomains("m=public.orders.nope")
	if err != nil {
		t.Fatal(err)
	}
	jd2, err := OpenJoinDomains(t.TempDir(), miss, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer jd2.Close()
	if err := ord.AttachJoinDomains(jd2); err == nil {
		t.Fatal("a member column the table does not have attached without error")
	}
}

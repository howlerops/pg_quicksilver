package mirror

import (
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// A mirror built without its domains, then reopened with them: the backfill is
// due until one rewrite has given every file its companion, the trigger is the
// compaction reason the sidecar acts on, and nothing is reported complete
// before that.
func TestBackfillDueUntilEveryFileHasItsCompanion(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq

	cust, ord := openDomainTables(t, root, nil)
	var ops []changestream.Change
	ids := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		ids = append(ids, dmUUID(i))
		ops = append(ops, ciIns(dmUUID(i)))
	}
	applyTxn(t, cust, &lsn, ops...)
	var oops []changestream.Change
	for i := 0; i < 120; i++ {
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

	// Domain added to the existing mirror.
	jd := domainsFor(t, root, dmSpec, 0)
	// One live Table per directory: close the first pair before reopening.
	cust.Close()
	ord.Close()
	cust, ord = openDomainTables(t, root, jd)
	if !ord.BackfillDue() || !cust.BackfillDue() {
		t.Fatal("a table whose files predate its domain is not due for a backfill")
	}
	if why := ord.CompactReason(); why != JoinBackfillReason {
		t.Fatalf("CompactReason = %q, want %q", why, JoinBackfillReason)
	}
	done, total, err := ord.BackfillProgress()
	if err != nil {
		t.Fatal(err)
	}
	if total == 0 || done != 0 {
		t.Fatalf("progress %d of %d before any rewrite, want 0 of >0", done, total)
	}

	// Interrupted: the rewrite is abandoned before it is published. The
	// manifest is the one it started from, so the table is still due, and a
	// restart (a fresh open) asks again.
	if c := ord.BeginCompaction(); c == nil {
		t.Fatal("BeginCompaction refused a due table with nothing else running")
	}
	ord.AbortCompaction()
	// A restart closes the Table (which waits for an abandoned rewrite's
	// cleanup) before the directory is opened again.
	ord.Close()
	cust.Close()
	cust, ord = openDomainTables(t, root, jd)
	if !ord.BackfillDue() || completeOf(t, ord, "customer_id") {
		t.Fatal("an abandoned backfill counted as complete after restart")
	}

	// The rewrite that completes it. Every file of both tables carries the
	// companion afterwards, so nothing is due and the reason is gone.
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, err := cust.Compact(); err != nil {
		t.Fatal(err)
	}
	if ord.BackfillDue() || cust.BackfillDue() {
		t.Fatal("still due after the rewrite gave every file its companion")
	}
	if why := ord.CompactReason(); why == JoinBackfillReason {
		t.Fatalf("CompactReason still %q after the backfill", why)
	}
	done, total, err = ord.BackfillProgress()
	if err != nil {
		t.Fatal(err)
	}
	if done != total {
		t.Fatalf("progress %d of %d after the backfill", done, total)
	}
	if !completeOf(t, ord, "customer_id") || !completeOf(t, cust, "id") {
		t.Fatal("tables are not complete after the backfill")
	}

	// A delta written after the backfill carries the companion too: not due.
	applyTxn(t, ord, &lsn, oiIns(9001, ids[0]))
	if ord.BackfillDue() {
		t.Fatal("a delta written with the domain attached made the table due")
	}
}

// A table with no domain attached is never due, whatever its files hold.
func TestBackfillNeverDueWithoutDomains(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	_, ord := openDomainTables(t, root, nil)
	applyTxn(t, ord, &lsn, oiIns(1, dmUUID(1)))
	if ord.BackfillDue() {
		t.Fatal("a table with no domain attached is due for a backfill")
	}
}

// An abandoned rewrite, then a successful one in the same process, on the same
// Table: the abandoned rewrite's name was taken from State.Seq, which the next
// rewrite increments, so its cleanup cannot remove the next rewrite's file.
func TestAbandonedRewriteThenSuccessfulOneInProcess(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	_, ord := openDomainTables(t, root, nil)
	var ids []string
	var ops []changestream.Change
	for i := 0; i < 60; i++ {
		ids = append(ids, dmUUID(i))
		ops = append(ops, oiIns(i+1, ids[i]))
	}
	applyTxn(t, ord, &lsn, ops...)
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}
	ord.Close()

	jd := domainsFor(t, root, dmSpec, 0)
	_, ord = openDomainTables(t, root, jd)
	if c := ord.BeginCompaction(); c == nil {
		t.Fatal("BeginCompaction refused")
	}
	ord.AbortCompaction()
	if _, err := ord.Compact(); err != nil {
		t.Fatalf("rewrite after an abandoned one: %v", err)
	}
	if ord.BackfillDue() {
		t.Fatal("still due after the rewrite that followed an abandoned one")
	}
	ord.Close()
}

// The same, but across a restart: the abandoned rewrite's Table is closed, and
// the directory is reopened, so the next rewrite reuses the name the abandoned
// one had. The close waits for the abandoned rewrite's cleanup.
func TestAbandonedRewriteThenRestart(t *testing.T) {
	root := t.TempDir()
	var lsn lsnSeq
	_, ord := openDomainTables(t, root, nil)
	var ops []changestream.Change
	for i := 0; i < 60; i++ {
		ops = append(ops, oiIns(i+1, dmUUID(i)))
	}
	applyTxn(t, ord, &lsn, ops...)
	if _, err := ord.Compact(); err != nil {
		t.Fatal(err)
	}
	ord.Close()

	jd := domainsFor(t, root, dmSpec, 0)
	_, ord = openDomainTables(t, root, jd)
	if c := ord.BeginCompaction(); c == nil {
		t.Fatal("BeginCompaction refused")
	}
	ord.AbortCompaction()
	ord.Close()

	_, ord = openDomainTables(t, root, jd)
	if !ord.BackfillDue() {
		t.Fatal("an abandoned backfill counted as complete after restart")
	}
	if _, err := ord.Compact(); err != nil {
		t.Fatalf("rewrite after restart: %v", err)
	}
	if ord.BackfillDue() {
		t.Fatal("still due after the rewrite after restart")
	}
	ord.Close()
}

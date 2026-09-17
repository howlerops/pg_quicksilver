package main

import (
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func txnOf(n int) changestream.Transaction {
	t := changestream.Transaction{CommitLSN: "0/1", NextLSN: "0/2"}
	t.Changes = make([]changestream.Change, n)
	return t
}

func changesIn(txns []changestream.Transaction) int {
	n := 0
	for _, t := range txns {
		n += len(t.Changes)
	}
	return n
}

// The cap exists so one tick cannot swallow a whole backlog: five consecutive
// ticks of the narrow shape took 849ms, 2452ms, 5706ms, 9332ms and 15528ms,
// each bigger than the last because the backlog outran the drain.
func TestCapBatchBoundsTheWorkInOneTick(t *testing.T) {
	defer func(prev int) { maxTickChanges = prev }(maxTickChanges)
	maxTickChanges = 1000

	var txns []changestream.Transaction
	for i := 0; i < 100; i++ {
		txns = append(txns, txnOf(300))
	}
	head := capBatch(txns)
	if len(head) == len(txns) {
		t.Fatal("the whole backlog was taken in one tick; the cap did nothing")
	}
	// Overshoot by less than one transaction is expected and required: the cut
	// lands on a transaction boundary, never inside one.
	if got := changesIn(head); got < maxTickChanges || got > maxTickChanges+300 {
		t.Errorf("took %d changes, want the first prefix at or just past %d",
			got, maxTickChanges)
	}
}

// Half a transaction in the mirror is a state the source never had. The cap
// must never buy a shorter tick with that.
func TestCapBatchNeverSplitsATransaction(t *testing.T) {
	defer func(prev int) { maxTickChanges = prev }(maxTickChanges)
	maxTickChanges = 10

	txns := []changestream.Transaction{txnOf(3), txnOf(4), txnOf(9), txnOf(2)}
	head := capBatch(txns)
	for i, h := range head {
		if len(h.Changes) != len(txns[i].Changes) {
			t.Fatalf("transaction %d was truncated from %d changes to %d",
				i, len(txns[i].Changes), len(h.Changes))
		}
	}
}

// A single transaction bigger than the cap is applied whole and overruns. That
// is the correct trade: the alternative is never applying it.
func TestCapBatchAppliesAnOversizedTransactionWhole(t *testing.T) {
	defer func(prev int) { maxTickChanges = prev }(maxTickChanges)
	maxTickChanges = 10

	txns := []changestream.Transaction{txnOf(5000)}
	head := capBatch(txns)
	if len(head) != 1 || len(head[0].Changes) != 5000 {
		t.Fatalf("an oversized transaction was not applied whole: %d txns, %d changes",
			len(head), changesIn(head))
	}
}

// Nothing is ever dropped: whatever the cap holds back is the remainder, and
// the loop puts it in `pending` for the next tick.
func TestCapBatchLosesNothing(t *testing.T) {
	defer func(prev int) { maxTickChanges = prev }(maxTickChanges)
	maxTickChanges = 50

	var txns []changestream.Transaction
	for i := 1; i <= 20; i++ {
		txns = append(txns, txnOf(i))
	}
	head := capBatch(txns)
	rest := txns[len(head):]
	if changesIn(head)+changesIn(rest) != changesIn(txns) {
		t.Error("changes went missing between the applied head and the remainder")
	}
}

// Zero restores the old unbounded behaviour, which is what the A/B flag is for.
func TestCapBatchDisabled(t *testing.T) {
	defer func(prev int) { maxTickChanges = prev }(maxTickChanges)
	maxTickChanges = 0

	txns := []changestream.Transaction{txnOf(100), txnOf(100), txnOf(100)}
	if len(capBatch(txns)) != 3 {
		t.Error("QS_MAX_TICK_CHANGES=0 should apply everything in one tick")
	}
}

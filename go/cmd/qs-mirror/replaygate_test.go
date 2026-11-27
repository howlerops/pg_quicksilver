package main

import (
	"fmt"
	"math"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// txnAt is a committed transaction whose commit record ends at end. NextLSN is
// the confirm position (commit start + 1), which is what the gate reads.
func txnAt(end string, label string) changestream.Transaction {
	return changestream.Transaction{CommitLSN: end, NextLSN: end, Changes: []changestream.Change{{Table: label}}}
}

func lsn(s string) uint64 { return changestream.ParseLSN(s) }

// An applier models Table.Apply's floor: a commit below the applied position is
// already in the mirror and is skipped, everything else is applied once, in
// order. It records what was applied, so a test can check no commit is applied
// twice and none is skipped.
type applier struct {
	applied []string
	floor   uint64
}

func (a *applier) apply(txns []changestream.Transaction) {
	for _, t := range txns {
		if lsn(t.CommitLSN) < a.floor {
			continue
		}
		a.applied = append(a.applied, t.CommitLSN)
		a.floor = lsn(t.CommitLSN) + 1
	}
}

func TestGatedPrefixAppliesOnlyWhatTheStandbyReplayed(t *testing.T) {
	txns := []changestream.Transaction{
		txnAt("0/10", "a"), txnAt("0/20", "b"), txnAt("0/30", "c"),
	}
	cases := []struct {
		replay string
		want   int
	}{
		{"0/0", 0},
		{"0/F", 0},    // before the first commit's end
		{"0/10", 1},   // the first commit ends exactly here: replayed
		{"0/1F", 1},   // mid-batch: the second is not replayed yet
		{"0/20", 2},   // boundary inclusive
		{"0/2A", 2},   // replay passed the second commit, not the third
		{"0/FFFF", 3}, // everything
	}
	for _, c := range cases {
		if got := gatedPrefix(txns, lsn(c.replay)); got != c.want {
			t.Errorf("replay %s: applies %d, want %d", c.replay, got, c.want)
		}
	}
}

// A commit held back holds back every commit after it, even one whose end the
// standby has passed: the mirror never applies out of commit order.
func TestGatedPrefixIsAPrefixNotASubset(t *testing.T) {
	txns := []changestream.Transaction{
		txnAt("0/10", "a"), txnAt("0/50", "b"), txnAt("0/30", "c"), // out-of-order ends are not produced by the stream; the rule must still stop at b
	}
	if got := gatedPrefix(txns, lsn("0/40")); got != 1 {
		t.Fatalf("replay 0/40 applies %d, want 1: c is past replay only if b is not in front of it", got)
	}
}

func TestGatedPrefixOfNothingIsNothing(t *testing.T) {
	if got := gatedPrefix(nil, lsn("0/FFFF")); got != 0 {
		t.Fatalf("empty batch applies %d", got)
	}
}

// The standby replays in steps while writes keep coming. Each step applies the
// prefix it allows; across all steps every commit is applied exactly once and
// in order, and nothing is applied before the standby has replayed it.
func TestHeldCommitsAreReleasedInStepsOnceEach(t *testing.T) {
	var all []changestream.Transaction
	for i := 1; i <= 20; i++ {
		all = append(all, txnAt(fmt.Sprintf("0/%X", i*16), fmt.Sprintf("t%d", i)))
	}
	var pending []changestream.Transaction
	var a applier
	var replay uint64
	for _, step := range []uint64{0x40, 0x40, 0x90, 0x90, 0x150, math.MaxUint32} {
		if step < replay {
			t.Fatalf("test error: replay must not go back")
		}
		replay = step
		// The stream hands over what it has read; the gate takes the prefix.
		pending = append(pending, all[len(a.applied)+len(pending):min(len(all), len(a.applied)+len(pending)+5)]...)
		n := gatedPrefix(pending, replay)
		for _, tx := range pending[:n] {
			if lsn(tx.NextLSN) > replay {
				t.Fatalf("applied %s while replay is %X", tx.CommitLSN, replay)
			}
		}
		a.apply(pending[:n])
		pending = append([]changestream.Transaction(nil), pending[n:]...)
	}
	if len(a.applied) != 20 {
		t.Fatalf("applied %d commits, want 20", len(a.applied))
	}
	for i := 1; i < len(a.applied); i++ {
		if lsn(a.applied[i]) <= lsn(a.applied[i-1]) {
			t.Fatalf("applied out of order at %d: %s after %s", i, a.applied[i], a.applied[i-1])
		}
	}
}

// A restart while commits are held: the slot was not confirmed past them, so
// the stream redelivers from the confirmed position. The table's floor discards
// what it already has. Nothing is lost and nothing is applied twice, whatever
// the replay position was at the moment of the kill.
func TestRestartWhileHeldResumesWithoutLossOrDuplication(t *testing.T) {
	var all []changestream.Transaction
	for i := 1; i <= 10; i++ {
		all = append(all, txnAt(fmt.Sprintf("0/%X", i*16), fmt.Sprintf("t%d", i)))
	}
	// First life: replay reaches 0x50, so commits 1..5 are applied and
	// confirmed; commits 6.. are read, held, and the process dies.
	var a applier
	a.apply(all[:gatedPrefix(all, 0x50)])

	// Second life: the slot may restart the stream at or before the position
	// the table already has (a confirmation that never reached the server), so
	// commits 1..5 come again along with everything after them. The floor is
	// the applied position, which is what the table's manifest holds.
	redelivered := all
	n := gatedPrefix(redelivered, 0x90)
	a.apply(redelivered[:n])
	if len(a.applied) != 9 {
		t.Fatalf("after restart applied %d commits, want 9 (commits 1..9)", len(a.applied))
	}
	seen := map[string]bool{}
	for _, c := range a.applied {
		if seen[c] {
			t.Fatalf("commit %s applied twice", c)
		}
		seen[c] = true
	}
}

// The idle advance writes applied_lsn = target-1 and confirms target. Neither
// may pass what the standby has replayed.
func TestIdleTargetNeverPassesReplay(t *testing.T) {
	cases := []struct {
		name   string
		p, rep uint64
		gated  bool
		want   uint64
	}{
		{"caught up", 0x100, 0x100, true, 0x100},
		{"replay behind the walsender: capped at replay+1", 0x100, 0x80, true, 0x81},
		{"replay not read yet: no advance", 0x100, 0, true, 0},
		{"gate off: the walsender's position stands", 0x100, 0x80, false, 0x100},
		{"walsender behind replay: its position stands", 0x40, 0x80, true, 0x40},
	}
	for _, c := range cases {
		if got := idleTarget(c.p, c.rep, c.gated); got != c.want {
			t.Errorf("%s: target %X, want %X", c.name, got, c.want)
		}
	}
}

// A snapshot taken at S is not reported (so not served) until the standby has
// replayed S. Once it has, it is reported.
func TestReportForHoldsASnapshotUntilReplayReachesIt(t *testing.T) {
	const S = 0x100
	if _, ok := reportFor(S, 0, 0x80, false); ok {
		t.Fatalf("snapshot at %X reported with replay at 0x80", uint64(S))
	}
	lsn, ok := reportFor(S, 0, S, false)
	if !ok || lsn != S {
		t.Fatalf("snapshot at replay %X: got (%X, %v), want (%X, true)", uint64(S), lsn, ok, uint64(S))
	}
}

// The idle position reported for a table is the publisher's LSN or the idle
// position, whichever is later, but never past what the standby has replayed.
func TestReportForClampsTheIdlePositionToReplay(t *testing.T) {
	cases := []struct {
		name                string
		published, pos, rep uint64
		current             bool
		want                uint64
	}{
		{"idle position past replay is clamped to replay", 0x10, 0x200, 0x50, true, 0x50},
		{"idle position within replay is used", 0x10, 0x40, 0x50, true, 0x40},
		{"lake not current: the idle position is not used", 0x10, 0x40, 0x50, false, 0x10},
		{"idle position behind published: published stands", 0x30, 0x20, 0x50, true, 0x30},
		{"gate off (no limit): the old rule", 0x10, 0x200, math.MaxUint64, true, 0x200},
	}
	for _, c := range cases {
		lsn, ok := reportFor(c.published, c.pos, c.rep, c.current)
		if !ok || lsn != c.want {
			t.Errorf("%s: got (%X, %v), want (%X, true)", c.name, lsn, ok, c.want)
		}
	}
}

// Replay is the one number this rule is about: a report never runs past it,
// whichever way the idle position and the publisher's LSN move.
func TestReportForNeverRunsPastReplay(t *testing.T) {
	for rep := uint64(1); rep < 0x60; rep++ {
		for pos := uint64(0); pos < 0x80; pos += 7 {
			for _, pub := range []uint64{0, 0x10, 0x30, 0x55} {
				lsn, ok := reportFor(pub, pos, rep, true)
				if !ok {
					if pub <= rep {
						t.Fatalf("pub %X rep %X: refused although published is not past replay", pub, rep)
					}
					continue
				}
				if lsn > rep && lsn > pub {
					t.Fatalf("pub %X pos %X rep %X: reported %X past replay", pub, pos, rep, lsn)
				}
			}
		}
	}
}

func TestEnvBoolSpellings(t *testing.T) {
	t.Setenv("QS_GATE_TEST", "off")
	if envBool("QS_GATE_TEST", true) {
		t.Fatal("off read as on")
	}
	t.Setenv("QS_GATE_TEST", "yes")
	if !envBool("QS_GATE_TEST", false) {
		t.Fatal("yes read as off")
	}
	t.Setenv("QS_GATE_TEST", "maybe")
	if !envBool("QS_GATE_TEST", true) {
		t.Fatal("unrecognised value must be the default")
	}
}

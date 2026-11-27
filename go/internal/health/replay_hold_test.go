package health

import (
	"strings"
	"testing"
	"time"
)

// A held mirror says it is waiting for local replay and gives the two
// positions, once its lag is past the SLO. A probe that said "lag exceeds"
// alone reads as a hung sidecar; this one says what it is waiting for.
func TestReplayHoldNamesBothPositionsWhenLagPassesTheSLO(t *testing.T) {
	h := New(time.Millisecond)
	h.RecordBootstrapped()
	h.RecordReplayHold("0/40", "0/50", 3)
	time.Sleep(10 * time.Millisecond)

	ok, why := h.Ready()
	if ok {
		t.Fatalf("ready while lag is past the SLO: %q", why)
	}
	for _, want := range []string{"waiting for local replay", "0/50", "0/40", "3 change(s) held back"} {
		if !strings.Contains(why, want) {
			t.Errorf("readiness %q does not contain %q", why, want)
		}
	}
}

// Within the SLO a held mirror is still ready, and says it is holding back.
func TestReplayHoldIsSaidWhileWithinTheSLO(t *testing.T) {
	h := New(time.Hour)
	h.RecordBootstrapped()
	h.RecordReplayHold("0/40", "0/50", 1)
	ok, why := h.Ready()
	if !ok || !strings.Contains(why, "waiting for local replay") {
		t.Fatalf("held within SLO: ready=%v why=%q", ok, why)
	}
	h.ClearReplayHold()
	ok, why = h.Ready()
	if !ok || strings.Contains(why, "waiting for local replay") {
		t.Fatalf("after the hold clears: ready=%v why=%q", ok, why)
	}
}

// The hold is in the snapshot, so /metrics and the probe body can carry it.
func TestSnapshotCarriesTheHold(t *testing.T) {
	h := New(time.Hour)
	h.RecordReplayHold("0/40", "0/50", 2)
	s := h.Snapshot()
	if !s.ReplayHeld || s.ReplayLSN != "0/40" || s.NeedLSN != "0/50" || s.HeldTransactions != 2 {
		t.Fatalf("snapshot %+v does not carry the hold", s)
	}
}

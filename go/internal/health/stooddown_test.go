package health

import (
	"strings"
	"testing"
	"time"
)

// A promoted node: the sidecar stood down, so /readyz must say so and must not
// report the last applied LSN and its lag as the mirror's. The probe still passes
// (the primary is not taken out of its Services by its own mirror), and the
// reason names the stand-down.
func TestStoodDownReportsTruthfully(t *testing.T) {
	h := New(time.Minute)
	h.RecordBootstrapped()
	h.RecordApply("0/C73CBF0", "0/CB173F8", 3)

	ok, why := h.Ready()
	if !ok || !strings.HasPrefix(why, "lag ") {
		t.Fatalf("standby before the stand-down: ok=%v why=%q", ok, why)
	}

	h.SetStoodDown(true)
	if !h.StoodDown() {
		t.Fatal("StoodDown() = false after SetStoodDown(true)")
	}
	ok, why = h.Ready()
	if !ok {
		t.Fatalf("a stood-down primary must pass the readiness probe, got ready=false: %q", why)
	}
	if !strings.Contains(why, "stood down") || !strings.Contains(why, "primary") || strings.Contains(why, "lag ") {
		t.Errorf("reason does not say the mirror is not fed: %q", why)
	}
	s := h.Snapshot()
	if !s.StoodDown || s.AppliedLSN != "" || s.HeadLSN != "" || s.LagBytes != 0 || s.LagSeconds != 0 {
		t.Errorf("stood-down snapshot still carries the mirror's position: %+v", s)
	}
	if m := h.Metrics(); !strings.Contains(m, "quicksilver_stood_down 1\n") {
		t.Errorf("metrics do not show the stand-down:\n%s", m)
	}

	// A stood-down primary is not made unready by a halt or a slot fault either:
	// neither describes the primary's own state.
	h.SetSlotFault("slot cannot be decoded")
	if ok, _ := h.Ready(); !ok {
		t.Error("a stood-down primary is unready because of a slot fault on its (dropped) slot")
	}
	h.SetSlotFault("")

	// Back to a standby: the position and the lag come back, and the gauge clears.
	h.SetStoodDown(false)
	ok, why = h.Ready()
	if !ok || !strings.HasPrefix(why, "lag ") {
		t.Errorf("after rejoining as a standby: ok=%v why=%q", ok, why)
	}
	if s := h.Snapshot(); s.StoodDown || s.AppliedLSN != "0/C73CBF0" {
		t.Errorf("after rejoining as a standby: %+v", s)
	}
	if m := h.Metrics(); !strings.Contains(m, "quicksilver_stood_down 0\n") {
		t.Errorf("gauge not cleared:\n%s", m)
	}
}

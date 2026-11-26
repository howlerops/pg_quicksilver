package mirror

import "testing"

// The process-wide limit: exactly cap(rewriteSlots) acquires succeed, the next
// one waits (fails without blocking), and a release makes one available again.
func TestRewriteSlotsAreProcessWide(t *testing.T) {
	n := cap(rewriteSlots)
	if n < 1 {
		t.Fatalf("cap(rewriteSlots) = %d, want at least 1", n)
	}
	for i := 0; i < n; i++ {
		if !tryAcquireRewriteSlot() {
			t.Fatalf("acquire %d of %d failed with the slots free", i+1, n)
		}
	}
	if got := rewriteSlotsInUse(); got != n {
		t.Errorf("in use = %d, want %d", got, n)
	}
	if tryAcquireRewriteSlot() {
		t.Errorf("acquire beyond the limit of %d succeeded", n)
	}
	releaseRewriteSlot()
	if !tryAcquireRewriteSlot() {
		t.Errorf("acquire after a release failed")
	}
	for i := 0; i < n; i++ {
		releaseRewriteSlot()
	}
	if got := rewriteSlotsInUse(); got != 0 {
		t.Errorf("after releasing everything, in use = %d, want 0", got)
	}
}

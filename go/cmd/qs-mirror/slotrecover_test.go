package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func pgErrOf(code, msg string) error {
	return fmt.Errorf("pump: %w", &pgconn.PgError{Code: code, Message: msg})
}

func TestSlotFaultOfPublicationIsOurPublicationOnly(t *testing.T) {
	ours := `publication "quicksilver" does not exist`
	cases := []struct {
		name string
		err  error
		pub  string
		want string
	}{
		{"our publication, undefined_object", pgErrOf("42704", ours), "quicksilver", faultPublicationGone},
		{"another publication", pgErrOf("42704", `publication "other" does not exist`), "quicksilver", ""},
		{"same message, other code", pgErrOf("XX000", ours), "quicksilver", ""},
		{"a missing slot is not this fault", pgErrOf("42704", `replication slot "quicksilver_x" does not exist`), "quicksilver", ""},
		{"invalidated slot by message", errors.New("START_REPLICATION: can no longer get changes from replication slot \"s\""), "quicksilver", faultSlotLost},
		{"unrelated error", errors.New("connection reset by peer"), "quicksilver", ""},
		{"nil", nil, "quicksilver", ""},
	}
	for _, c := range cases {
		if got := slotFaultOf(c.err, c.pub); got != c.want {
			t.Errorf("%s: slotFaultOf = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestSlotFaultUsesSlotStateWhenErrorIsSilent(t *testing.T) {
	plain := errors.New("stream ended")
	if got := slotFault(plain, slotState{Exists: true, WalStatus: "lost"}, "p"); got != faultSlotLost {
		t.Errorf("lost slot: got %q", got)
	}
	if got := slotFault(plain, slotState{Exists: true, Conflicting: true}, "p"); got != faultSlotConflict {
		t.Errorf("conflicting slot: got %q", got)
	}
	if got := slotFault(plain, slotState{Exists: true, WalStatus: "reserved"}, "p"); got != "" {
		t.Errorf("healthy slot with an ordinary error: got %q, want no fault", got)
	}
	if got := slotFault(plain, slotState{}, "p"); got != "" {
		t.Errorf("missing slot: got %q, want no fault (runMirror creates it)", got)
	}
}

func TestSlotRecoveryNeedsIdenticalFailuresWithoutProgress(t *testing.T) {
	var r slotRecovery
	now := time.Unix(1_000_000, 0)
	if a := r.observe(faultPublicationGone, "0/10", now); a != recoverWait {
		t.Fatalf("first failure: %v", a)
	}
	if a := r.observe(faultPublicationGone, "0/10", now); a != recoverWait {
		t.Fatalf("second failure: %v", a)
	}
	// Progress resets the count: the slot moved, so the failure is not the same
	// stuck one.
	if a := r.observe(faultPublicationGone, "0/20", now); a != recoverWait {
		t.Fatalf("after progress: %v", a)
	}
	if a := r.observe(faultPublicationGone, "0/20", now); a != recoverWait {
		t.Fatalf("second after progress: %v", a)
	}
	if a := r.observe(faultPublicationGone, "0/20", now); a != recoverNow {
		t.Fatalf("third identical failure with no progress: %v, want recoverNow", a)
	}
}

func TestSlotRecoveryAnOrdinaryErrorResetsTheCount(t *testing.T) {
	var r slotRecovery
	now := time.Unix(1, 0)
	r.observe(faultPublicationGone, "0/1", now)
	r.observe(faultPublicationGone, "0/1", now)
	if a := r.observe("", "", now); a != recoverWait {
		t.Fatalf("ordinary error: %v", a)
	}
	if a := r.observe(faultPublicationGone, "0/1", now); a != recoverWait {
		t.Fatalf("count should have restarted: %v", a)
	}
}

func TestSlotRecoveryIsBoundedPerFaultAndWindow(t *testing.T) {
	var r slotRecovery
	t0 := time.Unix(2_000_000, 0)
	fail := func(at time.Time) slotRecoveryAction {
		var a slotRecoveryAction
		for i := 0; i < slotRecoverAfter; i++ {
			a = r.observe(faultPublicationGone, "0/5", at)
		}
		return a
	}
	if a := fail(t0); a != recoverNow {
		t.Fatalf("first incident: %v", a)
	}
	r.recovered(faultPublicationGone, t0)

	// The freshly created slot fails the same way, soon after.
	if a := fail(t0.Add(time.Minute)); a != recoverGiveUp {
		t.Fatalf("recurrence inside the window: %v, want recoverGiveUp", a)
	}
	// A different fault is not this fault's record.
	var other slotRecoveryAction
	for i := 0; i < slotRecoverAfter; i++ {
		other = r.observe(faultSlotLost, "0/5", t0.Add(time.Minute))
	}
	if other != recoverNow {
		t.Fatalf("a different fault: %v, want recoverNow", other)
	}
	r.recovered(faultSlotLost, t0.Add(time.Minute))
	// Once the window has passed, the same fault is an incident again.
	if a := fail(t0.Add(slotRecoverWindow + 2*time.Minute)); a != recoverNow {
		t.Fatalf("after the window: %v, want recoverNow", a)
	}
}

func TestSlotRecoveryRecordSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	if f, at := loadRecovery(dir); f != "" || !at.IsZero() {
		t.Fatalf("no record yet: %q %v", f, at)
	}
	at := time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)
	if err := saveRecovery(dir, faultPublicationGone, at); err != nil {
		t.Fatal(err)
	}
	var r slotRecovery
	r.restore(dir)
	if r.lastFault != faultPublicationGone || !r.lastAt.Equal(at) {
		t.Fatalf("restored %q %v", r.lastFault, r.lastAt)
	}
	// A restarted process with a recent record gives up instead of recovering again.
	var n slotRecoveryAction
	for i := 0; i < slotRecoverAfter; i++ {
		n = r.observe(faultPublicationGone, "0/1", at.Add(time.Minute))
	}
	if n != recoverGiveUp {
		t.Fatalf("after restart: %v", n)
	}
}

func TestSlotFaultReasonNamesTheSlotAndPublication(t *testing.T) {
	r := slotFaultReason(faultPublicationGone, "qs_app_pod1", "qs_pub")
	for _, want := range []string{"qs_app_pod1", "qs_pub", "does not exist"} {
		if !strings.Contains(r, want) {
			t.Errorf("reason %q lacks %q", r, want)
		}
	}
	if slotFaultReason("nope", "s", "p") != "" {
		t.Error("an unknown fault has no reason")
	}
}

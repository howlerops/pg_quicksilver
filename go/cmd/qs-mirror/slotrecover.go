package main

// Recovery from a replication slot that can no longer be decoded.
//
// A slot's restart point is the oldest WAL it must still read. If the
// publication was dropped and created again after that point, the walsender
// decodes old WAL in which the publication does not exist and fails with
//
//	ERROR: publication "X" does not exist   (SQLSTATE 42704)
//
// Its restart point only moves when it is confirmed, and it cannot be confirmed
// past a record it cannot decode, so the failure repeats for good. The same is
// true of a slot PostgreSQL invalidated (wal_status = 'lost', because the WAL it
// needs was removed under max_slot_wal_keep_size) and of a slot marked
// conflicting. Before this file, supervise retried forever and the Pod never
// recovered without an operator.
//
// The recovery is the one runMirror already has for a slot it did not find: drop
// THIS Pod's slot and let the next attempt create it, which takes a consistent
// point and snapshots every table (runMirror, "new replication slot").
//
// It is bounded in three ways, all of them here:
//   - it needs slotRecoverAfter identical failures with no progress (the same
//     fault, and confirmed_flush_lsn unchanged) before it acts, so one
//     transient error never drops a slot;
//   - it acts at most once per fault per slotRecoverWindow, and the time of the
//     last recovery is kept in a file in the mirror directory, so a restart does
//     not reset it; a fault that comes back inside the window is an operator's;
//   - it only ever touches the slot named by QS_SLOT for this Pod (dropSlot).

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	slotRecoverAfter  = 3
	slotRecoverWindow = time.Hour
	// slotFaultDwell is how long a stream must run without error before a slot
	// fault recorded for it is cleared from readiness (pump).
	slotFaultDwell  = 30 * time.Second
	slotRecoverFile = ".slot-recovery"

	faultPublicationGone = "publication-missing"
	faultSlotLost        = "slot-invalidated"
	faultSlotConflict    = "slot-conflicting"
)

// slotFaultOf names the fault in err, if err is one this file knows how to
// recover from. It matches the publication error on the publication this Pod
// was configured with, and nothing else: another publication's error is not
// this Pod's to fix. SQLSTATE 42704 is undefined_object; PostgreSQL uses it for
// a missing publication, and the message is compared as well because the same
// code also means "replication slot does not exist", which is not a fault here.
func slotFaultOf(err error, publication string) string {
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42704" &&
		pgErr.Message == fmt.Sprintf("publication %q does not exist", publication) {
		return faultPublicationGone
	}
	if strings.Contains(err.Error(), "can no longer get changes from replication slot") {
		return faultSlotLost
	}
	return ""
}

// slotState is the server's view of this Pod's slot, read after a failure.
type slotState struct {
	Exists       bool
	WalStatus    string
	Invalidation string
	Conflicting  bool
	Confirmed    string
}

// readSlotState asks the primary about one slot. A missing slot is not an error:
// it is Exists false, which runMirror handles by creating it.
func readSlotState(ctx context.Context, conn *pgx.Conn, slot string) (slotState, error) {
	var s slotState
	var status, reason, confirmed *string
	err := conn.QueryRow(ctx, `SELECT wal_status, invalidation_reason, conflicting,
		confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slot).
		Scan(&status, &reason, &s.Conflicting, &confirmed)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotState{}, nil
	}
	if err != nil {
		return slotState{}, err
	}
	s.Exists = true
	if status != nil {
		s.WalStatus = *status
	}
	if reason != nil {
		s.Invalidation = *reason
	}
	if confirmed != nil {
		s.Confirmed = *confirmed
	}
	return s, nil
}

// slotFault is the fault for a failed attempt, from the error and the slot. An
// empty string means there is no fault this file acts on: an ordinary error,
// which supervise backs off from as before.
func slotFault(err error, st slotState, publication string) string {
	if f := slotFaultOf(err, publication); f != "" {
		return f
	}
	if st.Exists && st.WalStatus == "lost" {
		return faultSlotLost
	}
	if st.Exists && st.Conflicting {
		return faultSlotConflict
	}
	return ""
}

// slotRecoveryAction is what supervise does after a failed attempt.
type slotRecoveryAction int

const (
	recoverWait   slotRecoveryAction = iota // not yet: back off and try again
	recoverNow                              // drop this Pod's slot and re-snapshot
	recoverGiveUp                           // the same fault came back too soon after a recovery
)

// slotRecovery counts identical failures. It is not safe for concurrent use:
// supervise is its only caller.
type slotRecovery struct {
	fault     string    // the fault being counted
	confirmed string    // confirmed_flush_lsn when the count started
	count     int       // consecutive failures with this fault and no progress
	lastFault string    // the fault of the last recovery, if any
	lastAt    time.Time // when that recovery was made
}

// observe records one failed attempt and says what to do about it. fault is
// "" when the failure is not one this file acts on, which resets the count.
// confirmed is the slot's confirmed_flush_lsn now; a moved position is progress
// and starts the count again.
func (r *slotRecovery) observe(fault, confirmed string, now time.Time) slotRecoveryAction {
	if fault == "" {
		r.fault, r.confirmed, r.count = "", "", 0
		return recoverWait
	}
	if fault != r.fault || confirmed != r.confirmed {
		r.fault, r.confirmed, r.count = fault, confirmed, 0
	}
	r.count++
	if r.count < slotRecoverAfter {
		return recoverWait
	}
	if r.lastFault == fault && now.Sub(r.lastAt) < slotRecoverWindow {
		return recoverGiveUp
	}
	return recoverNow
}

// recovered records that a recovery was made for fault at now, and clears the
// count. The record is what makes the next recovery for the same fault wait
// slotRecoverWindow; the caller persists it with saveRecovery.
func (r *slotRecovery) recovered(fault string, now time.Time) {
	r.lastFault, r.lastAt = fault, now
	r.fault, r.confirmed, r.count = "", "", 0
}

// slotFaultReason is the sentence readiness and the logs say for a fault.
func slotFaultReason(fault, slot, publication string) string {
	switch fault {
	case faultPublicationGone:
		return fmt.Sprintf("replication slot %q cannot decode its WAL because publication %q does not exist "+
			"(it was dropped and created again after the slot's restart point)", slot, publication)
	case faultSlotLost:
		return fmt.Sprintf("replication slot %q was invalidated by PostgreSQL (its WAL was removed)", slot)
	case faultSlotConflict:
		return fmt.Sprintf("replication slot %q is marked conflicting", slot)
	}
	return ""
}

// The record of the last recovery lives next to the mirror, so a restart of the
// sidecar cannot reset the window. A missing or unreadable file means no recent
// recovery, which errs on the side of allowing one.

func loadRecovery(dir string) (fault string, at time.Time) {
	b, err := os.ReadFile(filepath.Join(dir, slotRecoverFile))
	if err != nil {
		return "", time.Time{}
	}
	f, ts, ok := strings.Cut(strings.TrimSpace(string(b)), " ")
	if !ok {
		return "", time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return "", time.Time{}
	}
	return f, t
}

func saveRecovery(dir, fault string, at time.Time) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp := filepath.Join(dir, slotRecoverFile+".tmp")
	line := fault + " " + at.UTC().Format(time.RFC3339Nano) + "\n"
	if err := os.WriteFile(tmp, []byte(line), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, slotRecoverFile))
}

// restore loads the persisted record of the last recovery into r.
func (r *slotRecovery) restore(dir string) {
	r.lastFault, r.lastAt = loadRecovery(dir)
}

// logFault is the WARN for a fault that has not yet been acted on, once per
// change of count, so a repeating error is visible without a line per retry.
func logFault(log *slog.Logger, fault, reason string, count int, confirmed string) {
	log.Warn("replication slot cannot be read; not recovering yet",
		"fault", fault, "reason", reason, "failures", count, "confirmed", confirmed,
		"recover_after", slotRecoverAfter)
}

// afterFailedAttempt is supervise's decision after runMirror returned err. It
// returns true when the slot was dropped, so that the next attempt creates it
// and snapshots every table. It never returns an error: whatever it cannot do,
// the supervisor's ordinary backoff still applies.
func (rec *slotRecovery) afterFailedAttempt(ctx context.Context, log *slog.Logger, o options,
	h healthSink, err error) bool {
	now := time.Now()
	var st slotState
	conn, cerr := pgx.Connect(ctx, o.primaryDSN())
	if cerr == nil {
		st, cerr = readSlotState(ctx, conn, o.slot)
		_ = conn.Close(context.WithoutCancel(ctx))
	}
	// If the primary cannot be reached the fault is judged from the error alone,
	// which is what the publication case needs; the slot state is only extra.
	fault := slotFault(err, st, o.publication)
	action := rec.observe(fault, st.Confirmed, now)
	if fault == "" {
		// Not a fault this file acts on, and not evidence the slot is fine: the
		// note stays until the stream has run for slotFaultDwell (pump).
		return false
	}
	reason := slotFaultReason(fault, o.slot, o.publication)
	switch action {
	case recoverWait:
		logFault(log, fault, reason, rec.count, st.Confirmed)
		h.SetSlotFault(reason + "; retrying before any recovery")
		return false
	case recoverGiveUp:
		msg := reason + "; automatic recovery already ran for this fault at " +
			rec.lastAt.UTC().Format(time.RFC3339) + " and the fault came back inside " +
			slotRecoverWindow.String() + ", so it is not repeated: an operator must look at it"
		log.Error("replication slot fault recurred after an automatic recovery; stopping automatic recovery",
			"fault", fault, "slot", o.slot, "last_recovery", rec.lastAt, "reason", reason)
		h.SetSlotFault(msg)
		return false
	}
	log.Warn("replication slot cannot be decoded and will not recover by itself; dropping this "+
		"Pod's slot and re-snapshotting its tables",
		"fault", fault, "reason", reason, "slot", o.slot, "confirmed", st.Confirmed,
		"failures", slotRecoverAfter)
	dropSlot(ctx, o, log)
	rec.recovered(fault, now)
	if serr := saveRecovery(o.mirrorPath, fault, now); serr != nil {
		log.Warn("could not record the slot recovery; a restart may recover again sooner",
			"dir", o.mirrorPath, "err", serr)
	}
	h.RecordSlotRecovery()
	h.SetSlotFault(reason + "; the slot was dropped and its tables are being re-snapshotted")
	return true
}

// healthSink is the part of *health.Health this file uses, so the decision can be
// driven by a test without a server.
type healthSink interface {
	SetSlotFault(string)
	RecordSlotRecovery()
}

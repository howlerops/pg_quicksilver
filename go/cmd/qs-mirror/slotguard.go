package main

// The mirror must never be the reason the primary goes down.
//
// A logical replication slot pins every WAL segment the consumer has not
// confirmed. That is the property the mirror depends on — it is what lets a
// restarted sidecar resume instead of re-snapshotting — and it is a loaded gun
// pointed at the database it is mirroring. If the mirror cannot keep up, the
// primary's pg_wal grows without bound until the volume fills, and PostgreSQL
// stops. The mirror is an optimisation; the primary is the database.
//
// docs/09 registered this as risk R7 — "logical slot fills pg_wal and takes
// down the primary", Med likelihood, High impact — and named the mitigation:
// max_slot_wal_keep_size plus alerting. Nothing implemented it, and the
// large-scale run reproduced it exactly. A 60-second workload put 11.7 million
// row-changes into a source draining at roughly half that rate; the slot held
// everything the mirror had not reached; 6.6 GB of free space became 272 KB and
// PostgreSQL went down with "the database system is not yet accepting
// connections".
//
// So the sidecar bounds its own slot. Every interval it asks how much WAL its
// slot is holding, and past the limit it does the only thing that helps: drops
// the slot and stops. The mirror is then unrecoverable and has to rebuild from
// a fresh snapshot, which is the documented lifecycle for an invalidated slot
// (docs/05) and is enormously cheaper than a primary that has run out of disk.
//
// This is a SECOND line of defence, not the first. The first is
// max_slot_wal_keep_size, which lets PostgreSQL enforce the same bound without
// depending on a sidecar that might itself be wedged — a guard that runs inside
// the process it is guarding cannot be trusted alone. The chart recommends it
// and this backs it up.

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// MaxSlotWAL is how much WAL this mirror's slot may pin before the mirror gives
// itself up. Zero disables the guard.
//
// 4 GB is chosen to be smaller than any volume a CNPG cluster is likely to run
// on and larger than any transient backlog a healthy mirror produces: during
// the large-scale run a mirror that was keeping up held single-digit megabytes,
// and one that was not passed four gigabytes in under a minute. The gap between
// those two numbers is what makes a threshold possible at all.
var maxSlotWAL = envInt("QS_MAX_SLOT_WAL_BYTES", 4<<30)

// slotWALInterval is how often to ask. The question costs a round trip and the
// thing it watches moves over seconds, not milliseconds — and asking every tick
// is exactly the mistake isPrimary made.
var slotWALInterval = envDuration("QS_SLOT_WAL_INTERVAL", 15*time.Second)

// ErrSlotTooFar is returned when the guard fires. The supervisor restarts the
// mirror, finds no slot, and bootstraps a new one from a fresh snapshot.
var ErrSlotTooFar = errors.New("this mirror's replication slot was holding more WAL than the primary can afford")

type slotGuard struct {
	slot    string
	conn    *pgx.Conn
	last    time.Time
	dropped bool
}

// check reports the WAL this slot is holding, and drops the slot if that has
// grown past the limit.
//
// It returns the retained size so a caller can log it even when nothing is
// wrong, because "how close were we" is the number an operator needs BEFORE the
// threshold is hit rather than after.
func (g *slotGuard) check(ctx context.Context, log *slog.Logger) (int64, error) {
	if maxSlotWAL <= 0 || time.Since(g.last) < slotWALInterval {
		return -1, nil
	}
	g.last = time.Now()

	var retained int64
	err := g.conn.QueryRow(ctx, `
		SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
		  FROM pg_replication_slots WHERE slot_name = $1`, g.slot).Scan(&retained)
	if err != nil {
		// A slot that has vanished is not a reason to tear anything down: the
		// stream will fail on its own and say so more precisely than this can.
		if err == pgx.ErrNoRows {
			return -1, nil
		}
		return -1, nil
	}
	if retained < maxSlotWAL {
		return retained, nil
	}

	log.Error("this mirror is too far behind to be safe: dropping its own slot",
		"slot", g.slot,
		"retained_bytes", retained,
		"limit_bytes", maxSlotWAL,
		"why", "a logical slot pins every WAL segment the consumer has not "+
			"confirmed, so a mirror that cannot keep up fills the PRIMARY's disk. "+
			"The mirror will rebuild from a fresh snapshot; the primary keeps serving. "+
			"Set max_slot_wal_keep_size so PostgreSQL enforces this without us.")

	// Drop it from a SEPARATE connection. The streaming connection is holding
	// the slot, and PostgreSQL will not drop a slot that is in use — so the
	// order is: stop streaming by returning the error, having already asked.
	// Returning without dropping would leave the supervisor restarting into the
	// same slot and the same wedge.
	g.dropped = true
	return retained, ErrSlotTooFar
}

// dropSlot removes the slot once the stream that held it is gone. It is called
// on the way out, from the supervisor's side of the failure.
func dropSlot(ctx context.Context, o options, slot string, log *slog.Logger) {
	conn, err := pgx.Connect(ctx, o.primaryDSN())
	if err != nil {
		log.Error("could not connect to drop the runaway slot; the primary is "+
			"still accumulating WAL and needs manual attention",
			"slot", slot, "err", err)
		return
	}
	defer conn.Close(ctx)
	// Terminate whatever still holds it, then drop. A slot with an active
	// walsender cannot be dropped, and the walsender belongs to the connection
	// that just died — which PostgreSQL may not have noticed yet.
	_, _ = conn.Exec(ctx,
		`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
		  WHERE slot_name = $1 AND active`, slot)
	for i := 0; i < 30; i++ {
		if _, err := conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, slot); err == nil {
			log.Warn("dropped this mirror's replication slot to protect the primary; "+
				"the next start will rebuild from a fresh snapshot", "slot", slot)
			return
		}
		time.Sleep(time.Second)
	}
	log.Error("could not drop the runaway slot after 30s; the primary is still "+
		"accumulating WAL and needs manual attention", "slot", slot)
}

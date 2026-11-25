package main

// "The slot exists" is not "the mirror may resume".
//
// A slot remembers what its consumer CONFIRMED. The mirror remembers what it
// durably APPLIED, per table, in state.json. Resuming is only sound when the
// second has reached the first, and nothing compared them: the slot existing
// was taken as proof. When the slot is ahead — a tick whose files did not reach
// disk before a power loss, a mirror directory restored from an older copy,
// anything else that moved the slot — the server will never send the
// transactions in between again, and the mirror resumes one hole short with
// plausible row counts and a passing readiness probe.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
	"github.com/howlerops/pg_quicksilver/go/internal/pgtext"
)

// resnapshot names a table that must not resume, and why.
type resnapshot struct {
	Table  string
	Reason string
}

// planResume decides, per table, between resuming and re-snapshotting. applied
// maps each table to its state.json applied_lsn; confirmed is the slot's
// confirmed_flush_lsn. Tables not returned resume.
//
// The comparison is against applied+1, and the +1 is not slack. The pump
// confirms NextLSN, which is the last applied commit's LSN plus one (confirming
// AT a commit LSN makes the server resend it), and PostgreSQL skips exactly the
// transactions whose commit record starts below confirmed_flush_lsn. So:
//
//	confirmed <= applied+1   every transaction the slot will skip is one this
//	                         table has applied. Resume. (When the slot is
//	                         BEHIND, the server replays a prefix and the
//	                         per-table floor in Table.Apply discards it.)
//	confirmed >  applied+1   a commit may lie in (applied, confirmed). The
//	                         server will not send it and the table does not
//	                         have it. Re-snapshot.
//
// A healthy restart therefore sees confirmed == applied+1 on every table, quiet
// ones included, because Apply advances applied_lsn for every transaction in
// the batch whether or not it touched the table.
func planResume(applied map[string]string, confirmed uint64) []resnapshot {
	var out []resnapshot
	for table, lsn := range applied {
		at := changestream.ParseLSN(lsn)
		switch {
		case lsn == "":
			out = append(out, resnapshot{table,
				"the table was added to a slot that already exists and has never been snapshotted"})
		case at == 0:
			out = append(out, resnapshot{table, fmt.Sprintf(
				"its applied_lsn %q cannot be read, so it cannot be shown to have reached the slot", lsn)})
		case confirmed > at+1:
			out = append(out, resnapshot{table, fmt.Sprintf(
				"the slot has confirmed %s but the mirror durably applied only %s; "+
					"the server will not resend what lies between",
				pglogrepl.LSN(confirmed), lsn)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Table < out[j].Table })
	return out
}

// slotConfirmedLSN reads the slot's confirmed position from the primary. A slot
// with none is an error rather than a zero: zero would compare as "behind every
// table" and wave the resume through, which is the bug this file exists for.
func slotConfirmedLSN(ctx context.Context, conn *pgx.Conn, slot string) (uint64, error) {
	var lsn *string
	err := conn.QueryRow(ctx,
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`,
		slot).Scan(&lsn)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("slot %q is not in pg_replication_slots", slot)
	}
	if err != nil {
		return 0, err
	}
	if lsn == nil || changestream.ParseLSN(*lsn) == 0 {
		return 0, fmt.Errorf("slot %q has no confirmed_flush_lsn", slot)
	}
	return changestream.ParseLSN(*lsn), nil
}

// resnapshotTables re-bootstraps the given tables while the others resume.
//
// The stream is about to restart from the slot's OLDER confirmed position, so
// each of these tables needs a snapshot and an LSN P such that
//
//	every transaction committing below P is in the snapshot, and
//	no transaction committing at or above P is.
//
// With that, the floor in Table.Apply (skip commit LSN < applied_lsn) drops
// exactly the replayed prefix and nothing else.
//
// pg_current_wal_lsn() followed by a SELECT does not give that P. A commit
// record is written to WAL BEFORE the transaction becomes visible to snapshots,
// and under synchronous replication the gap lasts as long as the standby takes
// to answer. A commit in that window is below the LSN just read, absent from
// the SELECT, and then skipped by the floor: a lost row, in the code path meant
// to repair lost rows. Erring the other way (an LSN that is too old) is safe
// but replays stale values over newer ones until the stream catches up.
//
// PostgreSQL already solves this for its own table sync: creating a logical
// slot exports a snapshot that is consistent with the slot's start point by
// construction. So a TEMPORARY slot is created purely to borrow that pair. It
// is never streamed from and goes away with its connection.
func resnapshotTables(
	ctx context.Context, log *slog.Logger, o options, conn *pgx.Conn,
	tables map[string]*mirror.Table, plan []resnapshot,
) error {
	dsn := pgtext.PinDSN(o.primaryDSN())
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	repl, err := pgconn.Connect(ctx, dsn+sep+"replication=database")
	if err != nil {
		return fmt.Errorf("replication connect for a consistent snapshot: %w", err)
	}
	// Closing this connection is what drops the temporary slot and releases the
	// exported snapshot, so it must outlive the transaction below.
	defer func() { _ = repl.Close(context.WithoutCancel(ctx)) }()

	tmp := snapshotSlotName(o.slot)
	res, err := pglogrepl.ParseCreateReplicationSlot(repl.Exec(ctx, fmt.Sprintf(
		`CREATE_REPLICATION_SLOT "%s" TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')`, tmp)))
	if err != nil {
		return fmt.Errorf("temporary slot %s for a consistent snapshot: %w", tmp, err)
	}
	if changestream.ParseLSN(res.ConsistentPoint) == 0 || res.SnapshotName == "" {
		return fmt.Errorf("temporary slot %s returned no consistent point or snapshot", tmp)
	}

	if _, err := conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(context.WithoutCancel(ctx), "ROLLBACK") }()
	if _, err := conn.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'",
		strings.ReplaceAll(res.SnapshotName, "'", "''"))); err != nil {
		return fmt.Errorf("adopt exported snapshot: %w", err)
	}

	for _, r := range plan {
		log.Warn("not resuming this table from the slot; re-snapshotting it",
			"table", r.Table, "why", r.Reason, "slot", o.slot, "at", res.ConsistentPoint)
		t0 := time.Now()
		n, err := tables[r.Table].Snapshot(ctx, conn, res.ConsistentPoint)
		if err != nil {
			return fmt.Errorf("snapshot %s: %w", r.Table, err)
		}
		log.Info("snapshot complete", "table", r.Table, "rows", n,
			"seconds", time.Since(t0).Seconds())
	}
	_, err = conn.Exec(ctx, "COMMIT")
	return err
}

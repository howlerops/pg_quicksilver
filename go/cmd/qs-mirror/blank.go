package main

// Clearing this node's reports before anything else (docs/40, "The registry is
// configuration; a node's state is its report").
//
// A node serves a registered table only when its report for that table is a
// real LSN. The reports live in the local server's shared memory, and they are
// not removed when the sidecar that made them goes away. So a sidecar that
// restarts with a shorter QS_TABLES, or whose table halts while it starts, would
// otherwise leave the previous process's "fresh" report standing, and the
// extension would keep serving that table from the lake with a relaxed max_lag.
//
// Three rules keep it closed:
//
//  1. At start (main), and at the start of every attempt (runMirror), this node
//     reports 0/0 for every table in the registry, configured or not. Nothing
//     this node can serve for them survives a restart.
//  2. A configured table is reported 0/0 until this process has vouched for it
//     (extLink.vouch, after the lake has been given its manifest). The reporter
//     never lets an earlier value stand (extension.go, lsns).
//  3. A registered table this sidecar is not configured for is 0/0 on this node
//     for as long as this sidecar runs with that configuration. The registry row
//     stays for the other sidecars; the row itself is cleaned up by the janitor
//     when no sidecar claims it (claims.go).
//
// A heap that is registered but has no entry at all is not served either: a
// report is what makes a table readable on a node, so the zero is belt and braces
// for the case where the local server restarted in between.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// blankRegisteredSQL reports 0/0 for every registered heap. Whether or not this
// sidecar mirrors it: that is what makes "not configured here" mean "not served
// here". The function is STRICT and checks registration itself.
const blankRegisteredSQL = `SELECT quicksilver.report_applied_lsn(m.heap, '0/0'::pg_lsn)
  FROM quicksilver.mirrors m`

// blankConfiguredSQL reports 0/0 for each configured heap by name. A table that
// is configured but not yet registered has no entry to clear; the statement is
// the same either way.
const blankConfiguredSQL = `SELECT quicksilver.report_applied_lsn(c.oid, '0/0'::pg_lsn)
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname || '.' || c.relname = ANY($1::text[])`

// blankOn clears this node's reports for the configured tables and every
// registered table, on an open connection to the local server. It is a no-op
// where the extension is not installed, which is the normal state of a database
// that does not route reads.
func blankOn(ctx context.Context, conn *pgx.Conn, configured []string) error {
	var have bool
	if err := conn.QueryRow(ctx, `SELECT to_regprocedure('quicksilver.report_applied_lsn(regclass,pg_lsn)') IS NOT NULL
		AND to_regclass('quicksilver.mirrors') IS NOT NULL`).Scan(&have); err != nil {
		return fmt.Errorf("asking whether pg_quicksilver is installed: %w", err)
	}
	if !have {
		return nil
	}
	if len(configured) > 0 {
		if _, err := conn.Exec(ctx, blankConfiguredSQL, configured); err != nil {
			return fmt.Errorf("clearing the reports of the configured tables: %w", err)
		}
	}
	if _, err := conn.Exec(ctx, blankRegisteredSQL); err != nil {
		return fmt.Errorf("clearing the reports of the registered tables: %w", err)
	}
	return nil
}

// blankLocal is blankOn on a connection of its own, to the local server.
func blankLocal(ctx context.Context, o options, configured []string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, o.localDSN())
	if err != nil {
		return fmt.Errorf("connecting to the local PostgreSQL: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	return blankOn(ctx, conn, configured)
}

// blankAtStart is the first thing main does. It is best effort: when the local
// server is not up yet there is nothing in its shared memory to clear either, and
// the reporter clears the registry on its first connection (extLink.run). The
// strict form is blankAttempt, which every attempt runs before it reads anything.
func blankAtStart(ctx context.Context, log *slog.Logger, o options) {
	if err := blankLocal(ctx, o, o.tables); err != nil {
		log.Info("this node's reports could not be cleared yet; the reporter will clear them "+
			"when the local PostgreSQL answers", "err", err)
	}
}

// blankAttempt is runMirror's form: the attempt does not start unless this node's
// reports are cleared, because a report left standing would be an answer about a
// mirror nobody is running. A server-side refusal (the extension is installed but
// not in shared_preload_libraries, so there is no shared memory and so no report
// to stand) does not stop mirroring: the reporter logs it, and nothing is served.
func blankAttempt(ctx context.Context, log *slog.Logger, o options) error {
	err := blankLocal(ctx, o, o.tables)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		log.Warn("the local server refused to clear this node's reports; pg_quicksilver's "+
			"shared memory may not be loaded (shared_preload_libraries), so there are none to clear",
			"err", err)
		return nil
	}
	return err
}

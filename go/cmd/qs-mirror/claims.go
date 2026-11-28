package main

// Who still mirrors a registered table (docs/40, "The registry is configuration").
//
// quicksilver.mirrors is one registry for every sidecar of the cluster, and a
// table that leaves every QS_TABLES keeps its row: the sidecars cannot tell it
// from a table another one still needs. So each sidecar keeps a CLAIM on the
// primary, quicksilver.sidecar_claims, one row per pod (its slot name), listing
// the tables it is configured for and when it last said so. A claim is written
// every claimHeartbeat by the sidecar's own reconcile, under the same advisory
// lock as every registry write (extension.go, sync), and the janitor on the
// primary removes a registry row, and its mirror relation, only when no claim
// seen within claimGrace names that table.
//
// Why removing is safe, and why the grace is what it is:
//
//   - A removed row costs nothing but a re-registration: the lake still holds the
//     table, and the next sidecar that is configured for it registers it again
//     (sync). Until then a node answers from its heap, which is always correct.
//   - A node that still has a report for the table cannot serve it once the row
//     is gone (the extension routes by registration first), so the removal ends
//     the one way a stale report could be used with a relaxed max_lag.
//   - The grace is for the sidecar that is down, not gone. Its claim is refreshed
//     every heartbeat while it runs; a pod that is down longer than claimGrace
//     loses its claim, and its rows go, and it re-registers when it returns. An
//     hour is far longer than a restart and short enough that a deleted pod stops
//     pinning a table's row.
//   - A claim row is only ever written and read under the advisory lock, so a
//     removal cannot interleave with a registration.
//   - Without the table (an extension older than this file) nothing is claimed
//     and nothing is removed: the sidecar says so once, and the registry is as it
//     was.

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	claimHeartbeat = 30 * time.Second
	claimGrace     = envDuration("QS_CLAIM_GRACE", time.Hour)
	// claimKeep is how long a claim row is kept after its last heartbeat. It is
	// only there for an operator reading the table: a claim past claimGrace has
	// no effect either way.
	claimKeep = 24 * time.Hour
)

// errClaimsAbsent means the extension on this primary has no sidecar_claims table.
var errClaimsAbsent = errors.New("quicksilver.sidecar_claims does not exist (an older pg_quicksilver)")

// claimLockSQL is the lock every registry writer takes (extension.go, sync). The
// janitor takes it too, so a removal and a claim or a registration cannot
// interleave.
const claimLockSQL = `SELECT pg_advisory_xact_lock(hashtext('quicksilver.mirrors'))`

// writeClaim says, on the primary, that the pod named node is configured for
// tables now. It does nothing else, and it is safe to call on every reconcile.
func writeClaim(ctx context.Context, conn *pgx.Conn, node string, tables []string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, claimLockSQL); err != nil {
		return err
	}
	var have bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('quicksilver.sidecar_claims') IS NOT NULL`).Scan(&have); err != nil {
		return err
	}
	if !have {
		return errClaimsAbsent
	}
	sorted := append([]string(nil), tables...)
	sort.Strings(sorted)
	if _, err := tx.Exec(ctx, `INSERT INTO quicksilver.sidecar_claims (node, tables, seen)
		VALUES ($1, $2, now())
		ON CONFLICT (node) DO UPDATE SET tables = EXCLUDED.tables, seen = EXCLUDED.seen`,
		node, sorted); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// claimer is the sidecar's side: it heartbeats its claim from reconcile, at most
// once every claimHeartbeat, and says once when it cannot.
type claimer struct {
	log    *slog.Logger
	node   string
	tables []string
	at     time.Time
	said   string
	absent bool
}

func (c *claimer) heartbeat(ctx context.Context, conn *pgx.Conn) {
	if c == nil || c.absent || time.Since(c.at) < claimHeartbeat {
		return
	}
	c.at = time.Now()
	err := writeClaim(ctx, conn, c.node, c.tables)
	switch {
	case err == nil:
		c.said = ""
	case errors.Is(err, errClaimsAbsent):
		c.absent = true
		c.log.Warn("pg_quicksilver on the primary has no sidecar claims table, so registry rows of "+
			"tables that leave every QS_TABLES are not cleaned up; update the extension to enable it",
			"err", err)
	case ctx.Err() == nil:
		if msg := err.Error(); msg != c.said {
			c.said = msg
			c.log.Warn("could not record this sidecar's claim on the registry; will retry", "err", err)
		}
	}
}

// regRow is a registered table: its heap by name and OID, and its mirror relation,
// with whether that is a view in the sidecar's own schema (the only kind the
// sidecar makes, and the only kind the janitor drops).
type regRow struct {
	Heap       string
	HeapOID    uint32
	Mirror     string
	MirrorView bool
}

// claimRow is one sidecar's claim.
type claimRow struct {
	Node   string
	Tables []string
	Seen   time.Time
}

// registryOrphans is the registered tables that no live claim names: the rows the
// janitor removes. A claim is live when it was seen within grace of now. The
// answer is sorted by heap name.
func registryOrphans(rows []regRow, claims []claimRow, now time.Time, grace time.Duration) []regRow {
	live := map[string]bool{}
	for _, c := range claims {
		if now.Sub(c.Seen) > grace {
			continue
		}
		for _, t := range c.Tables {
			live[t] = true
		}
	}
	var out []regRow
	for _, r := range rows {
		if !live[r.Heap] {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Heap < out[j].Heap })
	return out
}

const janitorRegistrySQL = `
SELECT n.nspname || '.' || c.relname, c.oid, m.mirror::text,
       COALESCE(mc.relkind = 'v' AND mn.nspname = $1, false)
  FROM quicksilver.mirrors m
  JOIN pg_catalog.pg_class c ON c.oid = m.heap
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_catalog.pg_class mc ON mc.oid = m.mirror
  LEFT JOIN pg_catalog.pg_namespace mn ON mn.oid = mc.relnamespace`

// sweepRegistry is the janitor's half: on the primary, under the registry lock,
// remove the registry rows of tables no live claim names, with their join domains
// and their mirror relations. It does nothing without the claims table.
func (j *janitor) sweepRegistry(ctx context.Context, conn *pgx.Conn, now time.Time, schema string) error {
	var have bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass('quicksilver.mirrors') IS NOT NULL
		AND to_regclass('quicksilver.sidecar_claims') IS NOT NULL`).Scan(&have); err != nil {
		return err
	}
	if !have {
		return nil
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, claimLockSQL); err != nil {
		return err
	}
	rows, err := collect(ctx, tx, janitorRegistrySQL, func(r *regRow) []any {
		return []any{&r.Heap, &r.HeapOID, &r.Mirror, &r.MirrorView}
	}, schema)
	if err != nil {
		return err
	}
	claims, err := collect(ctx, tx, `SELECT node, tables, seen FROM quicksilver.sidecar_claims`,
		func(c *claimRow) []any { return []any{&c.Node, &c.Tables, &c.Seen} })
	if err != nil {
		return err
	}
	// Claims nobody has refreshed for a long time are only noise.
	if _, err := tx.Exec(ctx, `DELETE FROM quicksilver.sidecar_claims
		WHERE seen < now() - make_interval(secs => $1)`, claimKeep.Seconds()); err != nil {
		return err
	}
	// A row goes only once it has been unclaimed for claimGrace, not at the first
	// sweep that sees it unclaimed: a rolling restart of the sidecars briefly
	// leaves a table with no live claim that the next pod will re-claim, and the
	// grace is what makes that a non-event. The clock is kept by the janitor, so
	// a janitor that restarts starts it again (later removal, never earlier).
	orphans := j.settle(registryOrphans(rows, claims, now, claimGrace), now)
	for _, o := range orphans {
		if _, err := tx.Exec(ctx, `DELETE FROM quicksilver.mirrors WHERE heap = $1::oid::regclass`, o.HeapOID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM quicksilver.join_domains WHERE heap = $1::oid::regclass`, o.HeapOID); err != nil {
			return err
		}
		if o.MirrorView {
			if _, err := tx.Exec(ctx, "DROP VIEW IF EXISTS "+o.Mirror); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, o := range orphans {
		j.log.Warn("slot janitor: removed the registry row of a table no sidecar mirrors any more "+
			"(no claim within the grace period); its reads go to the heap",
			"table", o.Heap, "grace", claimGrace)
	}
	return nil
}

// settle keeps, per heap, when it was first seen unclaimed, and returns the
// orphans that have been unclaimed for at least claimGrace. A heap that is claimed
// again is forgotten.
func (j *janitor) settle(orphans []regRow, now time.Time) []regRow {
	if j.unclaimed == nil {
		j.unclaimed = map[string]time.Time{}
	}
	seen := map[string]bool{}
	var out []regRow
	for _, o := range orphans {
		seen[o.Heap] = true
		since, ok := j.unclaimed[o.Heap]
		if !ok {
			j.unclaimed[o.Heap] = now
			since = now
		}
		if now.Sub(since) >= claimGrace {
			out = append(out, o)
		}
	}
	for h := range j.unclaimed {
		if !seen[h] {
			delete(j.unclaimed, h)
		}
	}
	return out
}

// collect runs one query in tx and scans every row into a T.
func collect[T any](ctx context.Context, tx pgx.Tx, sql string, scan func(*T) []any, args ...any) ([]T, error) {
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		var v T
		if err := rows.Scan(scan(&v)...); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// qs-slotsync answers the question docs/14 section C left open.
//
// On PostgreSQL 16 we measured that a logical replication slot does NOT survive
// a promotion: the slot is simply absent on the new primary, and the only
// recovery is an O(table size) re-snapshot. PostgreSQL 17 added logical slot
// synchronisation — the `FAILOVER` slot option plus `sync_replication_slots` on
// the standby — which is supposed to make the slot follow the promotion.
//
// "Supposed to" is exactly the kind of claim this project does not take on
// trust, and the failure mode is silent data loss, so the test has to prove
// three separate things:
//
//  1. the slot EXISTS on the new primary after a real promotion;
//
//  2. streaming RESUMES from it with no re-snapshot;
//
//  3. rows committed on the old primary AFTER the last confirmed LSN still
//     arrive. This is the one that matters. A slot that exists but has been
//     fast-forwarded to the current LSN is worse than no slot at all: the
//     consumer reconnects happily and silently loses everything in between.
//
//     go run ./cmd/qs-slotsync
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const (
	pg17       = "/usr/lib/postgresql/17/bin"
	base       = "/var/lib/postgresql/qs17"
	primaryDir = base + "/primary"
	standbyDir = base + "/standby"
	primaryDSN = "postgres://postgres@/postgres?host=/tmp&port=5443"
	standbyDSN = "postgres://postgres@/postgres?host=/tmp&port=5444"
	root       = base + "/mirror"

	slot      = "qs_sync_slot"
	physSlot  = "qs_standby_slot"
	pub       = "qs_sync_pub"
	tableName = "public.qs_sync"
)

var (
	cols  = map[string]string{"id": "integer", "sku": "text", "price": "numeric(12,2)"}
	order = []string{"id", "sku", "price"}
)

func main() {
	ctx := context.Background()
	var failures []string

	_ = os.RemoveAll(root)
	_ = os.MkdirAll(root, 0o755)

	conn, err := pgx.Connect(ctx, primaryDSN)
	must(err)

	fmt.Println("========================================================================")
	fmt.Println("0. Preconditions on the primary")
	fmt.Println("========================================================================")
	var ver string
	must(conn.QueryRow(ctx, "SHOW server_version_num").Scan(&ver))
	vernum, _ := strconv.Atoi(ver)
	fmt.Printf("  server_version_num = %d\n", vernum)
	if vernum < 170000 {
		fmt.Println("\nINCOMPLETE — this test requires PostgreSQL >= 17. Slot synchronisation")
		fmt.Println("does not exist before 17; running it on 16 would prove nothing.")
		os.Exit(2)
	}

	exec1(ctx, conn, `DROP TABLE IF EXISTS qs_sync CASCADE`)
	exec1(ctx, conn, `CREATE TABLE qs_sync(id int primary key, sku text, price numeric(12,2))`)
	exec1(ctx, conn, `DROP PUBLICATION IF EXISTS `+pub)
	exec1(ctx, conn, `CREATE PUBLICATION `+pub+` FOR TABLE qs_sync`)
	exec1(ctx, conn, `INSERT INTO qs_sync
		SELECT g, 'SKU-'||g, (g%1000)/10.0 FROM generate_series(1,3000) g`)

	// The physical slot the standby will hold. Without it the primary can
	// recycle WAL the standby still needs, and slot sync refuses to run.
	exec1(ctx, conn, `SELECT pg_drop_replication_slot('`+physSlot+`')
		FROM pg_replication_slots WHERE slot_name='`+physSlot+`'`)
	exec1(ctx, conn, `SELECT pg_create_physical_replication_slot('`+physSlot+`', true)`)

	// synchronized_standby_slots is the half of the feature that is easy to
	// forget and impossible to notice until a failover. It makes logical
	// decoding on the primary WAIT for the standby, so a logical consumer can
	// never be ahead of the node that will replace the primary. Without it the
	// synced slot is behind the consumer at promotion and rows are lost.
	exec1(ctx, conn, `ALTER SYSTEM SET synchronized_standby_slots = '`+physSlot+`'`)
	exec1(ctx, conn, `SELECT pg_reload_conf()`)
	fmt.Printf("  physical slot %q created; synchronized_standby_slots set\n", physSlot)

	fmt.Println("\n========================================================================")
	fmt.Println("1. Logical slot created WITH FAILOVER, snapshot, stream")
	fmt.Println("========================================================================")

	st, err := changestream.NewStreaming(ctx, primaryDSN, slot, pub, []string{tableName})
	must(err)
	st.Failover = true
	_ = st.DropSlot(ctx)
	consistent, created, err := st.CreateSlot(ctx)
	must(err)
	if !created {
		failures = append(failures, "slot already existed; test needs a fresh one")
	}

	var failoverFlag bool
	must(conn.QueryRow(ctx,
		`SELECT failover FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&failoverFlag))
	fmt.Printf("  slot %q created at %s, failover=%v\n", slot, consistent, failoverFlag)
	if !failoverFlag {
		failures = append(failures, "slot was not created with failover=true")
	}

	m, err := mirror.New(root, "public", "qs_sync", "id", cols, order)
	must(err)
	n, err := m.Snapshot(ctx, conn, consistent.String())
	must(err)
	fmt.Printf("  snapshot: %d rows at the consistent point\n", n)
	must(st.Start(ctx, consistent))

	fmt.Println("\n========================================================================")
	fmt.Println("2. Standby with sync_replication_slots = on")
	fmt.Println("========================================================================")
	if err := buildStandby(); err != nil {
		fmt.Println("  standby build failed:", err)
		report(failures, "could not build a standby")
	}
	sconn, err := pgx.Connect(ctx, standbyDSN)
	must(err)
	fmt.Println("  standby up, in recovery")

	// drive some traffic so the slot has a confirmed position worth syncing
	applied := drain(ctx, st, m, 0)
	writeRows(ctx, conn, 100000, 400)
	applied += drain(ctx, st, m, 3*time.Second)
	fmt.Printf("  applied %d transactions; slot confirmed through %s\n", applied, lastConfirm)

	synced := waitForSyncedSlot(ctx, sconn, 60*time.Second)
	if !synced {
		failures = append(failures, "logical slot never appeared on the standby as synced")
		report(failures, "slot sync did not happen; nothing downstream can be tested")
	}
	var syncLSN string
	must(sconn.QueryRow(ctx,
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`,
		slot).Scan(&syncLSN))
	fmt.Printf("  synced copy present on standby, confirmed_flush_lsn=%s\n", syncLSN)

	fmt.Println("\n========================================================================")
	fmt.Println("3. Rows committed AFTER the last confirm, then a real promotion")
	fmt.Println("========================================================================")

	// These are the rows the whole feature exists to protect: committed on the
	// old primary, never confirmed by us, and therefore recoverable only if the
	// synced slot carries a position BEHIND them.
	writeRows(ctx, conn, 200000, 500)
	var srcTotal int
	must(conn.QueryRow(ctx, `SELECT count(*) FROM qs_sync`).Scan(&srcTotal))
	mrows, err := m.Live()
	must(err)
	fmt.Printf("  source now %d rows; mirror holds %d (%d rows in flight)\n",
		srcTotal, len(mrows), srcTotal-len(mrows))
	if srcTotal-len(mrows) <= 0 {
		failures = append(failures,
			"no rows were actually in flight; the test would pass vacuously")
		report(failures, "could not create the in-flight window")
	}
	inFlight := srcTotal - len(mrows)

	_ = st.Close(ctx)
	fmt.Println("  consumer disconnected (simulating the node that dies with the primary)")

	if out, err := runCmd("su", "postgres", "-c",
		fmt.Sprintf("%s/pg_ctl -D %s promote -w", pg17, standbyDir)); err != nil {
		fmt.Println("  promote failed:", out)
		report(failures, "promotion failed")
	}
	// the old primary must go away, or we are testing split brain
	_, _ = runCmd("su", "postgres", "-c",
		fmt.Sprintf("%s/pg_ctl -D %s stop -m immediate", pg17, primaryDir))
	conn.Close(ctx)

	var inRecovery bool
	must(sconn.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery))
	fmt.Printf("  promoted: pg_is_in_recovery()=%v\n", inRecovery)

	fmt.Println("\n========================================================================")
	fmt.Println("4. Does the slot exist on the new primary, and where does it point?")
	fmt.Println("========================================================================")
	var present int
	var newLSN, slotSynced string
	must(sconn.QueryRow(ctx,
		`SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&present))
	fmt.Printf("  logical slot present on new primary: %d\n", present)
	if present == 0 {
		failures = append(failures, "slot did not survive promotion even on PG17")
		report(failures)
	}
	must(sconn.QueryRow(ctx,
		`SELECT confirmed_flush_lsn::text, synced::text
		   FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&newLSN, &slotSynced))
	fmt.Printf("  confirmed_flush_lsn=%s  synced=%s\n", newLSN, slotSynced)
	fmt.Printf("  our last confirm was  %s\n", lastConfirm)

	// The trap. synchronized_standby_slots is inherited by the standby through
	// postgresql.auto.conf, and it still names the PHYSICAL slot that the OLD
	// primary held for this node. That slot does not exist here. Logical
	// decoding then WAITS — forever, with a warning in the log and no error to
	// the client. The stream opens, reports the right starting LSN, and returns
	// nothing.
	//
	// So the GUC that makes slot sync safe before a failover is the GUC that
	// deadlocks the mirror after one. Clearing it is part of promotion, and it
	// has to be done by whatever drives the failover — for us, the plugin.
	var stale string
	must(sconn.QueryRow(ctx, `SHOW synchronized_standby_slots`).Scan(&stale))
	if stale != "" {
		var orphan int
		must(sconn.QueryRow(ctx,
			`SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1`,
			stale).Scan(&orphan))
		fmt.Printf("  synchronized_standby_slots=%q, and that slot exists here: %v\n",
			stale, orphan > 0)
		if orphan == 0 {
			fmt.Println("  -> decoding would block indefinitely; clearing it as promotion must")
			exec1(ctx, sconn, `ALTER SYSTEM SET synchronized_standby_slots = ''`)
			exec1(ctx, sconn, `SELECT pg_reload_conf()`)
		}
	}

	fmt.Println("\n========================================================================")
	fmt.Println("5. Resume — no re-snapshot — and account for every in-flight row")
	fmt.Println("========================================================================")
	before, err := m.Live()
	must(err)
	st2, err := changestream.NewStreaming(ctx, standbyDSN, slot, pub, []string{tableName})
	must(err)
	exists, err := st2.SlotExists(ctx)
	must(err)
	if !exists {
		failures = append(failures, "SlotExists says no on the new primary")
		report(failures)
	}
	// from = 0 means "resume at the server's confirmed position"
	must(st2.Start(ctx, 0))
	got := drain(ctx, st2, m, 8*time.Second)
	after, err := m.Live()
	must(err)
	fmt.Printf("  resumed without re-snapshot: %d transactions, mirror %d -> %d rows\n",
		got, len(before), len(after))

	if !converged(ctx, sconn, m) {
		failures = append(failures, "mirror did not converge after resuming on the new primary")
	}
	if len(after)-len(before) < inFlight {
		failures = append(failures, fmt.Sprintf(
			"recovered %d rows but %d were in flight — the slot was fast-forwarded and rows were LOST",
			len(after)-len(before), inFlight))
	} else {
		fmt.Printf("  all %d in-flight rows recovered from the synced slot\n", inFlight)
	}

	// and it keeps working: write to the new primary and follow it
	writeRows(ctx, sconn, 300000, 200)
	drain(ctx, st2, m, 4*time.Second)
	if !converged(ctx, sconn, m) {
		failures = append(failures, "mirror diverged on continued writes to the new primary")
	}
	_ = st2.Close(ctx)

	report(failures)
}

var lastConfirm string

func drain(ctx context.Context, st *changestream.Streaming, m *mirror.Table, d time.Duration) int {
	total := 0
	deadline := time.Now().Add(d)
	idle := 0
	for {
		txns, err := st.Transactions(ctx)
		must(err)
		if len(txns) == 0 {
			if time.Now().After(deadline) {
				idle++
				if idle > 3 {
					return total
				}
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		idle = 0
		_, err = m.Apply(txns)
		must(err)
		last := txns[len(txns)-1].NextLSN
		must(st.Confirm(ctx, last))
		lastConfirm = last
		total += len(txns)
	}
}

func writeRows(ctx context.Context, conn *pgx.Conn, from, n int) {
	for i := 0; i < n; i++ {
		id := from + i
		_, err := conn.Exec(ctx,
			`INSERT INTO qs_sync VALUES ($1,$2,$3)
			 ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price`,
			id, fmt.Sprintf("SKU-%d", id), fmt.Sprintf("%d.50", id%97))
		must(err)
	}
}

func waitForSyncedSlot(ctx context.Context, sconn *pgx.Conn, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		var n int
		if err := sconn.QueryRow(ctx,
			`SELECT count(*) FROM pg_replication_slots
			  WHERE slot_name=$1 AND synced AND confirmed_flush_lsn IS NOT NULL`,
			slot).Scan(&n); err == nil && n == 1 {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func converged(ctx context.Context, conn *pgx.Conn, m *mirror.Table) bool {
	rows, err := m.Live()
	must(err)
	mc, mh := mirror.Checksum(rows, m.Order)
	src, err := sourceRows(ctx, conn, m.Qualified, m.Order)
	must(err)
	sc, sh := mirror.Checksum(src, m.Order)
	ok := mc == sc && mh == sh
	verdict := "MATCH"
	if !ok {
		verdict = "DIVERGED"
	}
	fmt.Printf("  mirror %d rows / source %d rows -> %s\n", mc, sc, verdict)
	return ok
}

func sourceRows(ctx context.Context, conn *pgx.Conn, qualified string, order []string) ([]map[string]any, error) {
	sel := ""
	for i, c := range order {
		if i > 0 {
			sel += ", "
		}
		sel += `"` + c + `"::text`
	}
	rows, err := conn.Query(ctx, "SELECT "+sel+" FROM "+qualified)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(order))
		ptrs := make([]any, len(order))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		mm := make(map[string]any, len(order))
		for i, c := range order {
			mm[c] = vals[i]
		}
		out = append(out, mm)
	}
	return out, rows.Err()
}

func buildStandby() error {
	_, _ = runCmd("su", "postgres", "-c",
		fmt.Sprintf("%s/pg_ctl -D %s stop -m immediate", pg17, standbyDir))
	_ = os.RemoveAll(standbyDir)
	// -S binds the standby to the physical slot; -R writes primary_conninfo and
	// primary_slot_name. primary_conninfo MUST carry dbname or the slot sync
	// worker has no database to connect to and silently does nothing.
	if out, err := runCmd("su", "postgres", "-c", fmt.Sprintf(
		"%s/pg_basebackup -D %s -R -X stream -S %s -d 'host=/tmp port=5443 user=postgres dbname=postgres'",
		pg17, standbyDir, physSlot)); err != nil {
		return fmt.Errorf("basebackup: %v %s", err, out)
	}
	f, err := os.OpenFile(standbyDir+"/postgresql.auto.conf", os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, _ = f.WriteString("\nport = 5444\nsync_replication_slots = on\nhot_standby_feedback = on\n")
	_ = f.Close()
	if out, err := runCmd("su", "postgres", "-c", fmt.Sprintf(
		"%s/pg_ctl -D %s -l %s/standby.log -w start", pg17, standbyDir, standbyDir)); err != nil {
		return fmt.Errorf("start: %v %s", err, out)
	}
	time.Sleep(2 * time.Second)
	return nil
}

func runCmd(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func exec1(ctx context.Context, conn *pgx.Conn, sql string) {
	_, err := conn.Exec(ctx, sql)
	must(err)
}

func report(failures []string, skipped ...string) {
	fmt.Println("\n========================================================================")
	if len(skipped) > 0 {
		fmt.Println("INCOMPLETE — section skipped, which is NOT a pass:")
		for _, s := range skipped {
			fmt.Println("  - " + s)
		}
		for _, f := range failures {
			fmt.Println("  FAIL: " + f)
		}
		os.Exit(2)
	}
	if len(failures) == 0 {
		fmt.Println("PASS")
		return
	}
	fmt.Println("FAIL:")
	for _, f := range failures {
		fmt.Println("  - " + f)
	}
	os.Exit(1)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// qs-phase2 exercises three things that Phase 1 left open, together, because
// they are the same story: how the mirror stays correct when the source moves
// under it.
//
//	A. pgoutput over STREAMING replication (ADR-0009 items 1 and 2).
//	   No wal2json, no poll interval floor.
//	B. Initial snapshot that joins the stream with neither gap nor overlap.
//	C. FAILOVER. Promote a real standby and find out what actually happens to
//	   the logical slot and the mirror. This is the docs/05 lifecycle row most
//	   likely to break in production.
//
//	go run ./cmd/qs-phase2
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const (
	primaryDSN = "postgres://postgres@/postgres?host=/tmp&port=5433"
	standbyDSN = "postgres://postgres@/postgres?host=/tmp&port=5434"
	root       = "/var/lib/postgresql/qsbench/mirror_go_p2"
	slot       = "qs_p2_slot"
	pub        = "qs_p2_pub"
	pgBin      = "/usr/lib/postgresql/16/bin"
	standbyDir = "/var/lib/postgresql/qsbench/standby"
)

var (
	cols  = map[string]string{"id": "integer", "sku": "text", "price": "numeric(12,2)"}
	order = []string{"id", "sku", "price"}
	stop  atomic.Bool
)

func main() {
	skipFailover := flag.Bool("skip-failover", false, "run only A and B")
	flag.Parse()
	ctx := context.Background()
	var failures []string

	_ = os.RemoveAll(root)
	_ = os.MkdirAll(root, 0o755)

	conn, err := pgx.Connect(ctx, primaryDSN)
	must(err)
	defer conn.Close(ctx)

	must(exec2(ctx, conn, `DROP TABLE IF EXISTS qs_p2 CASCADE;
		CREATE TABLE qs_p2(id int primary key, sku text, price numeric(12,2))`))
	must(exec2(ctx, conn, `DROP PUBLICATION IF EXISTS `+pub))
	must(exec2(ctx, conn, `CREATE PUBLICATION `+pub+` FOR TABLE qs_p2`))

	// seed rows BEFORE the slot exists, so the snapshot has something the
	// stream will never deliver — that is what makes B a real test
	must(exec2(ctx, conn, `INSERT INTO qs_p2
		SELECT g, 'SKU-'||g, (g%1000)/10.0 FROM generate_series(1,5000) g`))

	fmt.Println("========================================================================")
	fmt.Println("A + B. pgoutput streaming, bootstrapped from a snapshot")
	fmt.Println("========================================================================")

	st, err := changestream.NewStreaming(ctx, primaryDSN, slot, pub, []string{"public.qs_p2"})
	must(err)
	_ = st.DropSlot(ctx)
	consistent, created, err := st.CreateSlot(ctx)
	must(err)
	fmt.Printf("  slot created=%v consistent point=%s\n", created, consistent)

	m, err := mirror.New(root, "public", "qs_p2", "id", cols, order)
	must(err)

	// snapshot FIRST, then stream from the consistent point: overlap is
	// harmless for an upsert store, a gap is not
	n, err := m.Snapshot(ctx, conn, consistent.String())
	must(err)
	fmt.Printf("  snapshot: %d rows at %s\n", n, consistent)

	must(st.Start(ctx, consistent))
	fmt.Println("  streaming started (no poll interval, no wal2json)")

	var wg sync.WaitGroup
	wg.Add(1)
	go writer(ctx, primaryDSN, 5000, &wg)

	pump := func() int {
		txns, err := st.Transactions(ctx)
		must(err)
		if len(txns) == 0 {
			return 0
		}
		_, err = m.Apply(txns)
		must(err)
		must(st.Confirm(ctx, txns[len(txns)-1].NextLSN))
		return len(txns)
	}

	total := 0
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		total += pump()
	}
	stop.Store(true)
	wg.Wait()
	for i := 0; i < 20; i++ {
		if pump() == 0 {
			time.Sleep(150 * time.Millisecond)
		}
	}
	fmt.Printf("  applied %d streamed transactions\n", total)
	if !converged(ctx, conn, m, "  ") {
		failures = append(failures, "did not converge after snapshot + stream")
	}

	// latency: streaming should beat the old 400ms poll floor outright
	lat := measureLatency(ctx, conn, st, m)
	fmt.Printf("  commit -> visible in mirror: %.0f ms (polling floor was the batch interval)\n", lat)

	if *skipFailover {
		report(failures)
		return
	}

	fmt.Println("\n========================================================================")
	fmt.Println("C. Failover — promote a real standby, see what survives")
	fmt.Println("========================================================================")
	stop.Store(false)

	if err := buildStandby(ctx); err != nil {
		report(failures, fmt.Sprintf("could not build standby: %v", err))
		return
	}
	fmt.Println("  standby built and streaming from primary")

	// does the logical slot exist on the standby? On PG16 it does not:
	// logical slot failover (the `failover` slot option plus
	// sync_replication_slots) landed in PG17.
	sconn, err := pgx.Connect(ctx, standbyDSN)
	if err != nil {
		report(failures, fmt.Sprintf("standby unreachable: %v", err))
		return
	}
	defer sconn.Close(ctx)

	var slotsOnStandby int
	_ = sconn.QueryRow(ctx,
		`SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&slotsOnStandby)
	fmt.Printf("  logical slot present on standby BEFORE promotion: %d\n", slotsOnStandby)

	fmt.Println("  promoting standby ...")
	if out, err := run("su", "postgres", "-c",
		fmt.Sprintf("%s/pg_ctl -D %s promote -w", pgBin, standbyDir)); err != nil {
		report(failures, fmt.Sprintf("promote failed: %v %s", err, out))
		return
	}
	time.Sleep(2 * time.Second)

	var inRecovery bool
	_ = sconn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	fmt.Printf("  promoted: pg_is_in_recovery()=%v\n", inRecovery)

	_ = sconn.QueryRow(ctx,
		`SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&slotsOnStandby)
	fmt.Printf("  logical slot present on new primary AFTER promotion: %d\n", slotsOnStandby)

	// the mirror must DETECT this rather than silently stall
	if slotsOnStandby == 0 {
		fmt.Println("\n  -> slot did NOT survive failover (expected on PG16; logical slot")
		fmt.Println("     failover needs PG17's `failover` slot option + sync_replication_slots)")
		fmt.Println("  -> recovery path: re-snapshot against the new primary")

		st2, err := changestream.NewStreaming(ctx, standbyDSN, slot, pub, []string{"public.qs_p2"})
		if err != nil {
			failures = append(failures, "could not connect to new primary: "+err.Error())
			report(failures)
			return
		}
		cp, _, err := st2.CreateSlot(ctx)
		if err != nil {
			failures = append(failures, "could not create slot on new primary: "+err.Error())
			report(failures)
			return
		}
		nrows, err := m.Snapshot(ctx, sconn, cp.String())
		if err != nil {
			failures = append(failures, "re-snapshot failed: "+err.Error())
			report(failures)
			return
		}
		fmt.Printf("  re-snapshotted %d rows at %s\n", nrows, cp)
		must(st2.Start(ctx, cp))

		wg.Add(1)
		go writer(ctx, standbyDSN, 20000, &wg)
		end := time.Now().Add(5 * time.Second)
		for time.Now().Before(end) {
			time.Sleep(200 * time.Millisecond)
			if txns, err := st2.Transactions(ctx); err == nil && len(txns) > 0 {
				_, _ = m.Apply(txns)
				_ = st2.Confirm(ctx, txns[len(txns)-1].NextLSN)
			}
		}
		stop.Store(true)
		wg.Wait()
		for i := 0; i < 20; i++ {
			txns, _ := st2.Transactions(ctx)
			if len(txns) == 0 {
				time.Sleep(150 * time.Millisecond)
				continue
			}
			_, _ = m.Apply(txns)
			_ = st2.Confirm(ctx, txns[len(txns)-1].NextLSN)
		}
		if !converged(ctx, sconn, m, "  ") {
			failures = append(failures, "did not converge against new primary after failover")
		}
		_ = st2.Close(ctx)
	} else {
		fmt.Println("  -> slot survived; stream can be resumed without a re-snapshot")
	}

	_ = st.Close(ctx)
	report(failures)
}

func measureLatency(ctx context.Context, conn *pgx.Conn, st *changestream.Streaming, m *mirror.Table) float64 {
	id := 900000 + rand.Intn(10000)
	t0 := time.Now()
	_, _ = conn.Exec(ctx, `INSERT INTO qs_p2 VALUES ($1,'latency','1.00')`, id)
	for time.Since(t0) < 5*time.Second {
		txns, err := st.Transactions(ctx)
		if err == nil && len(txns) > 0 {
			_, _ = m.Apply(txns)
			_ = st.Confirm(ctx, txns[len(txns)-1].NextLSN)
			rows, _ := m.Live()
			for _, r := range rows {
				if fmt.Sprint(r["id"]) == fmt.Sprint(id) {
					return float64(time.Since(t0).Milliseconds())
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return -1
}

func converged(ctx context.Context, conn *pgx.Conn, m *mirror.Table, indent string) bool {
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
	fmt.Printf("%smirror %d rows / source %d rows -> %s\n", indent, mc, sc, verdict)
	return ok
}

// buildStandby makes a real streaming standby with pg_basebackup.
func buildStandby(ctx context.Context) error {
	_, _ = run("su", "postgres", "-c",
		fmt.Sprintf("%s/pg_ctl -D %s stop -m immediate", pgBin, standbyDir))
	_ = os.RemoveAll(standbyDir)
	if out, err := run("su", "postgres", "-c",
		fmt.Sprintf("%s/pg_basebackup -h /tmp -p 5433 -U postgres -D %s -R -X stream",
			pgBin, standbyDir)); err != nil {
		return fmt.Errorf("basebackup: %v %s", err, out)
	}
	conf := standbyDir + "/postgresql.auto.conf"
	f, err := os.OpenFile(conf, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, _ = f.WriteString("\nport = 5434\n")
	_ = f.Close()
	if out, err := run("su", "postgres", "-c",
		fmt.Sprintf("%s/pg_ctl -D %s -l %s/standby.log -w start", pgBin, standbyDir, standbyDir)); err != nil {
		return fmt.Errorf("start: %v %s", err, out)
	}
	time.Sleep(2 * time.Second)
	return nil
}

func run(name string, args ...string) (string, error) {
	c := exec.Command(name, args...)
	out, err := c.CombinedOutput()
	return strings.TrimSpace(string(out)), err
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

func writer(ctx context.Context, dsn string, base int, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	n := base
	for !stop.Load() {
		n++
		_, _ = conn.Exec(ctx,
			`INSERT INTO qs_p2 VALUES ($1,$2,$3)
			 ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price`,
			n, fmt.Sprintf("SKU-%d", n), fmt.Sprintf("%.2f", rand.Float64()*99))
		if n%5 == 0 {
			_, _ = conn.Exec(ctx, `DELETE FROM qs_p2 WHERE id=$1`, n-3)
		}
		time.Sleep(8 * time.Millisecond)
	}
}

func exec2(ctx context.Context, conn *pgx.Conn, sql string) error {
	_, err := conn.Exec(ctx, sql)
	return err
}

func report(failures []string, skipped ...string) {
	if len(skipped) > 0 {
		fmt.Println("\n========================================================================")
		fmt.Println("INCOMPLETE — section skipped, which is NOT a pass:")
		for _, sk := range skipped {
			fmt.Println("  - " + sk)
		}
		if len(failures) > 0 {
			fmt.Println("FAIL:")
			for _, f := range failures {
				fmt.Println("  - " + f)
			}
		}
		os.Exit(2)
	}
	fmt.Println("\n========================================================================")
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

var _ = pglogrepl.LSN(0)

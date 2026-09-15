// qs-slice is the Go port of the Python convergence harness.
//
// Proves the property docs/06 says kills CDC products if wrong: the mirror
// converges to the source exactly, under concurrent writes, and is never
// observed holding half a transaction.
//
//	go run ./cmd/qs-slice -seconds 20
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const (
	dsn  = "postgres://postgres@/postgres?host=/tmp&port=5433"
	root = "/var/lib/postgresql/qsbench/mirror_go"
	slot = "quicksilver_go_slice"
)

var cols = map[string]string{
	"id": "integer", "tenant_id": "integer",
	"amount": "numeric(12,2)", "status": "text",
}
var order = []string{"id", "tenant_id", "amount", "status"}

const ddlInit = `
DROP TABLE IF EXISTS qs_orders, qs_orders_archive;
CREATE TABLE qs_orders(id int primary key, tenant_id int, amount numeric(12,2), status text);
CREATE TABLE qs_orders_archive(id int primary key, tenant_id int, amount numeric(12,2), status text);
`

func main() {
	seconds := flag.Float64("seconds", 20, "run duration")
	batchMS := flag.Float64("batch-ms", 400, "micro-batch interval")
	compactEvery := flag.Int("compact-every", 8, "compact every N batches")
	flag.Parse()

	ctx := context.Background()
	if err := os.RemoveAll(root); err != nil {
		fail(err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		fail(err)
	}

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fail(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, ddlInit); err != nil {
		fail(err)
	}

	stream := changestream.NewLogical(admin, slot,
		[]string{"public.qs_orders", "public.qs_orders_archive"})
	_ = stream.DropSlot(ctx)
	if err := stream.EnsureSlot(ctx); err != nil {
		fail(err)
	}

	orders, err := mirror.New(root, "public", "qs_orders", "id", cols, order)
	if err != nil {
		fail(err)
	}
	archive, err := mirror.New(root, "public", "qs_orders_archive", "id", cols, order)
	if err != nil {
		fail(err)
	}
	mirrors := []*mirror.Table{orders, archive}

	var stop atomic.Bool
	var moved atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go writer(ctx, &stop, &moved, &wg)

	fmt.Printf("%5s%7s%9s%9s%9s%9s  applied_lsn\n",
		"t", "txns", "upserts", "deletes", "lag_ms", "slot_MB")

	deadline := time.Now().Add(time.Duration(*seconds * float64(time.Second)))
	rounds, totTxn, violations := 0, 0, 0
	for time.Now().Before(deadline) {
		time.Sleep(time.Duration(*batchMS * float64(time.Millisecond)))
		t0 := time.Now()
		txns, err := stream.Transactions(ctx)
		if err != nil {
			fail(err)
		}
		if len(txns) == 0 {
			continue
		}
		up, del := 0, 0
		for _, m := range mirrors {
			st, err := m.Apply(txns)
			if err != nil {
				fail(err)
			}
			up += st.Upserts
			del += st.Deletes
		}
		last := txns[len(txns)-1]
		if err := stream.Confirm(ctx, last.NextLSN); err != nil {
			fail(err)
		}
		totTxn += len(txns)
		rounds++

		lagMB := 0.0
		if b, err := stream.SlotLagBytes(ctx); err == nil {
			lagMB = float64(b) / 1e6
		}
		fmt.Printf("%5.2f%7d%9d%9d%9.0f%9.1f  %s\n",
			time.Since(t0).Seconds(), len(txns), up, del,
			float64(time.Since(t0).Milliseconds()), lagMB, last.CommitLSN)

		// Atomicity probe after EVERY batch, not just at the end: a row visible
		// in both tables means a cross-table move was applied half-way.
		if n, err := bothTables(orders, archive); err == nil && n > 0 {
			violations++
			fmt.Printf("  !! ATOMICITY VIOLATION: %d rows in both tables\n", n)
		}

		if rounds%*compactEvery == 0 {
			for _, m := range mirrors {
				if _, err := m.Compact(); err != nil {
					fail(err)
				}
			}
		}
	}
	stop.Store(true)
	wg.Wait()

	for i := 0; i < 5; i++ { // drain
		txns, err := stream.Transactions(ctx)
		if err != nil || len(txns) == 0 {
			break
		}
		for _, m := range mirrors {
			if _, err := m.Apply(txns); err != nil {
				fail(err)
			}
		}
		_ = stream.Confirm(ctx, txns[len(txns)-1].NextLSN)
		totTxn += len(txns)
	}

	fmt.Printf("\napplied %d transactions; %d cross-table moves\n\n", totTxn, moved.Load())
	fmt.Println("=== convergence check (goal G7) ===")
	ok := true
	for _, m := range mirrors {
		rows, err := m.Live()
		if err != nil {
			fail(err)
		}
		mc, mh := mirror.Checksum(rows, order)
		src, err := sourceRows(ctx, admin, m.Qualified, order)
		if err != nil {
			fail(err)
		}
		sc, sh := mirror.Checksum(src, order)
		match := mc == sc && mh == sh
		ok = ok && match
		verdict := "MATCH"
		if !match {
			verdict = "DIVERGED"
		}
		fmt.Printf("  %-26s mirror %6d rows  source %6d rows   %s\n",
			m.Qualified, mc, sc, verdict)
	}

	fmt.Println("\n=== cross-table atomicity ===")
	fmt.Printf("  per-batch probes: %d, violations observed: %d\n", rounds, violations)
	var srcDupes int
	_ = admin.QueryRow(ctx,
		`SELECT count(*) FROM qs_orders o JOIN qs_orders_archive a USING (id)`).Scan(&srcDupes)
	n, _ := bothTables(orders, archive)
	status := "OK"
	if n != srcDupes {
		status = "ATOMICITY VIOLATION"
	}
	fmt.Printf("  rows in BOTH tables — source %d, mirror %d  %s\n", srcDupes, n, status)
	ok = ok && n == srcDupes && violations == 0

	_ = stream.DropSlot(ctx)
	if ok {
		fmt.Println("\nPASS — mirror converged")
		return
	}
	fmt.Println("\nFAIL")
	os.Exit(1)
}

func bothTables(a, b *mirror.Table) (int, error) {
	ra, err := a.Live()
	if err != nil {
		return 0, err
	}
	rb, err := b.Live()
	if err != nil {
		return 0, err
	}
	seen := make(map[string]bool, len(ra))
	for _, r := range ra {
		seen[fmt.Sprint(r["id"])] = true
	}
	n := 0
	for _, r := range rb {
		if seen[fmt.Sprint(r["id"])] {
			n++
		}
	}
	return n, nil
}

func sourceRows(ctx context.Context, conn *pgx.Conn, qualified string, order []string) ([]map[string]any, error) {
	sql := "SELECT "
	for i, c := range order {
		if i > 0 {
			sql += ", "
		}
		sql += `"` + c + `"::text`
	}
	sql += " FROM " + qualified
	rows, err := conn.Query(ctx, sql)
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
		m := make(map[string]any, len(order))
		for i, c := range order {
			m[c] = vals[i]
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func writer(ctx context.Context, stop *atomic.Bool, moved *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	next := 0
	for !stop.Load() {
		tx, err := conn.Begin(ctx)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		for i := 0; i < 1+rand.Intn(20); i++ {
			next++
			_, _ = tx.Exec(ctx,
				`INSERT INTO qs_orders VALUES ($1,$2,$3,'new')`,
				next, 1+rand.Intn(5), fmt.Sprintf("%.2f", rand.Float64()*500))
		}
		_, _ = tx.Exec(ctx, `UPDATE qs_orders SET amount = amount + 1, status='upd'
			WHERE id IN (SELECT id FROM qs_orders ORDER BY random() LIMIT 5)`)
		_, _ = tx.Exec(ctx, `DELETE FROM qs_orders WHERE id IN
			(SELECT id FROM qs_orders ORDER BY random() LIMIT 2)`)
		_ = tx.Commit(ctx)

		// cross-table move in ONE transaction: must never be half-visible
		tag, err := conn.Exec(ctx, `WITH picked AS (
			  SELECT id, tenant_id, amount, status FROM qs_orders ORDER BY random() LIMIT 2),
			ins AS (
			  INSERT INTO qs_orders_archive SELECT * FROM picked
			  ON CONFLICT (id) DO NOTHING RETURNING id)
			DELETE FROM qs_orders WHERE id IN (SELECT id FROM picked)`)
		if err == nil {
			moved.Add(tag.RowsAffected())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

// qs-phase1 is the Go port of the Python DDL + readiness harness.
//
//	A. DDL is handled, not survived. An ALTER TABLE lands mid-stream as a
//	   barrier; the mirror evolves, old Parquet files NULL-fill, and it still
//	   converges. An unsafe retype is REFUSED rather than applied.
//	B. Readiness gates on lag. Stall the ingest and the pod goes NOT READY
//	   within the SLO; resume and it recovers.
//
//	go run ./cmd/qs-phase1 -slo 3
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/ddl"
	"github.com/howlerops/pg_quicksilver/go/internal/health"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const (
	dsn  = "postgres://postgres@/postgres?host=/tmp&port=5433"
	root = "/var/lib/postgresql/qsbench/mirror_go_p1"
	slot = "quicksilver_go_p1"
	addr = "127.0.0.1:8091"
)

var (
	stop   atomic.Bool
	paused atomic.Bool
)

func main() {
	sloFlag := flag.Float64("slo", 3, "freshness SLO seconds")
	flag.Parse()
	slo := time.Duration(*sloFlag * float64(time.Second))

	ctx := context.Background()
	_ = os.RemoveAll(root)
	_ = os.MkdirAll(root, 0o755)

	conn, err := pgx.Connect(ctx, dsn)
	must(err)
	defer conn.Close(ctx)

	must(exec(ctx, conn, `DROP TABLE IF EXISTS qs_items;
		CREATE TABLE qs_items(id int primary key, sku text, price numeric(12,2))`))
	must(ddl.Setup(ctx, conn))

	cols, order, err := ddl.LiveColumns(ctx, conn, "public", "qs_items")
	must(err)
	m, err := mirror.New(root, "public", "qs_items", "id", cols, order)
	must(err)

	h := health.New(slo)
	_, err = h.Serve(addr)
	must(err)

	stream := changestream.NewLogical(conn, slot,
		[]string{"public.qs_items", ddl.LogTable})
	_ = stream.DropSlot(ctx)
	must(stream.EnsureSlot(ctx))

	var wg sync.WaitGroup
	wg.Add(1)
	go writer(ctx, &wg)

	var failures, halted []string

	// pump honours DDL barriers: a barrier must STOP the apply, not merely be
	// noticed. Applying post-DDL rows with the pre-DDL column list silently
	// drops the new column — row counts still match, only values are wrong.
	pump := func() bool {
		txns, err := stream.Transactions(ctx)
		must(err)
		if len(txns) == 0 {
			h.RecordCaughtUp() // idle != stale
			return false
		}
		cut := -1
		for i, t := range txns {
			for _, c := range t.Changes {
				if c.Qualified() == ddl.LogTable {
					cut = i
					break
				}
			}
			if cut >= 0 {
				break
			}
		}
		head := txns
		if cut >= 0 {
			head = txns[:cut+1]
		}
		_, err = m.Apply(head)
		must(err)
		last := head[len(head)-1]
		must(stream.Confirm(ctx, last.NextLSN))

		var headLSN string
		_ = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&headLSN)
		h.RecordApply(last.CommitLSN, headLSN, m.CompactionBacklog())

		if cut < 0 {
			return false
		}
		live, liveOrder, err := ddl.LiveColumns(ctx, conn, "public", "qs_items")
		must(err)
		d := ddl.Compare(m.Columns, live)
		if !d.Empty() {
			if d.Safe() {
				m.Evolve(live, liveOrder)
			} else {
				halted = append(halted, d.String()) // refuse, do not corrupt
			}
		}
		return true
	}

	converged := func() bool {
		paused.Store(true)
		time.Sleep(300 * time.Millisecond)
		for i := 0; i < 40; i++ {
			pump()
			txns, _ := stream.Transactions(ctx)
			if len(txns) == 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		rows, err := m.Live()
		must(err)
		mc, mh := mirror.Checksum(rows, m.Order)
		src, err := sourceRows(ctx, conn, "public.qs_items", m.Order)
		must(err)
		sc, sh := mirror.Checksum(src, m.Order)
		match := mc == sc && mh == sh
		h.RecordVerification(!match)
		verdict := "MATCH"
		if !match {
			verdict = "DIVERGED"
		}
		fmt.Printf("      mirror %d rows / source %d rows -> %s\n", mc, sc, verdict)
		paused.Store(false)
		return match
	}

	fmt.Println("========================================================================")
	fmt.Println("A. DDL barriers")
	fmt.Println("========================================================================")
	for i := 0; i < 10; i++ {
		pump()
		time.Sleep(150 * time.Millisecond)
	}
	fmt.Printf("  baseline columns: %v\n", m.Order)
	if !converged() {
		failures = append(failures, "baseline did not converge")
	}

	fmt.Println("\n  ALTER TABLE qs_items ADD COLUMN category text ...")
	must(exec(ctx, conn, "ALTER TABLE qs_items ADD COLUMN category text"))
	must(exec(ctx, conn, "UPDATE qs_items SET category='books' WHERE id % 3 = 0"))
	hit := false
	for i := 0; i < 12; i++ {
		if pump() {
			hit = true
		}
		time.Sleep(150 * time.Millisecond)
	}
	fmt.Printf("      DDL barrier seen in change stream: %v\n", hit)
	if !hit {
		failures = append(failures, "ADD COLUMN produced no barrier")
	}
	fmt.Printf("      evolved columns (by the barrier, not by hand): %v\n", m.Order)
	if _, ok := m.Columns["category"]; !ok {
		failures = append(failures, "barrier did not evolve the schema")
	}
	if !converged() {
		failures = append(failures, "did not converge after ADD COLUMN")
	}
	rows, _ := m.Live()
	nulls := 0
	for _, r := range rows {
		if r["category"] == nil {
			nulls++
		}
	}
	fmt.Printf("      rows with NULL category (old files NULL-filled): %d\n", nulls)

	fmt.Println("\n  ALTER TABLE qs_items DROP COLUMN sku ...")
	must(exec(ctx, conn, "ALTER TABLE qs_items DROP COLUMN sku"))
	for i := 0; i < 10; i++ {
		pump()
		time.Sleep(150 * time.Millisecond)
	}
	fmt.Printf("      evolved columns: %v\n", m.Order)
	if _, ok := m.Columns["sku"]; ok {
		failures = append(failures, "barrier did not drop the column")
	}
	if !converged() {
		failures = append(failures, "did not converge after DROP COLUMN")
	}

	fmt.Println("\n  ALTER TABLE qs_items ALTER COLUMN price TYPE text ...")
	must(exec(ctx, conn, "ALTER TABLE qs_items ALTER COLUMN price TYPE text"))
	for i := 0; i < 8; i++ {
		pump()
		time.Sleep(150 * time.Millisecond)
	}
	fmt.Printf("      halted: %v\n", halted)
	if len(halted) == 0 {
		failures = append(failures, "retype was NOT flagged unsafe — would corrupt silently")
	} else {
		fmt.Println("      REFUSED: mirroring halts for this table rather than " +
			"writing a lossy conversion (docs/06 policy)")
	}

	fmt.Println("\n========================================================================")
	fmt.Printf("B. readiness gating (freshnessSLO = %.1fs)\n", slo.Seconds())
	fmt.Println("========================================================================")
	for i := 0; i < 6; i++ {
		pump()
		time.Sleep(100 * time.Millisecond)
	}
	code, body := probe("/readyz")
	fmt.Printf("  while ingesting     HTTP %d  %s\n", code, body["reason"])
	if code != 200 {
		failures = append(failures, "not ready while healthy")
	}

	fmt.Printf("  stalling ingest for %.0fs (writes continue) ...\n", slo.Seconds()+2)
	t0 := time.Now()
	flipped := -1.0
	for time.Since(t0) < slo+2*time.Second {
		time.Sleep(250 * time.Millisecond)
		code, body = probe("/readyz")
		if code == 503 && flipped < 0 {
			flipped, _ = body["lag_seconds"].(float64)
			fmt.Printf("  NOT READY at lag     %.1fs\n", flipped)
		}
	}
	if flipped < 0 {
		failures = append(failures, "readiness never failed despite exceeding SLO")
	} else if flipped < slo.Seconds() {
		failures = append(failures,
			fmt.Sprintf("flipped at lag %.1fs, below SLO %.1fs", flipped, slo.Seconds()))
	}

	fmt.Println("  resuming ingest ...")
	t1 := time.Now()
	for time.Since(t1) < 15*time.Second {
		pump()
		if code, body = probe("/readyz"); code == 200 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	fmt.Printf("  after resume        HTTP %d  %s  (recovered in %.1fs)\n",
		code, body["reason"], time.Since(t1).Seconds())
	if code != 200 {
		failures = append(failures, fmt.Sprintf("did not recover: %v", body["reason"]))
	}

	fmt.Println("\n  /metrics:")
	fmt.Print(indentNonComments(h.Metrics()))

	stop.Store(true)
	wg.Wait()
	_ = stream.DropSlot(ctx)
	_ = ddl.Teardown(ctx, conn)

	fmt.Println("\n========================================================================")
	if len(failures) == 0 {
		fmt.Println("PASS — DDL barriers and readiness gating both hold")
		return
	}
	fmt.Println("FAIL:")
	for _, f := range failures {
		fmt.Println("  - " + f)
	}
	os.Exit(1)
}

func indentNonComments(s string) string {
	out := ""
	for _, line := range splitLines(s) {
		if line == "" || line[0] == '#' {
			continue
		}
		out += "    " + line + "\n"
	}
	return out
}

func splitLines(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func probe(path string) (int, map[string]any) {
	resp, err := http.Get("http://" + addr + path)
	if err != nil {
		return 0, map[string]any{}
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	return resp.StatusCode, m
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

func writer(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	n := 0
	for !stop.Load() {
		if paused.Load() {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		n++
		if _, err := conn.Exec(ctx,
			`INSERT INTO qs_items(id,sku,price) VALUES ($1,$2,$3)
			 ON CONFLICT (id) DO UPDATE SET price = EXCLUDED.price`,
			n, fmt.Sprintf("SKU-%04d", n%500),
			fmt.Sprintf("%.2f", rand.Float64()*999)); err != nil {
			// an ALTER COLUMN ... TYPE invalidates cached plans; reconnect
			// rather than spin failing, which would starve the test of data
			conn.Close(ctx)
			if c2, err2 := pgx.Connect(ctx, dsn); err2 == nil {
				conn = c2
			}
		}
		if n%7 == 0 {
			_, _ = conn.Exec(ctx, `DELETE FROM qs_items WHERE id = $1`, max(1, n-50))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func exec(ctx context.Context, conn *pgx.Conn, sql string) error {
	_, err := conn.Exec(ctx, sql)
	return err
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

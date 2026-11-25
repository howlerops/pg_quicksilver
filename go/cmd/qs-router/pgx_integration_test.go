package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// These run pgx through a live qs-router. They need validate/setup.sh's
// cluster and are skipped without it:
//
//	QS_ROUTER_DSN='host=127.0.0.1 port=5490 dbname=app user=postgres' go test ./cmd/qs-router/
//
// A skip is not a pass; the commit message says whether they ran.
func routerConn(t *testing.T, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	dsn := os.Getenv("QS_ROUTER_DSN")
	if dsn == "" {
		t.Skip("QS_ROUTER_DSN not set: no router to test against")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = mode
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// lastRoute asks the router where the previous statement went. It is a simple
// Query the router answers itself, so it does not disturb what it reports on.
func lastRoute(t *testing.T, c *pgx.Conn) string {
	t.Helper()
	res, err := c.PgConn().Exec(context.Background(), "SHOW qs.last_route").ReadAll()
	if err != nil || len(res) != 1 || len(res[0].Rows) != 1 {
		t.Fatalf("SHOW qs.last_route: %v %v", res, err)
	}
	return string(res[0].Rows[0][0])
}

func wantRoute(t *testing.T, c *pgx.Conn, side, what string) {
	t.Helper()
	if got := lastRoute(t, c); !strings.HasPrefix(got, side+":") {
		t.Errorf("%s went to %q, want %s", what, got, side)
	}
}

var execModes = map[string]pgx.QueryExecMode{
	"cache_statement": pgx.QueryExecModeCacheStatement, // named prepared statements
	"cache_describe":  pgx.QueryExecModeCacheDescribe,
	"describe_exec":   pgx.QueryExecModeDescribeExec, // unnamed statement kept across two round trips
	"exec":            pgx.QueryExecModeExec,
	"simple":          pgx.QueryExecModeSimpleProtocol,
}

// One prepared statement, two parameter values, two sides: the decision is
// made from the plan these values get, not from the statement's text.
func TestPgxPreparedStatementRoutesByParameter(t *testing.T) {
	for name, mode := range execModes {
		t.Run(name, func(t *testing.T) {
			c := routerConn(t, mode)
			ctx := context.Background()
			const q = "SELECT count(*) FROM orders WHERE customer_id = $1"
			for round := 0; round < 3; round++ { // past pgx's first-use prepare, into reuse
				var n int64
				if err := c.QueryRow(ctx, q, 1).Scan(&n); err != nil {
					t.Fatal(err)
				}
				wantRoute(t, c, "mirror", "customer 1 (30% of the table)")
				if n < 290000 || n > 310000 {
					t.Errorf("customer 1 has %d orders", n)
				}
				if err := c.QueryRow(ctx, q, 5).Scan(&n); err != nil {
					t.Fatal(err)
				}
				wantRoute(t, c, "heap", "customer 5 (about 70 rows)")
				if n < 30 || n > 200 {
					t.Errorf("customer 5 has %d orders", n)
				}
			}
			var id int64
			var note *string
			if err := c.QueryRow(ctx, "SELECT id, note FROM orders WHERE id = $1", int64(4242)).Scan(&id, &note); err != nil {
				t.Fatal(err)
			}
			wantRoute(t, c, "heap", "primary-key lookup")
			if id != 4242 || note == nil || *note != "n-4242" {
				t.Errorf("row 4242 came back as %d %v", id, note)
			}
		})
	}
}

// The mirror must return what the heap returns, typed as the heap types it.
func TestPgxMirrorAndHeapAgree(t *testing.T) {
	c := routerConn(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()
	read := func() (rows [][]any) {
		t.Helper()
		r, err := c.Query(ctx, "SELECT status, count(*), sum(amount), min(created_at), max(created_at) FROM orders WHERE customer_id > $1 GROUP BY status", 1)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		byStatus := map[string][]any{}
		for r.Next() {
			v, err := r.Values()
			if err != nil {
				t.Fatal(err)
			}
			byStatus[v[0].(string)] = v
		}
		if r.Err() != nil {
			t.Fatal(r.Err())
		}
		for _, s := range []string{"new", "paid", "returned", "shipped"} {
			rows = append(rows, byStatus[s])
		}
		return rows
	}
	mirror := read()
	wantRoute(t, c, "mirror", "the aggregate")
	if _, err := c.Exec(ctx, "SET qs.route = 'heap'"); err != nil {
		t.Fatal(err)
	}
	heap := read()
	wantRoute(t, c, "heap", "the aggregate under qs.route=heap")
	if _, err := c.Exec(ctx, "RESET qs.route"); err != nil {
		t.Fatal(err)
	}
	for i := range heap {
		for j := range heap[i] {
			hv, mv := heap[i][j], mirror[i][j]
			if ht, ok := hv.(time.Time); ok {
				if mt, ok := mv.(time.Time); !ok || !ht.Equal(mt) {
					t.Errorf("row %d col %d: heap %v, mirror %v", i, j, hv, mv)
				}
				continue
			}
			if !reflectEqual(hv, mv) {
				t.Errorf("row %d col %d: heap %#v, mirror %#v", i, j, hv, mv)
			}
		}
	}
	read()
	wantRoute(t, c, "mirror", "the aggregate after RESET qs.route")
}

func reflectEqual(a, b any) bool {
	// pgtype.Numeric compares by its text form.
	if av, ok := a.(interface{ MarshalJSON() ([]byte, error) }); ok {
		if bv, ok := b.(interface{ MarshalJSON() ([]byte, error) }); ok {
			x, _ := av.MarshalJSON()
			y, _ := bv.MarshalJSON()
			return bytes.Equal(x, y)
		}
		return false
	}
	return a == b
}

func TestPgxTransactionStaysOnTheHeap(t *testing.T) {
	c := routerConn(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := tx.QueryRow(ctx, "INSERT INTO audit_log(what) VALUES ($1) RETURNING id", "pgx-tx").Scan(&id); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&n); err != nil {
		t.Fatal(err)
	}
	wantRoute(t, c, "heap", "a count(*) inside a transaction")
	// A nested transaction is a SAVEPOINT. It has to work.
	nested, err := tx.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nested.Exec(ctx, "DELETE FROM audit_log WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if err := nested.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var what string
	if err := c.QueryRow(ctx, "SELECT what FROM audit_log WHERE id = $1", id).Scan(&what); err != nil || what != "pgx-tx" {
		t.Fatalf("the committed row: %q %v", what, err)
	}
	// Rolled back means gone.
	tx, _ = c.Begin(ctx)
	if _, err := tx.Exec(ctx, "DELETE FROM audit_log WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE id = $1", id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("after ROLLBACK the row count is %d (%v)", n, err)
	}
	// And outside it the same count(*) goes back to the mirror.
	if err := c.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&n); err != nil {
		t.Fatal(err)
	}
	wantRoute(t, c, "mirror", "count(*) after the transaction")
	if _, err := c.Exec(ctx, "DELETE FROM audit_log WHERE what = 'pgx-tx'"); err != nil {
		t.Fatal(err)
	}
}

func TestPgxCopyOutAndIn(t *testing.T) {
	c := routerConn(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()
	var buf bytes.Buffer
	tag, err := c.PgConn().CopyTo(ctx, &buf, "COPY (SELECT id, status FROM orders WHERE id <= 5000) TO STDOUT")
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 5000 || bytes.Count(buf.Bytes(), []byte("\n")) != 5000 {
		t.Errorf("COPY out: tag %s, %d lines", tag, bytes.Count(buf.Bytes(), []byte("\n")))
	}
	wantRoute(t, c, "heap", "COPY")

	rows := make([][]any, 20000)
	for i := range rows {
		rows[i] = []any{"pgx-copy"}
	}
	n, err := c.CopyFrom(ctx, pgx.Identifier{"audit_log"}, []string{"what"}, pgx.CopyFromRows(rows))
	if err != nil || n != 20000 {
		t.Fatalf("COPY in: %d %v", n, err)
	}
	// A COPY that fails part-way must leave the connection usable.
	_, err = c.PgConn().CopyFrom(ctx, strings.NewReader("1\tx\nnot-a-number\ty\n"), "COPY audit_log(id, what) FROM STDIN")
	if err == nil {
		t.Fatal("a malformed COPY succeeded")
	}
	tagd, err := c.Exec(ctx, "DELETE FROM audit_log WHERE what = 'pgx-copy'")
	if err != nil || tagd.RowsAffected() != 20000 {
		t.Fatalf("after COPY: %v %v", tagd, err)
	}
}

func TestPgxCancel(t *testing.T) {
	c := routerConn(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = c.PgConn().CancelRequest(ctx)
	}()
	start := time.Now()
	_, err := c.Exec(ctx, "SELECT pg_sleep(30)")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "57014" {
		t.Fatalf("want query_canceled, got %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("cancel took %s", d)
	}
	var one int
	if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("connection after cancel: %v", err)
	}
}

// A pgx batch is one pipeline: many Parse/Bind/Execute, Syncs between.
func TestPgxBatch(t *testing.T) {
	c := routerConn(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()
	b := &pgx.Batch{}
	b.Queue("SELECT count(*) FROM orders")
	b.Queue("SELECT count(*) FROM orders WHERE id = $1", int64(7))
	b.Queue("INSERT INTO audit_log(what) VALUES ($1)", "pgx-batch")
	b.Queue("SELECT count(*) FROM audit_log WHERE what = $1", "pgx-batch")
	br := c.SendBatch(ctx, b)
	var n int64
	for i, want := range []int64{-1, 1} {
		if err := br.QueryRow().Scan(&n); err != nil {
			t.Fatalf("batch item %d: %v", i, err)
		}
		if want >= 0 && n != want {
			t.Errorf("batch item %d = %d", i, n)
		}
	}
	if _, err := br.Exec(); err != nil {
		t.Fatal(err)
	}
	if err := br.QueryRow().Scan(&n); err != nil || n != 1 {
		t.Fatalf("the batch did not see its own insert: %d %v", n, err)
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "DELETE FROM audit_log WHERE what = 'pgx-batch'"); err != nil {
		t.Fatal(err)
	}
}

// Statements the mirror must never see, and one it cannot run.
func TestPgxFailClosed(t *testing.T) {
	c := routerConn(t, pgx.QueryExecModeCacheStatement)
	ctx := context.Background()
	var n int64
	heap := func(what, sql string, args ...any) {
		t.Helper()
		r, err := c.Query(ctx, sql, args...)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		for r.Next() {
		}
		r.Close()
		if r.Err() != nil {
			t.Fatalf("%s: %v", what, r.Err())
		}
		wantRoute(t, c, "heap", what)
	}
	heap("schema-qualified", "SELECT count(*) FROM public.orders")
	heap("FOR UPDATE", "SELECT id FROM orders WHERE id = $1 FOR UPDATE", int64(3))
	heap("a CTE that writes", "WITH w AS (INSERT INTO audit_log(what) VALUES ('pgx-cte') RETURNING id) SELECT count(*) FROM w, orders")
	heap("a PostgreSQL function over the big table", "SELECT order_total(5), count(*) FROM orders")
	heap("an unmirrored table", "SELECT count(*) FROM orders o JOIN audit_log a ON a.id = o.id")
	heap("integer division", "SELECT count(*) FROM orders WHERE id/2 = 5")
	heap("ORDER BY", "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status")
	heap("ctid", "SELECT count(*) FROM orders WHERE ctid = '(0,1)'")
	if err := c.QueryRow(ctx, "SELECT count(*) FROM orders WHERE id/2 = 5").Scan(&n); err != nil || n != 2 {
		t.Errorf("id/2 = 5 matches ids 10 and 11 in PostgreSQL; got %d (%v)", n, err)
	}
	// search_path must be exactly what the client left it as.
	var sp string
	if err := c.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&n); err != nil {
		t.Fatal(err)
	}
	wantRoute(t, c, "mirror", "count(*)")
	if err := c.QueryRow(ctx, "SHOW search_path").Scan(&sp); err != nil || strings.Contains(sp, "qs_mirror") {
		t.Errorf("search_path after a mirror statement: %q %v", sp, err)
	}
	// A different search_path is a different question, even for cached text.
	if _, err := c.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS elsewhere"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "CREATE TABLE IF NOT EXISTS elsewhere.orders(id int)"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "SET search_path = elsewhere, public"); err != nil {
		t.Fatal(err)
	}
	if err := c.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&n); err != nil || n != 0 {
		t.Errorf("elsewhere.orders is empty; the cached decision for public.orders answered %d (%v)", n, err)
	}
	wantRoute(t, c, "heap", "count(*) of a table that only shares a name")
	if _, err := c.Exec(ctx, "RESET search_path"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "DROP SCHEMA elsewhere CASCADE"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "DELETE FROM audit_log WHERE what = 'pgx-cte'"); err != nil {
		t.Fatal(err)
	}
}

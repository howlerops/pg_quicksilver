package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// These run pgx against a server with the pg_quicksilver extension loaded
// (ext/pg_quicksilver/validate/setup.sh) and are skipped without one:
//
//	QS_EXT_DSN='host=/tmp port=5541 dbname=app user=postgres' go test ./cmd/qs-extcheck/
//
// A skip is not a pass; docs/40 says whether they ran.
//
// Each check runs the same statement on a heap-only connection and on a routing
// one, compares the answers, and asks the extension which side answered.

const (
	bigAgg   = `SELECT count(*), sum(amount) FROM public.orders WHERE customer_id = $1`
	lookup   = `SELECT id, status FROM public.orders WHERE id = $1`
	unsafeQ  = `SELECT count(*) FROM public.orders WHERE id / 2 = $1`
	lastSide = `SELECT side, reason FROM quicksilver.last_decision()`
)

func extConn(t *testing.T, mode pgx.QueryExecMode, route string) *pgx.Conn {
	t.Helper()
	dsn := os.Getenv("QS_EXT_DSN")
	if dsn == "" {
		t.Skip("QS_EXT_DSN not set: no server to test against")
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
	if _, err := c.Exec(ctx, "SET quicksilver.route = "+route, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
	return c
}

func side(t *testing.T, c *pgx.Conn) (string, string) {
	t.Helper()
	var s, r string
	err := c.QueryRow(context.Background(), lastSide, pgx.QueryExecModeSimpleProtocol).Scan(&s, &r)
	if errors.Is(err, pgx.ErrNoRows) {
		// nothing was planned: a named statement reusing its generic (heap) plan
		return "heap", "no new decision: the cached generic plan was reused"
	}
	if err != nil {
		t.Fatalf("last_decision: %v", err)
	}
	return s, r
}

func forget(t *testing.T, c *pgx.Conn) {
	t.Helper()
	if _, err := c.Exec(context.Background(), "SELECT quicksilver.forget_last()", pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatal(err)
	}
}

var modes = map[string]pgx.QueryExecMode{
	"simple":          pgx.QueryExecModeSimpleProtocol,
	"exec":            pgx.QueryExecModeExec,
	"describe_exec":   pgx.QueryExecModeDescribeExec,
	"cache_describe":  pgx.QueryExecModeCacheDescribe,
	"cache_statement": pgx.QueryExecModeCacheStatement,
}

// A qualifying statement is answered by the mirror in every exec mode, a
// selective one and an unsafe one by the heap, and all three agree with a
// heap-only connection. Seven rounds, so that a named statement goes past the
// point where the plan cache would settle on a generic plan.
func TestRoutedInEveryMode(t *testing.T) {
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			heap := extConn(t, mode, "heap")
			auto := extConn(t, mode, "auto")
			for round := 0; round < 7; round++ {
				for _, tc := range []struct {
					sql  string
					arg  any
					want string
				}{{bigAgg, int32(1), "mirror"}, {bigAgg, int32(5), "heap"}, {lookup, int64(4242), "heap"}, {unsafeQ, int64(5), "heap"}} {
					var hn, an int64
					var hs, as *string
					forget(t, auto)
					if tc.sql == bigAgg {
						if err := heap.QueryRow(ctx, tc.sql, tc.arg).Scan(&hn, &hs); err != nil {
							t.Fatal(err)
						}
						if err := auto.QueryRow(ctx, tc.sql, tc.arg).Scan(&an, &as); err != nil {
							t.Fatal(err)
						}
					} else if tc.sql == lookup {
						hs, as = new(string), new(string)
						if err := heap.QueryRow(ctx, tc.sql, tc.arg).Scan(&hn, hs); err != nil {
							t.Fatal(err)
						}
						if err := auto.QueryRow(ctx, tc.sql, tc.arg).Scan(&an, as); err != nil {
							t.Fatal(err)
						}
					} else {
						if err := heap.QueryRow(ctx, tc.sql, tc.arg).Scan(&hn); err != nil {
							t.Fatal(err)
						}
						if err := auto.QueryRow(ctx, tc.sql, tc.arg).Scan(&an); err != nil {
							t.Fatal(err)
						}
					}
					if hn != an || (hs == nil) != (as == nil) || (hs != nil && *hs != *as) {
						t.Fatalf("round %d %q(%v): heap %d %v, routed %d %v", round, tc.sql, tc.arg, hn, hs, an, as)
					}
					if got, why := side(t, auto); got != tc.want {
						t.Fatalf("round %d %q(%v): answered by %s (%s), want %s", round, tc.sql, tc.arg, got, why, tc.want)
					}
				}
			}
		})
	}
}

// The RowDescription is the heap's whichever side answers: names, type OIDs
// and typmods, in text and in binary.
func TestRowDescriptionIsTheHeaps(t *testing.T) {
	const q = `SELECT status, count(*) AS n, sum(amount) AS total, min(created_at) AS first, max(id) AS last
	           FROM public.orders WHERE customer_id = $1 GROUP BY status ORDER BY count(*), max(id)`
	for name, mode := range modes {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			heap := extConn(t, mode, "heap")
			auto := extConn(t, mode, "auto")
			hr, err := heap.Query(ctx, q, int32(1))
			if err != nil {
				t.Fatal(err)
			}
			hf := append([]pgconn.FieldDescription(nil), hr.FieldDescriptions()...)
			hv, err := pgx.CollectRows(hr, func(r pgx.CollectableRow) ([]any, error) { return r.Values() })
			if err != nil {
				t.Fatal(err)
			}
			forget(t, auto)
			ar, err := auto.Query(ctx, q, int32(1))
			if err != nil {
				t.Fatal(err)
			}
			af := append([]pgconn.FieldDescription(nil), ar.FieldDescriptions()...) // the connection reuses the slice
			av, err := pgx.CollectRows(ar, func(r pgx.CollectableRow) ([]any, error) { return r.Values() })
			if err != nil {
				t.Fatal(err)
			}
			if got, why := side(t, auto); got != "mirror" {
				t.Fatalf("answered by %s (%s)", got, why)
			}
			if len(hf) != len(af) {
				t.Fatalf("%d columns from the heap, %d routed", len(hf), len(af))
			}
			for i := range hf {
				if hf[i].Name != af[i].Name || hf[i].DataTypeOID != af[i].DataTypeOID || hf[i].TypeModifier != af[i].TypeModifier ||
					hf[i].TableOID != af[i].TableOID || hf[i].TableAttributeNumber != af[i].TableAttributeNumber {
					t.Fatalf("column %d: heap %+v, routed %+v", i, hf[i], af[i])
				}
			}
			if len(hv) != len(av) || len(hv) == 0 {
				t.Fatalf("%d rows from the heap, %d routed", len(hv), len(av))
			}
			for i := range hv {
				for j := range hv[i] {
					h, a := hv[i][j], av[i][j]
					if ht, ok := h.(time.Time); ok {
						if !ht.Equal(a.(time.Time)) {
							t.Fatalf("row %d column %d: heap %v, routed %v", i, j, h, a)
						}
					} else if !equalValue(h, a) {
						t.Fatalf("row %d column %d: heap %#v, routed %#v", i, j, h, a)
					}
				}
			}
		})
	}
}

// A transaction block is routed where that is safe: READ COMMITTED, no savepoint
// open, nothing written yet. pg_duckdb aborts a transaction that takes a
// savepoint while its DuckDB transaction is open, and pgx's nested transaction
// is a savepoint; the extension ends DuckDB's transaction with each routed
// statement, so the nested transaction works, in every exec mode.
func TestTransactionWithSavepoint(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []pgx.QueryExecMode{
		pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec,
		pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol,
	} {
		c := extConn(t, mode, "auto")
		heap := extConn(t, mode, "heap")
		var wantN int64
		var wantS *string
		if err := heap.QueryRow(ctx, bigAgg, int32(1)).Scan(&wantN, &wantS); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, `CREATE TEMP TABLE qs_extcheck_w (i int)`); err != nil {
			t.Fatal(err)
		}
		read := func(q interface {
			QueryRow(context.Context, string, ...any) pgx.Row
		}, want string, where string) {
			t.Helper()
			var n int64
			var s *string
			forget(t, c)
			if err := q.QueryRow(ctx, bigAgg, int32(1)).Scan(&n, &s); err != nil {
				t.Fatalf("%v, %s: %v", mode, where, err)
			}
			if n != wantN || (s == nil) != (wantS == nil) || (s != nil && *s != *wantS) {
				t.Fatalf("%v, %s: %d rows, the heap says %d", mode, where, n, wantN)
			}
			if got, why := side(t, c); got != want {
				t.Fatalf("%v, %s: answered by %s (%s), want %s", mode, where, got, why, want)
			}
		}
		tx, err := c.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		read(tx, "mirror", "first statement of the transaction")
		nested, err := tx.Begin(ctx) // SAVEPOINT, after a routed statement
		if err != nil {
			t.Fatalf("%v: SAVEPOINT after a routed statement: %v", mode, err)
		}
		read(nested, "heap", "inside the savepoint")
		if err := nested.Commit(ctx); err != nil { // RELEASE
			t.Fatal(err)
		}
		read(tx, "mirror", "after RELEASE")
		// pgx rolls a nested transaction back with ROLLBACK TO SAVEPOINT and does not
		// release it: the rest of the transaction is inside that savepoint, and the heap's
		nested, err = tx.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := nested.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		read(tx, "heap", "after ROLLBACK TO SAVEPOINT")
		if _, err := tx.Exec(ctx, `INSERT INTO qs_extcheck_w VALUES (1)`); err != nil {
			t.Fatalf("%v: a write after routed statements: %v", mode, err)
		}
		read(tx, "heap", "after the transaction has written")
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("%v: COMMIT: %v", mode, err)
		}
		// a read-only transaction and a rolled-back one
		ro, err := c.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		read(ro, "mirror", "READ ONLY transaction")
		if err := ro.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		rr, err := c.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
		if err != nil {
			t.Fatal(err)
		}
		read(rr, "heap", "REPEATABLE READ transaction")
		if err := rr.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		read(c, "mirror", "after the transactions")
		c.Close(ctx)
		heap.Close(ctx)
	}
}

// A batch is one implicit transaction of extended-protocol statements.
func TestBatch(t *testing.T) {
	ctx := context.Background()
	c := extConn(t, pgx.QueryExecModeCacheStatement, "auto")
	b := &pgx.Batch{}
	b.Queue(bigAgg, int32(1))
	b.Queue(lookup, int64(7))
	b.Queue(bigAgg, int32(5))
	res := c.SendBatch(ctx, b)
	var n int64
	var s *string
	if err := res.QueryRow().Scan(&n, &s); err != nil || n != 300000 {
		t.Fatalf("first: %d %v", n, err)
	}
	var id int64
	var st string
	if err := res.QueryRow().Scan(&id, &st); err != nil || id != 7 {
		t.Fatalf("second: %d %v", id, err)
	}
	if err := res.QueryRow().Scan(&n, &s); err != nil || n != 70 {
		t.Fatalf("third: %d %v", n, err)
	}
	if err := res.Close(); err != nil {
		t.Fatal(err)
	}
}

func equalValue(a, b any) bool {
	aj, aerr := json.Marshal(a)
	bj, berr := json.Marshal(b)
	return aerr == nil && berr == nil && string(aj) == string(bj)
}

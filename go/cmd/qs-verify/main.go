// qs-verify compares a mirror on disk against the source table it claims to
// mirror, and reports MATCH or DIVERGED.
//
// This is an operational tool, not only a test one. Silent divergence is the
// failure mode a columnar mirror is most exposed to: the row counts stay
// plausible, queries keep answering, and nothing errors. Being able to ask
// "is this node telling the truth?" on demand is what makes the mirror safe to
// put in front of anyone.
//
//	qs-verify -dsn postgres://... -mirror /var/lib/postgresql/data/quicksilver \
//	          -table public.events
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/ddl"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

func main() {
	dsn := flag.String("dsn", "", "connection string for the source database")
	root := flag.String("mirror", "", "mirror root directory")
	table := flag.String("table", "", "qualified table name, e.g. public.events")
	flag.Parse()
	if *dsn == "" || *root == "" || *table == "" {
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	schema, name, ok := strings.Cut(*table, ".")
	if !ok {
		fail("table must be schema.table")
	}

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		fail("connect: %v", err)
	}
	defer conn.Close(ctx)

	cols, order, err := ddl.LiveColumns(ctx, conn, schema, name)
	if err != nil {
		fail("read columns: %v", err)
	}
	t, err := mirror.New(*root, schema, name, order[0], cols, order)
	if err != nil {
		fail("open mirror: %v", err)
	}

	rows, err := t.Live()
	if err != nil {
		fail("read mirror: %v", err)
	}
	mc, mh := mirror.Checksum(rows, order)

	src, err := sourceRows(ctx, conn, *table, order)
	if err != nil {
		fail("read source: %v", err)
	}
	sc, sh := mirror.Checksum(src, order)

	fmt.Printf("applied_lsn=%s\n", t.State.AppliedLSN)
	fmt.Printf("mirror %d rows (checksum %x)\n", mc, mh)
	fmt.Printf("source %d rows (checksum %x)\n", sc, sh)
	if mc == sc && mh == sh {
		fmt.Println("MATCH")
		return
	}
	fmt.Println("DIVERGED")
	os.Exit(1)
}

func sourceRows(ctx context.Context, conn *pgx.Conn, qualified string, order []string) ([]map[string]any, error) {
	sel := make([]string, len(order))
	for i, c := range order {
		sel[i] = `"` + c + `"::text`
	}
	rows, err := conn.Query(ctx, "SELECT "+strings.Join(sel, ", ")+" FROM "+qualified)
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

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(2)
}

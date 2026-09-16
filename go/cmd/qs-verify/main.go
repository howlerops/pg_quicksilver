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
	limit := flag.Int("explain-limit", 2_000_000, "max rows to diagnose on divergence")
	flag.Parse()
	explainLimit = *limit
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

	// Both sides stream. Materialising them was what got this tool OOM-killed
	// on a 5.7M-row mirror, and a verifier that dies at scale reports every
	// large mirror as broken.
	acc := mirror.NewAccumulator(order)
	if err := t.ForEachLive(func(r map[string]any) error {
		acc.Add(r)
		return nil
	}); err != nil {
		fail("read mirror: %v", err)
	}
	mc, mh := acc.Result()

	sacc := mirror.NewAccumulator(order)
	if err := eachSourceRow(ctx, conn, *table, order, func(r map[string]any) error {
		sacc.Add(r)
		return nil
	}); err != nil {
		fail("read source: %v", err)
	}
	sc, sh := sacc.Result()

	fmt.Printf("applied_lsn=%s\n", t.State.AppliedLSN)
	fmt.Printf("mirror %d rows (checksum %x)\n", mc, mh)
	fmt.Printf("source %d rows (checksum %x)\n", sc, sh)
	if mc == sc && mh == sh {
		fmt.Println("MATCH")
		return
	}
	fmt.Println("DIVERGED")
	if mc <= explainLimit && sc <= explainLimit {
		explain(ctx, conn, t, order)
	} else {
		fmt.Printf("  (%d rows is above the %d-row diagnostic limit; "+
			"re-run with -explain-limit to see which columns differ)\n",
			maxInt(mc, sc), explainLimit)
	}
	os.Exit(1)
}

// explain says WHICH rows and WHICH columns differ. "DIVERGED" on its own sends
// whoever reads it back to guessing, and the two failure modes it has to
// separate — a lagging mirror and a wrong one — look identical without this.
func explain(ctx context.Context, conn *pgx.Conn, t *mirror.Table, order []string) {
	key := t.Key
	m := map[string]map[string]any{}
	_ = t.ForEachLive(func(r map[string]any) error {
		m[fmt.Sprint(r[key])] = r
		return nil
	})
	s := map[string]map[string]any{}
	_ = eachSourceRow(ctx, conn, t.Qualified, order, func(r map[string]any) error {
		s[fmt.Sprint(r[key])] = r
		return nil
	})

	var onlyMirror, onlySource, differing []string
	for k := range s {
		if _, ok := m[k]; !ok {
			onlySource = append(onlySource, k)
		}
	}
	for k := range m {
		if _, ok := s[k]; !ok {
			onlyMirror = append(onlyMirror, k)
		}
	}
	colDiff := map[string]int{}
	for k, sr := range s {
		mr, ok := m[k]
		if !ok {
			continue
		}
		var cols []string
		for _, c := range order {
			if mirror.Render(sr[c]) != mirror.Render(mr[c]) {
				cols = append(cols, c)
				colDiff[c]++
			}
		}
		if len(cols) > 0 && len(differing) < 5 {
			differing = append(differing, fmt.Sprintf("%s=%s differs in %s",
				key, k, strings.Join(cols, ",")))
		}
	}

	fmt.Printf("  rows only in source: %d, only in mirror: %d\n",
		len(onlySource), len(onlyMirror))
	if len(colDiff) > 0 {
		fmt.Println("  differing columns (count of rows):")
		for _, c := range order {
			if colDiff[c] > 0 {
				fmt.Printf("    %-20s %d\n", c, colDiff[c])
			}
		}
	}
	for _, d := range differing {
		fmt.Println("  " + d)
	}
	for i, k := range onlySource {
		if i >= 3 {
			break
		}
		fmt.Printf("  missing from mirror: %s=%s\n", key, k)
	}
}

// explainLimit bounds the diagnostic, which unlike the checksum has to hold
// both sides in memory to compare them row by row.
var explainLimit = 2_000_000

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func eachSourceRow(ctx context.Context, conn *pgx.Conn, qualified string,
	order []string, fn func(map[string]any) error,
) error {
	sel := make([]string, len(order))
	for i, c := range order {
		sel[i] = `"` + c + `"::text`
	}
	rows, err := conn.Query(ctx, "SELECT "+strings.Join(sel, ", ")+" FROM "+qualified)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		vals := make([]any, len(order))
		ptrs := make([]any, len(order))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		m := make(map[string]any, len(order))
		for i, c := range order {
			m[c] = vals[i]
		}
		if err := fn(m); err != nil {
			return err
		}
	}
	return rows.Err()
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(2)
}

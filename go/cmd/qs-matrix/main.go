// qs-matrix runs the ingest path across the table shapes that actually break
// things, rather than the one shape a benchmark author would pick.
//
// The shapes are chosen because each stresses a different part of the pipeline,
// and because each is a real table somebody runs in production:
//
//	narrow      4 columns, insert-only        the baseline; also the shape that
//	                                          gives a column store the least to
//	                                          work with
//	wide        12 columns, insert-only       where projection pruning starts to
//	                                          pay
//	jsonb       one ~6 KB document per row,   the killer: PostgreSQL stores the
//	            UPDATEs that never touch it   document out of line and pgoutput
//	                                          does NOT resend it on an update
//	                                          that did not change it
//	churn       narrow, UPDATEs on a hot set  every update supersedes a row, so
//	                                          the mirror's write amplification
//	                                          is maximal and compaction never
//	                                          gets a rest
//	deletes     inserts mixed with deletes    deletion vectors on the hot path
//
// Each shape is measured AND verified. A throughput number from a mirror that
// does not match its source is not a result.
//
//	qs-matrix -shape jsonb -setup      create and seed
//	qs-matrix -shape jsonb -measure    run the workload and report
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

type shape struct {
	name    string
	create  string
	seed    string // $1 = row count
	columns []string
	// workload runs one unit of the shape's characteristic write pattern and
	// returns how many rows it touched.
	workload func(ctx context.Context, c *pgxpool.Conn, worker, iter, seeded int) (int, error)
	note     string
}

var shapes = map[string]*shape{
	"narrow": {
		name:    "narrow",
		create:  `CREATE TABLE m(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)`,
		seed:    `INSERT INTO m SELECT g,'SKU-'||(g%100000),(g%997)/7.0,now() FROM generate_series(1,$1::bigint) g`,
		columns: []string{"id", "sku", "amount", "ts"},
		note:    "4 columns, insert-only",
		workload: func(ctx context.Context, c *pgxpool.Conn, w, iter, seeded int) (int, error) {
			return insertBatch(ctx, c, w, iter,
				"INSERT INTO m(id,sku,amount,ts) VALUES ",
				func(id int) string {
					return fmt.Sprintf("(%d,'SKU-%d',%d.%02d,now())", id, id, id%1000, id%100)
				})
		},
	},

	"wide": {
		name: "wide",
		create: `CREATE TABLE m(id bigint primary key, sku text, amount numeric(12,2),
			ts timestamptz, region text, channel text, customer_id bigint,
			session_id text, status text, qty int, discount numeric(6,2), note text)`,
		seed: `INSERT INTO m SELECT g,'SKU-'||(g%100000),(g%997)/7.0,now(),
			(ARRAY['us-east','us-west','eu-west','ap-south'])[1+g%4],
			(ARRAY['web','mobile','api','partner'])[1+g%4],
			g%250000, md5(g::text), (ARRAY['ok','pending','refunded'])[1+g%3],
			1+g%9, (g%50)/10.0, repeat('x', (20+g%40)::int)
			FROM generate_series(1,$1::bigint) g`,
		columns: []string{"id", "sku", "amount", "ts", "region", "channel",
			"customer_id", "session_id", "status", "qty", "discount", "note"},
		note: "12 columns, insert-only",
		workload: func(ctx context.Context, c *pgxpool.Conn, w, iter, seeded int) (int, error) {
			return insertBatch(ctx, c, w, iter,
				"INSERT INTO m(id,sku,amount,ts,region,channel,customer_id,session_id,status,qty,discount,note) VALUES ",
				func(id int) string {
					return fmt.Sprintf(
						"(%d,'SKU-%d',%d.%02d,now(),'us-east','web',%d,md5('%d'),'ok',%d,1.50,'burst')",
						id, id, id%1000, id%100, id%250000, id, 1+id%9)
				})
		},
	},

	// The shape this whole matrix exists for. A document per row, updated
	// constantly on its scalar columns and almost never on the document.
	"jsonb": {
		name: "jsonb",
		create: `CREATE TABLE m(id bigint primary key, status text,
			updated_at timestamptz, doc jsonb)`,
		// The payload has to be both INCOMPRESSIBLE and DISTINCT PER ROW, and
		// getting either wrong produces a benchmark that measures nothing:
		//
		//   repeat('note ', 600)     compresses to almost nothing, so PostgreSQL
		//                            keeps the document inline — 8 KB of TOAST
		//                            across the whole table, testing no TOAST.
		//   an UNCORRELATED subquery PostgreSQL evaluates it once, so every row
		//                            gets the same document — 395 MB of TOAST in
		//                            the source and a 4 MB mirror, because
		//                            Parquet dictionary-encodes one value.
		//
		// Correlating on g fixes both: distinct md5 hex per row, out of line in
		// the source, and nothing for the column store to dictionary away.
		seed: `INSERT INTO m SELECT g, 'Prospecting', now(),
			jsonb_build_object(
			  'Id', 'OPP'||g, 'AccountId', '001'||g, 'Name', 'Deal '||g,
			  'StageName','Prospecting', 'Amount', (g%50000)*1.0,
			  'Owner', jsonb_build_object('Id','005x','Name','Rep '||(g%500)),
			  'Products', (SELECT jsonb_agg(jsonb_build_object('Sku','P'||p,'Qty',p))
			                 FROM generate_series(1,12) p),
			  'Blob', (SELECT string_agg(md5(g::text||p::text),'') FROM generate_series(1,100) p))
			FROM generate_series(1,$1::bigint) g`,
		columns: []string{"id", "status", "updated_at", "doc"},
		note:    "~6 KB jsonb per row; UPDATEs that never touch the document",
		workload: func(ctx context.Context, c *pgxpool.Conn, w, iter, seeded int) (int, error) {
			// Update scalar columns only. The document is unchanged, so
			// PostgreSQL does not rewrite it and pgoutput does not resend it.
			// This is the case that silently blanked the column before
			// mirror.carryForward existed.
			const n = 200
			lo := ((w*1000 + iter*n) % maxInt(seeded-n, 1)) + 1
			_, err := c.Exec(ctx, `UPDATE m SET status = $1, updated_at = now()
				WHERE id BETWEEN $2 AND $3`,
				[]string{"Qualification", "Proposal", "Negotiation", "ClosedWon"}[iter%4],
				lo, lo+n-1)
			return n, err
		},
	},

	"churn": {
		name:    "churn",
		create:  `CREATE TABLE m(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)`,
		seed:    `INSERT INTO m SELECT g,'SKU-'||(g%100000),(g%997)/7.0,now() FROM generate_series(1,$1::bigint) g`,
		columns: []string{"id", "sku", "amount", "ts"},
		note:    "UPDATEs concentrated on a hot 1% of rows",
		workload: func(ctx context.Context, c *pgxpool.Conn, w, iter, seeded int) (int, error) {
			const n = 200
			hot := maxInt(seeded/100, 1000)
			lo := ((w*997 + iter*n) % maxInt(hot-n, 1)) + 1
			_, err := c.Exec(ctx,
				`UPDATE m SET amount = amount + 0.01, ts = now() WHERE id BETWEEN $1 AND $2`,
				lo, lo+n-1)
			return n, err
		},
	},

	"deletes": {
		name:    "deletes",
		create:  `CREATE TABLE m(id bigint primary key, sku text, amount numeric(12,2), ts timestamptz)`,
		seed:    `INSERT INTO m SELECT g,'SKU-'||(g%100000),(g%997)/7.0,now() FROM generate_series(1,$1::bigint) g`,
		columns: []string{"id", "sku", "amount", "ts"},
		note:    "inserts mixed 1:1 with deletes of older rows",
		workload: func(ctx context.Context, c *pgxpool.Conn, w, iter, seeded int) (int, error) {
			base := 100_000_000 + w*10_000_000 + iter*200
			var b strings.Builder
			b.WriteString("INSERT INTO m(id,sku,amount,ts) VALUES ")
			for i := 0; i < 200; i++ {
				if i > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, "(%d,'NEW-%d',1.00,now())", base+i, base+i)
			}
			if _, err := c.Exec(ctx, b.String()); err != nil {
				return 0, err
			}
			lo := ((w*811 + iter*200) % maxInt(seeded-200, 1)) + 1
			if _, err := c.Exec(ctx, `DELETE FROM m WHERE id BETWEEN $1 AND $2`,
				lo, lo+199); err != nil {
				return 200, err
			}
			return 400, nil
		},
	},
}

func insertBatch(ctx context.Context, c *pgxpool.Conn, w, iter int,
	prefix string, row func(int) string,
) (int, error) {
	const n = 500
	base := 10_000_000 + w*50_000_000 + iter*n
	var b strings.Builder
	b.WriteString(prefix)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(row(base + i))
	}
	_, err := c.Exec(ctx, b.String())
	return n, err
}

var (
	dsn        = flag.String("dsn", "postgres://postgres@localhost:5443/app", "primary")
	shapeName  = flag.String("shape", "narrow", "one of: narrow, wide, jsonb, churn, deletes")
	setup      = flag.Bool("setup", false, "create and seed the table")
	measure    = flag.Bool("measure", false, "run the workload and report")
	settle     = flag.Bool("settle", false, "wait until the mirror has applied everything, then exit")
	rows       = flag.Int("rows", 500000, "rows to seed")
	mirrorRoot = flag.String("mirror", "", "mirror root (for measure)")
	pid        = flag.String("pid", "", "sidecar pid, for CPU and RSS")
	seconds    = flag.Int("seconds", 20, "workload duration")
	writers    = flag.Int("writers", 6, "concurrent writers")
	markers    = flag.Int("markers", 60, "commit-to-visible samples")
)

func main() {
	flag.Parse()
	sh := shapes[*shapeName]
	if sh == nil {
		fail("unknown shape %q", *shapeName)
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn+"?pool_max_conns=24")
	must(err)
	defer pool.Close()

	switch {
	case *setup:
		doSetup(ctx, pool, sh)
	case *settle:
		// Verification must not race the apply loop. A mirror that is merely
		// BEHIND reports the same symptom as one that is WRONG — matching row
		// counts and a different checksum — so the harness has to rule lag out
		// before it is allowed to call anything a divergence.
		if *mirrorRoot == "" {
			fail("-mirror is required for -settle")
		}
		if !settleQuiet(ctx, pool, sh) {
			fail("mirror did not settle; a divergence reported now would be lag")
		}
	case *measure:
		doMeasure(ctx, pool, sh)
	default:
		fail("pass -setup or -measure")
	}
}

func doSetup(ctx context.Context, pool *pgxpool.Pool, sh *shape) {
	mustExec(ctx, pool, `DROP TABLE IF EXISTS m CASCADE`)
	mustExec(ctx, pool, sh.create)
	t0 := time.Now()
	_, err := pool.Exec(ctx, sh.seed, *rows)
	must(err)
	mustExec(ctx, pool, `ANALYZE m`)
	var size, n int64
	must(pool.QueryRow(ctx,
		`SELECT pg_total_relation_size('m'), count(*) FROM m`).Scan(&size, &n))
	var toast int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE(pg_total_relation_size(reltoastrelid),0)
		FROM pg_class WHERE oid='m'::regclass`).Scan(&toast)
	fmt.Printf("  shape=%s  %s\n", sh.name, sh.note)
	fmt.Printf("  seeded %s rows in %.1fs; %s heap+indexes", commas(n), time.Since(t0).Seconds(), human(size))
	if toast > 0 {
		fmt.Printf(" (of which %s TOAST)", human(toast))
	}
	fmt.Println()
}

func doMeasure(ctx context.Context, pool *pgxpool.Pool, sh *shape) {
	if *mirrorRoot == "" {
		fail("-mirror is required for -measure")
	}
	var seeded int
	must(pool.QueryRow(ctx, `SELECT count(*) FROM m`).Scan(&seeded))
	waitCaughtUp(ctx, pool, 5*time.Minute)
	startCPU, _ := procStats()

	// ---- sustained workload ---------------------------------------------
	var touched atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	t0 := time.Now()
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c, err := pool.Acquire(ctx)
			if err != nil {
				return
			}
			defer c.Release()
			for iter := 0; ; iter++ {
				select {
				case <-stop:
					return
				default:
				}
				n, err := sh.workload(ctx, c, w, iter, seeded)
				if err != nil {
					return
				}
				touched.Add(int64(n))
			}
		}(w)
	}
	time.Sleep(time.Duration(*seconds) * time.Second)
	close(stop)
	wg.Wait()
	writeDur := time.Since(t0)

	var lastLSN string
	must(pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lastLSN))
	trickleStop := make(chan struct{})
	go trickle(ctx, pool, trickleStop)
	drainT0 := time.Now()
	drained := waitAppliedThrough(sh, 10*time.Minute, lastLSN)
	drain := time.Since(drainT0)
	close(trickleStop)

	n := touched.Load()
	total := writeDur + drain
	fmt.Printf("  workload      %s row-changes in %.1fs (%.0f/s into PostgreSQL)\n",
		commas(n), writeDur.Seconds(), float64(n)/writeDur.Seconds())
	if !drained {
		fmt.Printf("  DRAIN FAILED  still behind after %.0fs\n", drain.Seconds())
	} else {
		fmt.Printf("  drained       %.1fs -> %.0f row-changes/s through the mirror\n",
			drain.Seconds(), float64(n)/total.Seconds())
	}

	// ---- latency ---------------------------------------------------------
	lat := measureLatency(ctx, pool, sh, *markers)
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		fmt.Printf("  latency       p50=%.0fms p90=%.0fms p99=%.0fms max=%.0fms (n=%d)\n",
			ms(pct(lat, 50)), ms(pct(lat, 90)), ms(pct(lat, 99)),
			ms(lat[len(lat)-1]), len(lat))
	}

	// ---- storage and cost ------------------------------------------------
	// VACUUM first. An update-heavy shape leaves dead tuples behind, and
	// comparing a bloated heap against a compacted mirror would report a
	// storage ratio that is mostly PostgreSQL's vacuum debt.
	mustExec(ctx, pool, `VACUUM (ANALYZE) m`)
	var heap, live int64
	must(pool.QueryRow(ctx, `SELECT pg_total_relation_size('m'), count(*) FROM m`).Scan(&heap, &live))
	mir := dirBytes(*mirrorRoot)
	ratio := 0.0
	if mir > 0 {
		ratio = float64(heap) / float64(mir)
	}
	fmt.Printf("  storage       source %s / mirror %s -> %.1fx smaller (%s live rows)\n",
		human(heap), human(mir), ratio, commas(live))

	endCPU, endRSS := procStats()
	if endCPU > 0 && n > 0 {
		cpu := endCPU - startCPU
		fmt.Printf("  sidecar       %.1fs CPU (%.0f us/row-change), RSS %s\n",
			cpu, cpu*1e6/float64(n), human(endRSS))
	}
	st := readState(sh)
	fmt.Printf("  files         %d base + %d delta\n", len(st.BaseFiles), len(st.DeltaFiles))
}

// ---- observing ---------------------------------------------------------

type state struct {
	AppliedLSN string   `json:"applied_lsn"`
	BaseFiles  []string `json:"base_files"`
	DeltaFiles []string `json:"delta_files"`
}

func readState(sh *shape) state {
	var s state
	b, err := os.ReadFile(fmt.Sprintf("%s/public.m/state.json", *mirrorRoot))
	if err == nil {
		_ = json.Unmarshal(b, &s)
	}
	return s
}

func waitAppliedThrough(sh *shape, limit time.Duration, lsn string) bool {
	want := changestream.ParseLSN(lsn)
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if changestream.ParseLSN(readState(sh).AppliedLSN) >= want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func waitCaughtUp(ctx context.Context, pool *pgxpool.Pool, limit time.Duration) {
	var lsn string
	_ = pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	trickleStop := make(chan struct{})
	go trickle(ctx, pool, trickleStop)
	ok := waitAppliedThrough(nil, limit, lsn)
	close(trickleStop)
	if !ok {
		fmt.Println("  WARNING: mirror was not caught up before the run")
	}
}

// settleQuiet waits until the mirror has applied everything, on a database with
// no other writers.
//
// The obvious version — note the WAL position, run a trickle so the watermark
// keeps moving, wait for applied_lsn to reach it — is wrong in a way that costs
// exactly one row, every time. applied_lsn is the commit LSN of the last
// APPLIED transaction, so it only advances when something else commits; the
// trickle that makes the target reachable is itself writing rows, and its final
// commit lands after the wait returns. The source then has one row the mirror
// has not seen, and the harness reports a DIVERGENCE that is really lag.
//
// The fix is a pair of IDEMPOTENT markers. Committing M1 then M2 guarantees
// applied_lsn can advance past M1, and because both markers upsert the same key
// to the same value, M2 being unapplied cannot change the comparison.
func settleQuiet(ctx context.Context, pool *pgxpool.Pool, sh *shape) bool {
	const markerID = 999_999_999
	mark := func() string {
		var lsn string
		err := pool.QueryRow(ctx, `WITH ins AS (
			INSERT INTO m(id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id
			RETURNING 1) SELECT pg_current_wal_lsn()::text FROM ins`, markerID).Scan(&lsn)
		if err != nil {
			return ""
		}
		return lsn
	}
	for attempt := 0; attempt < 3; attempt++ {
		first := mark()
		_ = mark()
		if first == "" {
			return false
		}
		if waitAppliedThrough(sh, 5*time.Minute, first) {
			return true
		}
	}
	return false
}

func trickle(ctx context.Context, pool *pgxpool.Pool, stop <-chan struct{}) {
	c, err := pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer c.Release()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	id := 900_000_000
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			id++
			// A key that exists in every shape: the primary key, and nothing else.
			_, _ = c.Exec(ctx, `INSERT INTO m(id) VALUES ($1)
				ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id`, id)
		}
	}
}

func measureLatency(ctx context.Context, pool *pgxpool.Pool, sh *shape, n int) []time.Duration {
	c, err := pool.Acquire(ctx)
	if err != nil {
		return nil
	}
	defer c.Release()
	out := make([]time.Duration, 0, n)
	id := 950_000_000
	for i := 0; i < n; i++ {
		id++
		t0 := time.Now()
		var lsn string
		err := c.QueryRow(ctx, `WITH ins AS (
			INSERT INTO m(id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET id=EXCLUDED.id
			RETURNING 1) SELECT pg_current_wal_lsn()::text FROM ins`, id).Scan(&lsn)
		if err != nil {
			continue
		}
		// keep the watermark moving so the target LSN is reachable
		stop := make(chan struct{})
		go trickle(ctx, pool, stop)
		ok := waitAppliedThrough(sh, 30*time.Second, lsn)
		close(stop)
		if ok {
			out = append(out, time.Since(t0))
		}
	}
	return out
}

func procStats() (float64, int64) {
	if *pid == "" {
		return 0, 0
	}
	stat, err := os.ReadFile("/proc/" + strings.TrimSpace(*pid) + "/stat")
	if err != nil {
		return 0, 0
	}
	rest := string(stat)
	if i := strings.LastIndex(rest, ")"); i >= 0 {
		rest = rest[i+2:]
	}
	f := strings.Fields(rest)
	if len(f) < 13 {
		return 0, 0
	}
	ut, _ := strconv.ParseFloat(f[11], 64)
	st, _ := strconv.ParseFloat(f[12], 64)
	var rss int64
	if b, err := os.ReadFile("/proc/" + strings.TrimSpace(*pid) + "/status"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fs := strings.Fields(line)
				if len(fs) >= 2 {
					kb, _ := strconv.ParseInt(fs[1], 10, 64)
					rss = kb * 1024
				}
			}
		}
	}
	return (ut + st) / 100.0, rss
}

// ---- helpers -----------------------------------------------------------

func dirBytes(path string) int64 {
	out, err := exec.Command("du", "-sb", path).Output()
	if err != nil {
		return 0
	}
	n, _ := strconv.ParseInt(strings.Fields(string(out))[0], 10, 64)
	return n
}

func pct(sorted []time.Duration, p int) time.Duration {
	i := (len(sorted)*p + 99) / 100
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func human(b int64) string {
	const u = 1024
	if b < u {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(u), 0
	for n := b / u; n >= u; n /= u {
		div *= u
		exp++
	}
	return fmt.Sprintf("%.0f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func commas(n int64) string {
	s := strconv.FormatInt(n, 10)
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func mustExec(ctx context.Context, pool *pgxpool.Pool, sql string) {
	_, err := pool.Exec(ctx, sql)
	must(err)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}

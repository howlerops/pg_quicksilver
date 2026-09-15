// qs-perf measures the production ingest path — the qs-mirror sidecar, running
// as its own process, configured only by the environment the plugin sets.
//
// Four numbers, chosen because they are the ones that decide whether the design
// works at all:
//
//	snapshot rate         how long a mirror takes to become useful at all
//	drain rate            the ceiling: how fast the mirror applies a backlog
//	commit-to-visible     the freshness SLO's real constraint, p50/p95/p99
//	storage ratio         heap bytes vs mirror bytes, which is the economics
//
// Latency is measured against applied_lsn rather than by reading rows back.
// "Visible" means the mirror has durably applied through the LSN the commit
// landed at, which is an O(1) check; polling for a row would measure the read
// path instead and would get slower as the mirror grew, making the number a
// function of the benchmark rather than the system.
//
//	go run ./cmd/qs-perf -dsn ... -mirror /path -health 127.0.0.1:9199
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
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

var (
	dsn        = flag.String("dsn", "postgres://postgres@localhost:5443/app", "primary")
	mirrorRoot = flag.String("mirror", "", "mirror root directory")
	table      = flag.String("table", "public.events", "mirrored table")
	health     = flag.String("health", "127.0.0.1:9199", "sidecar health address")
	pidFile    = flag.String("pid", "", "sidecar pid, for CPU and RSS")
	burst      = flag.Int("burst", 200000, "rows in the drain-rate burst")
	markers    = flag.Int("markers", 200, "commit-to-visible samples")
	rate       = flag.Int("rate", 200, "writes per second during the latency run")
	writers    = flag.Int("writers", 8, "concurrent writers for the burst")
)

func main() {
	flag.Parse()
	if *mirrorRoot == "" {
		fmt.Fprintln(os.Stderr, "-mirror is required")
		os.Exit(2)
	}
	ctx := context.Background()

	var failed bool
	pool, err := pgxpool.New(ctx, *dsn+"?pool_max_conns=32")
	must(err)
	defer pool.Close()

	fmt.Println("========================================================================")
	fmt.Println("Quicksilver ingest performance — the qs-mirror sidecar, as deployed")
	fmt.Println("========================================================================")
	fmt.Printf("host: %s\n", hostInfo())
	fmt.Printf("table: %s   burst: %d rows   writers: %d\n", *table, *burst, *writers)

	// The mirror must already be caught up, or every number below is measuring
	// the tail of someone else's backlog.
	waitCaughtUp(ctx, pool, 120*time.Second)
	base := snapshotState()
	startCPU, startRSS := procStats()

	// ---- 1. drain rate -------------------------------------------------
	// Write a burst as fast as the writers can, then measure from the moment
	// the last commit lands to the moment the mirror has applied through it.
	// That interval divided by the rows is the apply ceiling, with the source's
	// own write throughput excluded.
	fmt.Println("\n-- 1. drain rate (apply ceiling) ---------------------------------------")
	wrote, writeDur, lastLSN := writeBurst(ctx, pool, *burst, *writers)
	fmt.Printf("  source wrote  %d rows in %.2fs (%.0f rows/s into PostgreSQL)\n",
		wrote, writeDur.Seconds(), float64(wrote)/writeDur.Seconds())
	// A trickle of writes must continue during the drain. applied_lsn is the
	// commit LSN of the last APPLIED transaction, so with no further commits it
	// stops just short of the WAL position the burst ended at and the wait never
	// completes — a property of the watermark, not of the mirror. The trickle is
	// a few rows a second and keeps the watermark meaningful.
	trickleStop := make(chan struct{})
	go trickle(ctx, pool, trickleStop)
	drainStart := time.Now()
	drained := waitAppliedThrough(lastLSN, 10*time.Minute)
	drain := time.Since(drainStart)
	close(trickleStop)
	if !drained {
		fmt.Println("  FAIL: mirror never caught up with the burst")
		failed = true
	}
	total := writeDur + drain
	fmt.Printf("  mirror drained the remaining backlog in %.2fs\n", drain.Seconds())
	fmt.Printf("  end-to-end     %.2fs -> %.0f rows/s sustained through the mirror\n",
		total.Seconds(), float64(wrote)/total.Seconds())

	// ---- 2. commit-to-visible -------------------------------------------
	fmt.Println("\n-- 2. commit-to-visible latency ---------------------------------------")
	fmt.Printf("  %d samples at ~%d writes/s background load\n", *markers, *rate)
	lat := measureLatency(ctx, pool, *markers, *rate)
	if len(lat) == 0 {
		fmt.Println("  FAIL: no latency samples")
		failed = true
	}
	if len(lat) > 0 {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	fmt.Printf("  n=%d  p50=%.1fms  p90=%.1fms  p95=%.1fms  p99=%.1fms  max=%.1fms\n",
		len(lat), ms(pct(lat, 50)), ms(pct(lat, 90)), ms(pct(lat, 95)),
		ms(pct(lat, 99)), ms(lat[len(lat)-1]))
	}

	// ---- 3. storage ------------------------------------------------------
	fmt.Println("\n-- 3. storage ---------------------------------------------------------")
	var heap, rows int64
	must(pool.QueryRow(ctx, `SELECT pg_total_relation_size($1), count(*) FROM `+*table,
		*table).Scan(&heap, &rows))
	mir := dirBytes(*mirrorRoot)
	fmt.Printf("  source %s heap+indexes for %s rows\n", human(heap), commas(rows))
	fmt.Printf("  mirror %s parquet\n", human(mir))
	if mir > 0 {
		fmt.Printf("  ratio  %.1fx smaller  (%.1f bytes/row in the mirror)\n",
			float64(heap)/float64(mir), float64(mir)/float64(rows))
	}

	// ---- 4. what it cost -------------------------------------------------
	fmt.Println("\n-- 4. sidecar cost ----------------------------------------------------")
	endCPU, endRSS := procStats()
	if endCPU > 0 {
		cpu := endCPU - startCPU
		fmt.Printf("  CPU     %.1fs over the whole run\n", cpu)
		if wrote > 0 {
			fmt.Printf("  per row %.1f us of sidecar CPU\n", cpu*1e6/float64(wrote))
		}
		fmt.Printf("  RSS     %s -> %s\n", human(startRSS), human(endRSS))
	} else {
		fmt.Println("  (no -pid given; CPU and RSS not measured)")
	}

	final := snapshotState()
	fmt.Printf("\n  applied_lsn %s -> %s\n", base.AppliedLSN, final.AppliedLSN)
	fmt.Printf("  base files %d, delta files %d\n", len(final.BaseFiles), len(final.DeltaFiles))
	fmt.Println("\n========================================================================")
	if failed {
		fmt.Println("FAILED")
		os.Exit(1)
	}
	fmt.Println("DONE")
}

// ---- workload ----------------------------------------------------------

func writeBurst(ctx context.Context, pool *pgxpool.Pool, n, conc int) (int, time.Duration, string) {
	var done atomic.Int64
	const batch = 500
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				return
			}
			defer conn.Release()
			id := 10_000_000 + w*10_000_000
			for {
				if int(done.Load()) >= n {
					return
				}
				var b strings.Builder
				b.WriteString("INSERT INTO events(id,sku,amount,ts,region,channel,customer_id,session_id,status,qty,discount,note) VALUES ")
				for i := 0; i < batch; i++ {
					if i > 0 {
						b.WriteByte(',')
					}
					id++
					fmt.Fprintf(&b,
						"(%d,'SKU-%d',%d.%02d,now(),'us-east','web',%d,md5('%d'),'ok',%d,1.50,'burst')",
						id, id, id%1000, id%100, id%250000, id, 1+id%9)
				}
				if _, err := conn.Exec(ctx, b.String()); err != nil {
					return
				}
				done.Add(batch)
			}
		}(w)
	}
	wg.Wait()
	dur := time.Since(start)

	var lsn string
	_ = pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	return int(done.Load()), dur, lsn
}

// trickle keeps a few commits per second flowing so that applied_lsn — the
// commit LSN of the last applied transaction — keeps advancing.
func trickle(ctx context.Context, pool *pgxpool.Pool, stop <-chan struct{}) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return
	}
	defer conn.Release()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	id := 80_000_000
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			id++
			_, _ = conn.Exec(ctx,
				`INSERT INTO events(id,sku,amount,ts,region,channel,customer_id,session_id,status,qty,discount,note)
				 VALUES ($1,'TRICKLE',0.01,now(),'ap-south','partner',1,'t','ok',1,0.00,'trickle')
				 ON CONFLICT (id) DO UPDATE SET ts=now()`, id)
		}
	}
}

// measureLatency inserts one marker row at a time under steady background load
// and waits for the mirror's applied_lsn to pass the commit.
func measureLatency(ctx context.Context, pool *pgxpool.Pool, n, perSec int) []time.Duration {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			conn, err := pool.Acquire(ctx)
			if err != nil {
				return
			}
			defer conn.Release()
			tick := time.NewTicker(time.Second / time.Duration(max(perSec/4, 1)))
			defer tick.Stop()
			id := 50_000_000 + w*5_000_000
			for {
				select {
				case <-stop:
					return
				case <-tick.C:
					id++
					_, _ = conn.Exec(ctx,
						`INSERT INTO events(id,sku,amount,ts,region,channel,customer_id,session_id,status,qty,discount,note)
						 VALUES ($1,$2,$3,now(),'eu-west','api',1,'s','ok',1,0.00,'bg')
						 ON CONFLICT (id) DO UPDATE SET amount=EXCLUDED.amount`,
						id, fmt.Sprintf("BG-%d", id), fmt.Sprintf("%d.00", id%97))
				}
			}
		}(w)
	}
	defer func() { close(stop); wg.Wait() }()

	out := make([]time.Duration, 0, n)
	conn, err := pool.Acquire(ctx)
	must(err)
	defer conn.Release()

	for i := 0; i < n; i++ {
		id := 90_000_000 + rand.Intn(5_000_000)
		t0 := time.Now()
		var lsn string
		err := conn.QueryRow(ctx,
			`WITH ins AS (
			   INSERT INTO events(id,sku,amount,ts,region,channel,customer_id,session_id,status,qty,discount,note)
			   VALUES ($1,'MARKER',1.00,now(),'us-west','web',1,'m','ok',1,0.00,'marker')
			   ON CONFLICT (id) DO UPDATE SET ts=now() RETURNING 1)
			 SELECT pg_current_wal_lsn()::text FROM ins`, id).Scan(&lsn)
		if err != nil {
			continue
		}
		if waitAppliedThrough(lsn, 30*time.Second) {
			out = append(out, time.Since(t0))
		}
		time.Sleep(20 * time.Millisecond)
	}
	return out
}

// ---- observing the mirror ----------------------------------------------

type state struct {
	AppliedLSN string   `json:"applied_lsn"`
	BaseFiles  []string `json:"base_files"`
	DeltaFiles []string `json:"delta_files"`
}

func snapshotState() state {
	var s state
	schema, name, _ := strings.Cut(*table, ".")
	b, err := os.ReadFile(fmt.Sprintf("%s/%s.%s/state.json", *mirrorRoot, schema, name))
	if err == nil {
		_ = json.Unmarshal(b, &s)
	}
	return s
}

// waitAppliedThrough polls the mirror's durable watermark. The file is written
// after the data files, so reading it never observes a half-applied batch.
func waitAppliedThrough(lsn string, limit time.Duration) bool {
	want := changestream.ParseLSN(lsn)
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if changestream.ParseLSN(snapshotState().AppliedLSN) >= want {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return false
}

func waitCaughtUp(ctx context.Context, pool *pgxpool.Pool, limit time.Duration) {
	var lsn string
	_ = pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if !waitAppliedThrough(lsn, limit) {
		fmt.Println("  WARNING: mirror was not caught up at the start of the run")
	}
}

func procStats() (cpuSeconds float64, rss int64) {
	if *pidFile == "" {
		return 0, 0
	}
	pid := strings.TrimSpace(*pidFile)
	if b, err := os.ReadFile(pid); err == nil {
		pid = strings.TrimSpace(string(b))
	}
	stat, err := os.ReadFile("/proc/" + pid + "/stat")
	if err != nil {
		return 0, 0
	}
	// utime and stime are fields 14 and 15 after the comm field, which may
	// itself contain spaces — so split after the closing parenthesis.
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
	hz := 100.0
	cpuSeconds = (ut + st) / hz

	if b, err := os.ReadFile("/proc/" + pid + "/status"); err == nil {
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
	return cpuSeconds, rss
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

func hostInfo() string {
	cpus := "?"
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		cpus = strconv.Itoa(strings.Count(string(b), "processor\t:"))
	}
	mem := ""
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "MemTotal:") {
				mem = strings.TrimSpace(strings.TrimPrefix(l, "MemTotal:"))
			}
		}
	}
	return fmt.Sprintf("%s vCPU, %s", cpus, mem)
}

func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
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
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
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

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

var (
	_ = http.Get
	_ = io.Discard
	_ = pgx.Identifier{}
)

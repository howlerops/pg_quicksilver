// qs-mirror is the sidecar the CNPG-I plugin injects into every instance Pod.
// It is the long-running production form of what qs-phase1 and qs-phase2 proved
// in harness form: bootstrap by snapshot, stream pgoutput, apply with DDL
// barriers, and gate readiness on freshness.
//
// The one behaviour that is new here, and the reason this is not just the
// harness with a loop around it, is ROLE. The same Pod spec runs on every
// instance, so the sidecar has to work out for itself whether this node is a
// replica (build the mirror) or the primary (stand down), and it has to notice
// when that changes underneath it. Promotion is not an event we are told about;
// it is a fact we observe.
//
// Configuration is entirely environment, set by internal/plugin/lifecycle.go.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/ddl"
	"github.com/howlerops/pg_quicksilver/go/internal/health"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
	"github.com/howlerops/pg_quicksilver/go/internal/pgtext"
)

type options struct {
	cluster      string
	mode         string
	ingest       string
	tables       []string
	slot         string
	publication  string
	mirrorPath   string
	freshnessSLO time.Duration
	primaryHost  string
	primaryPort  string
	localPort    string
	database     string
	user         string
	password     string
	podName      string
	localSocket  string
	healthAddr   string
}

func loadOptions() options {
	o := options{
		cluster:     env("QS_CLUSTER", ""),
		mode:        env("QS_MODE", "shadow"),
		ingest:      env("QS_INGEST", "logical"),
		slot:        env("QS_SLOT", "quicksilver"),
		publication: env("QS_PUBLICATION", "quicksilver"),
		mirrorPath:  env("QS_MIRROR_PATH", "/var/lib/postgresql/data/quicksilver"),
		primaryHost: env("QS_PRIMARY_HOST", ""),
		primaryPort: env("QS_PRIMARY_PORT", "5432"),
		localPort:   env("QS_LOCAL_PORT", "5432"),
		database:    env("QS_DATABASE", "app"),
		user:        env("QS_PGUSER", "postgres"),
		password:    env("QS_PGPASSWORD", ""),
		podName:     env("QS_POD_NAME", ""),
		localSocket: env("QS_LOCAL_SOCKET_DIR", "/controller/run"),
		healthAddr:  env("QS_HEALTH_ADDR", ":9187"),
	}
	for _, t := range strings.Split(env("QS_TABLES", ""), ",") {
		if t = strings.TrimSpace(t); t != "" {
			o.tables = append(o.tables, t)
		}
	}
	d, err := time.ParseDuration(env("QS_FRESHNESS_SLO", "30s"))
	if err != nil {
		d = 30 * time.Second
	}
	o.freshnessSLO = d
	return o
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func (o options) primaryDSN() string {
	dsn := fmt.Sprintf("postgres://%s@%s:%s/%s", o.user, o.primaryHost, o.primaryPort, o.database)
	if o.password != "" {
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
			o.user, o.password, o.primaryHost, o.primaryPort, o.database)
	}
	// Pinned: the mirror stores the TEXT PostgreSQL renders, and that text is a
	// property of the session, not of the value. See internal/pgtext.
	return pgtext.PinDSN(dsn + "?sslmode=prefer")
}

// localDSN talks to the PostgreSQL in this same Pod over its unix socket. It is
// used for one thing only — asking whether this node is in recovery — so it
// deliberately does not carry a password: peer authentication on the socket is
// both sufficient and the only thing available before the app secret is read.
func (o options) localDSN() string {
	return pgtext.PinDSN(fmt.Sprintf("postgres://postgres@/%s?host=%s&port=%s",
		o.database, o.localSocket, o.localPort))
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	o := loadOptions()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Optional pprof, off unless asked for. Profiling the apply path in place
	// is how the O(table)-per-batch costs in docs/18 were found; guessing at
	// them produced two wrong fixes first.
	if addr := os.Getenv("QS_DEBUG_ADDR"); addr != "" {
		go func() {
			log.Info("pprof listening", "addr", addr)
			_ = http.ListenAndServe(addr, nil)
		}()
	}

	h := health.New(o.freshnessSLO)
	if _, err := h.Serve(o.healthAddr); err != nil {
		log.Error("health server", "err", err)
		os.Exit(1)
	}
	log.Info("quicksilver mirror starting",
		"cluster", o.cluster, "pod", o.podName, "mode", o.mode,
		"ingest", o.ingest, "tables", o.tables, "slo", o.freshnessSLO)

	if o.mode == "off" {
		log.Info("mode: off — not building a mirror; idling so the Pod stays healthy")
		<-ctx.Done()
		return
	}
	if o.ingest != "logical" {
		log.Error("unsupported ingest mode", "ingest", o.ingest)
		os.Exit(1)
	}

	supervise(ctx, log, o, h)
}

// supervise is the outer loop. It owns exactly two decisions: what role this
// node currently has, and how long to wait before trying again after a failure.
func supervise(ctx context.Context, log *slog.Logger, o options, h *health.Health) {
	backoff := time.Second
	wasPrimary := false

	for ctx.Err() == nil {
		primary, err := isPrimary(ctx, o)
		switch {
		case err != nil:
			log.Warn("cannot determine local role", "err", err)
			sleep(ctx, 5*time.Second)
			continue

		case primary:
			if !wasPrimary {
				log.Info("this node is the primary — standing down; the mirror is built on replicas")
				// A node that just became the primary carries the standby
				// settings it inherited, and one of them stops logical decoding
				// dead: synchronized_standby_slots still names the physical slot
				// the OLD primary held for this node. Decoding then waits
				// forever, with a warning in the log and no error to any client
				// (docs/15). Nothing else clears it, so we do.
				if err := clearStaleSyncSlots(ctx, o, log); err != nil {
					log.Warn("could not reconcile synchronized_standby_slots", "err", err)
				}
				wasPrimary = true
			}
			h.RecordCaughtUp() // a node that builds no mirror is never stale
			sleep(ctx, 10*time.Second)
			continue
		}

		wasPrimary = false
		err = runMirror(ctx, log, o, h)
		if ctx.Err() != nil {
			return
		}
		// The slot guard fires from inside the apply loop, which cannot drop
		// the slot while its own stream is holding it. So the drop happens
		// here, once that stream is gone — and it must happen before the retry,
		// or the next start resumes the same runaway slot and the primary keeps
		// filling up.
		if errors.Is(err, ErrSlotTooFar) {
			dropSlot(ctx, o, o.slot, log)
		}
		log.Error("mirror stopped", "err", err, "retry_in", backoff)
		sleep(ctx, backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func isPrimary(ctx context.Context, o options) (bool, error) {
	conn, err := pgx.Connect(ctx, o.localDSN())
	if err != nil {
		return false, err
	}
	defer conn.Close(ctx)
	var inRecovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return false, err
	}
	return !inRecovery, nil
}

// roleWatch answers "has this node been promoted?" without opening a connection
// to ask.
//
// The apply loop asks once per tick, and the version above CONNECTS every time:
// five PostgreSQL backends a second at the 200 ms interval, and far more during
// a drain, where the ticker refires in a millisecond. It did not show up while
// ticks were huge, because one connection amortised over fifteen seconds of
// apply is nothing. Bounding the tick made it the dominant per-tick cost and
// took 13% off the drain rate — the cap did not cost that throughput, this did.
//
// The connection is reused and reopened on error. The QUESTION is asked exactly
// as often as before, so the window in which a promoted node could keep
// streaming is unchanged; only the cost of asking is gone.
type roleWatch struct {
	o    options
	conn *pgx.Conn
}

func (r *roleWatch) isPrimary(ctx context.Context) (bool, error) {
	if r.conn == nil || r.conn.IsClosed() {
		c, err := pgx.Connect(ctx, r.o.localDSN())
		if err != nil {
			return false, err
		}
		r.conn = c
	}
	var inRecovery bool
	if err := r.conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		// A broken connection must not read as "not promoted". Drop it so the
		// next tick reconnects, and report the error rather than a guess.
		_ = r.conn.Close(ctx)
		r.conn = nil
		return false, err
	}
	return !inRecovery, nil
}

func (r *roleWatch) close(ctx context.Context) {
	if r.conn != nil {
		_ = r.conn.Close(ctx)
		r.conn = nil
	}
}

// clearStaleSyncSlots removes synchronized_standby_slots entries naming slots
// that do not exist on this server. It is deliberately narrow: an entry that
// DOES resolve belongs to a standby this node is legitimately waiting for, and
// removing it would let a logical consumer get ahead of the node that will
// replace this one — which is the data-loss case the setting exists to prevent.
func clearStaleSyncSlots(ctx context.Context, o options, log *slog.Logger) error {
	conn, err := pgx.Connect(ctx, o.localDSN())
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var raw string
	if err := conn.QueryRow(ctx, "SHOW synchronized_standby_slots").Scan(&raw); err != nil {
		return err
	}
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	var keep []string
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		var n int
		if err := conn.QueryRow(ctx,
			"SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", name).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			keep = append(keep, name)
		} else {
			log.Info("dropping a synchronized_standby_slots entry that does not exist here",
				"slot", name)
		}
	}
	if len(keep) == len(strings.Split(raw, ",")) {
		return nil
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(
		"ALTER SYSTEM SET synchronized_standby_slots = '%s'", strings.Join(keep, ","))); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "SELECT pg_reload_conf()")
	return err
}

// runMirror owns one connected lifetime: bootstrap or resume, then stream until
// something breaks. Returning an error is how it asks the supervisor to retry.
func runMirror(ctx context.Context, log *slog.Logger, o options, h *health.Health) error {
	conn, err := pgx.Connect(ctx, o.primaryDSN())
	if err != nil {
		return fmt.Errorf("connect to primary: %w", err)
	}
	defer conn.Close(ctx)

	if err := ddl.Setup(ctx, conn); err != nil {
		return fmt.Errorf("ddl setup: %w", err)
	}
	published := append(append([]string{}, o.tables...), ddl.LogTable)
	if err := ensurePublication(ctx, conn, o.publication, published); err != nil {
		return fmt.Errorf("publication: %w", err)
	}

	tables := map[string]*mirror.Table{}
	for _, qualified := range o.tables {
		schema, name, ok := strings.Cut(qualified, ".")
		if !ok {
			return fmt.Errorf("table %q is not schema.table", qualified)
		}
		cols, order, err := ddl.LiveColumns(ctx, conn, schema, name)
		if err != nil {
			return fmt.Errorf("read columns for %s: %w", qualified, err)
		}
		key, err := primaryKeyColumn(ctx, conn, schema, name)
		if err != nil {
			return fmt.Errorf("%s: %w", qualified, err)
		}
		t, err := mirror.New(o.mirrorPath, schema, name, key, cols, order)
		if err != nil {
			return err
		}
		if reason := t.Halted(); reason != "" {
			// Durable, so a restart cannot quietly resume. Readiness goes false
			// and stays false: this node must leave the endpoint.
			h.RecordVerification(true)
			return fmt.Errorf("%s is halted and will not resume without an operator: %s",
				qualified, reason)
		}
		tables[qualified] = t
	}

	st, err := changestream.NewStreaming(ctx, o.primaryDSN(), o.slot, o.publication, published)
	if err != nil {
		return err
	}
	defer func() { _ = st.Close(context.WithoutCancel(ctx)) }()
	st.Failover = true

	consistent, created, err := st.CreateSlot(ctx)
	if err != nil {
		return fmt.Errorf("create slot: %w", err)
	}

	// Three cases, and only one of them is a plain resume:
	//
	//   created + no mirror  -> snapshot at the consistent point (first start)
	//   created + old mirror -> snapshot; the slot is new, so the WAL between
	//                           the mirror's LSN and now is gone. Resuming here
	//                           would leave a hole nothing ever fills.
	//   exists  + mirror     -> resume from the server's confirmed position.
	//
	// The second case is the one that is tempting to skip because the mirror
	// looks populated. It is exactly the silent-data-loss shape.
	if created {
		log.Info("new replication slot; bootstrapping by snapshot", "at", consistent)
		// Readiness stays false throughout. A node whose snapshot is still
		// running has nothing to serve, and its lag reads as zero because lag
		// is measured from process start — so without the bootstrap gate it
		// would join the read endpoint immediately with an empty mirror.
		for q, t := range tables {
			t0 := time.Now()
			n, err := t.Snapshot(ctx, conn, consistent.String())
			if err != nil {
				return fmt.Errorf("snapshot %s: %w", q, err)
			}
			log.Info("snapshot complete", "table", q, "rows", n,
				"seconds", time.Since(t0).Seconds())
		}
		if err := st.Start(ctx, consistent); err != nil {
			return err
		}
	} else {
		// A table can join a slot the others are already using — QS_TABLES
		// gained an entry, or the `tables:` pattern started matching one.
		// snapshot.go has listed that as a case needing a snapshot since it was
		// written; the decision here was made per-SLOT rather than per-TABLE,
		// so it never got one.
		//
		// What that produced was the shape this project keeps finding. The new
		// table received every change from the moment it was added and nothing
		// before it: 1,010 rows against a source of 6,000, no error, no warning,
		// and a mirror that answers questions. The end-to-end suite found it the
		// first time it ran two tables through one sidecar.
		//
		// The snapshot is taken at the CURRENT LSN while the stream resumes from
		// the slot's older position, so this depends on the per-table LSN floor
		// in Table.Apply to discard the replayed prefix. Without that floor this
		// would write pre-snapshot values over post-snapshot ones.
		var fresh []string
		for q, t := range tables {
			if t.State.AppliedLSN == "" {
				fresh = append(fresh, q)
			}
		}
		if len(fresh) > 0 {
			sort.Strings(fresh)
			at, err := currentLSN(ctx, conn)
			if err != nil {
				return fmt.Errorf("current lsn for a newly added table: %w", err)
			}
			log.Info("tables added to an existing slot; snapshotting just those",
				"tables", fresh, "at", at)
			for _, q := range fresh {
				t0 := time.Now()
				n, err := tables[q].Snapshot(ctx, conn, at)
				if err != nil {
					return fmt.Errorf("snapshot %s: %w", q, err)
				}
				log.Info("snapshot complete", "table", q, "rows", n,
					"seconds", time.Since(t0).Seconds())
			}
		}
		log.Info("resuming from the existing slot", "slot", o.slot)
		if err := st.Start(ctx, 0); err != nil {
			return err
		}
	}
	h.RecordBootstrapped()

	return pump(ctx, log, o, h, conn, st, tables)
}

func pump(
	ctx context.Context, log *slog.Logger, o options, h *health.Health,
	conn *pgx.Conn, st *changestream.Streaming, tables map[string]*mirror.Table,
) error {
	// pending is the queue this loop owns. Transactions() DRAINS the stream's
	// buffer, so whatever a DDL barrier cuts off has to be held here — there is
	// nothing to re-read it from. Dropping it instead loses every change
	// committed after a schema change, in the one shape this project keeps
	// finding: row counts stay plausible and nothing errors.
	var pending []changestream.Transaction
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	// One connection for the role check, held for the life of the loop rather
	// than opened per tick. See roleWatch.
	role := &roleWatch{o: o}
	defer role.close(ctx)

	// And a guard on how much WAL this mirror's slot may pin. See slotguard.go:
	// a mirror that cannot keep up fills the PRIMARY's disk, which the
	// large-scale run demonstrated by doing it.
	guard := &slotGuard{slot: o.slot, conn: conn, last: time.Now()}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		tt := newTickTimer()

		// A promotion of THIS node means we are now the primary and must stop.
		// Checking here rather than only in the supervisor keeps the window
		// small: a mirror that keeps streaming after its node is promoted is
		// reading its own writes.
		if primary, err := role.isPrimary(ctx); err == nil && primary {
			return fmt.Errorf("this node was promoted; handing back to the supervisor")
		}
		tt.done("is_primary")

		fresh, err := st.Transactions(ctx)
		if err != nil {
			return err
		}
		tt.done("read_stream")
		pending = append(pending, fresh...)
		if len(pending) == 0 {
			// Idle is not stale. An unchanging source is perfectly fresh, and
			// conflating the two reports a quiet database as broken.
			h.RecordCaughtUp()
			tt.report(log)
			continue
		}
		txns := pending

		// A DDL transaction is a barrier: apply up to and including it, then
		// re-read the catalog before anything after it is applied. Noticing the
		// barrier without stopping writes post-DDL rows with the pre-DDL column
		// list, which drops the new column while row counts still match.
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
		// And a SIZE barrier, for the same reason the DDL one exists: whatever
		// is not applied this tick is held in `pending` and applied on the next.
		//
		// Without it a tick applies whatever the stream handed over, and during
		// catch-up that grows without limit — measured at 849ms, 2452ms,
		// 5706ms, 9332ms, 15528ms on five consecutive ticks of the narrow
		// shape, each one bigger because the backlog outran the drain. A tick
		// that takes fifteen seconds is fifteen seconds in which nothing else
		// in this loop runs: no confirm, no freshness update, no compaction
		// swap. It is the largest stall in the system and it was invisible
		// until the phases were timed, because the latency samples are taken
		// after the drain and never saw it.
		//
		// Capping costs no throughput. The ticker already fires again in a
		// millisecond whenever `pending` is non-empty, so the same work is done
		// in more, smaller ticks — which is the entire point.
		head = capBatch(head)
		pending = append([]changestream.Transaction(nil), txns[len(head):]...)

		for _, t := range tables {
			if _, err := t.Apply(head); err != nil {
				return fmt.Errorf("apply: %w", err)
			}
		}
		tt.done("apply")
		last := head[len(head)-1]
		if err := st.Confirm(ctx, last.NextLSN); err != nil {
			return fmt.Errorf("confirm: %w", err)
		}
		tt.done("confirm")

		var headLSN string
		_ = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&headLSN)
		tt.done("head_lsn")

		if retained, err := guard.check(ctx, log); err != nil {
			return err
		} else if retained >= 0 {
			h.RecordSlotRetention(retained)
		}
		tt.done("slot_guard")
		backlog := 0
		for _, t := range tables {
			backlog += t.CompactionBacklog()
		}
		h.RecordApply(last.CommitLSN, headLSN, backlog)

		if cut >= 0 {
			if err := evolve(ctx, log, h, conn, tables); err != nil {
				h.RecordVerification(true)
				return err
			}
			tt.done("evolve")
		}

		// Compaction is triggered by how much has CHANGED, not by how many
		// batches have gone by. It rewrites every live row, so a batch-count
		// trigger makes the cost scale with the table while the trigger scales
		// with traffic — measured at 5M rows as a 30x throughput collapse and a
		// multi-gigabyte RSS spike (docs/18).
		for q, t := range tables {
			// A finished rewrite is swapped in here, on this goroutine, which is
			// what lets the swap decide liveness against the current index with
			// no locking.
			if t.Compacting() {
				if n, err := t.FinishCompaction(); err != nil {
					log.Warn("compaction failed", "table", q, "err", err)
				} else if n > 0 {
					log.Info("compacted", "table", q, "rows", n)
				}
				// The rewrite itself is asynchronous; THIS is the part that is
				// not. The swap walks the rewritten file deciding liveness
				// against the current index, so it is O(rows in the file) on
				// the apply goroutine, and on a shape with a large base and
				// small deltas it is the only candidate for a tail.
				tt.done("finish_compaction")
			}
			switch {
			case t.Compacting() && t.ShouldMergeDeltas():
				// Merging is allowed DURING a rewrite, over the deltas the
				// rewrite is not reading. Blocking it let the file count grow
				// unchecked behind a rewrite that heavy churn had already made
				// stale (docs/19).
				n, err := t.MergeDeltas()
				if err != nil {
					log.Warn("delta merge failed", "table", q, "err", err)
				} else if n > 0 {
					log.Info("merged deltas during compaction", "table", q, "rows", n)
				}
				tt.done("merge_during_compaction")

			case t.Compacting():
				// a rewrite is in flight and the file count is fine

			case t.ShouldCompact():
				// Enough of the TABLE has changed to justify rewriting the base.
				// This runs in the BACKGROUND: doing it inline is what put p99
				// commit-to-visible at 24.6s against a p50 of 206ms (docs/19).
				if c := t.BeginCompaction(); c != nil {
					log.Info("compaction started", "table", q,
						"base_rows", t.State.BaseRows, "delta_rows", t.State.DeltaRows)
				}
				tt.done("begin_compaction")

			case t.ShouldMergeDeltas():
				// too many files to read through, but not enough churn to pay
				// for a base rewrite — merge the deltas instead, which costs
				// only what the deltas hold
				t0 := time.Now()
				n, err := t.MergeDeltas()
				if err != nil {
					log.Warn("delta merge failed", "table", q, "err", err)
					continue
				}
				log.Info("merged deltas", "table", q, "rows", n,
					"seconds", time.Since(t0).Seconds())
				tt.done("merge_deltas")
			}
		}

		tt.report(log)

		// Whatever the barrier cut off is held in `pending` and applied on the
		// next tick, against the schema we just re-read.
		if len(pending) > 0 {
			ticker.Reset(time.Millisecond)
		} else {
			ticker.Reset(200 * time.Millisecond)
		}
	}
}

// evolve re-reads the catalog after a DDL barrier. Per docs/06: changes we can
// apply faithfully are applied; anything else stops mirroring that table loudly
// rather than writing values into the wrong columns.
func evolve(
	ctx context.Context, log *slog.Logger, h *health.Health,
	conn *pgx.Conn, tables map[string]*mirror.Table,
) error {
	for qualified, t := range tables {
		schema, name, _ := strings.Cut(qualified, ".")
		live, liveOrder, err := ddl.LiveColumns(ctx, conn, schema, name)
		if err != nil {
			return err
		}
		d := ddl.Compare(t.Columns, live)
		if d.Empty() {
			continue
		}
		if !d.Safe() {
			return fmt.Errorf("halting on unsupported schema change to %s: %s", qualified, d)
		}

		// An added column may already hold values in rows the change stream will
		// never mention. ADD COLUMN ... DEFAULT writes no row-level WAL at all.
		added := make([]string, 0, len(d.Added))
		for name := range d.Added {
			added = append(added, name)
		}
		bf, err := ddl.InspectBackfill(ctx, conn, schema, name, added)
		if err != nil {
			return err
		}
		if len(bf.Rewritten) > 0 {
			reason := fmt.Sprintf(
				"column(s) %s were backfilled by a table rewrite; their values never "+
					"appear in the change stream", strings.Join(bf.Rewritten, ", "))
			if err := t.Halt(reason); err != nil {
				return err
			}
			// A volatile default or a stored generated column rewrote the heap:
			// every pre-existing row got its own value and none of it reached
			// the change stream. Serving NULL there would be silent corruption,
			// so the mirror stops and says why. Recovery is an operator-ordered
			// rebuild of this table.
			return fmt.Errorf("halting %s: %s; the mirror must be rebuilt", qualified, reason)
		}
		// A rewrite in flight is reading this table's column list on another
		// goroutine. Rather than lock the schema for the length of a rewrite,
		// throw the rewrite away — it is recomputable, and the next one starts
		// from the evolved schema.
		t.AbortCompaction()
		if err := t.Evolve(live, liveOrder, bf.MissingVals); err != nil {
			return err
		}
		log.Info("schema evolved at a DDL barrier", "table", qualified,
			"change", d.String(), "backfilled", bf.MissingVals)
	}
	return nil
}

func ensurePublication(ctx context.Context, conn *pgx.Conn, name string, tables []string) error {
	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)", name).Scan(&exists); err != nil {
		return err
	}
	quoted := make([]string, 0, len(tables))
	for _, t := range tables {
		schema, tbl, ok := strings.Cut(t, ".")
		if !ok {
			return fmt.Errorf("table %q is not schema.table", t)
		}
		quoted = append(quoted, pgx.Identifier{schema, tbl}.Sanitize())
	}
	if !exists {
		_, err := conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			pgx.Identifier{name}.Sanitize(), strings.Join(quoted, ", ")))
		return err
	}
	// SET TABLE is idempotent and converges the publication on the configured
	// list, so adding a table to `tables:` takes effect without an operator
	// having to touch the database.
	_, err := conn.Exec(ctx, fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s",
		pgx.Identifier{name}.Sanitize(), strings.Join(quoted, ", ")))
	return err
}

// primaryKeyColumn finds the single-column primary key the mirror needs as its
// upsert key. A composite or absent primary key is refused rather than guessed:
// the mirror is a key-addressed store, and choosing the wrong key produces a
// mirror that merges unrelated rows.
func primaryKeyColumn(ctx context.Context, conn *pgx.Conn, schema, table string) (string, error) {
	rows, err := conn.Query(ctx, `
		SELECT a.attname
		  FROM pg_index i
		  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		 WHERE i.indrelid = format('%I.%I', $1::text, $2::text)::regclass
		   AND i.indisprimary`, schema, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	switch len(cols) {
	case 1:
		return cols[0], nil
	case 0:
		return "", fmt.Errorf("no primary key; Quicksilver mirrors key-addressed tables only")
	default:
		return "", fmt.Errorf("composite primary key (%s) is not supported yet",
			strings.Join(cols, ", "))
	}
}

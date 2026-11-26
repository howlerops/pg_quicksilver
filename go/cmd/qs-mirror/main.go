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
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
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
	slot         string // this Pod's own slot; see slotname.go
	slotBase     string // the configured name it was derived from
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
		healthAddr:  env("QS_HEALTH_ADDR", ":9188"), // NOT 9187: CNPG's instance manager owns that
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
	// QS_SLOT is the same on every Pod; the slot must not be.
	o.slotBase = o.slot
	o.slot = podSlotName(o.slotBase, o.podName)
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
	changestream.SpoolDir = filepath.Join(o.mirrorPath, ".spool")

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
		"ingest", o.ingest, "tables", o.tables, "slo", o.freshnessSLO,
		"slot", o.slot, "configured_slot", o.slotBase)
	if o.podName == "" {
		log.Warn("QS_POD_NAME is not set, so the replication slot is not unique to "+
			"this sidecar; a second sidecar given the same QS_SLOT would share one "+
			"cursor with it and silently miss rows", "slot", o.slot)
	}

	if o.mode == "off" {
		log.Info("mode: off — not building a mirror; idling so the Pod stays healthy")
		<-ctx.Done()
		return
	}
	if o.ingest != "logical" {
		log.Error("unsupported ingest mode", "ingest", o.ingest)
		os.Exit(1)
	}

	if err := openLake(ctx, log, o, h); err != nil {
		log.Error("QS_DUCKLAKE is on and the lake cannot be opened", "err", err)
		os.Exit(1)
	}
	if lake != nil {
		defer lake.Close()
	}

	// Halted tables, from the manifests, BEFORE anything connects. Opening a
	// table needs the source's catalog, and a probe that waits for the source
	// to answer reports "still bootstrapping" about a Pod whose real condition
	// is on its own disk and will not pass by itself.
	h.SetServeWithHalted(serveWithHalted())
	h.SetTables(len(o.tables))
	for _, qualified := range o.tables {
		schema, name, ok := strings.Cut(qualified, ".")
		if !ok {
			continue // runMirror reports it
		}
		if reason := mirror.HaltedOnDisk(o.mirrorPath, schema, name); reason != "" {
			h.SetHalted(qualified, reason)
			log.Error("table is halted in its manifest and stays out of service",
				"table", qualified, "reason", reason,
				"serve_with_halted_tables", serveWithHalted())
		}
	}

	supervise(ctx, log, o, h)
}

// supervise is the outer loop. It owns exactly two decisions: what role this
// node currently has, and how long to wait before trying again after a failure.
func supervise(ctx context.Context, log *slog.Logger, o options, h *health.Health) {
	backoff := time.Second
	wasPrimary := false
	// The local socket is not there yet for the first seconds of a Pod's life:
	// this is a native sidecar, so it starts BEFORE PostgreSQL. Logging that at
	// WARN on every 5-second tick produced a hundred identical lines in eight
	// minutes on the first real cluster run, and buried the one line that
	// mattered. Say it once, then again only when the answer changes.
	var lastRoleErr string

	for ctx.Err() == nil {
		primary, err := isPrimary(ctx, o)
		switch {
		case err != nil:
			if msg := err.Error(); msg != lastRoleErr {
				log.Warn("cannot determine local role; waiting for the local PostgreSQL",
					"err", err)
				lastRoleErr = msg
			}
			sleep(ctx, 5*time.Second)
			continue

		case primary:
			lastRoleErr = ""
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
				releaseOwnSlot(ctx, o, log, "this node was promoted and no longer consumes it")
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
			dropSlot(ctx, o, log)
		}
		// ErrHalted now means EVERY table is halted: one halted table among
		// several is skipped and the stream carries on, confirming as it goes
		// (halt.go). With none left there is nothing to stream for, so nothing
		// confirms the slot and nothing guards it either: the guard lives in
		// the pump. Left alone the slot pins WAL on the primary for as long as
		// the halt lasts, and each table is rebuilt from a snapshot regardless.
		// So the slot goes now. Once is enough; dropSlot is quiet when there
		// is nothing to drop. The next attempt re-reads the manifests, which
		// is also how a rebuild request is noticed without a restart here.
		if errors.Is(err, ErrHalted) {
			dropSlot(ctx, o, log)
		}
		log.Error("mirror stopped", "err", err, "retry_in", backoff)
		sleep(ctx, backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// ErrHalted is runMirror or pump stopping because EVERY table's manifest records
// a durable halt. See supervise for what happens to the slot.
var ErrHalted = errors.New("the mirror is halted")

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

// warnStaleSyncSlots says out loud when the PRIMARY is configured to hold
// logical decoding back for a slot that does not exist.
//
// This is the same hazard clearStaleSyncSlots fixes, seen from the other side.
// That one runs when this node IS the primary and can repair it. From a replica
// the setting belongs to a server this process does not own, so the only honest
// thing to do is report it — but reporting it matters more here, because this
// is the side where the symptom appears.
//
// The symptom is nothing. A failover slot whose synchronized_standby_slots
// names a missing slot never advances: no error reaches the client, the stream
// stays connected, the snapshot succeeds, /readyz passes, and the mirror simply
// stops gaining rows. It looks exactly like an idle source. docs/15 documents
// the mechanism; this project then spent a suite run rediscovering it, because
// a benchmark script had left the setting behind and nothing said so.
//
// Only slots with failover=true are affected, which is why a quick check with
// an ordinary slot — pg_recvlogical, say — comes back healthy and points the
// investigation away from the cause.
func warnStaleSyncSlots(ctx context.Context, conn *pgx.Conn, slot string, log *slog.Logger) {
	var raw string
	if err := conn.QueryRow(ctx, "SHOW synchronized_standby_slots").Scan(&raw); err != nil {
		return // not fatal: this is a diagnostic, not a dependency
	}
	if strings.TrimSpace(raw) == "" {
		return
	}
	var missing []string
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name == "" {
			continue
		}
		var n int
		if err := conn.QueryRow(ctx,
			"SELECT count(*) FROM pg_replication_slots WHERE slot_name=$1", name).Scan(&n); err != nil {
			return
		}
		if n == 0 {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return
	}
	log.Warn("the primary holds logical decoding back for a slot that does not exist; "+
		"this mirror will connect, pass readiness and then never advance",
		"synchronized_standby_slots", raw,
		"missing", missing,
		"slot", slot,
		"fix", "on the primary: ALTER SYSTEM SET synchronized_standby_slots = '' (or to slots that exist), then SELECT pg_reload_conf()")
}

// runMirror owns one connected lifetime: bootstrap or resume, then stream until
// something breaks. Returning an error is how it asks the supervisor to retry.
func runMirror(ctx context.Context, log *slog.Logger, o options, h *health.Health) error {
	conn, err := pgx.Connect(ctx, o.primaryDSN())
	if err != nil {
		return fmt.Errorf("connect to primary: %w", err)
	}
	defer conn.Close(ctx)

	// Before anything else, because everything after it succeeds either way and
	// the failure it names is silent.
	warnStaleSyncSlots(ctx, conn, o.slot, log)

	if err := ddl.Setup(ctx, conn); err != nil {
		return fmt.Errorf("ddl setup: %w", err)
	}
	published := append(append([]string{}, o.tables...), ddl.LogTable)
	if err := ensurePublication(ctx, conn, o.publication, published); err != nil {
		return fmt.Errorf("publication: %w", err)
	}

	tables := map[string]*mirror.Table{}
	// Every way out of here abandons these Tables: supervise calls runMirror
	// again and that opens new ones. Left open, each kept its key index mapped
	// and a timer that would go on unlinking files in a directory the new
	// Table now owns.
	defer func() {
		for _, t := range tables {
			t.Close()
		}
	}()
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
		lakeAttach(t)
		tables[qualified] = t
	}

	// Halted tables. Durable, so a restart cannot quietly resume one: it stays
	// in `tables` so every loop below can see it and step over it, and it
	// leaves that state in exactly one way — an operator's rebuild request.
	haltedNow, requested := map[string]string{}, map[string]bool{}
	for q, t := range tables {
		haltedNow[q] = t.Halted()
		requested[q] = rebuildRequested(t.Dir)
	}
	hplan := planHalted(haltedNow, requested)
	for _, q := range hplan.Stale {
		log.Warn("discarding a rebuild request for a table that is not halted",
			"table", q, "file", filepath.Join(tables[q].Dir, rebuildMarker))
		_ = os.Remove(filepath.Join(tables[q].Dir, rebuildMarker))
	}
	for _, q := range append(append([]string{}, hplan.Stay...), hplan.Rebuild...) {
		h.SetHalted(q, tables[q].Halted())
	}
	if len(hplan.Stay) == len(tables) {
		// Before the slot is touched: there is nothing to stream for.
		return errAllHalted(tables)
	}
	rebuilding := map[string]bool{}
	for _, q := range hplan.Rebuild {
		rebuilding[q] = true
		log.Info("rebuild requested for a halted table; it will be re-snapshotted",
			"table", q, "halted_for", tables[q].Halted())
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
			if t.Halted() != "" && !rebuilding[q] {
				continue // halted, and nobody asked: not even a snapshot
			}
			t0 := time.Now()
			n, err := t.Snapshot(ctx, conn, consistent.String())
			if err != nil {
				// A source value this table cannot hold halts the table, here
				// as in the pump; the rest still bootstrap.
				if herr := haltOrFail(log, h, q, t, fmt.Errorf("snapshot %s: %w", q, err)); herr != nil {
					return herr
				}
				continue
			}
			log.Info("snapshot complete", "table", q, "rows", n,
				"seconds", time.Since(t0).Seconds())
			if rebuilding[q] {
				if err := finishRebuild(log, h, q, t); err != nil {
					return err
				}
			}
		}
		if err := st.Start(ctx, consistent); err != nil {
			return err
		}
	} else {
		// The slot existing proves nothing about THIS mirror. Each table is
		// compared with the slot's confirmed position and resumes only if it has
		// reached it; see resume.go for the comparison and for why the snapshot
		// of the ones that have not is taken the way it is.
		//
		// The same path serves a table that joined a slot the others were
		// already using — QS_TABLES gained an entry, or the `tables:` pattern
		// started matching one. That decision used to be per-SLOT, and what it
		// produced was the shape this project keeps finding: 1,010 rows against a
		// source of 6,000, no error, no warning, and a mirror that answers
		// questions.
		//
		// The stream resumes from the slot's older position either way, so a
		// re-snapshotted table depends on the per-table LSN floor in Table.Apply
		// to discard the replayed prefix.
		confirmed, err := slotConfirmedLSN(ctx, conn, o.slot)
		if err != nil {
			return fmt.Errorf("cannot compare the mirror with its slot, so will not resume: %w", err)
		}
		applied := make(map[string]string, len(tables))
		for q, t := range tables {
			// A halted table's applied_lsn stopped when it halted, so the
			// comparison would always say "re-snapshot" — which is the rebuild
			// nobody asked for.
			if t.Halted() != "" {
				continue
			}
			applied[q] = t.State.AppliedLSN
		}
		plan := planResume(applied, confirmed)
		// The rebuilds an operator asked for go through the same path, for the
		// same reason it exists: the stream resumes from the slot's older
		// position, so the table needs a snapshot with an exact LSN floor.
		for _, q := range hplan.Rebuild {
			plan = append(plan, resnapshot{q, "an operator asked for this halted table to be rebuilt (" +
				rebuildMarker + " file)"})
		}
		// resnapshotTables stops at the first table that fails. If that table
		// failed because the source holds something it cannot represent, it is
		// halted (or stays halted) and the others are tried again without it;
		// anything else is the process's problem, as before.
		for len(plan) > 0 {
			err := resnapshotTables(ctx, log, o, conn, tables, plan)
			if err == nil {
				break
			}
			rest := plan[:0:0]
			for _, r := range plan {
				if classifyTableError(err, tables[r.Table].Halted()) == haltTable &&
					strings.Contains(err.Error(), "snapshot "+r.Table+":") {
					if herr := haltOrFail(log, h, r.Table, tables[r.Table], err); herr != nil {
						return herr
					}
					if rebuilding[r.Table] {
						log.Error("the requested rebuild failed and the table stays halted; "+
							"the request is kept, so fix the source and restart to try again",
							"table", r.Table, "err", err)
						delete(rebuilding, r.Table)
					}
					continue
				}
				rest = append(rest, r)
			}
			if len(rest) == len(plan) {
				return err
			}
			plan = rest
		}
		for _, q := range hplan.Rebuild {
			if rebuilding[q] {
				if err := finishRebuild(log, h, q, tables[q]); err != nil {
					return err
				}
			}
		}
		log.Info("resuming from the existing slot", "slot", o.slot,
			"confirmed", pglogrepl.LSN(confirmed))
		if err := st.Start(ctx, 0); err != nil {
			return err
		}
	}
	// The key index, BEFORE readiness rather than on the first change.
	//
	// Nothing built one until now: Snapshot deliberately does not (it stopped
	// writing a persisted index, and ensureIndex reads the key column back from
	// the base file instead), and a resume does not either. So the index was
	// built lazily, inside the first Apply — which put the whole cost behind a
	// probe that had already passed. Measured at 4M rows:
	//
	//	ready in 0.04s, RSS 25 MB       <- the read endpoint adds this Pod
	//	first change after restart: 2.3s, RSS peaked at 507 MB
	//	a second, identical write:  0.5s
	//
	// ~1.8s of stall on a Pod Kubernetes had already been told was fine, and a
	// memory high-water mark an operator sizing a limit at startup never sees.
	//
	// Paying for it here makes both honest, and costs a restarted Pod that same
	// time before it returns to service. That is the right side of the trade for
	// a sidecar behind a readiness probe: a slow Pod is a scheduling problem, a
	// Pod that lies is an outage. The cost is logged per table so that an
	// operator whose restart budget this breaks can see exactly what it was.
	built := make([]string, 0, len(tables))
	for q, t := range tables {
		if t.Halted() != "" {
			continue // out of service: nothing will look a key up in it
		}
		built = append(built, q)
	}
	if len(built) == 0 {
		// Every table that was not already halted halted during its snapshot.
		return errAllHalted(tables)
	}
	sort.Strings(built) // a stable order, so two restarts log comparably
	for _, q := range built {
		t0 := time.Now()
		keys, err := tables[q].BuildIndex()
		if err != nil {
			return fmt.Errorf("build index %s: %w", q, err)
		}
		log.Info("key index built", "table", q, "keys", keys,
			"seconds", time.Since(t0).Seconds())
	}

	lakeReconcile(tables)
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
	warnedStuck := false

	// confirmedLSN is the newest position this loop may consider confirmed. It
	// starts at the highest applied_lsn + 1, which the resume check has shown
	// to be at or beyond the slot's confirmed_flush_lsn (resume.go), so an idle
	// position below it — the walsender re-decoding from restart_lsn after a
	// restart — is never confirmed backwards.
	var confirmedLSN uint64
	for _, t := range tables {
		if t.Halted() != "" {
			continue
		}
		if at := changestream.ParseLSN(t.State.AppliedLSN) + 1; at > confirmedLSN {
			confirmedLSN = at
		}
	}
	var lastIdleConfirm time.Time
	// For the stuck-stream test below: the position at the previous slot
	// measurement, and how many measurements in a row found the slot far
	// behind without that position having moved.
	measuredAt, stalled := confirmedLSN, 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		tt := newTickTimer()

		// Tables halted since the last tick (the apply below, a schema change,
		// a background rewrite) are reported to readiness here, and stepped
		// over by every loop in this function from now on. The stream is still
		// read and still confirmed, so the slot advances past the halted
		// table's changes and pins no WAL for it: they are discarded, because
		// the only way back for that table is a snapshot anyway.
		if noteHalted(log, h, tables) == 0 {
			return errAllHalted(tables)
		}

		// A promotion of THIS node means we are now the primary and must stop.
		// Checking here rather than only in the supervisor keeps the window
		// small: a mirror that keeps streaming after its node is promoted is
		// reading its own writes.
		if primary, err := role.isPrimary(ctx); err == nil && primary {
			return fmt.Errorf("this node was promoted; handing back to the supervisor")
		}
		tt.done("is_primary")

		// BACKPRESSURE. Do not take from the stream when there is already
		// more waiting here than a tick will apply.
		//
		// Capping what a tick APPLIES without capping what is READ does not
		// slow the backlog down — it moves it from PostgreSQL's WAL, where it
		// costs disk, into this process's heap, where it costs RSS. The
		// large-scale run measured the result: 2.4 GB resident for a mirror
		// holding a 139 MB table, 125 bytes per live row, and a qs-verify
		// OOM-killed beside it.
		//
		// This check is only half of the bound, and for a while it was the
		// only half: skipping Transactions() left the stream's receiver
		// reading the socket into its own uncapped buffer, so the backlog
		// accumulated one layer down instead (47 MB to 1.27 GB of heap in
		// seven seconds). The other half is in changestream.Streaming: at
		// maxStreamBufferedChanges it stops reading the socket, while still
		// sending status updates so the walsender does not time it out, and
		// TCP flow control holds the rest in WAL. Together: at most
		// maxPendingChanges + 2 x maxStreamBufferedChanges row-changes in
		// memory, plus one transaction — pgoutput proto v1 delivers a
		// transaction whole however large it is.
		//
		// Nothing is lost by not taking: the slot holds what is not read, and
		// the slot guard already bounds how much WAL that may be.
		queued := 0
		for _, t := range pending {
			queued += t.Len()
		}
		if queued < maxPendingChanges {
			fresh, err := st.Transactions(ctx)
			if err != nil {
				return err
			}
			pending = append(pending, fresh...)
		}
		tt.done("read_stream")
		if len(pending) == 0 {
			// Idle is not stale. An unchanging source is perfectly fresh, and
			// conflating the two reports a quiet database as broken.
			//
			// But "the stream handed me nothing" has TWO causes and this branch
			// used to assume the harmless one. The other is a stream that has
			// stopped delivering, and the mirror cannot tell them apart from
			// the inside — so it reported `ready: true, lag 0.0s` for three
			// minutes while receiving nothing and sitting 24 MB behind, with a
			// walsender wedged in `catchup` because synchronized_standby_slots
			// named a slot that no longer existed (docs/15's trap, arriving on
			// a primary rather than after a promotion).
			//
			// The slot itself distinguishes them. An idle source retains almost
			// nothing; a stuck stream retains more with every commit. So the
			// question the guard already asks every interval answers this one
			// too: silence plus a growing slot is not idleness.
			//
			// IDLE ADVANCE. Before asking, move the slot past WAL that was not
			// for this mirror. The source being quiet FOR US is not the source
			// being quiet: other tables, vacuum and other databases write WAL
			// the walsender decodes and sends nothing for, and a confirmation
			// only ever followed a transaction. So a caught-up mirror's slot
			// stood still, pinned all of it, read as "stuck" below, and at
			// QS_MAX_SLOT_WAL_BYTES the guard dropped the slot of a mirror
			// that was missing nothing — with /readyz at 200 throughout.
			//
			// IdleLSN is a position P such that every transaction committing
			// below P has already been handed to this loop, and `pending` is
			// empty, so every one of them is applied. That makes P confirmable.
			//
			// The values, and why these two:
			//
			//	applied_lsn  <-  P-1     written first, fsynced, on EVERY table
			//	confirm      <-  P       only after that
			//
			// which is the pump's own convention (applied = commit, confirm =
			// commit+1) with P-1 standing where the commit LSN stands. Both
			// readers of applied_lsn stay exact:
			//
			//   - planResume re-snapshots when confirmed > applied+1. Here
			//     confirmed == applied+1, and applied is durable before the
			//     confirmation is even queued, so a crash between the two
			//     leaves the slot BEHIND the mirror, which is the replayable
			//     direction.
			//   - Table.Apply skips commit LSN < applied_lsn, i.e. < P-1. Every
			//     transaction not yet handed out commits at or above P, so the
			//     floor cannot touch it; and PostgreSQL skips only commits
			//     starting below the confirmed P, so it is still sent. A
			//     transaction open across this advance commits later, above P,
			//     and restart_lsn — which the server holds at the oldest
			//     transaction still in progress regardless of what is
			//     confirmed — keeps its WAL.
			//
			// It goes through Apply with a transaction that changes nothing
			// rather than a new way to write applied_lsn: that is the code path
			// that already owns the floor and the fsync. A table whose floor is
			// above P-1 (re-snapshotted at a later point) ignores it.
			//
			// Rate-limited because each advance is a state.json fsync per
			// table, and a keepalive can arrive for every few bytes of WAL.
			if p := st.IdleLSN(); p > confirmedLSN && time.Since(lastIdleConfirm) >= idleConfirmInterval {
				marker := changestream.Transaction{
					CommitLSN: pglogrepl.LSN(p - 1).String(),
					NextLSN:   pglogrepl.LSN(p).String(),
				}
				for _, t := range tables {
					if t.Halted() != "" {
						continue
					}
					if _, err := t.Apply([]changestream.Transaction{marker}); err != nil {
						return fmt.Errorf("idle advance: %w", err)
					}
				}
				if err := st.Confirm(ctx, marker.NextLSN); err != nil {
					return fmt.Errorf("idle confirm: %w", err)
				}
				log.Debug("confirmed WAL that held nothing for this mirror",
					"slot", o.slot, "from", pglogrepl.LSN(confirmedLSN).String(),
					"to", marker.NextLSN)
				confirmedLSN, lastIdleConfirm = p, time.Now()
				tt.done("idle_confirm")
			}

			retained, unconfirmed, gerr := guard.check(ctx, log)
			if gerr != nil {
				return gerr
			}
			if retained >= 0 {
				h.RecordSlotRetention(retained)
			}
			// What "stuck" has to mean now that an idle mirror confirms. A slot
			// far behind is no longer evidence by itself: one burst of
			// unrelated WAL puts it a segment behind until the next idle
			// advance lands. The evidence is that it is far behind AND the
			// position this loop holds has not moved since the previous
			// measurement AND the stream is not offering one to move to — for
			// a whole interval, twice, so a burst that arrives just before a
			// measurement is not a fault. And the verdict stands until the next
			// measurement: it used to be recomputed on every tick from "no
			// measurement this tick", which cleared it 200 ms after it was
			// reached, so readiness never did fail.
			if unconfirmed >= 0 {
				if unconfirmed >= stuckSlotBytes && confirmedLSN == measuredAt && st.IdleLSN() <= confirmedLSN {
					stalled++
				} else {
					stalled = 0
				}
				measuredAt = confirmedLSN
			}
			// UNCONFIRMED, not retained. See slotguard.go: retention is what the
			// slot costs the primary in disk; confirmation is how far behind
			// this mirror actually is. Using the first gave a mirror that was
			// 103 bytes behind a "your stream is stuck" warning at 537 MB.
			if stalled >= 2 {
				// Deliberately NOT RecordCaughtUp: lag_seconds keeps growing
				// from the last real progress, so readiness fails once the SLO
				// is exceeded and this node leaves the read endpoint. Serving
				// stale rows behind a green light is the failure this whole
				// project is arranged against.
				if !warnedStuck {
					warnedStuck = true
					log.Warn("the stream is delivering nothing while this mirror's "+
						"slot keeps growing, so this is NOT an idle source: readiness "+
						"will fail once the freshness SLO is exceeded",
						"slot", o.slot, "unconfirmed_bytes", unconfirmed,
						"retained_bytes", retained,
						"check", "does synchronized_standby_slots name a slot that "+
							"exists? A missing one makes logical decoding wait forever "+
							"with no error to any client (docs/15)")
				}
			} else {
				warnedStuck = false
				h.RecordCaughtUp()
			}
			// Maintenance runs on idle ticks too. It used to run only below, on
			// a tick that had applied something, which got the timing exactly
			// backwards: a mirror that stopped receiving writes also stopped
			// asking whether to compact, and stopped SWAPPING IN a rewrite that
			// had already finished. So a burst that ended mid-rewrite left the
			// new base file written and unused, and a burst that ended just
			// short of the churn threshold left the base under a deletion
			// vector — count(*) at 332 ms instead of 3 on a 10M-row table —
			// until the next write, however long that took to arrive. Idle is
			// when a rewrite is cheapest and when its result lasts longest.
			maintain(log, tables, tt)
			lakeRewrites(log, tables)
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
			// Touches, not a walk over t.Changes: a spooled transaction has
			// none in memory, and a DDL inside one is still a barrier.
			if t.Touches(ddl.LogTable) {
				cut = i
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

		for q, t := range tables {
			if t.Halted() != "" {
				continue
			}
			if _, err := t.Apply(head); err != nil {
				// Either this table cannot continue — it is halted, durably,
				// and the others apply this same batch — or the process cannot.
				// classifyTableError (halt.go) is where that is decided.
				if herr := haltOrFail(log, h, q, t, err); herr != nil {
					return fmt.Errorf("apply: %w", herr)
				}
			}
		}
		if noteHalted(log, h, tables) == 0 {
			// Not confirmed: with no table left the slot is about to be dropped.
			return errAllHalted(tables)
		}
		tt.done("apply")
		last := head[len(head)-1]
		if err := st.Confirm(ctx, last.NextLSN); err != nil {
			return fmt.Errorf("confirm: %w", err)
		}
		tt.done("confirm")
		if at := changestream.ParseLSN(last.NextLSN); at > confirmedLSN {
			confirmedLSN = at
		}

		var headLSN string
		_ = conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&headLSN)
		tt.done("head_lsn")

		if retained, _, err := guard.check(ctx, log); err != nil {
			return err
		} else if retained >= 0 {
			h.RecordSlotRetention(retained)
		}
		tt.done("slot_guard")
		backlog := 0
		for _, t := range tables {
			if t.Halted() != "" {
				continue
			}
			backlog += t.CompactionBacklog()
		}
		h.RecordApply(last.CommitLSN, headLSN, backlog)

		if cut >= 0 {
			// An unsupported change halts its table inside evolve and is not
			// an error; what comes back here is the catalog being unreadable
			// or a manifest that could not be written.
			if err := evolve(ctx, log, h, conn, tables); err != nil {
				return err
			}
			if noteHalted(log, h, tables) == 0 {
				return errAllHalted(tables)
			}
			tt.done("evolve")
		}

		maintain(log, tables, tt)
		lakeRewrites(log, tables)
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

// maintain is the part of a tick that is about the mirror's files rather than
// the stream: swap in a rewrite that has finished, start one that is due, merge
// deltas that have piled up. It runs on EVERY tick, whether or not the tick
// applied anything — see the idle branch of pump for what happened when it did
// not.
//
// That is affordable because none of the questions it asks costs anything when
// the answer is no: CompactReason is counters and a clock, and ShouldMergeDeltas
// is a length.
func maintain(log *slog.Logger, tables map[string]*mirror.Table, tt *tickTimer) {
	// Compaction is triggered by how much has CHANGED, not by how many
	// batches have gone by. It rewrites every live row, so a batch-count
	// trigger makes the cost scale with the table while the trigger scales
	// with traffic — measured at 5M rows as a 30x throughput collapse and a
	// multi-gigabyte RSS spike (docs/18).
	for q, t := range tables {
		if t.Halted() != "" {
			continue
		}
		// A finished rewrite is swapped in here, on this goroutine, which is
		// what lets the swap decide liveness against the current index with
		// no locking.
		if t.Compacting() {
			if n, err := t.FinishCompaction(); err != nil {
				log.Warn("compaction failed", "table", q, "err", err)
				// Or the trigger that started it starts it again on the next
				// tick, and now every tick is one.
				t.CompactionFailed()
			} else if n > 0 {
				log.Info("compacted", "table", q, "rows", n,
					"seconds", t.LastRewrite().Seconds())
			}
			// The rewrite itself is asynchronous; THIS is the part that is
			// not. The swap walks the rewritten file deciding liveness
			// against the current index, so it is O(rows in the file) on
			// the apply goroutine, and on a shape with a large base and
			// small deltas it is the only candidate for a tail.
			tt.done("finish_compaction")
		}
		reason := t.CompactReason()
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

		case reason != "":
			// Enough of the TABLE has changed to justify rewriting the base,
			// or it has gone quiet with a deletion vector still on it.
			// This runs in the BACKGROUND: doing it inline is what put p99
			// commit-to-visible at 24.6s against a p50 of 206ms (docs/19).
			//
			// `reason` names WHICH trigger pulled — churn, dead-fraction or
			// idle (mirror.CompactReason says what each is for).
			// Without it, an A/B of the dead-fraction trigger came back with
			// identical compaction counts in both arms and no way to tell
			// "the knob costs nothing" from "the knob never fired".
			if c := t.BeginCompaction(); c != nil {
				log.Info("compaction started", "table", q, "why", reason,
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
}

// evolve re-reads the catalog after a DDL barrier. Per docs/06: changes we can
// apply faithfully are applied; anything else stops mirroring that table loudly
// rather than writing values into the wrong columns.
func evolve(
	ctx context.Context, log *slog.Logger, h *health.Health,
	conn *pgx.Conn, tables map[string]*mirror.Table,
) error {
	for qualified, t := range tables {
		if t.Halted() != "" {
			continue
		}
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
			// Durable, like the rewrite case below. It used to be an error
			// only, so the restart that followed re-read the catalog, took the
			// new schema as though it had always been there, and resumed.
			if err := t.Halt(fmt.Sprintf("halting %s: unsupported schema change (%s); "+
				"the mirror must be rebuilt", qualified, d)); err != nil {
				return err
			}
			recordHalt(log, h, qualified, t)
			continue
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
			// so this table stops and says why; the others carry on. Recovery
			// is an operator-ordered rebuild of this table (halt.go).
			recordHalt(log, h, qualified, t)
			continue
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

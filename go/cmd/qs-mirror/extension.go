package main

// The sidecar's half of pg_quicksilver (ext/pg_quicksilver, docs/40): the
// extension routes a read to a table's mirror relation when it is safe, and
// it believes three things somebody has to keep true. This file keeps them.
//
//  1. On the PRIMARY, because the registry is a table: every mirrored table
//     has a mirror relation, registered in quicksilver.mirrors, and the lake
//     it reads is in quicksilver.lakes. The mirror relation is a pg_duckdb
//     view over the table's DuckLake table. It names the lake table and no
//     file and no path, so its text is the same on every instance and is not
//     generated again when the mirror compacts; the catalog's path is in
//     quicksilver.lakes and is the same on every Pod. The extension attaches
//     the lake in a session itself, the first time it needs it and only where
//     the catalog file exists: there is no login trigger, so there is nothing
//     here that can stop a login on a node without a mirror.
//
//  2. On THIS node: quicksilver.report_applied_lsn(table, lsn), for every
//     table, with the LSN a reader of the lake gets — the publisher's last
//     published one, not the manifest's, because the publisher is
//     asynchronous. 0/0 for a table that must not be read here (halted,
//     being rebuilt, refused in the lake, not yet published); the extension
//     then answers from the heap on this node whatever the registry says.
//
//  3. Nothing about infinity. quicksilver.mirrors.finite_columns existed
//     because the mirror once could not hold an infinite date or timestamp.
//     It does now, as DuckDB's own infinity, and the equality harness reads
//     it back identical through DuckLake and pg_duckdb (docs/40), so the
//     allowlist no longer asks for the column and the sidecar does not
//     maintain it.
//
// All of it needs QS_DUCKLAKE=on: the lake table is the only mirror relation
// that survives a compaction. QS_EXTENSION=off turns this file off.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
	"github.com/howlerops/pg_quicksilver/go/internal/ducklake"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const (
	extName        = "pg_quicksilver"
	extLakeAlias   = "lake"
	extViewComment = "quicksilver mirror relation; maintained by the sidecar, do not edit. "
	// How often a report is repeated when nothing moved. The reports live in
	// the local server's shared memory and are gone when it restarts.
	extHeartbeat = time.Second
	extRecheck   = time.Minute
	extRetry     = 15 * time.Second
)

// castable are the types a pg_duckdb row field can be cast to as the heap
// declares them. Anything else is published as text; the extension then keeps
// a statement that reads the column on the heap ("stored as a different type
// in the mirror"). Found by trying each (pg_duckdb 1.2.0): a cast to an enum
// or inet is refused when the view is created, one to jsonb or an array when
// it is planned or run — and a view with one such column cannot be read at
// all, whichever columns the statement names.
var castable = map[uint32]bool{
	16: true, 21: true, 23: true, 20: true, // boolean, smallint, integer, bigint
	700: true, 701: true, 1700: true, // real, double precision, numeric
	25: true, 1043: true, 1042: true, // text, varchar, char
	2950: true, 17: true, 114: true, // uuid, bytea, json
	1082: true, 1083: true, 1266: true, 1114: true, 1184: true, 1186: true, // date, time, timetz, timestamp, timestamptz, interval
}

type extColumn struct {
	Name, Type string
	TypeOID    uint32
}

// extLink is the state of both halves. The primary half runs on the pump's
// goroutine and its connection; the local half is one goroutine with one kept
// connection of its own, so the apply loop never waits for the local server.
type extLink struct {
	log    *slog.Logger
	o      options
	off    bool
	schema string

	mu      sync.Mutex
	blocked map[string]bool // tables the pump says must not be read: halted
	known   map[string]bool // tables in QS_TABLES

	// primary half
	dirty     bool
	checkedAt time.Time
	present   bool
	saidWhy   string
	lastHalt  string
	// lastJoins is the join domain configuration the last observe saw, and
	// haveJoins says the primary has quicksilver.join_domains (an extension
	// older than it has none, and the mirror then carries no companions).
	lastJoins string
	haveJoins bool

	// Join domain completeness, this node's half (see the join report below).
	// jdCfg, jdPub and jdPend are under mu; the connection and what was last
	// sent are under jdMu, which every report and every flip takes.
	jdCfg       map[string][]string        // configured member columns by table
	jdPub       map[string]map[string]bool // complete columns of the manifest the lake serves
	jdPend      map[string]map[string]bool // columns a manifest being published is incomplete for
	jdMu        sync.Mutex
	jdConn      *pgx.Conn
	jdSent      map[joinKey]bool
	jdSentAt    time.Time
	jdAbsent    bool
	jdAbsentAt  time.Time
	jdLastErr   string
	sendJoinsFn func(map[joinKey]bool) error // tests only; nil sends to the local server

	// what the last report said, for the tests and for /metrics
	reported map[string]string

	// The stream's idle position (see position). The pump owns the stream;
	// these three are how the reporter reads it without asking the pump.
	stream atomic.Pointer[changestream.Streaming]
	idleOK atomic.Bool
	gen    atomic.Uint64
	// applied is the end of the last transaction the pump applied in this
	// stream, when it holds nothing unapplied: see position.
	applied atomic.Uint64
	// replay is the standby's replay position as the pump last read it, or
	// no limit with the gate off (setReplay, reportFor).
	replay atomic.Uint64
	tick   time.Duration
	// nudge is the pump saying "I just finished applying": the reporter looks
	// now, and keeps looking every millisecond until the lake has published
	// what was applied, instead of finding out on its next 25 ms tick.
	nudge chan struct{}
}

// ext is nil until main sets it; every method is safe on nil.
var ext *extLink

func newExtLink(log *slog.Logger, o options) *extLink {
	e := &extLink{log: log, o: o, dirty: true,
		schema:  env("QS_EXTENSION_SCHEMA", "quicksilver_mirror"),
		blocked: map[string]bool{}, known: map[string]bool{}, reported: map[string]string{},
		jdCfg: map[string][]string{}, jdPub: map[string]map[string]bool{},
		jdPend: map[string]map[string]bool{}, jdSent: map[joinKey]bool{},
		nudge: make(chan struct{}, 1)}
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QS_EXTENSION"))) {
	case "off", "0", "false", "no":
		e.off = true
	}
	for _, q := range o.tables {
		e.known[q] = true
	}
	e.tick = 25 * time.Millisecond
	if d, err := time.ParseDuration(os.Getenv("QS_EXTENSION_REPORT_INTERVAL")); err == nil && d >= time.Millisecond {
		e.tick = d
	}
	// Until the pump says otherwise nothing is clamped (the gate is off) or
	// nothing is reported past replay (the gate is on and no position is known).
	e.replay.Store(0)
	if !gateOnReplay {
		e.replay.Store(^uint64(0))
	}
	return e
}

// setReplay is the standby's replay position as the pump last read it. The
// reporter may not vouch for a position past it (reportFor). With the gate off
// it is set to no limit, so the reporter is unchanged from before the gate.
func (e *extLink) setReplay(replay uint64, gated bool) {
	if e == nil {
		return
	}
	if !gated {
		replay = ^uint64(0)
	}
	if e.replay.Swap(replay) != replay {
		if pumpEvents {
			select {
			case e.nudge <- struct{}{}:
			default:
			}
		}
	}
}

// reportFor is what the reporter says for one table, given the lake's
// published LSN for it (0 when none), the position this node may vouch for
// (pos, 0 when none), whether the lake's current manifest is the published one,
// and the standby's replay position. It is the replay gate's report rule
// (replaygate.go):
//
//   - a table whose published LSN is past replay is not to be read here yet:
//     its snapshot is at a point this node has not replayed, so the answer is
//     ok=false and the caller reports 0/0;
//   - otherwise the published LSN stands, and the idle position is used only
//     as far as replay, so a report never runs past what this node has
//     replayed.
func reportFor(published, pos, replay uint64, current bool) (lsn uint64, ok bool) {
	if published > replay {
		return 0, false
	}
	lsn = published
	if current && pos > lsn {
		if pos > replay {
			pos = replay
		}
		if pos > lsn {
			lsn = pos
		}
	}
	return lsn, true
}

func (e *extLink) say(why, msg string, args ...any) {
	if e.saidWhy != why {
		e.saidWhy = why
		e.log.Info(msg, args...)
	}
}

// ---- which tables must not be read here ---------------------------------------

// observe records which tables are halted. Called by the pump every tick and
// wherever a table halts, so the next report (at most one extTick away) says
// 0/0 for it; the lake refuses it too, but only once the publisher has got to
// the manifest that says so.
func (e *extLink) observe(tables map[string]*mirror.Table) {
	if e == nil || e.off {
		return
	}
	halted := make([]string, 0)
	e.mu.Lock()
	for q, t := range tables {
		h := t.Halted() != ""
		if h {
			halted = append(halted, q)
		}
		e.blocked[q] = h
	}
	e.mu.Unlock()
	sort.Strings(halted)
	if s := strings.Join(halted, ","); s != e.lastHalt {
		// a halted table leaves the registry, a rebuilt one returns: at the next tick, not the next retry
		e.lastHalt, e.dirty, e.checkedAt = s, true, time.Time{}
	}
	// The join domain configuration changes the registry: its companion is
	// exposed, or it is not, at the next tick. Completeness does not (joinSig).
	if s := joinSig(joinsWanted(tables)); s != e.lastJoins {
		e.lastJoins, e.dirty, e.checkedAt = s, true, time.Time{}
	}
	e.mu.Lock()
	for q, t := range tables {
		var cols []string
		for _, m := range t.JoinMemberships() {
			cols = append(cols, m.Column)
		}
		if len(cols) == 0 {
			delete(e.jdCfg, q)
		} else {
			e.jdCfg[q] = cols
		}
	}
	e.mu.Unlock()
}

// extJoin is a join domain member column the mirror exposes: the heap column,
// its domain, and its companion. The companion is exposed for every configured
// member, whether or not every file carries it (a file that does not reads it
// as NULL). Whether a node may compare companions is that node's report, not
// the registry's (see the join report below).
type extJoin struct {
	Column, Domain, Companion string
}

// joinsWanted is the configured join domain columns of every table that is not
// halted, by qualified name, in column order. It depends on the configuration
// only: a column does not leave the registry because its files are incomplete.
func joinsWanted(tables map[string]*mirror.Table) map[string][]extJoin {
	out := map[string][]extJoin{}
	for q, t := range tables {
		if t.Halted() != "" {
			continue
		}
		for _, m := range t.JoinMemberships() {
			out[q] = append(out[q], extJoin{Column: m.Column, Domain: m.Domain, Companion: m.Companion})
		}
	}
	return out
}

// joinSig is a string that changes when joinsWanted's answer does.
func joinSig(w map[string][]extJoin) string {
	names := make([]string, 0, len(w))
	for q := range w {
		names = append(names, q)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, q := range names {
		b.WriteString(q)
		for _, j := range w[q] {
			fmt.Fprintf(&b, "|%s,%s,%s", j.Column, j.Domain, j.Companion)
		}
		b.WriteString("\n")
	}
	return b.String()
}

// filterJoins keeps the join columns that the heap table still has.
func filterJoins(joins []extJoin, cols []extColumn) []extJoin {
	var out []extJoin
	for _, j := range joins {
		for _, c := range cols {
			if c.Name == j.Column {
				out = append(out, j)
				break
			}
		}
	}
	return out
}

// joinRowsChange is what quicksilver.join_domains must gain for one heap
// table: the rows that are new or different. A row that is already right needs
// nothing.
//
// It never says what to DELETE. quicksilver.join_domains is one registry for
// every sidecar that shares the database: a sidecar that does not mirror a join
// domain's table on its own node must not remove the row another sidecar
// relies on. Completeness is per node (shared memory, not this table), so a
// row this node no longer wants is harmless configuration and stays. Removing
// it is an operator's decision, made once for every sidecar: see docs/16, the
// join domain cleanup statement.
func joinRowsChange(have map[string]extJoin, want []extJoin) (put []extJoin) {
	for _, j := range want {
		if h, ok := have[j.Column]; !ok || h != j {
			put = append(put, j)
		}
	}
	return put
}

// The pump brackets every tick with these. Between pumpBusy and pumpIdle a
// transaction may have been handed out by the stream and not applied.
func (e *extLink) pumpBusy() {
	if e != nil {
		e.idleOK.Store(false)
		e.gen.Add(1)
	}
}

// pumpIdle says the pump holds nothing unapplied. through is the end LSN of
// the last transaction it applied from THIS stream, or zero if it has applied
// none yet: the stream is ordered by commit, so every transaction committing
// below it has been applied too (or was already in the mirror when the stream
// began). The manifests' own applied LSN is where a commit record STARTS; a
// node that can see that commit has replayed to where it ENDS, and without
// this a mirror that had applied everything would look a record behind.
func (e *extLink) pumpIdle(through uint64) {
	if e != nil {
		if through > e.applied.Load() {
			e.applied.Store(through)
		}
		e.idleOK.Store(true)
		if pumpEvents {
			select {
			case e.nudge <- struct{}{}:
			default:
			}
		}
	}
}

func (e *extLink) attachStream(st *changestream.Streaming) {
	if e != nil {
		e.idleOK.Store(false)
		e.gen.Add(1)
		e.applied.Store(0)
		e.stream.Store(st)
	}
}

// position is a WAL position P such that every transaction committing below
// P is in the mirror's manifests, or zero when there is none to vouch for.
//
// It is the later of the stream's idle position (changestream.IdleLSN: the
// walsender has decoded up to P and nothing for this publication below P is
// still in flight) and the end of the last transaction the pump applied
// (pumpIdle), taken while the pump holds nothing unapplied. The pump says so
// with idleOK, and gen moves whenever it is about to take more from the
// stream; both are read before AND after, so a tick that came and went in
// between is noticed.
//
// This is what lets a mirror be reported as current while the primary writes
// WAL that is not for its tables. Nothing is persisted for it (the manifest's
// own applied LSN follows every few seconds, because that costs fsyncs): a
// report lives in the local server's shared memory and needs no durability.
func (e *extLink) position() uint64 {
	st := e.stream.Load()
	if st == nil || !e.idleOK.Load() {
		return 0
	}
	g := e.gen.Load()
	p := st.IdleLSN()
	if a := e.applied.Load(); a > p {
		p = a
	}
	if !e.idleOK.Load() || e.gen.Load() != g || e.stream.Load() != st {
		return 0
	}
	return p
}

// lsns is what to report now, per table: how far a reader of the lake table
// is guaranteed to see, or 0/0 for a table that must not be read.
//
// The lake is published asynchronously, so it is the publisher's LSN that
// counts, not the manifest's. The idle position is added only when the lake
// has published the newest manifest it was given, and it is read BEFORE the
// publisher's state: position P vouches for the manifests submitted by then,
// and a publisher that has nothing outstanding afterwards has published all
// of those.
func (e *extLink) lsns() map[string]string {
	out := make(map[string]string, len(e.known))
	pos := e.position()
	replay := e.replay.Load()
	var stats map[string]ducklake.TableStats
	if lake != nil {
		stats, _ = lake.Stats()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for q := range e.known {
		st, ok := stats[q]
		if ok && st.PublishedLSN == "" && st.Refused == "" && st.PublishedAt.IsZero() {
			ok = false // submitted, and the publisher has not got to it yet
		}
		if !ok && !e.blocked[q] {
			// Nothing is known yet: this process has just started and has
			// not submitted the table. Whatever the last process reported
			// stands (the lake is as it left it, and it reported 0/0 itself
			// if the table was not to be read), rather than every restart
			// of the sidecar taking every table off the mirror for a moment.
			continue
		}
		if e.blocked[q] || st.Refused != "" || st.PublishedLSN == "" {
			out[q] = "0/0"
			continue
		}
		// Not the manifest sequence numbers: they count files, and a manifest
		// that only marks rows dead has its predecessor's.
		_, current := lake.Current(q)
		lsn, ok := reportFor(parseLSNBytes(st.PublishedLSN), pos, replay, current)
		if !ok {
			// Published past what this node has replayed (a snapshot at a point
			// the standby has not reached): not to be read here yet.
			out[q] = "0/0"
			continue
		}
		out[q] = fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
	}
	return out
}

// ---- the local half: report_applied_lsn ---------------------------------------

const extReportSQL = `
SELECT quicksilver.report_applied_lsn(c.oid, v.lsn)
  FROM unnest($1::text[], $2::text[], $3::pg_lsn[]) AS v(s, t, lsn)
  JOIN pg_catalog.pg_namespace n ON n.nspname = v.s
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = v.t`

// run reports until ctx ends. One statement per report, on a connection that
// is kept; a report is sent when an LSN moved and otherwise once a heartbeat.
// Every failure is survivable: the connection is dropped and made again, and
// what the extension does with a table nobody reports is answer from the heap.
func (e *extLink) run(ctx context.Context) {
	if e == nil || e.off {
		return
	}
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close(context.WithoutCancel(ctx))
		}
	}()
	var last map[string]string
	var sentAt, absentAt time.Time
	var lastErr string
	tick := time.NewTicker(e.tick)
	defer tick.Stop()
	// After a nudge: look every millisecond for as long as the lake is still
	// publishing what the pump applied, bounded so a publish that fails or is
	// slow costs one burst and then the ordinary tick.
	var fastUntil time.Time
	fast := time.NewTimer(time.Hour)
	defer fast.Stop()
	for {
		ticked := false
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			ticked = true
		case <-e.nudge:
			fastUntil = time.Now().Add(extFastWindow)
		case <-fast.C:
		}
		if !fastUntil.IsZero() {
			if time.Now().Before(fastUntil) && e.lakeBehind() {
				fast.Reset(time.Millisecond)
			} else {
				fastUntil = time.Time{}
			}
		}
		if lake == nil {
			continue // nothing publishes a mirror relation; reconcile says so once
		}
		if !absentAt.IsZero() && time.Since(absentAt) < extRecheck {
			continue
		}
		// Ask the walsender where it is, so the idle position follows a
		// source that is writing WAL for other tables.
		if st := e.stream.Load(); ticked && st != nil && e.idleOK.Load() {
			st.RequestKeepalive()
		}
		now := e.lsns()
		if conn != nil && sameLSNs(now, last) && time.Since(sentAt) < extHeartbeat {
			continue
		}
		if conn == nil {
			c, err := pgx.Connect(ctx, e.o.localDSN())
			if err != nil {
				if msg := err.Error(); msg != lastErr {
					lastErr = msg
					e.log.Warn("cannot reach the local PostgreSQL to report applied LSNs; "+
						"reads stay on the heap here until it answers", "err", err)
				}
				continue
			}
			var have bool
			if err := c.QueryRow(ctx, `SELECT to_regprocedure('quicksilver.report_applied_lsn(regclass,pg_lsn)') IS NOT NULL`).Scan(&have); err != nil || !have {
				_ = c.Close(ctx)
				absentAt = time.Now() // not installed here (yet): look again in a minute
				continue
			}
			conn, absentAt, last = c, time.Time{}, nil
		}
		schemas, names, lsns := make([]string, 0, len(now)), make([]string, 0, len(now)), make([]string, 0, len(now))
		for q, lsn := range now {
			s, n, _ := strings.Cut(q, ".")
			schemas, names, lsns = append(schemas, s), append(names, n), append(lsns, lsn)
		}
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := conn.Exec(rctx, extReportSQL, schemas, names, lsns)
		cancel()
		if err != nil {
			if msg := err.Error(); msg != lastErr {
				lastErr = msg
				e.log.Warn("reporting applied LSNs to the local PostgreSQL failed; reconnecting", "err", err)
			}
			_ = conn.Close(context.WithoutCancel(ctx))
			conn = nil
			continue
		}
		if lastErr != "" {
			e.log.Info("reporting applied LSNs to the local PostgreSQL again")
			lastErr = ""
		}
		last, sentAt = now, time.Now()
		e.mu.Lock()
		e.reported = now
		e.mu.Unlock()
	}
}

// ---- join domain completeness: this node's report ------------------------------
//
// pg_quicksilver rewrites a uuid join to the companions only where this node's
// shared memory says both columns are complete (quicksilver.report_join_complete).
// Those reports must never say complete for a manifest the lake can serve that
// has a companion-less file of the column. Three rules keep that true. Nothing
// is persisted: a restart starts from nothing reported, which is incomplete.
//
//  1. Complete means PUBLISHED. A column is reported complete only once the
//     manifest the lake serves carries it complete (afterPublish).
//  2. Incomplete comes BEFORE publication. A manifest that is incomplete for a
//     column is published only after the column has been reported incomplete
//     and the report acknowledged (beforePublish), so a reader can never see
//     that manifest while the column is still reported complete. A column goes
//     back to complete only after a complete manifest is published, so no
//     complete report can get ahead of an incomplete manifest still to come.
//  3. Reports are sent one at a time (jdMu), so a flip to incomplete is never
//     overtaken by an older complete report.
//
// pg_quicksilver adds the last step: a scan that read a column it found complete
// checks, when it ends, that the column's report is still the one it found (an
// epoch the report sets when the column becomes complete). A flip that happens
// while the scan runs is then an error, not a wrong answer. The registry
// (quicksilver.join_domains) is static and is not written for completeness, so
// the report is the only thing that moves.
type joinKey struct{ table, column string }

const extJoinReportSQL = `
SELECT quicksilver.report_join_complete(c.oid, v.col::name, v.ok)
  FROM unnest($1::text[], $2::text[], $3::text[], $4::bool[]) AS v(s, t, col, ok)
  JOIN pg_catalog.pg_namespace n ON n.nspname = v.s
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = v.t`

// joinWantLocked is what each configured column should be reported as now:
// complete when the published manifest has it complete and no manifest pending
// publication is incomplete for it. Caller holds e.mu.
func (e *extLink) joinWantLocked() map[joinKey]bool {
	want := map[joinKey]bool{}
	for q, cols := range e.jdCfg {
		for _, c := range cols {
			want[joinKey{q, c}] = e.jdPub[q][c] && !e.jdPend[q][c]
		}
	}
	return want
}

func sameJoinReports(a, b map[joinKey]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// reportJoinsLocked sends the join completeness state if it differs from what
// was last sent, or a heartbeat is due; force sends regardless. Caller holds
// jdMu. An error means the state did not reach the node; it is sent again on
// the next call.
func (e *extLink) reportJoinsLocked(ctx context.Context, force bool) error {
	e.mu.Lock()
	want := e.joinWantLocked()
	e.mu.Unlock()
	if len(want) == 0 {
		return nil
	}
	if !force && sameJoinReports(want, e.jdSent) && time.Since(e.jdSentAt) < extHeartbeat {
		return nil
	}
	if e.jdAbsent && !force && time.Since(e.jdAbsentAt) < extRecheck {
		return nil
	}
	if e.sendJoinsFn != nil { // tests: the state machine without a database
		if err := e.sendJoinsFn(want); err != nil {
			return err
		}
		e.jdSent, e.jdSentAt = want, time.Now()
		return nil
	}
	if e.jdConn == nil {
		c, err := pgx.Connect(ctx, e.o.localDSN())
		if err != nil {
			return err
		}
		var have bool
		if err := c.QueryRow(ctx, `SELECT to_regprocedure('quicksilver.report_join_complete(regclass,name,boolean)') IS NOT NULL`).Scan(&have); err != nil {
			_ = c.Close(ctx)
			return err
		}
		if !have {
			// Nothing on this node reads its mirrors through pg_quicksilver, so
			// nothing can be rewritten here and publication need not wait.
			_ = c.Close(ctx)
			e.jdAbsent, e.jdAbsentAt = true, time.Now()
			return nil
		}
		e.jdConn, e.jdAbsent = c, false
	}
	var schemas, tables, cols []string
	var oks []bool
	for k, ok := range want {
		s, t, _ := strings.Cut(k.table, ".")
		schemas = append(schemas, s)
		tables = append(tables, t)
		cols = append(cols, k.column)
		oks = append(oks, ok)
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// A node that is not a standby (a primary, or one that was promoted) reads its
	// own tables and reports nothing complete: the reports of its standby days are
	// replaced by this one, within a heartbeat of the promotion.
	var standby bool
	if err := e.jdConn.QueryRow(rctx, `SELECT pg_is_in_recovery()`).Scan(&standby); err != nil {
		_ = e.jdConn.Close(context.WithoutCancel(ctx))
		e.jdConn = nil
		return err
	}
	if !standby {
		for i := range oks {
			oks[i] = false
		}
		for k := range want {
			want[k] = false
		}
	}
	if _, err := e.jdConn.Exec(rctx, extJoinReportSQL, schemas, tables, cols, oks); err != nil {
		_ = e.jdConn.Close(context.WithoutCancel(ctx))
		e.jdConn = nil
		return err
	}
	if e.jdLastErr != "" {
		e.log.Info("reporting join domain completeness to the local PostgreSQL again")
		e.jdLastErr = ""
	}
	e.jdSent, e.jdSentAt = want, time.Now()
	return nil
}

// runJoins sends join completeness until ctx ends: what afterPublish made
// complete goes out here, and the state is heartbeated, so a report lost with a
// restart of the local server comes back.
func (e *extLink) runJoins(ctx context.Context) {
	if e == nil || e.off {
		return
	}
	defer func() {
		e.jdMu.Lock()
		if e.jdConn != nil {
			_ = e.jdConn.Close(context.WithoutCancel(ctx))
			e.jdConn = nil
		}
		e.jdMu.Unlock()
	}()
	tick := time.NewTicker(e.tick)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		e.jdMu.Lock()
		if err := e.reportJoinsLocked(ctx, false); err != nil && ctx.Err() == nil {
			if msg := err.Error(); msg != e.jdLastErr {
				e.jdLastErr = msg
				e.log.Warn("reporting join domain completeness to the local PostgreSQL failed; joins on those columns stay on the heap until it answers", "err", err)
			}
		}
		e.jdMu.Unlock()
	}
}

// beforePublish is the lake's hook before a manifest is served (rule 2). For a
// manifest incomplete for some configured column it reports those columns
// incomplete, and only then returns nil. If the report does not reach the node
// it fails, and the publish is tried again. A manifest complete for every
// column needs no report.
func (e *extLink) beforePublish(m *mirror.LakeManifest) error {
	if e == nil || e.off {
		return nil
	}
	complete := map[string]bool{}
	for _, c := range m.JoinComplete {
		complete[c] = true
	}
	q := m.Qualified()
	e.jdMu.Lock()
	defer e.jdMu.Unlock()
	e.mu.Lock()
	var pend []string
	for _, c := range m.JoinMembers {
		if !complete[c] {
			pend = append(pend, c)
		}
	}
	if len(m.JoinMembers) > 0 {
		e.jdCfg[q] = m.JoinMembers
	}
	if len(pend) > 0 {
		if e.jdPend[q] == nil {
			e.jdPend[q] = map[string]bool{}
		}
		for _, c := range pend {
			e.jdPend[q][c] = true
		}
	}
	e.mu.Unlock()
	if len(pend) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return e.reportJoinsLocked(ctx, true)
}

// afterPublish is the lake's hook once a manifest is the one it serves (rule 1).
// Nothing is sent here: runJoins sends the complete state at its next tick.
func (e *extLink) afterPublish(m *mirror.LakeManifest) {
	if e == nil || e.off {
		return
	}
	pub := map[string]bool{}
	for _, c := range m.JoinComplete {
		pub[c] = true
	}
	q := m.Qualified()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(m.JoinMembers) > 0 {
		e.jdCfg[q] = m.JoinMembers
	}
	e.jdPub[q] = pub
	delete(e.jdPend, q)
}

// extFastWindow bounds the millisecond polling that follows a nudge.
const extFastWindow = 100 * time.Millisecond

// lakeBehind reports whether some table that can be read has a manifest the
// lake has not published yet: the one case in which looking again in a
// millisecond can find something new.
func (e *extLink) lakeBehind() bool {
	if lake == nil {
		return false
	}
	stats, _ := lake.Stats()
	e.mu.Lock()
	defer e.mu.Unlock()
	for q := range e.known {
		if e.blocked[q] || stats[q].Refused != "" {
			continue
		}
		if _, current := lake.Current(q); !current {
			return true
		}
	}
	return false
}

func sameLSNs(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// Metrics is what was last reported, per table, as an LSN in bytes (0: told
// not to read it).
func (e *extLink) Metrics() string {
	if e == nil || e.off {
		return ""
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.reported) == 0 {
		return ""
	}
	names := make([]string, 0, len(e.reported))
	for q := range e.reported {
		names = append(names, q)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteString("# HELP quicksilver_extension_reported_lsn The applied LSN last reported to pg_quicksilver on this node; 0 means the table was reported unreadable.\n")
	b.WriteString("# TYPE quicksilver_extension_reported_lsn gauge\n")
	for _, q := range names {
		fmt.Fprintf(&b, "quicksilver_extension_reported_lsn{table=%q} %d\n", q, parseLSNBytes(e.reported[q]))
	}
	return b.String()
}

func parseLSNBytes(s string) uint64 {
	var hi, lo uint64
	if _, err := fmt.Sscanf(s, "%X/%X", &hi, &lo); err != nil {
		return 0
	}
	return hi<<32 | lo
}

// ---- the primary half: mirror relations and the registry ----------------------

// extViewName is the mirror relation's name in the dedicated schema:
// "schema.table" as one identifier, which cannot collide across schemas.
func extViewName(qualified string) string {
	if len(qualified) <= 63 {
		return qualified
	}
	sum := sha256.Sum256([]byte(qualified))
	return qualified[:54] + "_" + hex.EncodeToString(sum[:4])
}

func extQuote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// extMirrorRole owns the mirror schema and its views. It is created by
// pg_quicksilver--0.1.sql, and it is the only role that may read them
// (quicksilver_mirror_reader; pg_quicksilver's C code runs the mirror side as it).
const extMirrorRole = "quicksilver_mirror_reader"

// extViewSQL is the mirror relation: the heap's column names, in the heap's
// order, cast to the heap's types, over the lake table by name.
//
// joins are the companions (docs/16, "Join domains"), exposed after the heap's
// columns as bigint under their own names. Nothing reads them by position: the
// substitution maps heap columns by attribute number, and a whole-row reference
// is never routed (docs/40, "Join keys"). With none, the view is what it was.
func extViewSQL(mirrorSchema, heapSchema, heapTable string, cols []extColumn, joins []extJoin) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE VIEW %s.%s AS\nSELECT ", extQuote(mirrorSchema), extQuote(extViewName(heapSchema+"."+heapTable)))
	for i, c := range cols {
		if i > 0 {
			b.WriteString(",\n       ")
		}
		typ := c.Type
		if !castable[c.TypeOID] {
			typ = "text"
		}
		fmt.Fprintf(&b, "(r['%s'])::%s AS %s", strings.ReplaceAll(c.Name, "'", "''"), typ, extQuote(c.Name))
	}
	for _, j := range joins {
		fmt.Fprintf(&b, ",\n       (r['%s'])::bigint AS %s", strings.ReplaceAll(j.Companion, "'", "''"), extQuote(j.Companion))
	}
	fmt.Fprintf(&b, "\nFROM duckdb.query($qs$ SELECT * FROM %s.%s.%s $qs$) r",
		extLakeAlias, extQuote(heapSchema), extQuote(heapTable))
	return b.String()
}

func extSignature(viewSQL string) string {
	sum := sha256.Sum256([]byte(viewSQL))
	return extViewComment + "sha256:" + hex.EncodeToString(sum[:])
}

// reconcile makes the primary's registry match the tables this sidecar
// mirrors, when something that decides it has changed: at start, after a DDL
// barrier, when a table halts or is rebuilt. It never fails the pump; what it
// could not do it tries again.
func (e *extLink) reconcile(ctx context.Context, conn *pgx.Conn, tables map[string]*mirror.Table) {
	if e == nil || e.off {
		return
	}
	e.observe(tables)
	switch {
	case e.dirty && time.Since(e.checkedAt) >= extRetry:
	case !e.present && time.Since(e.checkedAt) >= extRecheck:
	case e.checkedAt.IsZero():
	default:
		return
	}
	e.checkedAt = time.Now()
	var have, haveLakes, haveJoins bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1),
		to_regclass('quicksilver.lakes') IS NOT NULL,
		to_regclass('quicksilver.join_domains') IS NOT NULL`, extName).Scan(&have, &haveLakes, &haveJoins); err != nil {
		e.log.Warn("could not ask the primary whether pg_quicksilver is installed", "err", err)
		return
	}
	e.haveJoins = haveJoins
	e.present = have && haveLakes
	switch {
	case !have:
		e.say("absent", "pg_quicksilver is not installed in this database; no mirror relation is registered "+
			"(CREATE EXTENSION pg_quicksilver on the primary to route reads; this is looked at again every minute)",
			"database", e.o.database)
		return
	case !haveLakes:
		e.say("old", "pg_quicksilver is installed but has no quicksilver.lakes table; this sidecar needs a newer extension")
		return
	case lake == nil:
		e.say("nolake", "pg_quicksilver is installed but QS_DUCKLAKE is off, so there is no mirror relation "+
			"that survives a compaction; nothing is registered and reads stay on the heap")
		e.present = false
		return
	}
	desired := map[string]bool{}
	for q, t := range tables {
		if t.Halted() == "" {
			desired[q] = true
		}
	}
	joins := joinsWanted(tables)
	if len(joins) > 0 && !e.haveJoins {
		e.say("nojoins", "pg_quicksilver has no quicksilver.join_domains (an older extension): the join domain "+
			"companions are not exposed; joins are compared on their uuid columns")
		joins = nil
	}
	if err := e.sync(ctx, conn, desired, joins); err != nil {
		e.log.Warn("could not bring pg_quicksilver's registry to the mirrored tables; will retry",
			"err", err, "retry_in", extRetry)
		return
	}
	e.dirty = false
}

// markDirty asks for a reconcile at the next tick: the heap's columns changed.
func (e *extLink) markDirty() {
	if e != nil {
		e.dirty, e.checkedAt = true, time.Time{}
	}
}

func (e *extLink) sync(ctx context.Context, conn *pgx.Conn, desired map[string]bool, joins map[string][]extJoin) error {
	// DuckDB's ducklake and sqlite extensions, which pg_duckdb then loads in
	// every backend of every instance. Not in the transaction below: this is
	// DuckDB's doing, and a failure (no way out to the extension repository,
	// and the image does not carry them) must not undo the rest.
	for _, x := range []string{"ducklake", "sqlite"} {
		var n int
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM duckdb.extensions WHERE name = $1`, x).Scan(&n); err != nil {
			return fmt.Errorf("pg_duckdb is not installed in this database (duckdb.extensions): %w", err)
		}
		if n > 0 {
			continue
		}
		if _, err := conn.Exec(ctx, `SELECT duckdb.install_extension($1)`, x); err != nil {
			e.log.Warn("could not install a DuckDB extension the lake needs; mirror relations will not plan "+
				"until it is there, and reads stay on the heap", "extension", x, "err", err)
		}
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// Every sidecar of the cluster does this; one at a time.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('quicksilver.mirrors'))`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO quicksilver.lakes AS l (alias, catalog) VALUES ($1, $2)
		ON CONFLICT (alias) DO UPDATE SET catalog = EXCLUDED.catalog WHERE l.catalog IS DISTINCT FROM EXCLUDED.catalog`,
		extLakeAlias, lakeCatalogPath(e.o)); err != nil {
		return fmt.Errorf("quicksilver.lakes: %w", err)
	}
	if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+extQuote(e.schema)); err != nil {
		return err
	}
	// The mirror relations belong to the role pg_quicksilver runs the mirror
	// side as, and PUBLIC may not reach them: a role that could read a mirror
	// directly would read a table it has no privilege on (docs/40, "Ordinary roles").
	// Only when it is not so already: every DDL statement on the primary is
	// logged by the DDL capture (quicksilver.ddl_log), and the sidecar takes
	// each one as a barrier. A statement that changes nothing must not run.
	var schemaWrong bool
	if err := tx.QueryRow(ctx, `
		SELECT pg_get_userbyid(n.nspowner) IS DISTINCT FROM $2
		       OR EXISTS (SELECT 1 FROM aclexplode(n.nspacl) a WHERE a.grantee = 0)
		  FROM pg_namespace n WHERE n.nspname = $1`, e.schema, extMirrorRole).Scan(&schemaWrong); err != nil {
		return err
	}
	if schemaWrong {
		if _, err := tx.Exec(ctx, "ALTER SCHEMA "+extQuote(e.schema)+" OWNER TO "+extQuote(extMirrorRole)); err != nil {
			return fmt.Errorf("%s: owner: %w", e.schema, err)
		}
		if _, err := tx.Exec(ctx, "REVOKE ALL ON SCHEMA "+extQuote(e.schema)+" FROM PUBLIC"); err != nil {
			return err
		}
	}

	names := make([]string, 0, len(desired))
	for q := range desired {
		names = append(names, q)
	}
	sort.Strings(names)
	keep := make([]string, 0, len(names))
	var made, kept []string
	for _, q := range names {
		schema, table, _ := strings.Cut(q, ".")
		view := extViewName(q)
		keep = append(keep, view)
		rows, err := tx.Query(ctx, `
			SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.atttypid::oid
			  FROM pg_attribute a
			 WHERE a.attrelid = to_regclass(format('%I.%I', $1::text, $2::text))
			   AND a.attnum > 0 AND NOT a.attisdropped
			 ORDER BY a.attnum`, schema, table)
		if err != nil {
			return err
		}
		cols, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (extColumn, error) {
			var c extColumn
			return c, r.Scan(&c.Name, &c.Type, &c.TypeOID)
		})
		if err != nil {
			return err
		}
		if len(cols) == 0 {
			// The table is gone on the source; the pump will say so. Its join
			// rows are left alone: the registry is shared, and a row whose heap
			// is gone names an OID nothing else resolves to (see joinRowsChange).
			continue
		}
		want := filterJoins(joins[q], cols)
		if !e.haveJoins {
			want = nil
		}
		viewSQL := extViewSQL(e.schema, schema, table, cols, want)
		sig := extSignature(viewSQL)
		var haveSig *string
		var registered, foreign *bool
		if err := tx.QueryRow(ctx, `
			WITH v AS (SELECT to_regclass(format('%I.%I', $3::text, $4::text)) AS oid),
			     h AS (SELECT to_regclass(format('%I.%I', $1::text, $2::text)) AS oid)
			SELECT obj_description(v.oid, 'pg_class'),
			       (SELECT m.mirror = v.oid FROM quicksilver.mirrors m WHERE m.heap = h.oid),
			       (SELECT n.nspname <> $3 FROM quicksilver.mirrors m
			          JOIN pg_class c ON c.oid = m.mirror JOIN pg_namespace n ON n.oid = c.relnamespace
			         WHERE m.heap = h.oid)
			  FROM v, h`, schema, table, e.schema, view).Scan(&haveSig, &registered, &foreign); err != nil {
			return err
		}
		if foreign != nil && *foreign {
			e.say("foreign:"+q, "a table is registered in quicksilver.mirrors with a mirror relation that is not "+
				"this sidecar's; leaving it alone", "table", q)
			continue
		}
		// The registry rows that are new or different are written once the view
		// is made, which is what exposes their companions (docs/40, "Join keys").
		// A row this node no longer wants is left in the registry (joinRowsChange).
		var put []extJoin
		if e.haveJoins {
			if put, err = e.joinRowsToWrite(ctx, tx, schema, table, want); err != nil {
				return err
			}
		}
		rel := extQuote(e.schema) + "." + extQuote(view)
		// A view made before its owner was the mirror role is handed over, kept or not.
		// Only when the owner is not the mirror role already (see the schema above:
		// a DDL statement that changes nothing must not run).
		var wrongOwner bool
		if err := tx.QueryRow(ctx, `
			SELECT pg_get_userbyid(c.relowner) IS DISTINCT FROM $2
			  FROM pg_class c WHERE c.oid = to_regclass($1)`, rel, extMirrorRole).Scan(&wrongOwner); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if wrongOwner {
			if _, err := tx.Exec(ctx, "ALTER VIEW "+rel+" OWNER TO "+extQuote(extMirrorRole)); err != nil {
				return fmt.Errorf("%s: owner: %w", q, err)
			}
		}
		if haveSig != nil && *haveSig == sig && registered != nil && *registered {
			if err := e.putJoins(ctx, tx, schema, table, put); err != nil {
				return err
			}
			kept = append(kept, q)
			continue
		}
		if _, err := tx.Exec(ctx, "DROP VIEW IF EXISTS "+rel); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
		// Created by the sidecar's role (a DDL event trigger on the primary runs
		// as the role doing the DDL, so the mirror role cannot create it), then
		// handed to the mirror role in the same transaction.
		if _, err := tx.Exec(ctx, viewSQL); err != nil {
			return fmt.Errorf("%s: create the mirror relation: %w", q, err)
		}
		if _, err := tx.Exec(ctx, "ALTER VIEW "+rel+" OWNER TO "+extQuote(extMirrorRole)); err != nil {
			return fmt.Errorf("%s: owner: %w", q, err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("COMMENT ON VIEW %s IS '%s'", rel, strings.ReplaceAll(sig, "'", "''"))); err != nil {
			return err
		}
		// Not register_mirror(): that resets max_lag_bytes and enabled, which
		// are the operator's to set and survive a re-created view.
		if _, err := tx.Exec(ctx, `
			INSERT INTO quicksilver.mirrors AS m (heap, mirror)
			VALUES (to_regclass(format('%I.%I', $1::text, $2::text)), to_regclass(format('%I.%I', $3::text, $4::text)))
			ON CONFLICT (heap) DO UPDATE SET mirror = EXCLUDED.mirror`, schema, table, e.schema, view); err != nil {
			return fmt.Errorf("%s: register: %w", q, err)
		}
		if err := e.putJoins(ctx, tx, schema, table, put); err != nil {
			return err
		}
		made = append(made, q)
	}

	// Ours, and no longer wanted: the table left QS_TABLES, or is halted.
	rows, err := tx.Query(ctx, `
		SELECT c.oid, c.relname
		  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relkind = 'v' AND c.relname <> ALL($2::text[])
		   AND obj_description(c.oid, 'pg_class') LIKE $3`, e.schema, keep, extViewComment+"%")
	if err != nil {
		return err
	}
	type gone struct {
		oid  uint32
		name string
	}
	drop, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (gone, error) {
		var g gone
		return g, r.Scan(&g.oid, &g.name)
	})
	if err != nil {
		return err
	}
	var dropped []string
	for _, g := range drop {
		// The view that exposes the companions goes; the join rows stay. They
		// are the registry every sidecar shares, and the heap may well still
		// be mirrored by another (joinRowsChange; docs/16 has the cleanup).
		if _, err := tx.Exec(ctx, `DELETE FROM quicksilver.mirrors WHERE mirror = $1::oid`, g.oid); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "DROP VIEW IF EXISTS "+extQuote(e.schema)+"."+extQuote(g.name)); err != nil {
			return err
		}
		dropped = append(dropped, g.name)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if len(made)+len(dropped) > 0 || e.saidWhy != "synced" {
		e.saidWhy = "synced"
		e.log.Info("pg_quicksilver registry brought to the mirrored tables", "schema", e.schema,
			"created", made, "unchanged", len(kept), "dropped", dropped,
			"lake", lakeCatalogPath(e.o))
	}
	return nil
}

// joinRowsOf is what quicksilver.join_domains holds for one heap table, by column.
func joinRowsOf(ctx context.Context, tx pgx.Tx, schema, table string) (map[string]extJoin, error) {
	rows, err := tx.Query(ctx, `
		SELECT column_name::text, domain::text, companion::text
		  FROM quicksilver.join_domains
		 WHERE heap = to_regclass(format('%I.%I', $1::text, $2::text))`, schema, table)
	if err != nil {
		return nil, err
	}
	have := map[string]extJoin{}
	for rows.Next() {
		var j extJoin
		if err := rows.Scan(&j.Column, &j.Domain, &j.Companion); err != nil {
			rows.Close()
			return nil, err
		}
		have[j.Column] = j
	}
	rows.Close()
	return have, rows.Err()
}

// joinRowsToWrite is the rows this node must write for a heap table, given the
// rows the registry holds and the ones this node is configured for. It deletes
// nothing: see joinRowsChange. Nothing is written when nothing differs: the
// registry's trigger tells every backend to reread it, and a sync that changes
// nothing must not do that.
func (e *extLink) joinRowsToWrite(ctx context.Context, tx pgx.Tx, schema, table string, want []extJoin) ([]extJoin, error) {
	have, err := joinRowsOf(ctx, tx, schema, table)
	if err != nil {
		return nil, err
	}
	return joinRowsChange(have, want), nil
}

// putJoins writes the rows joinRowsToWrite returned, once the view exposes them.
func (e *extLink) putJoins(ctx context.Context, tx pgx.Tx, schema, table string, put []extJoin) error {
	for _, j := range put {
		if _, err := tx.Exec(ctx, `
			INSERT INTO quicksilver.join_domains AS d (heap, column_name, domain, companion)
			VALUES (to_regclass(format('%I.%I', $1::text, $2::text)), $3::text, $4::text, $5::text)
			ON CONFLICT (heap, column_name) DO UPDATE SET domain = EXCLUDED.domain, companion = EXCLUDED.companion`,
			schema, table, j.Column, j.Domain, j.Companion); err != nil {
			return fmt.Errorf("%s.%s.%s: join domain: %w", schema, table, j.Column, err)
		}
	}
	return nil
}

// lakeCatalogPath is the catalog a reader attaches; the same on every Pod.
func lakeCatalogPath(o options) string {
	return ducklake.CatalogPathIn(o.mirrorPath, os.Getenv("QS_DUCKLAKE_PATH"))
}

package changestream

// Streaming pgoutput consumer — ADR-0009 items 1 and 2, which are really one
// change: pgoutput IS consumed over the streaming replication protocol.
//
// What this replaces and why:
//
//	wal2json      -> pgoutput      removes a third-party extension from the
//	                               image and the output_plugin_libraries
//	                               allowlisting. pgoutput ships with Postgres.
//	peek-polling  -> START_REPLICATION  the poll interval was a hard floor on
//	                               latency; the server now pushes.
//
// Two things pgoutput gives us that wal2json did not:
//
//   - Relation messages carry the live column list and type OIDs, so the
//     decoder learns the schema from the stream itself rather than querying
//     pg_attribute. DDL still needs the barrier (a Relation message arrives
//     only when a changed table is next written to), but the column mapping
//     is now self-describing.
//   - Values arrive as exact TEXT in proto v1, so numerics never touch a
//     float. The correctness tax that dominated the Python profile is simply
//     absent.
//
// Ordering and durability are unchanged: a Transaction is emitted only at
// Commit, and the LSN confirmed back to the server is the commit LSN + 1 —
// the streaming equivalent of wal2json's nextlsn, and the same trap. Confirming
// at the commit LSN itself makes the server resend that transaction forever.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/howlerops/pg_quicksilver/go/internal/pgtext"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// DefaultMaxBufferedChanges is the cap NewStreaming gives a Streaming: how many
// row-changes of COMMITTED transactions may wait in memory for Transactions()
// before the receiver stops reading the socket. cmd/qs-mirror sets it from its
// own queue bound (QS_MAX_STREAM_BUFFERED_CHANGES) so the two are sized
// together. Zero or negative disables the cap.
var DefaultMaxBufferedChanges = 250_000

// DefaultStatusInterval is how often a standby status update is sent whether or
// not the server asked for one. It is the only thing keeping the walsender
// from timing the connection out while the receiver is paused (see receive),
// so it must stay well under wal_sender_timeout (default 60s). 10s is what
// PostgreSQL's own walreceiver uses (wal_receiver_status_interval).
var DefaultStatusInterval = 10 * time.Second

// replConn is the part of the replication connection the receiver uses. It
// exists so the buffering and pausing logic can be tested without PostgreSQL.
type replConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	SendStatus(ctx context.Context, pos pglogrepl.LSN) error
}

type pgReplConn struct{ c *pgconn.PgConn }

func (p pgReplConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return p.c.ReceiveMessage(ctx)
}

func (p pgReplConn) SendStatus(ctx context.Context, pos pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, p.c,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pos})
}

// weight is what a transaction costs against MaxBufferedChanges. A transaction
// with no changes for our tables still occupies a slice element, so it counts
// as one; otherwise a flood of them would be unbounded.
func weight(t *Transaction) int {
	if n := len(t.Changes); n > 0 {
		return n
	}
	return 1
}

// Streaming consumes pgoutput over the replication protocol. Received
// transactions are buffered; Transactions() drains the buffer, so callers keep
// the same micro-batching shape they had with polling.
//
// The buffer is bounded (MaxBufferedChanges). When it is full the receiver
// stops reading the socket, TCP flow control stops the walsender, and the
// backlog stays in PostgreSQL's WAL behind the slot — where the slot guard
// bounds it — instead of in this process's heap.
type Streaming struct {
	// MaxBufferedChanges and StatusInterval default from the package variables
	// above. Set before Start.
	MaxBufferedChanges int
	StatusInterval     time.Duration

	// Failover asks the server to create the slot with FAILOVER true, so a
	// standby running sync_replication_slots keeps a synchronised copy and the
	// slot survives promotion. PostgreSQL 17+ only; on 16 the slot is simply
	// gone after a failover and the mirror must be re-snapshotted
	// (docs/14 section C).
	Failover bool

	conn        *pgconn.PgConn
	rc          replConn
	slot        string
	publication string
	tables      map[string]bool

	mu              sync.Mutex
	buffered        []Transaction
	bufferedChanges int // sum of weight() over buffered
	err             error
	confirmed       pglogrepl.LSN
	confirmDirty    bool               // confirmed moved since the last status update
	interrupt       context.CancelFunc // aborts the receiver's blocking read
	received        pglogrepl.LSN
	idle            pglogrepl.LSN // see IdleLSN; zero when there is none
	relations       map[uint32]*pglogrepl.RelationMessage

	// wake rouses a paused receiver: the buffer was drained, or there is a new
	// position to confirm.
	wake chan struct{}

	cur    *Transaction // receiver goroutine only
	cancel context.CancelFunc
	done   chan struct{}
}

// NewStreaming dials a dedicated replication connection. dsn must NOT already
// carry replication=database; it is added here.
func NewStreaming(ctx context.Context, dsn, slot, publication string, tables []string) (*Streaming, error) {
	// A logical replication connection is a backend of its own, and pgoutput
	// renders every value as text using ITS session settings — not the ones the
	// snapshot ran under. Pinning here as well as at the caller is deliberate:
	// this is the session that renders most of the mirror's lifetime.
	dsn = pgtext.PinDSN(dsn)
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	conn, err := pgconn.Connect(ctx, dsn+sep+"replication=database")
	if err != nil {
		return nil, fmt.Errorf("replication connect: %w", err)
	}
	t := make(map[string]bool, len(tables))
	for _, x := range tables {
		t[x] = true
	}
	s := newStreaming(pgReplConn{conn}, t)
	s.conn, s.slot, s.publication = conn, slot, publication
	return s, nil
}

func newStreaming(rc replConn, tables map[string]bool) *Streaming {
	return &Streaming{
		MaxBufferedChanges: DefaultMaxBufferedChanges,
		StatusInterval:     DefaultStatusInterval,
		rc:                 rc,
		tables:             tables,
		relations:          map[uint32]*pglogrepl.RelationMessage{},
		wake:               make(chan struct{}, 1),
		done:               make(chan struct{}),
	}
}

// CreateSlot creates the replication slot if absent, returning the consistent
// point — the LSN an initial snapshot must be taken as of, so that snapshot and
// stream join up with neither gap nor overlap.
func (s *Streaming) CreateSlot(ctx context.Context) (pglogrepl.LSN, bool, error) {
	// pglogrepl still emits the pre-v15 positional grammar, which has no room
	// for FAILOVER. Build the parenthesised form ourselves (v15+) and let
	// pglogrepl parse the result set, which is unchanged.
	opts := []string{}
	if s.Failover {
		opts = append(opts, "FAILOVER true")
	}
	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput", quoteIdent(s.slot))
	if len(opts) > 0 {
		sql += " (" + strings.Join(opts, ", ") + ")"
	}
	res, err := pglogrepl.ParseCreateReplicationSlot(s.conn.Exec(ctx, sql))
	if err != nil {
		// already exists: fine, we resume from the server's confirmed position
		if strings.Contains(err.Error(), "already exists") {
			return 0, false, nil
		}
		return 0, false, err
	}
	lsn, perr := pglogrepl.ParseLSN(res.ConsistentPoint)
	if perr != nil {
		return 0, true, nil
	}
	return lsn, true, nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// SlotExists reports whether the slot is present on the server this Streaming
// is connected to. After a promotion this is the difference between "resume"
// and "rebuild", and it must be checked explicitly: a consumer that reconnects
// and merely sees no data cannot tell an idle source from a vanished slot.
func (s *Streaming) SlotExists(ctx context.Context) (bool, error) {
	res := s.conn.Exec(ctx, fmt.Sprintf(
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '%s'",
		strings.ReplaceAll(s.slot, "'", "''")))
	all, err := res.ReadAll()
	if err != nil {
		return false, err
	}
	for _, r := range all {
		for _, row := range r.Rows {
			if len(row) > 0 && string(row[0]) != "0" {
				return true, nil
			}
		}
	}
	return false, nil
}

func (s *Streaming) DropSlot(ctx context.Context) error {
	err := pglogrepl.DropReplicationSlot(ctx, s.conn, s.slot,
		pglogrepl.DropReplicationSlotOptions{Wait: true})
	if err != nil && strings.Contains(err.Error(), "does not exist") {
		return nil
	}
	return err
}

// Start begins streaming from `from` (use 0 to resume at the slot's confirmed
// position) and runs until Close.
func (s *Streaming) Start(ctx context.Context, from pglogrepl.LSN) error {
	err := pglogrepl.StartReplication(ctx, s.conn, s.slot, from,
		pglogrepl.StartReplicationOptions{PluginArgs: []string{
			// proto_version 1 delivers a transaction only at its commit, whole.
			// MaxBufferedChanges therefore bounds how many COMMITTED changes
			// wait here, not the size of one transaction: a single transaction
			// larger than the cap is still materialised entirely in memory
			// before it can be handed on. Bounding that needs streaming of
			// in-progress transactions (proto_version 2+, "streaming 'on'"),
			// which this consumer does not implement.
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.publication),
		}})
	if err != nil {
		return fmt.Errorf("START_REPLICATION: %w", err)
	}
	s.confirmed = from
	s.run(ctx)
	return nil
}

func (s *Streaming) run(ctx context.Context) {
	rctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	go s.receive(rctx)
}

// receive is the ONLY goroutine that touches the replication connection once
// streaming has started: it reads, and it writes every standby status update.
// Confirm used to write from the caller's goroutine, and pgconn's
// context-deadline watcher on the read side could make that concurrent write
// fail with an i/o timeout. Confirm now records the position and wakes this
// loop.
//
// Backpressure. When the buffer holds MaxBufferedChanges the loop stops
// calling ReceiveMessage. The kernel socket buffer fills, the walsender blocks
// in its send, and the backlog stays in WAL. Two consequences are designed
// for:
//
//   - The walsender still enforces wal_sender_timeout while blocked, and its
//     keepalive requests are sitting unread in the socket. So status updates
//     are sent on a timer (StatusInterval) regardless of what was read; the
//     walsender processes replies even while its own send is blocked, and any
//     reply resets its timeout.
//   - The pause depends only on COMMITTED, buffered changes, never on the
//     transaction being assembled. A transaction bigger than the cap is read
//     to its commit, buffered, and then pauses the loop until it is drained;
//     an empty buffer always reads. There is no state in which neither side
//     can make progress.
func (s *Streaming) receive(ctx context.Context) {
	defer close(s.done)
	interval := s.StatusInterval
	if interval <= 0 {
		interval = DefaultStatusInterval
	}
	nextStatus := time.Now().Add(interval)
	replyNow := false
	for {
		if ctx.Err() != nil {
			return
		}

		// One critical section decides what this iteration does, so a Confirm
		// can never slip between "nothing to send" and "blocked in a read with
		// no way to be interrupted".
		s.mu.Lock()
		pos := s.confirmed
		send := s.confirmDirty || replyNow || !time.Now().Before(nextStatus)
		paused := s.MaxBufferedChanges > 0 && s.bufferedChanges >= s.MaxBufferedChanges
		var rctx context.Context
		var cancel context.CancelFunc
		if send {
			s.confirmDirty = false
		} else if !paused {
			rctx, cancel = context.WithDeadline(ctx, nextStatus)
			s.interrupt = cancel
		}
		s.mu.Unlock()

		if send {
			if err := s.rc.SendStatus(ctx, pos); err != nil {
				if ctx.Err() == nil {
					s.setErr(err)
				}
				return
			}
			replyNow = false
			nextStatus = time.Now().Add(interval)
			continue
		}

		if paused {
			t := time.NewTimer(time.Until(nextStatus))
			select {
			case <-ctx.Done():
			case <-s.wake:
			case <-t.C:
			}
			t.Stop()
			continue
		}

		raw, err := s.rc.ReceiveMessage(rctx)
		interrupted := rctx.Err() != nil
		s.mu.Lock()
		s.interrupt = nil
		s.mu.Unlock()
		cancel()
		if err != nil {
			// Deadline (a status update is due) or Confirm's interrupt: neither
			// is a failure, and pgconn keeps a partially read message for the
			// next call. A genuine failure that coincided is seen next time.
			if interrupted || pgconn.Timeout(err) {
				continue
			}
			s.setErr(err)
			return
		}

		cd, ok := raw.(*pgproto3.CopyData)
		if !ok || len(cd.Data) == 0 {
			continue
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			k, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				continue
			}
			if k.ReplyRequested {
				replyNow = true
			}
			// See IdleLSN. s.cur belongs to this goroutine; the buffer is read
			// under the mutex so the decision and Transactions() cannot
			// interleave.
			s.mu.Lock()
			if s.cur == nil && len(s.buffered) == 0 {
				s.idle = k.ServerWALEnd
			}
			s.mu.Unlock()
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				s.setErr(err)
				return
			}
			s.mu.Lock()
			s.received = xld.WALStart
			// Data invalidates the idle position: whatever this message
			// belongs to is not applied yet, and only a LATER keepalive, with
			// nothing buffered, can vouch for a position again.
			s.idle = 0
			s.mu.Unlock()
			if err := s.handle(xld.WALData); err != nil {
				s.setErr(err)
				return
			}
		}
	}
}

func (s *Streaming) handle(data []byte) error {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return err
	}
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		s.mu.Lock()
		s.relations[m.RelationID] = m
		s.mu.Unlock()

	case *pglogrepl.BeginMessage:
		s.cur = &Transaction{CommitLSN: m.FinalLSN.String()}

	case *pglogrepl.CommitMessage:
		if s.cur != nil {
			s.cur.CommitLSN = m.CommitLSN.String()
			// Confirm through commit+1: confirming AT the commit LSN makes the
			// server resend this transaction forever. Same trap as wal2json's
			// nextlsn, different spelling.
			s.cur.NextLSN = (m.CommitLSN + 1).String()
			s.mu.Lock()
			s.buffered = append(s.buffered, *s.cur)
			s.bufferedChanges += weight(s.cur)
			s.mu.Unlock()
			s.cur = nil
		}

	case *pglogrepl.InsertMessage:
		s.appendChange(OpInsert, m.RelationID, m.Tuple, nil)
	case *pglogrepl.UpdateMessage:
		s.appendChange(OpUpdate, m.RelationID, m.NewTuple, m.OldTuple)
	case *pglogrepl.DeleteMessage:
		s.appendChange(OpDelete, m.RelationID, nil, m.OldTuple)
	case *pglogrepl.TruncateMessage:
		for _, rid := range m.RelationIDs {
			s.appendChange(OpTruncate, rid, nil, nil)
		}
	}
	return nil
}

func (s *Streaming) appendChange(op Op, relID uint32, newTup, oldTup *pglogrepl.TupleData) {
	if s.cur == nil {
		return
	}
	s.mu.Lock()
	rel := s.relations[relID]
	s.mu.Unlock()
	if rel == nil {
		return
	}
	qualified := rel.Namespace + "." + rel.RelationName
	if !s.tables[qualified] {
		return
	}
	c := Change{
		Op: op, Schema: rel.Namespace, Table: rel.RelationName,
		LSN: s.cur.CommitLSN,
	}
	if newTup != nil {
		c.Row = decodeTuple(rel, newTup)
	}
	if oldTup != nil {
		c.Key = decodeTuple(rel, oldTup)
	}
	// For DELETE with REPLICA IDENTITY DEFAULT the old tuple holds only the
	// key columns; that is all the mirror needs to tombstone the row.
	s.cur.Changes = append(s.cur.Changes, c)
}

// decodeTuple keeps values as their exact TEXT. proto v1 sends text format, so
// numerics arrive as the decimal string Postgres printed — no float anywhere.
func decodeTuple(rel *pglogrepl.RelationMessage, t *pglogrepl.TupleData) map[string]any {
	out := make(map[string]any, len(t.Columns))
	for i, col := range t.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case 'n': // null
			out[name] = nil
		case 'u': // unchanged TOAST value — not sent
			// Leaving it absent is deliberate, and the distinction is load-
			// bearing: an ABSENT column means "unchanged", a PRESENT nil means
			// "set to NULL". The writer carries absent columns forward from the
			// mirror (mirror.carryForward).
			//
			// The comment that used to be here claimed the writer already did
			// that. It did not. Every UPDATE to a row with a large unchanged
			// column silently wrote NULL over it — no error, no change in row
			// count, and invisible to anything but a column-by-column
			// comparison. See internal/mirror/toast_test.go.
			continue
		case 't':
			out[name] = string(col.Data)
		}
	}
	return out
}

func (s *Streaming) setErr(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

// Transactions drains whatever has been received since the last call, and
// lets a receiver paused on a full buffer resume.
func (s *Streaming) Transactions(ctx context.Context) ([]Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	out := s.buffered
	s.buffered = nil
	s.bufferedChanges = 0
	select {
	case s.wake <- struct{}{}:
	default:
	}
	return out, nil
}

// BufferedChanges is how many row-changes are waiting for Transactions().
func (s *Streaming) BufferedChanges() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bufferedChanges
}

// Confirm tells the server everything through lsn is durable. Call only after
// the mirror write is fsynced.
//
// It does not write to the connection: it records the position and wakes the
// receiver, which sends the status update (see receive). So a nil return means
// "recorded", and a failed send surfaces as the stream's error on this or the
// next call. That is no weaker than before — a status update is not
// acknowledged by the server, and an unconfirmed transaction is replayed and
// discarded by the per-table floor, never lost.
func (s *Streaming) Confirm(ctx context.Context, lsn string) error {
	p, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if p > s.confirmed {
		s.confirmed = p
		s.confirmDirty = true
		// Rouse the receiver whether it is blocked in a read or paused.
		if s.interrupt != nil {
			s.interrupt()
		}
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// IdleLSN is a position the server has decoded up to with nothing left for
// this consumer below it, or zero when there is no such position right now.
//
// Without it a caught-up mirror never confirms anything while the primary
// writes WAL that is not for its publication: other tables, vacuum, another
// database. Confirm only ever followed a transaction, so the slot stood still,
// pinned every segment written since, and the slot guard eventually dropped a
// slot whose consumer was missing nothing.
//
// The position is the walsender's keepalive, which carries the end of the last
// WAL record it has decoded. Decoding a commit record emits its transaction
// before that end is reported, and the stream is ordered, so by the time a
// keepalive saying P is read, every transaction whose commit record starts
// below P has been read too. It is recorded only when none of those is still
// here: nothing buffered for Transactions() and no transaction being assembled.
// Any XLogData clears it. So a non-zero answer means
//
//	every transaction committing below P was handed out by an EARLIER
//	Transactions() call, and every transaction not yet handed out commits at or
//	above P
//
// which is what pg_recvlogical relies on when it flushes a keepalive's
// position. Whether those earlier transactions are APPLIED is the caller's to
// know; it must hold none unapplied before treating P as confirmable, and it
// must ask after its Transactions() call, not before.
//
// P can be BELOW the slot's confirmed position: after a restart the walsender
// decodes again from restart_lsn and reports where it has got to. The caller
// must not confirm backwards.
func (s *Streaming) IdleLSN() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(s.idle)
}

func (s *Streaming) ReceivedLSN() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received.String()
}

func (s *Streaming) Close(ctx context.Context) error {
	if s.cancel != nil {
		s.cancel()
		<-s.done
		// The receiver has exited, so this goroutine is now the only writer.
		// Flush a confirmation it did not get to send; without it a clean
		// shutdown would make the next start replay the last tick.
		s.mu.Lock()
		dirty, pos, failed := s.confirmDirty, s.confirmed, s.err != nil
		s.confirmDirty = false
		s.mu.Unlock()
		if dirty && !failed {
			fctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_ = s.rc.SendStatus(fctx, pos)
			cancel()
		}
	}
	if s.conn == nil {
		return nil
	}
	return s.conn.Close(ctx)
}

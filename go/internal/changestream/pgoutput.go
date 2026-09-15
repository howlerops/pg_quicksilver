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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Streaming consumes pgoutput over the replication protocol. Received
// transactions are buffered; Transactions() drains the buffer, so callers keep
// the same micro-batching shape they had with polling.
type Streaming struct {
	conn        *pgconn.PgConn
	slot        string
	publication string
	tables      map[string]bool

	mu       sync.Mutex
	buffered []Transaction
	err      error

	relations map[uint32]*pglogrepl.RelationMessage
	cur       *Transaction
	confirmed pglogrepl.LSN
	received  pglogrepl.LSN
	cancel    context.CancelFunc
	done      chan struct{}
}

// NewStreaming dials a dedicated replication connection. dsn must NOT already
// carry replication=database; it is added here.
func NewStreaming(ctx context.Context, dsn, slot, publication string, tables []string) (*Streaming, error) {
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
	return &Streaming{
		conn: conn, slot: slot, publication: publication, tables: t,
		relations: map[uint32]*pglogrepl.RelationMessage{},
		done:      make(chan struct{}),
	}, nil
}

// CreateSlot creates the replication slot if absent, returning the consistent
// point — the LSN an initial snapshot must be taken as of, so that snapshot and
// stream join up with neither gap nor overlap.
func (s *Streaming) CreateSlot(ctx context.Context) (pglogrepl.LSN, bool, error) {
	res, err := pglogrepl.CreateReplicationSlot(ctx, s.conn, s.slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
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
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.publication),
		}})
	if err != nil {
		return fmt.Errorf("START_REPLICATION: %w", err)
	}
	s.confirmed = from
	rctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	go s.receive(rctx)
	return nil
}

func (s *Streaming) receive(ctx context.Context) {
	defer close(s.done)
	nextDeadline := time.Now().Add(10 * time.Second)
	for {
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(nextDeadline) {
			// keepalive: without this the server eventually drops the
			// connection, and the slot stops advancing.
			s.mu.Lock()
			pos := s.confirmed
			s.mu.Unlock()
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn,
				pglogrepl.StandbyStatusUpdate{WALWritePosition: pos}); err != nil {
				s.setErr(err)
				return
			}
			nextDeadline = time.Now().Add(10 * time.Second)
		}

		rctx, cancel := context.WithDeadline(ctx, nextDeadline)
		raw, err := s.conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) || ctx.Err() != nil {
				continue
			}
			s.setErr(err)
			return
		}

		cd, ok := raw.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			k, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err == nil && k.ReplyRequested {
				nextDeadline = time.Now()
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				s.setErr(err)
				return
			}
			s.mu.Lock()
			s.received = xld.WALStart
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
			// Leaving it absent is deliberate: writing nil would blank a
			// column the source did not change. The writer carries the prior
			// value forward because the row is re-read from the mirror.
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

// Transactions drains whatever has been received since the last call.
func (s *Streaming) Transactions(ctx context.Context) ([]Transaction, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	out := s.buffered
	s.buffered = nil
	return out, nil
}

// Confirm tells the server everything through lsn is durable. Call only after
// the mirror write is fsynced.
func (s *Streaming) Confirm(ctx context.Context, lsn string) error {
	p, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if p > s.confirmed {
		s.confirmed = p
	}
	pos := s.confirmed
	s.mu.Unlock()
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pos})
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
	}
	return s.conn.Close(ctx)
}

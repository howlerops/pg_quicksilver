// Package changestream is the seam every ingest path produces into.
//
// Logical replication today, physical-WAL-on-standby in phase 3, the archive
// tee for backfill — all yield the same Transaction values, so batching,
// columnar write, compaction and verification are written once. Per ADR-0009
// the phase 3 decoder is a Postgres background worker in C or Rust; it emits
// across this same boundary as a process, not a function call, which is why
// nothing has to be written twice.
//
// Two invariants the rest of the system depends on:
//
//  1. Transactions are whole. A Transaction is yielded only once its COMMIT has
//     been seen. If the mirror applied half a transaction that moved a row
//     between tables, a join across them would see it twice or not at all —
//     an unreproducible bug class, designed out here rather than patched later.
//
//  2. Every transaction carries both its commit LSN and the LSN to confirm
//     THROUGH. Postgres' upto_lsn stops *before* the record at that position,
//     so confirming at commit_lsn re-reads the transaction forever and stalls
//     the slot permanently. wal2json's nextlsn exists for exactly this.
package changestream

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	json "github.com/goccy/go-json"
	"github.com/jackc/pgx/v5"
)

type Op string

const (
	OpInsert   Op = "insert"
	OpUpdate   Op = "update"
	OpDelete   Op = "delete"
	OpTruncate Op = "truncate"
)

// ParseLSN turns "2/F9D4C6A8" into a comparable uint64.
func ParseLSN(lsn string) uint64 {
	parts := strings.SplitN(lsn, "/", 2)
	if len(parts) != 2 {
		return 0
	}
	hi, _ := strconv.ParseUint(parts[0], 16, 64)
	lo, _ := strconv.ParseUint(parts[1], 16, 64)
	return hi<<32 | lo
}

type Change struct {
	Op     Op
	Schema string
	Table  string
	LSN    string
	// Row holds the new tuple for insert/update; Key holds the replica
	// identity for update/delete. Values keep their exact decimal TEXT for
	// numerics — see decodeValue.
	Row map[string]any
	Key map[string]any
}

func (c Change) Qualified() string { return c.Schema + "." + c.Table }

type Transaction struct {
	CommitLSN string
	// NextLSN is what to confirm through to actually consume this transaction.
	NextLSN string
	Changes []Change
}

// Stream is the interface phase 3 swaps out, not the callers.
type Stream interface {
	Transactions(ctx context.Context) ([]Transaction, error)
	Confirm(ctx context.Context, lsn string) error
}

// Logical reads Postgres logical replication via wal2json (Path 2 in docs/03).
//
// Uses pg_logical_slot_peek_changes rather than the streaming replication
// protocol: pull-based and SQL-callable, so no protocol implementation. It also
// caps latency at the poll interval, which is why production must stream. Peek
// (not get) means nothing is consumed until Confirm, so a crash mid-write
// replays rather than loses.
type Logical struct {
	conn   *pgx.Conn
	slot   string
	tables map[string]bool
}

func NewLogical(conn *pgx.Conn, slot string, tables []string) *Logical {
	t := make(map[string]bool, len(tables))
	for _, x := range tables {
		t[x] = true
	}
	return &Logical{conn: conn, slot: slot, tables: t}
}

const slotOpts = `'format-version','2','include-lsn','true','include-transaction','true'`

func (l *Logical) EnsureSlot(ctx context.Context) error {
	var exists bool
	err := l.conn.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`,
		l.slot).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = l.conn.Exec(ctx,
		`SELECT pg_create_logical_replication_slot($1,'wal2json')`, l.slot)
	return err
}

func (l *Logical) DropSlot(ctx context.Context) error {
	_, err := l.conn.Exec(ctx,
		`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
		 WHERE slot_name=$1`, l.slot)
	return err
}

func (l *Logical) SlotLagBytes(ctx context.Context) (int64, error) {
	var lag *int64
	err := l.conn.QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint
		 FROM pg_replication_slots WHERE slot_name=$1`, l.slot).Scan(&lag)
	if err != nil || lag == nil {
		return 0, err
	}
	return *lag, nil
}

// wal2json format-version 2 message.
type wmsg struct {
	Action  string  `json:"action"`
	LSN     string  `json:"lsn"`
	NextLSN string  `json:"nextlsn"`
	Schema  string  `json:"schema"`
	Table   string  `json:"table"`
	Columns []wcol  `json:"columns"`
	Ident   []wcol  `json:"identity"`
}

type wcol struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// decodeValue keeps numerics as their exact decimal TEXT rather than routing
// them through float64. Python needed parse_float=Decimal for this and paid
// heavily; in Go it is just "don't unmarshal into a float".
func decodeValue(raw json.RawMessage) any {
	s := strings.TrimSpace(string(raw))
	switch {
	case s == "" || s == "null":
		return nil
	case s == "true":
		return true
	case s == "false":
		return false
	case s[0] == '"':
		var out string
		if err := json.Unmarshal(raw, &out); err != nil {
			return s
		}
		return out
	default:
		return s // number: exact text, parsed by the writer against its schema
	}
}

func toChange(m wmsg) Change {
	c := Change{Schema: m.Schema, Table: m.Table, LSN: m.LSN}
	switch m.Action {
	case "I":
		c.Op = OpInsert
	case "U":
		c.Op = OpUpdate
	case "D":
		c.Op = OpDelete
	case "T":
		c.Op = OpTruncate
	}
	if len(m.Columns) > 0 {
		c.Row = make(map[string]any, len(m.Columns))
		for _, col := range m.Columns {
			c.Row[col.Name] = decodeValue(col.Value)
		}
	}
	if len(m.Ident) > 0 {
		c.Key = make(map[string]any, len(m.Ident))
		for _, col := range m.Ident {
			c.Key[col.Name] = decodeValue(col.Value)
		}
	}
	return c
}

// Transactions returns whole transactions, in commit order.
func (l *Logical) Transactions(ctx context.Context) ([]Transaction, error) {
	rows, err := l.conn.Query(ctx, fmt.Sprintf(
		`SELECT data FROM pg_logical_slot_peek_changes($1,NULL,NULL,%s)`, slotOpts),
		l.slot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Transaction
	var cur *Transaction
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var m wmsg
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		switch m.Action {
		case "B":
			cur = &Transaction{CommitLSN: m.LSN}
		case "C":
			if cur != nil {
				cur.CommitLSN = m.LSN
				cur.NextLSN = m.NextLSN
				if cur.NextLSN == "" {
					cur.NextLSN = cur.CommitLSN
				}
				out = append(out, *cur) // only now — the COMMIT is seen
				cur = nil
			}
		case "I", "U", "D", "T":
			if cur == nil {
				continue
			}
			if !l.tables[m.Schema+"."+m.Table] {
				continue
			}
			cur.Changes = append(cur.Changes, toChange(m))
		}
	}
	return out, rows.Err()
}

// Confirm consumes everything through lsn. Call only after the mirror write is
// durable; confirming early is how a crash loses data.
func (l *Logical) Confirm(ctx context.Context, lsn string) error {
	rows, err := l.conn.Query(ctx, fmt.Sprintf(
		`SELECT 1 FROM pg_logical_slot_get_changes($1,$2,NULL,%s)`, slotOpts),
		l.slot, lsn)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

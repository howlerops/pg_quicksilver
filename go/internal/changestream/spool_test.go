package changestream

// The spool, and the receiver's handling of transactions the server streams
// before they commit. As in pgoutput_test.go the messages are real pgoutput
// bytes (protocol version 2 here) through a fake connection, so the decode and
// spool path under test is the production one and none of it needs PostgreSQL.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func collect(t *testing.T, txn Transaction) []Change {
	t.Helper()
	var out []Change
	if err := txn.Each(func(c Change) error { out = append(out, c); return nil }); err != nil {
		t.Fatal(err)
	}
	return out
}

func spoolFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.spool"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// What goes in comes out: absent stays absent, NULL stays NULL, the key tuple
// stays separate from the row, two tables keep their own columns, and the
// file can be read more than once.
func TestSpoolRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cols := map[string][]string{
		"public.a": {"id", "doc", "note"},
		"s.b":      {"k", "v"},
	}
	in := []Change{
		{Op: OpInsert, Schema: "public", Table: "a", Row: map[string]any{"id": "1", "doc": "{}", "note": nil}},
		{Op: OpUpdate, Schema: "public", Table: "a",
			Row: map[string]any{"id": "2", "note": "doc is absent: unchanged TOAST"},
			Key: map[string]any{"id": "1"}},
		{Op: OpDelete, Schema: "s", Table: "b", Key: map[string]any{"k": "x"}},
		{Op: OpTruncate, Schema: "s", Table: "b"},
		{Op: OpInsert, Schema: "s", Table: "b", Row: map[string]any{"k": "", "v": string(make([]byte, 70_000))}},
	}
	txn, err := NewSpooledTransaction(dir, "0/AA", "0/AB", cols, in)
	if err != nil {
		t.Fatal(err)
	}
	if !txn.Spooled() || txn.Len() != len(in) || txn.Changes != nil {
		t.Fatalf("Spooled=%v Len=%d Changes=%v", txn.Spooled(), txn.Len(), txn.Changes)
	}
	if !txn.Touches("public.a") || !txn.Touches("s.b") || txn.Touches("public.c") {
		t.Fatal("Touches is wrong")
	}
	for pass := 0; pass < 2; pass++ {
		got := collect(t, txn)
		if len(got) != len(in) {
			t.Fatalf("pass %d: %d changes, want %d", pass, len(got), len(in))
		}
		for i := range in {
			want := in[i]
			want.LSN = "0/AA"
			if !reflect.DeepEqual(got[i], want) {
				t.Errorf("pass %d change %d:\n got %#v\nwant %#v", pass, i, got[i], want)
			}
		}
	}
	// absent and NULL are different statements and must come back as such
	got := collect(t, txn)
	if _, present := got[1].Row["doc"]; present {
		t.Error("an absent column came back present")
	}
	if v, present := got[0].Row["note"]; !present || v != nil {
		t.Error("a NULL column did not come back as a present nil")
	}
	txn.Release()
	if left := spoolFiles(t, dir); len(left) != 0 {
		t.Fatalf("Release left %v", left)
	}
}

// Cutting a spool back removes the changes after the cut and the table
// definitions first written after it, and what is written next reads back.
func TestSpoolTruncate(t *testing.T) {
	sp, err := newSpool(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a := &relDef{schema: "public", table: "a", cols: []string{"id"}}
	b := &relDef{schema: "public", table: "b", cols: []string{"id"}}
	ins := func(def *relDef, id int) {
		t.Helper()
		if err := sp.add(def, Change{Op: OpInsert, Schema: def.schema, Table: def.table,
			Row: map[string]any{"id": fmt.Sprint(id)}}); err != nil {
			t.Fatal(err)
		}
	}
	ins(a, 1)
	off, n := sp.off, sp.n
	ins(b, 2) // b's definition is written here, after the cut
	ins(a, 3)
	if err := sp.truncate(off, n); err != nil {
		t.Fatal(err)
	}
	ins(b, 4) // so it must be written again
	ins(a, 5)
	if err := sp.finish(); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := sp.each("0/1", func(c Change) error {
		got = append(got, c.Table+c.Row["id"].(string))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{"a1", "b4", "a5"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if sp.n != 3 {
		t.Fatalf("count %d", sp.n)
	}
}

// ---- protocol version 2 messages -------------------------------------------

func streamStart(xid uint32, first bool) []byte {
	b := u32([]byte{'S'}, xid)
	if first {
		return append(b, 1)
	}
	return append(b, 0)
}

func streamStop() []byte { return []byte{'E'} }

func streamCommit(xid uint32, lsn uint64) []byte {
	return u64(u64(u64(append(u32([]byte{'c'}, xid), 0), lsn), lsn+8), 0)
}

func streamAbort(xid, subxid uint32) []byte { return u32(u32([]byte{'A'}, xid), subxid) }

// sInsert is an insert inside a stream block: it carries the xid of the
// (sub)transaction that made it.
func sInsert(xid uint32, id int) []byte {
	v := fmt.Sprint(id)
	b := u16(append(u32(u32([]byte{'I'}, xid), relID), 'N'), 1)
	return append(u32(append(b, 't'), uint32(len(v))), v...)
}

func sRelation(xid uint32) []byte {
	b := u32(u32([]byte{'R'}, xid), relID)
	b = cstr(cstr(b, "public"), "events")
	b = u16(append(b, 'd'), 1)
	b = cstr(append(b, 1), "id")
	return u32(u32(b, 20), 0xffffffff)
}

func xl(payloads ...[]byte) []pgproto3.BackendMessage {
	var out []pgproto3.BackendMessage
	for _, p := range payloads {
		out = append(out, xlog(1, p))
	}
	return out
}

func startSpooling(t *testing.T, threshold ...int) (*Streaming, *fakeConn, string) {
	t.Helper()
	f := &fakeConn{msgs: make(chan pgproto3.BackendMessage), t: t}
	s := newStreaming(f, map[string]bool{"public.events": true})
	s.MaxBufferedChanges = 0
	s.StatusInterval = time.Hour
	s.SpoolDir = t.TempDir()
	if len(threshold) > 0 {
		s.SpoolThresholdChanges = threshold[0]
	}
	s.run(context.Background())
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s, f, s.SpoolDir
}

func drain(t *testing.T, s *Streaming, want int) []Transaction {
	t.Helper()
	var out []Transaction
	waitFor(t, fmt.Sprintf("%d transactions", want), func() bool {
		got, err := s.Transactions(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, got...)
		return len(out) >= want
	})
	return out
}

func ids(t *testing.T, txn Transaction) []string {
	t.Helper()
	var out []string
	for _, c := range collect(t, txn) {
		out = append(out, c.Row["id"].(string))
	}
	return out
}

// Two transactions streamed at once, their blocks interleaved, with a small
// ordinary transaction committing in between. Each comes out whole, in commit
// order, holding only its own changes; the small one never touches the disk.
func TestStreamedTransactionsInterleave(t *testing.T) {
	s, f, dir := startSpooling(t)
	var msgs []pgproto3.BackendMessage
	msgs = append(msgs, xl(
		streamStart(10, true), sRelation(10), sInsert(10, 1), sInsert(10, 2), streamStop(),
		streamStart(20, true), sInsert(20, 100), streamStop(),
		streamStart(10, false), sInsert(10, 3), streamStop(),
	)...)
	msgs = append(msgs, txn(0x500, 7, 2)...) // ordinary: Begin, two inserts, Commit
	msgs = append(msgs, xl(
		streamStart(20, false), sInsert(20, 101), sInsert(20, 102), streamStop(),
		streamCommit(20, 0x600),
		streamStart(10, false), sInsert(10, 4), streamStop(),
		streamCommit(10, 0x700),
	)...)
	stop := feed(f, msgs)
	defer stop()

	got := drain(t, s, 3)
	if len(got) != 3 {
		t.Fatalf("%d transactions", len(got))
	}
	small, t20, t10 := got[0], got[1], got[2]
	if small.Spooled() || len(small.Changes) != 2 || small.CommitLSN != "0/500" {
		t.Errorf("the small transaction: spooled=%v changes=%d lsn=%s",
			small.Spooled(), len(small.Changes), small.CommitLSN)
	}
	if !t20.Spooled() || t20.CommitLSN != "0/600" || t20.NextLSN != "0/601" ||
		!reflect.DeepEqual(ids(t, t20), []string{"100", "101", "102"}) {
		t.Errorf("xid 20: spooled=%v lsn=%s next=%s ids=%v", t20.Spooled(), t20.CommitLSN, t20.NextLSN, ids(t, t20))
	}
	if !t10.Spooled() || t10.CommitLSN != "0/700" ||
		!reflect.DeepEqual(ids(t, t10), []string{"1", "2", "3", "4"}) {
		t.Errorf("xid 10: spooled=%v lsn=%s ids=%v", t10.Spooled(), t10.CommitLSN, ids(t, t10))
	}
	if t10.Len() != 4 || t20.Len() != 3 {
		t.Errorf("Len: %d and %d", t10.Len(), t20.Len())
	}
	for _, c := range collect(t, t10) {
		if c.LSN != "0/700" {
			t.Errorf("a spooled change carries LSN %s, want its commit", c.LSN)
		}
	}

	// A spool lives until its transaction is confirmed, and no longer.
	if n := len(spoolFiles(t, dir)); n != 2 {
		t.Fatalf("%d spool files before any confirm, want 2", n)
	}
	if err := s.Confirm(context.Background(), t20.NextLSN); err != nil {
		t.Fatal(err)
	}
	if n := len(spoolFiles(t, dir)); n != 1 {
		t.Fatalf("%d spool files after confirming the first, want 1", n)
	}
	if err := s.Confirm(context.Background(), t10.NextLSN); err != nil {
		t.Fatal(err)
	}
	if n := len(spoolFiles(t, dir)); n != 0 {
		t.Fatalf("%d spool files after confirming both", n)
	}
}

// A rolled-back subtransaction's changes are never delivered: not its own, and
// not those of a subtransaction nested inside it. What the transaction did
// before the savepoint and after the rollback is.
func TestStreamedSubtransactionAbort(t *testing.T) {
	s, f, _ := startSpooling(t)
	stop := feed(f, xl(
		streamStart(10, true), sRelation(10),
		sInsert(10, 1),   // top level
		sInsert(11, 2),   // savepoint a: released, so it stays
		sInsert(12, 900), // savepoint b
		sInsert(13, 901), //   savepoint c inside b, released into it
		sInsert(12, 902), //   back in b
		streamStop(),
		streamAbort(10, 13), // ROLLBACK TO b arrives as c's abort, then b's
		streamAbort(10, 12),
		streamStart(10, false),
		sInsert(10, 3),   // top level again
		sInsert(14, 903), // savepoint d, rolled back in the same block's wake
		streamStop(),
		streamAbort(10, 14),
		streamAbort(10, 99), // a subtransaction that sent nothing: no-op
		streamStart(10, false), sInsert(10, 4), streamStop(),
		streamCommit(10, 0x900),
	))
	defer stop()
	got := drain(t, s, 1)
	if want := []string{"1", "2", "3", "4"}; !reflect.DeepEqual(ids(t, got[0]), want) {
		t.Fatalf("delivered %v, want %v", ids(t, got[0]), want)
	}
	if got[0].Len() != 4 {
		t.Fatalf("Len %d", got[0].Len())
	}
}

// A transaction that aborts after part of it was streamed delivers nothing and
// leaves no file; the transactions around it are untouched.
func TestStreamedTransactionAbort(t *testing.T) {
	s, f, dir := startSpooling(t)
	var msgs []pgproto3.BackendMessage
	msgs = append(msgs, xl(
		streamStart(10, true), sRelation(10), sInsert(10, 1), sInsert(10, 2), streamStop(),
		streamStart(20, true), sInsert(20, 50), streamStop(),
		streamAbort(10, 10),
		streamCommit(20, 0x400),
	)...)
	msgs = append(msgs, txn(0x500, 7, 1)...)
	stop := feed(f, msgs)
	defer stop()
	got := drain(t, s, 2)
	if len(got) != 2 || !reflect.DeepEqual(ids(t, got[0]), []string{"50"}) || got[1].Len() != 1 {
		t.Fatalf("delivered %d transactions; first holds %v", len(got), ids(t, got[0]))
	}
	if n := len(spoolFiles(t, dir)); n != 1 {
		t.Fatalf("%d spool files, want only the committed transaction's", n)
	}
}

// A transaction the server did NOT stream stays in memory while it is small
// and moves to a spool when it crosses the threshold, with nothing lost at the
// crossing.
func TestWholeTransactionSpillsPastThreshold(t *testing.T) {
	s, f, dir := startSpooling(t, 5)
	var msgs []pgproto3.BackendMessage
	msgs = append(msgs, xlog(1, relationMsg()))
	msgs = append(msgs, txn(0x100, 1, 5)...)    // at the threshold: memory
	msgs = append(msgs, txn(0x200, 100, 12)...) // past it: spooled
	stop := feed(f, msgs)
	defer stop()
	got := drain(t, s, 2)
	if got[0].Spooled() || len(got[0].Changes) != 5 {
		t.Errorf("the small transaction: spooled=%v changes=%d", got[0].Spooled(), len(got[0].Changes))
	}
	if !got[1].Spooled() || got[1].Len() != 12 || got[1].Changes != nil {
		t.Fatalf("the large transaction: spooled=%v len=%d", got[1].Spooled(), got[1].Len())
	}
	var want []string
	for i := 100; i < 112; i++ {
		want = append(want, fmt.Sprint(i))
	}
	if !reflect.DeepEqual(ids(t, got[1]), want) {
		t.Fatalf("delivered %v", ids(t, got[1]))
	}
	if s.BufferedChanges() != 0 {
		t.Fatalf("buffer accounting after a drain: %d", s.BufferedChanges())
	}
	if n := len(spoolFiles(t, dir)); n != 1 {
		t.Fatalf("%d spool files", n)
	}
}

// Spool files found at start belong to a process that died; the slot resends
// what was in them.
func TestSpoolsAreGarbageAtStart(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"123-1.spool", "123-2.spool"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("half a transaction"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	keep := filepath.Join(dir, "not-a-spool")
	_ = os.WriteFile(keep, nil, 0o644)
	removeSpools(dir)
	if left := spoolFiles(t, dir); len(left) != 0 {
		t.Fatalf("left %v", left)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("removed a file that was not a spool")
	}
}

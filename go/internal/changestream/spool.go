package changestream

// A transaction too large to hold in memory, kept on disk instead.
//
// The receiver used to materialise every transaction as []Change, a map per
// row, before anything downstream saw it: a single 1.5M-row transaction peaked
// at 1.1 GB of live heap. MaxBufferedChanges never bounded that — it counts
// COMMITTED changes, and one transaction is indivisible.
//
// So a large transaction is written here as it arrives and read back as a
// stream when it is applied. What stays in memory is one bufio buffer while
// writing and one Change at a time while reading.
//
// FORMAT. A spool is a sequence of records, each a uvarint length and that many
// bytes of payload. The payload's first byte says what it is:
//
//	'R'  a table definition: schema, table, column count, column names (each a
//	     uvarint length and bytes). Its ordinal among the 'R' records of the
//	     file is the id the changes after it refer to it by.
//	'I' 'U' 'D' 'T'  a change: uvarint definition id, one flags byte (1 = a
//	     new tuple follows, 2 = an old/key tuple follows), then the tuples. A
//	     tuple is one entry per column of the definition, in its order:
//	       'a'  absent — pgoutput's "unchanged TOAST", which must stay
//	            distinguishable from NULL (see decodeTuple)
//	       'n'  NULL
//	       't'  uvarint length and the value's text
//
// Column names are written once per table rather than once per value, which is
// most of why a spooled change is a fraction of its in-memory size.
//
// DURABILITY. None, deliberately. A spool is never fsynced and never survives
// the process: the slot has not been confirmed past a transaction that is
// still in a spool, so after a crash PostgreSQL sends it again and the file
// left behind is garbage. Streaming.Start deletes whatever it finds.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

// SpoolDir is where spool files go. cmd/qs-mirror points it under the mirror
// path, so a large transaction costs disk on the volume that was sized for the
// data rather than in the container's memory limit or its root filesystem.
// Empty means a directory under os.TempDir, created on first use.
var SpoolDir = ""

// SpoolThresholdChanges and SpoolThresholdBytes are when a transaction that
// arrived whole (not streamed by the server) stops being held in memory:
// whichever is crossed first. A transaction the server streams in progress is
// spooled from its first change, because the server only streams what has
// already outgrown its own logical_decoding_work_mem.
var (
	SpoolThresholdChanges = 50_000
	SpoolThresholdBytes   = 32 << 20
)

// relDef is what a spool needs to know about a table: enough to turn a tuple
// back into a map. A new Relation message makes a new relDef, so a column list
// that changes inside a transaction gets a definition of its own.
type relDef struct {
	schema, table string
	cols          []string
}

type spoolDef struct {
	def *relDef
	off int64 // where its 'R' record starts; a truncate below this forgets it
}

// Spool is one transaction's changes on disk.
type Spool struct {
	path string
	f    *os.File
	w    *bufio.Writer
	off  int64 // bytes written, including what bufio still holds
	n    int   // changes written

	ids     map[*relDef]int
	defs    []spoolDef
	touched map[string]bool // qualified names of tables with a change in here
	buf     []byte
}

var spoolSeq atomic.Int64

func spoolDirOrTemp(dir string) string {
	if dir == "" {
		return filepath.Join(os.TempDir(), "qs-spool")
	}
	return dir
}

func newSpool(dir string) (*Spool, error) {
	dir = spoolDirOrTemp(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%d-%d.spool", os.Getpid(), spoolSeq.Add(1)))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &Spool{
		path: path, f: f, w: bufio.NewWriterSize(f, 256<<10),
		ids: map[*relDef]int{}, touched: map[string]bool{},
	}, nil
}

// removeSpools deletes every spool file in dir. Called at start: whatever is
// there belongs to a process that died, and the slot will resend it.
func removeSpools(dir string) {
	m, _ := filepath.Glob(filepath.Join(spoolDirOrTemp(dir), "*.spool"))
	for _, p := range m {
		_ = os.Remove(p)
	}
}

func (s *Spool) record(payload []byte) error {
	var l [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(l[:], uint64(len(payload)))
	if _, err := s.w.Write(l[:n]); err != nil {
		return err
	}
	if _, err := s.w.Write(payload); err != nil {
		return err
	}
	s.off += int64(n + len(payload))
	return nil
}

func appendStr(b []byte, v string) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func appendTuple(b []byte, def *relDef, row map[string]any) ([]byte, error) {
	seen := 0
	for _, name := range def.cols {
		v, ok := row[name]
		if !ok {
			b = append(b, 'a')
			continue
		}
		seen++
		switch x := v.(type) {
		case nil:
			b = append(b, 'n')
		case string:
			b = appendStr(append(b, 't'), x)
		default:
			return nil, fmt.Errorf("spool: column %s holds a %T, and only text and NULL are spooled", name, v)
		}
	}
	if seen != len(row) {
		return nil, fmt.Errorf("spool: a row of %s.%s carries a column its relation does not list",
			def.schema, def.table)
	}
	return b, nil
}

func opByte(op Op) byte {
	switch op {
	case OpInsert:
		return 'I'
	case OpUpdate:
		return 'U'
	case OpDelete:
		return 'D'
	}
	return 'T'
}

// add appends one change.
func (s *Spool) add(def *relDef, c Change) error {
	id, ok := s.ids[def]
	if !ok {
		id = len(s.defs)
		b := append(s.buf[:0], 'R')
		b = appendStr(appendStr(b, def.schema), def.table)
		b = binary.AppendUvarint(b, uint64(len(def.cols)))
		for _, col := range def.cols {
			b = appendStr(b, col)
		}
		s.buf = b
		off := s.off
		if err := s.record(b); err != nil {
			return err
		}
		s.ids[def] = id
		s.defs = append(s.defs, spoolDef{def, off})
		s.touched[def.schema+"."+def.table] = true
	}
	b := append(s.buf[:0], opByte(c.Op))
	b = binary.AppendUvarint(b, uint64(id))
	var flags byte
	if c.Row != nil {
		flags |= 1
	}
	if c.Key != nil {
		flags |= 2
	}
	b = append(b, flags)
	var err error
	if c.Row != nil {
		if b, err = appendTuple(b, def, c.Row); err != nil {
			return err
		}
	}
	if c.Key != nil {
		if b, err = appendTuple(b, def, c.Key); err != nil {
			return err
		}
	}
	s.buf = b
	if err := s.record(b); err != nil {
		return err
	}
	s.n++
	return nil
}

// truncate cuts the spool back to a position it was at earlier: off bytes and
// n changes. It is how a rolled-back subtransaction is undone.
func (s *Spool) truncate(off int64, n int) error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	if err := s.f.Truncate(off); err != nil {
		return err
	}
	if _, err := s.f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	s.off, s.n = off, n
	// A definition written after the cut is gone with it, and the next change
	// to that table must write it again.
	for len(s.defs) > 0 && s.defs[len(s.defs)-1].off >= off {
		delete(s.ids, s.defs[len(s.defs)-1].def)
		s.defs = s.defs[:len(s.defs)-1]
	}
	return nil
}

// finish ends writing. The file stays until remove.
func (s *Spool) finish() error {
	if s.f == nil {
		return nil
	}
	err := s.w.Flush()
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	s.f, s.w, s.buf, s.ids, s.defs = nil, nil, nil, nil, nil
	return err
}

func (s *Spool) remove() {
	if s.f != nil {
		_ = s.f.Close()
		s.f = nil
	}
	_ = os.Remove(s.path)
}

func readStr(b []byte) (string, []byte, error) {
	l, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < l {
		return "", nil, errors.New("spool: truncated record")
	}
	return string(b[n : n+int(l)]), b[n+int(l):], nil
}

func readTuple(b []byte, def *relDef) (map[string]any, []byte, error) {
	row := make(map[string]any, len(def.cols))
	for _, name := range def.cols {
		if len(b) == 0 {
			return nil, nil, errors.New("spool: truncated tuple")
		}
		tag := b[0]
		b = b[1:]
		switch tag {
		case 'a':
		case 'n':
			row[name] = nil
		case 't':
			v, rest, err := readStr(b)
			if err != nil {
				return nil, nil, err
			}
			row[name], b = v, rest
		default:
			return nil, nil, fmt.Errorf("spool: unknown value tag %q", tag)
		}
	}
	return row, b, nil
}

// each reads the spool from the start, calling fn once per change in the order
// they were written. It opens the file itself, so it can be called more than
// once — the pump applies one batch to every table in turn.
func (s *Spool) each(lsn string, fn func(Change) error) error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256<<10)
	var defs []*relDef
	var buf []byte
	for {
		l, err := binary.ReadUvarint(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if uint64(cap(buf)) < l {
			buf = make([]byte, l)
		}
		b := buf[:l]
		if _, err := io.ReadFull(r, b); err != nil {
			return fmt.Errorf("spool: %w", err)
		}
		if len(b) == 0 {
			return errors.New("spool: empty record")
		}
		kind := b[0]
		b = b[1:]
		if kind == 'R' {
			def := &relDef{}
			if def.schema, b, err = readStr(b); err != nil {
				return err
			}
			if def.table, b, err = readStr(b); err != nil {
				return err
			}
			nc, n := binary.Uvarint(b)
			if n <= 0 {
				return errors.New("spool: truncated definition")
			}
			b = b[n:]
			for i := uint64(0); i < nc; i++ {
				var col string
				if col, b, err = readStr(b); err != nil {
					return err
				}
				def.cols = append(def.cols, col)
			}
			defs = append(defs, def)
			continue
		}
		id, n := binary.Uvarint(b)
		if n <= 0 || id >= uint64(len(defs)) || len(b) < n+1 {
			return errors.New("spool: change refers to a table the spool never defined")
		}
		def := defs[id]
		flags := b[n]
		b = b[n+1:]
		c := Change{Schema: def.schema, Table: def.table, LSN: lsn}
		switch kind {
		case 'I':
			c.Op = OpInsert
		case 'U':
			c.Op = OpUpdate
		case 'D':
			c.Op = OpDelete
		case 'T':
			c.Op = OpTruncate
		default:
			return fmt.Errorf("spool: unknown record kind %q", kind)
		}
		if flags&1 != 0 {
			if c.Row, b, err = readTuple(b, def); err != nil {
				return err
			}
		}
		if flags&2 != 0 {
			if c.Key, _, err = readTuple(b, def); err != nil {
				return err
			}
		}
		if err := fn(c); err != nil {
			return err
		}
	}
}

// ---- Transaction: one shape for both ---------------------------------------

// Len is how many changes the transaction holds, wherever they are. It is what
// len(t.Changes) used to mean, and what every budget counted in changes (the
// tick cap, the pump's queue, the stream's buffer) should be given.
func (t Transaction) Len() int {
	if t.spool != nil {
		return t.spool.n
	}
	return len(t.Changes)
}

// Spooled reports whether the changes are on disk rather than in Changes.
func (t Transaction) Spooled() bool { return t.spool != nil }

// Touches reports whether the transaction changes the given table. For a
// spooled transaction it is answered from what was noted while writing, so
// asking does not read the file; it may say yes for a table whose only changes
// were in a rolled-back subtransaction, never no for one that has some.
func (t Transaction) Touches(qualified string) bool {
	if t.spool != nil {
		return t.spool.touched[qualified]
	}
	for _, c := range t.Changes {
		if c.Qualified() == qualified {
			return true
		}
	}
	return false
}

// Each calls fn for every change, in order. It is the one way to read a
// transaction that works whether or not it was spooled, and it holds one
// change at a time: fn must not keep more of them than it means to pay for.
func (t Transaction) Each(fn func(Change) error) error {
	if t.spool != nil {
		return t.spool.each(t.CommitLSN, fn)
	}
	for _, c := range t.Changes {
		if err := fn(c); err != nil {
			return err
		}
	}
	return nil
}

// Release deletes the spool file, if there is one. Streaming does this itself
// when the transaction is confirmed; it is exported for a Transaction made
// with NewSpooledTransaction.
func (t Transaction) Release() {
	if t.spool != nil {
		t.spool.remove()
	}
}

// NewSpooledTransaction writes changes to a spool under dir and returns the
// transaction that reads them back. columns gives each table's column order,
// by qualified name. It is how a test, or an ingest path other than Streaming,
// makes a transaction of the large kind.
func NewSpooledTransaction(dir, commitLSN, nextLSN string,
	columns map[string][]string, changes []Change,
) (Transaction, error) {
	sp, err := newSpool(dir)
	if err != nil {
		return Transaction{}, err
	}
	defs := map[string]*relDef{}
	for _, c := range changes {
		q := c.Qualified()
		def := defs[q]
		if def == nil {
			def = &relDef{schema: c.Schema, table: c.Table, cols: columns[q]}
			defs[q] = def
		}
		if err := sp.add(def, c); err != nil {
			sp.remove()
			return Transaction{}, err
		}
	}
	if err := sp.finish(); err != nil {
		sp.remove()
		return Transaction{}, err
	}
	return Transaction{CommitLSN: commitLSN, NextLSN: nextLSN, spool: sp}, nil
}

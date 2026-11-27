package mirror

// Initial snapshot / bootstrap.
//
// Needed in three situations, and the same code serves all of them:
//   - a table newly matches the `tables:` pattern
//   - a mirror node is created or its storage is lost
//   - a failover invalidated the replication slot, so the stream cannot be
//     resumed and the mirror must be rebuilt (docs/05 lifecycle matrix)
//
// The correctness requirement is that snapshot and stream join up with neither
// gap nor overlap. Postgres gives us exactly the tool: CREATE_REPLICATION_SLOT
// returns a consistent point, and a REPEATABLE READ snapshot taken at that LSN
// sees precisely the state the stream will start from. Take the snapshot first,
// then stream from the consistent point, and every row is applied exactly once.
//
// This implementation takes the simpler route available when the slot already
// exists: snapshot at the CURRENT LSN and start the stream there. Overlap is
// then possible but harmless — the mirror is an upsert-by-key store, so
// replaying a row that the snapshot already contains converges to the same
// state. A gap would not be harmless, which is why the order is snapshot-then-
// stream and never the reverse.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Snapshot replaces the mirror's contents with the table's current rows and
// sets applied_lsn to the LSN the caller should stream from.
//
// What a reader sees WHILE it runs is decided by beginSnapshot, and is never
// an empty table: that is a state the source did not have.
func (t *Table) Snapshot(ctx context.Context, conn *pgx.Conn, atLSN string) (int, error) {
	old, err := t.beginSnapshot()
	if err != nil {
		return 0, err
	}
	kept := t.ahead
	n, err := t.snapshotRows(ctx, conn, atLSN)
	if err != nil {
		if kept {
			// The manifest on disk is still the previous one and State is not.
			t.broken = true
		}
		return 0, err
	}
	// The previous contents, now that the manifest which replaces them is
	// durable and published: through the grace, like any superseded file.
	t.discard(old)
	return n, nil
}

// beginSnapshot empties the mirror in memory and decides what stands in for
// it until the new contents are published. It returns the files to discard
// once they are.
//
// The first statement of a snapshot used to be truncate(), whose manifest
// write published an EMPTY, servable snapshot at the old applied_lsn — and
// that stood for as long as the bootstrap or re-snapshot took. Readiness does
// not cover it: readiness is one bit per Pod, false during a first bootstrap
// only, while a re-snapshot of one table (resume.go, a rebuild) runs on a Pod
// that may be serving the others, and a reader of snapshot.json consults no
// probe at all.
//
// So one of two things happens instead, and which is a question of disk:
//
// KEEP. The previous contents stay on disk, named by the manifest and by the
// published snapshot, until the new base file is complete; nothing is written
// here at all. A reader goes on getting the previous table — old, and whole —
// and a crash leaves that same manifest, exactly as it was. This costs the
// old files beside the new one for the length of the snapshot, which is the
// kind of holding RetireMaxBytes exists to bound, so it is done only when
// everything on disk fits under that cap. A first bootstrap has nothing on
// disk and takes this path trivially: nothing is published until there is a
// table to publish.
//
// REFUSE. Otherwise the empty manifest is written and the files released
// first, as before, because a volume that fills stops every table — but the
// snapshot published with it refuses, like a halted one (rebuildingReason),
// until the new contents replace it.
func (t *Table) beginSnapshot() (old []string, err error) {
	if t.broken {
		return nil, ErrNeedsReopen
	}
	old = t.allFiles()
	if t.canHold(old) {
		t.forgetFiles()
		t.ahead = true
		return old, nil
	}
	t.rebuilding = rebuildingReason
	return nil, t.truncate()
}

// canHold reports whether the files listed may stay on disk while their
// replacement is written: whether they fit under RetireMaxBytes. The list is
// the whole directory, so it already counts what is waiting out a grace.
func (t *Table) canHold(paths []string) bool {
	max := t.publisher().capNow()
	if max <= 0 {
		return true
	}
	var held int64
	for _, path := range paths {
		if filepath.Base(filepath.Dir(path)) == "index" {
			continue
		}
		if fi, err := os.Lstat(path); err == nil {
			held += fi.Size()
		}
	}
	return held <= max
}

func (t *Table) snapshotRows(ctx context.Context, conn *pgx.Conn, atLSN string) (int, error) {
	rows, err := sourceQuery(ctx, conn, t.Qualified, t.Order)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	// Rows stream from the source straight into the Parquet writer. Holding the
	// whole table first is one Go map per row before a byte is written — on the
	// jsonb shape that is a 6 KB document per row for the entire table, and it
	// is most of why that shape bootstraps an order of magnitude slower than the
	// narrow one.
	t.State.Seq++
	name := fmt.Sprintf("%06d.parquet", t.State.Seq)
	path := fmt.Sprintf("%s/base/%s", t.Dir, name)
	w, err := t.newParquetWriter(path, nil, true)
	if err != nil {
		return 0, err
	}

	// Rows go from the scan straight into the writer in COLUMN ORDER, and no
	// key -> position map is built or written along the way.
	//
	// Where the bootstrap second actually goes, measured on 400,000 narrow rows
	// against a 2.2s bootstrap:
	//
	//	query + wire + scan            200 ms     9%
	//	building the index map         169 ms     8%
	//	marshalling it to JSON         511 ms    23%
	//
	// The marshal dominates because encoding/json SORTS map keys, so writing
	// that file is a sort of 400,000 strings — plus 6.2 MB on disk per base
	// file. And what it held was "the key at row i", for a file whose row i
	// holds that key in its key column. ensureIndex reads it back from the
	// file instead, one column, which is what a column store is for.
	//
	// docs/25 blamed the per-row map for this shape's 16.5 MB/s and docs/26
	// recorded that removing it changed nothing. This is where the time was.
	//
	// The key column is still not turned into a map here. It IS written, as it
	// passes, to the base file's index (kidx.go): sorted fixed-width records in
	// runs of bounded size, so that the first ensureIndex maps a file instead
	// of reading ten million keys back out of the one just written.
	var kb *kidxBuilder
	ki := -1
	if DiskIndex {
		kb = t.newKidxBuilder(name, 0, false)
		for i, c := range t.Order {
			if c == t.Key {
				ki = i
			}
		}
	}
	n := 0
	for rows.Next() {
		vals := outputText(rows, len(t.Order))
		if kb != nil {
			// A key the index cannot hold makes the builder give the file up,
			// and ensureIndex reads the column as it always did.
			if ki < 0 {
				kb.abort()
				kb = nil
			} else if err := kb.add(t.keyOf(vals[ki]), int32(n), 0, false); err != nil || kb.nExtras > 0 {
				kb.abort()
				kb = nil
			}
		}
		n++
		if err := w.AppendValues(vals); err != nil {
			w.Close()
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		w.Close()
		return 0, err
	}
	if err := w.Close(); err != nil {
		return 0, err
	}
	if kb != nil {
		// Written and closed again: nothing here needs the index yet, and the
		// one ensureIndex opens is checked against the base file like any other.
		// A failure costs only the rebuild.
		if n == 0 {
			kb.abort()
		} else if x, _, _, err := kb.finish(); err == nil {
			x.close()
		}
	}

	if n > 0 {
		t.State.BaseFiles = []string{name}
	} else {
		// An empty table gets no base file, exactly as before; the footer-only
		// file the writer just produced would otherwise sit in the manifest
		// forever as a file with nothing in it.
		_ = os.Remove(path)
	}
	t.State.DeltaFiles = nil
	t.State.BaseRows = n
	t.State.DeltaRows = 0
	t.State.AppliedLSN = atLSN
	return n, t.finishSnapshot()
}

// sourceQuery reads a table as the text of each type's OUTPUT FUNCTION, which
// is what pgoutput sends for the same value.
//
// It used to select `col::text`, and a cast to text is a different function
// from the one that prints a value. For most types they agree. Where they do
// not, the snapshot stored one spelling and the stream another in the same
// column, and the mirror disagreed with itself:
//
//	type        col::text          output function (and pgoutput)
//	char(8)     ab                 ab······  (padded to 8)
//	inet        192.168.1.7/32     192.168.1.7
//	boolean     true               t
//	"char"[]    differ in quoting, likewise any array of the above
//
// There is no cast that fixes this for every type, including ones defined
// tomorrow, and there does not need to be one: the simple query protocol
// returns every column in TEXT format, and text format IS the output
// function's result. So the columns are selected bare and read raw. The
// session settings that shape that text — DateStyle, IntervalStyle,
// extra_float_digits, bytea_output — are pinned on every connection by
// internal/pgtext, for the stream and for this alike.
func sourceQuery(ctx context.Context, conn *pgx.Conn, qualified string, order []string) (pgx.Rows, error) {
	sel := make([]string, len(order))
	for i, c := range order {
		sel[i] = sqlIdent(c)
	}
	return conn.Query(ctx, "SELECT "+strings.Join(sel, ", ")+" FROM "+qualified,
		pgx.QueryExecModeSimpleProtocol)
}

// outputText is one row of a sourceQuery: a string per column, nil for NULL.
// The raw values are only valid until the next row, so they are copied.
func outputText(rows pgx.Rows, n int) []any {
	raw := rows.RawValues()
	vals := make([]any, n)
	for i := 0; i < n && i < len(raw); i++ {
		if raw[i] != nil {
			vals[i] = string(raw[i])
		}
	}
	return vals
}

// SourceRows streams a table from PostgreSQL in the form the mirror stores
// it: one value per column of order, text as the type's output function
// prints it, nil for NULL. A verifier that reads the source any other way —
// `col::text`, say — compares the mirror against a spelling the mirror was
// never sent.
func SourceRows(ctx context.Context, conn *pgx.Conn, qualified string,
	order []string, fn func(vals []any) error,
) error {
	rows, err := sourceQuery(ctx, conn, qualified, order)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(outputText(rows, len(order))); err != nil {
			return err
		}
	}
	return rows.Err()
}

// finishSnapshot writes the manifest of the new contents, which is also what
// ends whatever beginSnapshot left standing for them.
func (t *Table) finishSnapshot() error {
	t.rebuilding = ""
	t.ahead = false
	// These rows were read from the source, by this binary: whatever the
	// files they replace held, the new ones are at the current writer format.
	t.freshContents = true
	return t.saveState()
}

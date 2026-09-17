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

	"github.com/jackc/pgx/v5"
)

// Snapshot replaces the mirror's contents with the table's current rows and
// sets applied_lsn to the LSN the caller should stream from.
func (t *Table) Snapshot(ctx context.Context, conn *pgx.Conn, atLSN string) (int, error) {
	if err := t.truncate(); err != nil {
		return 0, err
	}

	sel := ""
	for i, c := range t.Order {
		if i > 0 {
			sel += ", "
		}
		sel += `"` + c + `"::text`
	}
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT %s FROM %s", sel, t.Qualified))
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

	// Rows go from the scan straight into the writer in COLUMN ORDER. The old
	// route built a map[string]any per row so the writer could look the values
	// back out by name — one allocation and N inserts on the way in, N lookups
	// on the way out, per row, to move a slice to a slice. Bootstrap is per-row
	// CPU bound (docs/25) and that was the per-row work.
	//
	// `ptrs` is built once and reused: it holds the ADDRESSES of the value
	// slots, and the slots themselves are fresh per row because the writer
	// keeps them until the row group is flushed.
	index := map[string]int{}
	keyAt := 0
	for i, c := range t.Order {
		if c == t.Key {
			keyAt = i
		}
	}
	n := 0
	for rows.Next() {
		vals := make([]any, len(t.Order))
		ptrs := make([]any, len(t.Order))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			w.Close()
			return 0, err
		}
		index[t.keyOf(vals[keyAt]).String()] = n
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

	if n > 0 {
		t.State.BaseFiles = []string{name}
		if err := writeJSON(fmt.Sprintf("%s/index/%s.idx.json", t.Dir,
			trimParquet(name)), index); err != nil {
			return 0, err
		}
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
	return n, t.saveState()
}

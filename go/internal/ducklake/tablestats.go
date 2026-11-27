package ducklake

// Table-level column statistics: ducklake_table_column_stats.
//
// The catalog has two levels of statistics. ducklake_file_column_stats says
// what one file holds and is what files are pruned with; this package has
// always written it. ducklake_table_column_stats says what the TABLE holds,
// one row per column, and was left empty. DuckDB reads it as the column's
// statistics for the optimizer, and with it answers `SELECT min(c), max(c)`
// from the catalog: measured on a 10M-row table, 54-61 ms of CPU without the
// rows and 3 ms with them.
//
// WHAT THE ROWS MEAN. The DuckLake specification calls them statistics over
// the whole table and DuckLake itself only ever widens them: it does not
// narrow min_value when the smallest row is deleted (checked against DuckDB
// 2.0.1's own lake: after `DELETE ... WHERE id = (min)` the row still says the
// old minimum, with min_is_exact = 1). So a reader may treat them as BOUNDS
// that every live row is inside, and must not treat them as values once rows
// have been deleted; DuckDB does exactly that, and scans for min() as soon as
// the table has a delete file (TestTableColumnStats* hold it to that).
//
// What this publisher writes is tighter than that and never looser:
//
//   - the bounds are recomputed on every publish from the files that are live
//     in the new snapshot, so a file a rewrite retired no longer widens them;
//   - min_is_exact / max_is_exact are 1 only while no live file has a
//     deletion vector. With one, the smallest value a file holds may be a dead
//     row, the bound is still a bound, and the flag says so;
//   - a bound is written only when every live file that holds a value has one
//     (a float column, a file without statistics, or a column an older file
//     reads back as a non-NULL default: no bound, which is "unknown");
//   - contains_null is 1 unless every live file says it has none.
//
// A wrong row here is a wrong ANSWER, not a slow one: the optimizer removes a
// filter the statistics say cannot match. So the conformance test runs every
// predicate against the lake with these rows in place.

import (
	"context"
	"database/sql"
	"math/big"
	"strings"
)

// textLess is the order of a column type's min_value / max_value text, as
// this package writes it (stats.go), or nil when there is none it can vouch
// for.
func textLess(typ string) func(a, b string) bool {
	switch {
	case intRank[typ] > 0, strings.HasPrefix(typ, "uint"):
		return func(a, b string) bool {
			x, ok1 := new(big.Int).SetString(a, 10)
			y, ok2 := new(big.Int).SetString(b, 10)
			return ok1 && ok2 && x.Cmp(y) < 0
		}
	case strings.HasPrefix(typ, "decimal("):
		return func(a, b string) bool {
			x, ok1 := new(big.Rat).SetString(a)
			y, ok2 := new(big.Rat).SetString(b)
			return ok1 && ok2 && x.Cmp(y) < 0
		}
	case typ == "boolean", typ == "varchar", typ == "uuid":
		// false < true; text in byte order, which is DuckDB's; a uuid in
		// lower-case hex, which sorts as its bytes do.
		return func(a, b string) bool { return a < b }
	case typ == "date", typ == "timestamp", typ == "timestamptz":
		// Fixed-width, years 0001-9999 only (timeText), so the text sorts
		// as the instants do.
		return func(a, b string) bool { return a < b }
	}
	return nil
}

// setTableColumnStats restates ducklake_table_column_stats for one table from
// the statistics of the files live in the snapshot being written.
func setTableColumnStats(ctx context.Context, q queryer, work *lakeTable) error {
	type agg struct {
		files    int64 // live files with a statistics row for the column
		null     bool  // some file has, or may have, a NULL
		unknown  bool  // some file holds values without bounds
		min, max *string
	}
	var liveFiles, withRows, deleted int64
	if err := q.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(f.record_count > 0), 0),
			(SELECT count(*) FROM ducklake_delete_file d WHERE d.table_id = ?1 AND d.end_snapshot IS NULL)
		FROM ducklake_data_file f WHERE f.table_id = ?1 AND f.end_snapshot IS NULL`, work.id).
		Scan(&liveFiles, &withRows, &deleted); err != nil {
		return err
	}
	less := map[int64]func(a, b string) bool{}
	aggs := map[int64]*agg{}
	for _, c := range work.cols {
		less[c.id] = textLess(c.typ)
		aggs[c.id] = &agg{}
	}
	rows, err := q.QueryContext(ctx, `SELECT s.column_id, s.value_count, s.null_count, s.min_value, s.max_value
		FROM ducklake_file_column_stats s JOIN ducklake_data_file f ON f.data_file_id = s.data_file_id
		WHERE f.table_id = ? AND f.end_snapshot IS NULL AND f.record_count > 0`, work.id)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var values, nulls sql.NullInt64
		var lo, hi sql.NullString
		if err := rows.Scan(&id, &values, &nulls, &lo, &hi); err != nil {
			rows.Close()
			return err
		}
		a := aggs[id]
		if a == nil {
			continue // a column dropped since the file was written
		}
		a.files++
		if !nulls.Valid || nulls.Int64 > 0 {
			a.null = true
		}
		if nulls.Valid && values.Valid && values.Int64 == 0 {
			continue // nothing but NULLs: no value to bound
		}
		cmp := less[id]
		if cmp == nil || !lo.Valid || !hi.Valid {
			a.unknown = true
			continue
		}
		if a.min == nil || cmp(lo.String, *a.min) {
			v := lo.String
			a.min = &v
		}
		if a.max == nil || cmp(*a.max, hi.String) {
			v := hi.String
			a.max = &v
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM ducklake_table_column_stats WHERE table_id = ?`, work.id); err != nil {
		return err
	}
	for _, c := range work.cols {
		a := aggs[c.id]
		if a.files < withRows {
			// Some live file does not have the column: its rows read back as
			// the column's initial default. NULL is a NULL; anything else is
			// a value this code does not place among the bounds.
			if c.initial == nil {
				a.null = true
			} else {
				a.unknown = true
			}
		}
		var lo, hi, exact any
		if !a.unknown && a.min != nil && a.max != nil {
			lo, hi, exact = *a.min, *a.max, 0
			if deleted == 0 {
				exact = 1
			}
		}
		null := 0
		if a.null {
			null = 1
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_table_column_stats VALUES (?, ?, ?, NULL, ?, ?, NULL, ?, ?)`,
			work.id, c.id, null, lo, hi, exact, exact); err != nil {
			return err
		}
	}
	return nil
}

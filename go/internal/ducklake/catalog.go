package ducklake

// Everything this package knows about the DuckLake catalog's tables is in
// this file: the format version, the DDL, and every statement that reads or
// writes a catalog row. A format bump is a change here and nowhere else.
//
// The DDL is what DuckDB 2.0.1's ducklake extension (08089b16dc) creates in a
// SQLite catalog, copied from sqlite_master, declared types included: DuckDB
// reads the catalog through its sqlite extension, which goes by them.
//
// Two tables are not DuckLake's, and DuckLake does not look at them:
//
//	qs_mirror_table  one row per mirror table: the manifest the lake stands
//	                 at, its applied LSN (what a reader gates freshness on),
//	                 why it is refused if it is, and the next column id.
//	qs_mirror_file   which mirror file a data file is a link to, and which
//	                 vector generation its delete file was written from.
//
// The column-id counter is not bookkeeping. DuckLake has none; it takes the
// next id from the rows it has. This publisher removes the rows of expired
// columns, and a name mapping or a statistics row of a file that predates the
// drop still says "field 5". If a later column were given 5, that file's old
// values would be read as the new column.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite"
)

// FormatVersion is the DuckLake catalog format this package writes. It
// refuses a catalog that says anything else.
const FormatVersion = "1.1"

const author = "qs-mirror"

var catalogDDL = []string{
	`CREATE TABLE ducklake_metadata("key" VARCHAR NOT NULL, "value" VARCHAR NOT NULL, "scope" VARCHAR, scope_id BIGINT)`,
	`CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time VARCHAR, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT)`,
	`CREATE TABLE ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR)`,
	`CREATE TABLE ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BIGINT, parent_schema_id BIGINT)`,
	`CREATE TABLE ducklake_table(table_id BIGINT, table_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BIGINT)`,
	`CREATE TABLE ducklake_view(view_id BIGINT, view_uuid VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, "sql" VARCHAR, column_aliases VARCHAR)`,
	`CREATE TABLE ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "key" VARCHAR, "value" VARCHAR)`,
	`CREATE TABLE ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "key" VARCHAR, "value" VARCHAR)`,
	`CREATE TABLE ducklake_view_column_tag(view_id BIGINT, column_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, "key" VARCHAR, "value" VARCHAR)`,
	`CREATE TABLE ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BIGINT, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT, row_group_count BIGINT)`,
	`CREATE TABLE ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BIGINT, extra_stats VARCHAR, min_is_exact BIGINT, max_is_exact BIGINT)`,
	`CREATE TABLE ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BIGINT, extra_stats VARCHAR)`,
	`CREATE TABLE ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BIGINT, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT, row_group_count BIGINT)`,
	`CREATE TABLE ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BIGINT, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR)`,
	`CREATE TABLE ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT)`,
	`CREATE TABLE ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BIGINT, contains_nan BIGINT, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR, min_is_exact BIGINT, max_is_exact BIGINT)`,
	`CREATE TABLE ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)`,
	`CREATE TABLE ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, "transform" VARCHAR)`,
	`CREATE TABLE ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR)`,
	`CREATE TABLE ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BIGINT, schedule_start VARCHAR)`,
	`CREATE TABLE ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)`,
	`CREATE TABLE ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, "type" VARCHAR)`,
	`CREATE TABLE ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BIGINT)`,
	`CREATE TABLE ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT)`,
	`CREATE TABLE ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT)`,
	`CREATE TABLE ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, "sql" VARCHAR, "type" VARCHAR)`,
	`CREATE TABLE ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR)`,
	`CREATE TABLE ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT)`,
	`CREATE TABLE ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR)`,

	`CREATE TABLE qs_mirror_table(table_name VARCHAR PRIMARY KEY, table_id BIGINT, next_column_id BIGINT, manifest_seq BIGINT, applied_lsn VARCHAR, refusal VARCHAR, snapshot_id BIGINT, published_at VARCHAR)`,
	`CREATE TABLE qs_mirror_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, rel VARCHAR, dv_gen BIGINT)`,
	watermarkDDL, // watermark.go

	// The reader's lookups are all "rows of this table live at snapshot S".
	`CREATE INDEX qs_idx_data_file ON ducklake_data_file(table_id, end_snapshot)`,
	`CREATE INDEX qs_idx_delete_file ON ducklake_delete_file(table_id, end_snapshot)`,
	`CREATE INDEX qs_idx_file_stats ON ducklake_file_column_stats(data_file_id)`,
}

// catalog is the SQLite database. One connection, one writer.
//
// How it is opened is the concurrency contract with readers, which are other
// processes (DuckDB's sqlite extension, inside PostgreSQL backends or python):
//
//	journal_mode=WAL    a reader sees the last committed transaction and is
//	                    never blocked by, and never blocks, the writer. A
//	                    publish is ONE transaction, so a reader sees all of a
//	                    snapshot or none of it.
//	synchronous=FULL    a commit is on disk when it returns. The publisher
//	                    unlinks files on the strength of a committed expiry,
//	                    and that must not be un-committed by a power cut.
//	                    It is one fsync per publish, off the apply path.
//	busy_timeout        only another WRITER can make this one wait, and there
//	                    is none (see lock.go); it is there so that a stray
//	                    one produces a wait and an error, not a failure at
//	                    the first statement.
//	wal_autocheckpoint  passive, so a checkpoint never waits for a reader;
//	journal_size_limit  bounds the WAL file once readers let one complete.
type catalog struct {
	db *sql.DB
}

func openCatalog(ctx context.Context, path, dataPath string) (*catalog, error) {
	q := url.Values{}
	for _, p := range []string{"busy_timeout(10000)", "journal_mode(WAL)", "synchronous(FULL)",
		"wal_autocheckpoint(256)", "journal_size_limit(4194304)"} {
		q.Add("_pragma", p)
	}
	q.Set("_txlock", "immediate")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	c := &catalog{db: db}
	if err := c.init(ctx, dataPath); err != nil {
		db.Close()
		return nil, err
	}
	return c, nil
}

func (c *catalog) close() error { return c.db.Close() }

// init creates an empty lake, or checks that the one there is ours to write.
func (c *catalog) init(ctx context.Context, dataPath string) error {
	var n int
	if err := c.db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE name = 'ducklake_metadata'`).Scan(&n); err != nil {
		return fmt.Errorf("ducklake catalog: %w", err)
	}
	if n == 0 {
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		for _, s := range catalogDDL {
			if _, err := tx.ExecContext(ctx, s); err != nil {
				return fmt.Errorf("ducklake catalog: %s: %w", s, err)
			}
		}
		for _, kv := range [][2]string{{"version", FormatVersion}, {"created_by", author},
			{"data_path", dataPath}, {"encrypted", "false"}} {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO ducklake_metadata("key", "value") VALUES (?, ?)`, kv[0], kv[1]); err != nil {
				return err
			}
		}
		stmts := []struct {
			q    string
			args []any
		}{
			{`INSERT INTO ducklake_snapshot VALUES (0, ?, 0, 1, 0)`, []any{"1970-01-01 00:00:00.000000+00"}},
			{`INSERT INTO ducklake_snapshot_changes VALUES (0, 'created_schema:"main"', ?, NULL, NULL)`, []any{author}},
			{`INSERT INTO ducklake_schema VALUES (0, ?, 0, NULL, 'main', 'main/', 1, NULL)`, []any{newUUID()}},
		}
		for _, s := range stmts {
			if _, err := tx.ExecContext(ctx, s.q, s.args...); err != nil {
				return err
			}
		}
		return tx.Commit()
	}
	var version string
	if err := c.db.QueryRowContext(ctx,
		`SELECT "value" FROM ducklake_metadata WHERE "key" = 'version'`).Scan(&version); err != nil {
		return fmt.Errorf("ducklake catalog has no format version: %w", err)
	}
	if version != FormatVersion {
		return fmt.Errorf("ducklake catalog is format %s and this publisher writes only %s: "+
			"it will not write into it (a newer DuckDB has migrated the catalog, or it was made by "+
			"another tool; remove the lake directory to have it rebuilt from the mirror)",
			version, FormatVersion)
	}
	if err := c.db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE name = 'qs_mirror_table'`).Scan(&n); err != nil || n == 0 {
		return errors.New("ducklake catalog was not created by qs-mirror and will not be written to")
	}
	// A catalog made before the watermark was carried here gains its table.
	if _, err := c.db.ExecContext(ctx, watermarkDDL); err != nil {
		return fmt.Errorf("ducklake catalog: %w", err)
	}
	// The mirror may have been moved; file paths in the catalog are relative.
	_, err := c.db.ExecContext(ctx,
		`UPDATE ducklake_metadata SET "value" = ? WHERE "key" = 'data_path' AND "value" <> ?`, dataPath, dataPath)
	return err
}

// ---- reading ----------------------------------------------------------------

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// head is the counters of the newest snapshot, which every new one starts from.
type head struct {
	snapshot, schemaVersion, nextCatalogID, nextFileID int64
}

// readHead is the first statement of every publish, inside its transaction,
// and it checks the format version again there: a DuckDB that attached the
// catalog read-write since this process opened it may have migrated it, and
// rows of this format written into a catalog of another are not a lake.
func readHead(ctx context.Context, q queryer) (h head, err error) {
	var version string
	err = q.QueryRowContext(ctx, `SELECT snapshot_id, schema_version, next_catalog_id, next_file_id,
			(SELECT "value" FROM ducklake_metadata WHERE "key" = 'version')
		FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1`).
		Scan(&h.snapshot, &h.schemaVersion, &h.nextCatalogID, &h.nextFileID, &version)
	if err == nil && version != FormatVersion {
		err = fmt.Errorf("ducklake catalog is now format %s and this publisher writes only %s: it will not write into it",
			version, FormatVersion)
	}
	return h, err
}

// loadTable reads what the catalog says about a mirror table now. This is the
// publisher's whole memory of it: nothing about a table is kept anywhere the
// catalog transaction does not cover.
func loadTable(ctx context.Context, q queryer, schema, name string) (*lakeTable, error) {
	lt := &lakeTable{schema: schema, name: name, schemaID: -1, files: map[string]*lakeFile{},
		mappings: map[string]int64{}, nextColumnID: 1}
	err := q.QueryRowContext(ctx, `SELECT schema_id FROM ducklake_schema
		WHERE schema_name = ? AND end_snapshot IS NULL`, schema).Scan(&lt.schemaID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var seq sql.NullInt64
	var lsn, refusal sql.NullString
	err = q.QueryRowContext(ctx, `SELECT next_column_id, manifest_seq, applied_lsn, refusal
		FROM qs_mirror_table WHERE table_name = ?`, schema+"."+name).
		Scan(&lt.nextColumnID, &seq, &lsn, &refusal)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	lt.known = err == nil
	lt.seq, lt.lsn, lt.refusal = seq.Int64, lsn.String, refusal.String
	if lt.schemaID < 0 {
		return lt, nil
	}
	err = q.QueryRowContext(ctx, `SELECT view_id, "sql" FROM ducklake_view
		WHERE schema_id = ? AND view_name = ? AND end_snapshot IS NULL`, lt.schemaID, name).
		Scan(&lt.viewID, &lt.viewSQL)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	lt.hasView = err == nil
	err = q.QueryRowContext(ctx, `SELECT table_id FROM ducklake_table
		WHERE schema_id = ? AND table_name = ? AND end_snapshot IS NULL`, lt.schemaID, name).Scan(&lt.id)
	if errors.Is(err, sql.ErrNoRows) {
		return lt, nil
	}
	if err != nil {
		return nil, err
	}
	lt.exists = true
	if err := q.QueryRowContext(ctx, `SELECT next_row_id FROM ducklake_table_stats WHERE table_id = ?`,
		lt.id).Scan(&lt.nextRowID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	rows, err := q.QueryContext(ctx, `SELECT column_id, column_order, column_name, column_type, initial_default
		FROM ducklake_column WHERE table_id = ? AND end_snapshot IS NULL ORDER BY column_order`, lt.id)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var c lakeColumn
		var dflt sql.NullString
		if err := rows.Scan(&c.id, &c.order, &c.name, &c.typ, &dflt); err != nil {
			rows.Close()
			return nil, err
		}
		if dflt.Valid {
			c.initial = &dflt.String
		}
		lt.cols = append(lt.cols, c)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	rows, err = q.QueryContext(ctx, `SELECT f.data_file_id, f.path, f.record_count, f.file_size_bytes,
			m.rel, m.dv_gen, d.delete_file_id
		FROM ducklake_data_file f
		JOIN qs_mirror_file m ON m.data_file_id = f.data_file_id
		LEFT JOIN ducklake_delete_file d ON d.data_file_id = f.data_file_id AND d.end_snapshot IS NULL
		WHERE f.table_id = ? AND f.end_snapshot IS NULL`, lt.id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		f := &lakeFile{}
		var del sql.NullInt64
		if err := rows.Scan(&f.id, &f.name, &f.rows, &f.size, &f.rel, &f.dvGen, &del); err != nil {
			return nil, err
		}
		f.hasDelete = del.Valid
		lt.files[f.rel] = f
	}
	return lt, rows.Err()
}

// referencedFiles lists every file under the data path the catalog still
// answers for, relative to it: live or ended, and those whose unlink is owed.
func referencedFiles(ctx context.Context, q queryer) (map[string]bool, error) {
	out := map[string]bool{}
	for _, s := range []string{
		`SELECT s.path || t.path || f.path FROM ducklake_data_file f
			JOIN ducklake_table t ON t.table_id = f.table_id JOIN ducklake_schema s ON s.schema_id = t.schema_id`,
		`SELECT s.path || t.path || f.path FROM ducklake_delete_file f
			JOIN ducklake_table t ON t.table_id = f.table_id JOIN ducklake_schema s ON s.schema_id = t.schema_id`,
		`SELECT path FROM ducklake_files_scheduled_for_deletion`,
	} {
		rows, err := q.QueryContext(ctx, s)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				rows.Close()
				return nil, err
			}
			out[p] = true
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scheduledFiles(ctx context.Context, q queryer) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT path FROM ducklake_files_scheduled_for_deletion`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func clearScheduled(ctx context.Context, q queryer) error {
	_, err := q.ExecContext(ctx, `DELETE FROM ducklake_files_scheduled_for_deletion`)
	return err
}

func liveMappings(ctx context.Context, q queryer) (map[int64]bool, error) {
	rows, err := q.QueryContext(ctx, `SELECT mapping_id FROM ducklake_column_mapping`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// counts is what the catalog holds, for the growth metrics and tests.
type Counts struct {
	Snapshots, DataFiles, DeleteFiles, ColumnStats, Columns, Mappings int64
}

func readCounts(ctx context.Context, q queryer) (c Counts, err error) {
	err = q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM ducklake_snapshot),
		(SELECT count(*) FROM ducklake_data_file), (SELECT count(*) FROM ducklake_delete_file),
		(SELECT count(*) FROM ducklake_file_column_stats), (SELECT count(*) FROM ducklake_column),
		(SELECT count(*) FROM ducklake_column_mapping)`).
		Scan(&c.Snapshots, &c.DataFiles, &c.DeleteFiles, &c.ColumnStats, &c.Columns, &c.Mappings)
	return c, err
}

// ---- writing: one snapshot --------------------------------------------------

func insertSchema(ctx context.Context, q queryer, id, snap int64, name string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO ducklake_schema VALUES (?, ?, ?, NULL, ?, ?, 1, NULL)`,
		id, newUUID(), snap, name, name+"/")
	return err
}

func insertTable(ctx context.Context, q queryer, id, snap, schemaID int64, name string) error {
	if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_table VALUES (?, ?, ?, NULL, ?, ?, ?, 1)`,
		id, newUUID(), snap, schemaID, name, name+"/"); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `INSERT INTO ducklake_table_stats VALUES (?, 0, 0, 0)`, id)
	return err
}

// endTable drops a table as of snap: the table, its columns and every file.
func endTable(ctx context.Context, q queryer, id, snap int64) error {
	for _, s := range []string{
		`UPDATE ducklake_table SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL`,
		`UPDATE ducklake_column SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL`,
		`UPDATE ducklake_data_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL`,
		`UPDATE ducklake_delete_file SET end_snapshot = ? WHERE table_id = ? AND end_snapshot IS NULL`,
	} {
		if _, err := q.ExecContext(ctx, s, snap, id); err != nil {
			return err
		}
	}
	return nil
}

func insertView(ctx context.Context, q queryer, id, snap, schemaID int64, name, sqlText string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO ducklake_view VALUES (?, ?, ?, NULL, ?, ?, 'duckdb', ?, '')`,
		id, newUUID(), snap, schemaID, name, sqlText)
	return err
}

func endView(ctx context.Context, q queryer, id, snap int64) error {
	_, err := q.ExecContext(ctx,
		`UPDATE ducklake_view SET end_snapshot = ? WHERE view_id = ? AND end_snapshot IS NULL`, snap, id)
	return err
}

func insertColumn(ctx context.Context, q queryer, tableID, snap int64, c lakeColumn) error {
	var initial, dflt any = nil, "NULL"
	if c.initial != nil {
		initial, dflt = *c.initial, *c.initial
	}
	_, err := q.ExecContext(ctx, `INSERT INTO ducklake_column VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, 1, NULL, 'literal', 'duckdb')`,
		c.id, snap, tableID, c.order, c.name, c.typ, initial, dflt)
	return err
}

func endColumn(ctx context.Context, q queryer, tableID, columnID, snap int64) error {
	_, err := q.ExecContext(ctx, `UPDATE ducklake_column SET end_snapshot = ?
		WHERE table_id = ? AND column_id = ? AND end_snapshot IS NULL`, snap, tableID, columnID)
	return err
}

func insertSchemaVersion(ctx context.Context, q queryer, snap, version, tableID int64) error {
	_, err := q.ExecContext(ctx, `INSERT INTO ducklake_schema_versions VALUES (?, ?, ?)`, snap, version, tableID)
	return err
}

// insertMapping records how a file without field ids maps to columns: by name.
func insertMapping(ctx context.Context, q queryer, id, tableID int64, names []string, fields []int64) error {
	if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_column_mapping VALUES (?, ?, 'map_by_name')`,
		id, tableID); err != nil {
		return err
	}
	for i := range names {
		if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_name_mapping VALUES (?, ?, ?, ?, NULL, 0)`,
			id, i, names[i], fields[i]); err != nil {
			return err
		}
	}
	return nil
}

type columnStat struct {
	columnID                 int64
	sizeBytes, values, nulls int64
	nullsKnown               bool
	min, max                 *string
}

func insertDataFile(ctx context.Context, q queryer, tableID, snap int64, f *lakeFile,
	info *fileInfo, rowIDStart, mappingID int64, stats []columnStat) error {
	if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_data_file VALUES
		(?, ?, ?, NULL, NULL, ?, 1, 'parquet', ?, ?, ?, ?, NULL, NULL, ?, NULL, ?)`,
		f.id, tableID, snap, f.name, info.rows, info.size, info.footer, rowIDStart, mappingID, info.rowGroups); err != nil {
		return err
	}
	if _, err := q.ExecContext(ctx, `INSERT INTO qs_mirror_file VALUES (?, ?, ?, 0)`, f.id, tableID, f.rel); err != nil {
		return err
	}
	for _, s := range stats {
		var nulls, min, max, exact any
		if s.nullsKnown {
			nulls = s.nulls
		}
		if s.min != nil && s.max != nil {
			min, max, exact = *s.min, *s.max, 1
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_file_column_stats VALUES
			(?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)`,
			f.id, tableID, s.columnID, s.sizeBytes, s.values, nulls, min, max, exact, exact); err != nil {
			return err
		}
	}
	return nil
}

func endDataFile(ctx context.Context, q queryer, id, snap int64) error {
	if _, err := q.ExecContext(ctx, `UPDATE ducklake_data_file SET end_snapshot = ? WHERE data_file_id = ?`,
		snap, id); err != nil {
		return err
	}
	return endDeleteFile(ctx, q, id, snap)
}

func endDeleteFile(ctx context.Context, q queryer, dataFileID, snap int64) error {
	_, err := q.ExecContext(ctx, `UPDATE ducklake_delete_file SET end_snapshot = ?
		WHERE data_file_id = ? AND end_snapshot IS NULL`, snap, dataFileID)
	return err
}

func insertDeleteFile(ctx context.Context, q queryer, id, tableID, snap, dataFileID int64,
	name string, d *deleteInfo, gen int) error {
	if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_delete_file VALUES
		(?, ?, ?, NULL, ?, ?, 1, 'parquet', ?, ?, ?, NULL, NULL, ?)`,
		id, tableID, snap, dataFileID, name, d.count, d.size, d.footer, d.rowGroups); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `UPDATE qs_mirror_file SET dv_gen = ? WHERE data_file_id = ?`, gen, dataFileID)
	return err
}

// setTableStats restates the table's totals from its live files.
func setTableStats(ctx context.Context, q queryer, tableID, nextRowID int64) error {
	_, err := q.ExecContext(ctx, `UPDATE ducklake_table_stats SET next_row_id = ?2,
		record_count = (SELECT coalesce(sum(record_count), 0) FROM ducklake_data_file
			WHERE table_id = ?1 AND end_snapshot IS NULL),
		file_size_bytes = (SELECT coalesce(sum(file_size_bytes), 0) FROM ducklake_data_file
			WHERE table_id = ?1 AND end_snapshot IS NULL)
		WHERE table_id = ?1`, tableID, nextRowID)
	return err
}

func insertSnapshot(ctx context.Context, q queryer, h head, at string, changes []string, message, extra string) error {
	if _, err := q.ExecContext(ctx, `INSERT INTO ducklake_snapshot VALUES (?, ?, ?, ?, ?)`,
		h.snapshot, at, h.schemaVersion, h.nextCatalogID, h.nextFileID); err != nil {
		return err
	}
	_, err := q.ExecContext(ctx, `INSERT INTO ducklake_snapshot_changes VALUES (?, ?, ?, ?, ?)`,
		h.snapshot, strings.Join(changes, ","), author, message, extra)
	return err
}

// saveMirrorTable records where the lake stands for a table. snapshot is the
// DuckLake snapshot that state was published as, or the one already current
// when nothing but the LSN moved.
func saveMirrorTable(ctx context.Context, q queryer, lt *lakeTable, snapshot int64, at string) error {
	_, err := q.ExecContext(ctx, `INSERT INTO qs_mirror_table VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(table_name) DO UPDATE SET table_id = excluded.table_id,
			next_column_id = excluded.next_column_id, manifest_seq = excluded.manifest_seq,
			applied_lsn = excluded.applied_lsn, refusal = excluded.refusal,
			snapshot_id = excluded.snapshot_id, published_at = excluded.published_at`,
		lt.schema+"."+lt.name, lt.id, lt.nextColumnID, lt.seq, lt.lsn, lt.refusal, snapshot, at)
	return err
}

// ---- expiry -----------------------------------------------------------------

// expirySnapshot is the oldest snapshot to KEEP at cutoff: the newest one
// that already existed then. Everything before it had been superseded by
// cutoff; it had not, so a reader may have started on it since. Zero rows to
// expire is reported as ok == false.
func expirySnapshot(ctx context.Context, q queryer, cutoff string) (keep int64, ok bool, err error) {
	var k, oldest sql.NullInt64
	err = q.QueryRowContext(ctx, `SELECT (SELECT max(snapshot_id) FROM ducklake_snapshot WHERE snapshot_time <= ?),
		(SELECT min(snapshot_id) FROM ducklake_snapshot)`, cutoff).Scan(&k, &oldest)
	if err != nil || !k.Valid || k.Int64 <= oldest.Int64 {
		return 0, false, err
	}
	return k.Int64, true, nil
}

// expireBefore removes every catalog row no snapshot from keep onwards can
// see, and records the files those rows named as owed an unlink. It returns
// how many snapshots went.
func expireBefore(ctx context.Context, q queryer, keep int64, now string) (int64, error) {
	const dead = `end_snapshot IS NOT NULL AND end_snapshot <= ?`
	for _, s := range []string{
		`INSERT INTO ducklake_files_scheduled_for_deletion
			SELECT f.data_file_id, s.path || t.path || f.path, 1, ?2 FROM ducklake_data_file f
			JOIN ducklake_table t ON t.table_id = f.table_id JOIN ducklake_schema s ON s.schema_id = t.schema_id
			WHERE f.end_snapshot IS NOT NULL AND f.end_snapshot <= ?1`,
		`INSERT INTO ducklake_files_scheduled_for_deletion
			SELECT f.delete_file_id, s.path || t.path || f.path, 1, ?2 FROM ducklake_delete_file f
			JOIN ducklake_table t ON t.table_id = f.table_id JOIN ducklake_schema s ON s.schema_id = t.schema_id
			WHERE f.end_snapshot IS NOT NULL AND f.end_snapshot <= ?1`,
	} {
		if _, err := q.ExecContext(ctx, s, keep, now); err != nil {
			return 0, err
		}
	}
	for _, s := range []string{
		`DELETE FROM ducklake_file_column_stats WHERE data_file_id IN
			(SELECT data_file_id FROM ducklake_data_file WHERE ` + dead + `)`,
		`DELETE FROM qs_mirror_file WHERE data_file_id IN
			(SELECT data_file_id FROM ducklake_data_file WHERE ` + dead + `)`,
		`DELETE FROM ducklake_data_file WHERE ` + dead,
		`DELETE FROM ducklake_delete_file WHERE ` + dead,
		`DELETE FROM ducklake_column WHERE ` + dead,
		`DELETE FROM ducklake_view WHERE ` + dead,
		`DELETE FROM ducklake_table WHERE ` + dead,
		// A table's schema version at snapshot S is the newest row at or
		// before S, so the newest row at or before keep stays.
		`DELETE FROM ducklake_schema_versions WHERE begin_snapshot < (SELECT max(v.begin_snapshot)
			FROM ducklake_schema_versions v
			WHERE v.table_id = ducklake_schema_versions.table_id AND v.begin_snapshot <= ?)`,
	} {
		if _, err := q.ExecContext(ctx, s, keep); err != nil {
			return 0, err
		}
	}
	for _, s := range []string{
		`DELETE FROM ducklake_schema_versions WHERE table_id NOT IN (SELECT table_id FROM ducklake_table)`,
		`DELETE FROM ducklake_table_stats WHERE table_id NOT IN (SELECT table_id FROM ducklake_table)`,
		`DELETE FROM ducklake_table_column_stats WHERE table_id NOT IN (SELECT table_id FROM ducklake_table)`,
		`DELETE FROM ducklake_name_mapping WHERE mapping_id NOT IN
			(SELECT mapping_id FROM ducklake_data_file WHERE mapping_id IS NOT NULL)`,
		`DELETE FROM ducklake_column_mapping WHERE mapping_id NOT IN
			(SELECT mapping_id FROM ducklake_data_file WHERE mapping_id IS NOT NULL)`,
	} {
		if _, err := q.ExecContext(ctx, s); err != nil {
			return 0, err
		}
	}
	if _, err := q.ExecContext(ctx, `DELETE FROM ducklake_snapshot_changes WHERE snapshot_id < ?`, keep); err != nil {
		return 0, err
	}
	res, err := q.ExecContext(ctx, `DELETE FROM ducklake_snapshot WHERE snapshot_id < ?`, keep)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// TableState is where the lake stands for one mirror table.
type TableState struct {
	// AppliedLSN is how far the lake table has applied: the LSN of the
	// manifest it was published from, or the mirror watermark's when that
	// speaks for a table standing at exactly that manifest (watermark.go).
	AppliedLSN string
	// ManifestLSN is the applied_lsn of the manifest it was published from:
	// the table's last change.
	ManifestLSN string
	ManifestSeq int64
	Snapshot    int64
	Refusal     string
	PublishedAt string
}

// ReadTable reads a table's state from a catalog without writing to it: what
// qs-query prints, and what a freshness check that is not DuckDB can use.
func ReadTable(ctx context.Context, catalogPath, schema, table string) (TableState, error) {
	var st TableState
	db, err := sql.Open("sqlite", "file:"+catalogPath+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return st, err
	}
	defer db.Close()
	var lsn, refusal, at sql.NullString
	err = db.QueryRowContext(ctx, `SELECT applied_lsn, manifest_seq, snapshot_id, refusal, published_at
		FROM qs_mirror_table WHERE table_name = ?`, schema+"."+table).
		Scan(&lsn, &st.ManifestSeq, &st.Snapshot, &refusal, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return st, fmt.Errorf("%s.%s has not been published to this lake", schema, table)
	}
	st.AppliedLSN, st.Refusal, st.PublishedAt = lsn.String, refusal.String, at.String
	st.ManifestLSN = st.AppliedLSN
	if err == nil {
		st.AppliedLSN = effectiveLSN(ctx, db, schema+"."+table, st.ManifestLSN)
	}
	return st, err
}

// ---- what a reader runs -----------------------------------------------------

// AttachSQL is the DuckDB statement that attaches the lake read-only.
func AttachSQL(catalogPath, alias string) string {
	return fmt.Sprintf("ATTACH 'ducklake:sqlite:%s' AS %s (READ_ONLY);",
		strings.ReplaceAll(catalogPath, "'", "''"), quoteIdent(alias))
}

// FreshnessSQL is the query a reader gates on: the LSN the lake table has
// applied through, and why it is refused if it is. It reads the catalog
// database DuckLake attaches beside the lake.
//
// applied_lsn is the EFFECTIVE one: the mirror's watermark where it speaks
// for the manifest this table was published from, so that a table nothing
// has written to is not reported as stale (watermark.go). manifest_lsn, the
// last column, is the LSN of that manifest: the table's last change.
func FreshnessSQL(alias, schema, table string) string {
	meta := quoteIdent("__ducklake_metadata_" + alias)
	return fmt.Sprintf("SELECT coalesce(w.applied_lsn, t.applied_lsn) AS applied_lsn, t.manifest_seq, t.snapshot_id, t.refusal, "+
		"t.applied_lsn AS manifest_lsn FROM %s.qs_mirror_table t LEFT JOIN %s.qs_mirror_watermark w "+
		"ON w.table_name = t.table_name AND w.manifest_lsn = t.applied_lsn WHERE t.table_name = '%s';",
		meta, meta, strings.ReplaceAll(schema+"."+table, "'", "''"))
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// refusedSQL is the body of the view that stands in for a table that must not
// be served. It fails by naming a relation that does not exist, and the name
// is the message; see haltedSQL in internal/mirror/publish.go for why an
// expression that raises is not used.
func refusedSQL(qualified, reason string) string {
	return "SELECT * FROM " + quoteIdent("quicksilver mirror "+qualified+
		" is halted and must not be served: "+reason)
}

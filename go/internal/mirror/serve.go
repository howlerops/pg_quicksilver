package mirror

// Publishing the mirror as something a query engine can read.
//
// Every number in docs/19 is the write path. The premise of the project is the
// other side: that a columnar mirror answers analytical queries a read replica
// cannot. Getting there needs one thing the mirror did not have — a way for an
// engine that is not this process to see the LOGICAL table rather than the
// files.
//
// A plain glob over the directory is wrong in three ways, each silently:
//
//   - it reads rows the deletion vectors have retired, so deleted and
//     superseded rows come back;
//   - it reads column-partial deltas as though they were whole rows, so every
//     column a patch does not carry reads as NULL;
//   - it reads files the manifest no longer names, and misses ones it does.
//
// ViewSQL answers all three in the only place that knows: the manifest. It
// emits a SELECT — over `read_parquet` and `read_json`, nothing engine-specific
// beyond that — which any engine that can read Parquet and JSON can evaluate,
// and which produces exactly what ForEachLive produces in this process.
//
// The SQL is a SNAPSHOT of a manifest, exactly like the in-process read path.
// Publishing a view that "never goes stale" would mean either pinning files
// forever or answering from a mixture of two manifests, and the second is the
// bug round five was about. What a reader gets instead is in publish.go: the
// writer publishes this SQL after every manifest change, with a sequence number
// to notice the change by, and the files a snapshot names outlive it by a grace
// period so the query already running on it finishes. Past that the engine
// says a file is missing, and the reader picks up the current snapshot.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// sqlType is the engine type a column reads back as: what every branch of the
// view is cast to, whatever the file under it holds, and what a file
// predating an ADD COLUMN is given a typed literal of.
//
// It is the right-hand column of the table in types.go. A numeric stored as
// text reads as DOUBLE — see there for why — and everything else reads as the
// type it is stored as.
func sqlType(pgType string) string {
	if k := temporalOf(pgType); k != notTemporal {
		return temporalSQLType(k)
	}
	dt := arrowType(pgType)
	switch d := dt.(type) {
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d,%d)", d.Precision, d.Scale)
	case *arrow.StringType:
		if _, _, _, isNumeric := numericSpec(pgType); isNumeric &&
			!strings.HasSuffix(strings.TrimSpace(pgType), "[]") {
			return "DOUBLE"
		}
		return "VARCHAR"
	}
	switch dt.ID() {
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INTEGER"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.BINARY:
		return "BLOB"
	case arrow.EXTENSION:
		return "UUID"
	}
	return "VARCHAR"
}

// parquetSQLType is the engine type a Parquet column reads as, from its
// footer: the same vocabulary as sqlType, so that the two can be compared.
func parquetSQLType(c *schema.Column) string {
	switch lt := c.LogicalType().(type) {
	case schema.DecimalLogicalType:
		return fmt.Sprintf("DECIMAL(%d,%d)", lt.Precision(), lt.Scale())
	case schema.DateLogicalType:
		return "DATE"
	case schema.TimeLogicalType:
		return "TIME"
	case schema.TimestampLogicalType:
		if lt.IsAdjustedToUTC() {
			return "TIMESTAMP WITH TIME ZONE"
		}
		return "TIMESTAMP"
	case schema.UUIDLogicalType:
		return "UUID"
	case schema.StringLogicalType:
		return "VARCHAR"
	case schema.IntLogicalType:
		if lt.BitWidth() == 16 && lt.IsSigned() {
			return "SMALLINT"
		}
	}
	switch c.PhysicalType() {
	case parquet.Types.Boolean:
		return "BOOLEAN"
	case parquet.Types.Int32:
		return "INTEGER"
	case parquet.Types.Int64:
		return "BIGINT"
	case parquet.Types.Float:
		return "FLOAT"
	case parquet.Types.Double:
		return "DOUBLE"
	case parquet.Types.ByteArray, parquet.Types.FixedLenByteArray:
		return "BLOB"
	}
	return "VARCHAR"
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func sqlIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// fileColumns reads a Parquet file's columns from its footer.
//
// The map says which columns the file HAS, keyed by name, as it always did.
// It also says how the file holds them, under keys no column can have (a
// PostgreSQL identifier cannot contain a NUL): fileTypeKey(name, T) is present
// when the column reads as engine type T, and legacyInfKey(name) when a
// temporal column holds the -infinity sentinel the writer used before
// temporal.go changed it. They travel in the same map because the map is what
// the footer cache holds, and a second cache would be a second thing to
// invalidate when a path is reused.
func fileColumns(path string) (map[string]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()
	out := map[string]bool{}
	md := rdr.MetaData()
	for i := 0; i < md.Schema.NumColumns(); i++ {
		c := md.Schema.Column(i)
		out[c.Name()] = true
		typ := parquetSQLType(c)
		out[fileTypeKey(c.Name(), typ)] = true
		if hasLegacyNegInfinity(md, i, typ) {
			out[legacyInfKey(c.Name())] = true
		}
	}
	return out, nil
}

func fileTypeKey(col, sqlType string) string { return "\x00type\x00" + col + "\x00" + sqlType }

func legacyInfKey(col string) string { return "\x00neginf\x00" + col }

// hasLegacyNegInfinity reports whether a temporal column of this file holds
// the old -infinity sentinel, the type's minimum, in any row group. The
// minimum of the domain is the minimum of any chunk that holds it, so the
// footer's statistics answer this without reading a page; a chunk with no
// statistics is assumed to.
func hasLegacyNegInfinity(md *metadata.FileMetaData, col int, typ string) bool {
	if typ != "DATE" && typ != "TIMESTAMP" && typ != "TIMESTAMP WITH TIME ZONE" {
		return false
	}
	for g := 0; g < md.NumRowGroups(); g++ {
		cc, err := md.RowGroup(g).ColumnChunk(col)
		if err != nil {
			return true
		}
		st, err := cc.Statistics()
		if err != nil || st == nil || !st.HasMinMax() {
			if cc.NumValues() > 0 && (st == nil || st.NullCount() < cc.NumValues()) {
				return true
			}
			continue
		}
		switch x := st.(type) {
		case *metadata.Int64Statistics:
			if x.Min() == legacyNegInfinity {
				return true
			}
		case *metadata.Int32Statistics:
			if int64(x.Min()) == legacyNegInfinityDate {
				return true
			}
		}
	}
	return false
}

// columnExpr is how one branch of the view selects a column the file has.
//
// A table's files do not all hold a column the same way: a mirror written
// before types.go has every integer as BIGINT, a uuid as text, an
// unconstrained numeric as DECIMAL(38,9), and none of those files is
// rewritten on upgrade. A UNION ALL over a VARCHAR branch and a UUID one
// either fails to plan or quietly makes every branch VARCHAR, so each branch
// is brought to the DECLARED type here. When the file already holds it the
// column is named bare — no cast for the engine to see through before it
// pushes a filter into the scan.
func (t *Table) columnExpr(c string, have map[string]bool) string {
	id := sqlIdent(c)
	pg := t.typeOf(c)
	want := sqlType(pg)
	var expr string
	switch {
	case have[fileTypeKey(c, want)]:
		expr = id
	case want == "BLOB" && have[fileTypeKey(c, "VARCHAR")]:
		// PostgreSQL's hex form, `\x` and two digits a byte. A plain cast
		// would read the backslash as the engine's own escape syntax.
		expr = fmt.Sprintf("from_hex(substr(%s, 3))", id)
	default:
		expr = fmt.Sprintf("CAST(%s AS %s)", id, want)
	}
	if have[legacyInfKey(c)] {
		// The file predates the change of -infinity's sentinel and holds the
		// type's minimum, which the engine reads as a finite date in 5877642
		// BC (290309 BC for a timestamp). Only a file whose statistics say it
		// holds one pays for this.
		lit := "TIMESTAMP '290309-12-22 (BC) 00:00:00'"
		if want == "DATE" {
			lit = "DATE '5877642-06-25 (BC)'"
		}
		expr = fmt.Sprintf("CASE WHEN %s < %s THEN CAST('-infinity' AS %s) ELSE %s END",
			expr, lit, want, expr)
	}
	if expr == id {
		return id
	}
	return expr + " AS " + id
}

// literalExpr is a typed literal for a column a file does not have.
func (t *Table) literalExpr(c string) string {
	want := sqlType(t.typeOf(c))
	mv, ok := t.State.MissingVals[c]
	switch {
	case !ok:
		return fmt.Sprintf("CAST(NULL AS %s) AS %s", want, sqlIdent(c))
	case want == "BLOB" && strings.HasPrefix(mv, `\x`):
		return fmt.Sprintf("from_hex(%s) AS %s", sqlString(mv[2:]), sqlIdent(c))
	}
	return fmt.Sprintf("CAST(%s AS %s) AS %s", sqlString(mv), want, sqlIdent(c))
}

// branch is one file's contribution: its columns projected to `want`, with the
// deletion vector the manifest names applied.
func (t *Table) branch(rel string, want []string) (string, error) {
	abs := filepath.Join(t.Dir, rel)
	have, err := t.columnsOf(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrStaleManifest
		}
		return "", err
	}
	sel := make([]string, 0, len(want))
	for _, c := range want {
		if have[c] {
			sel = append(sel, t.columnExpr(c, have))
			continue
		}
		// This file predates an ADD COLUMN. NULL is right only when the column
		// was added without a default; otherwise PostgreSQL reads these rows
		// back as attmissingval and so must the view.
		sel = append(sel, t.literalExpr(c))
	}

	// file_row_number is asked for ONLY where a deletion vector needs it.
	//
	// Requesting it everywhere is what the first version did, and it is not
	// free: it is a generated column, so an engine that could have answered
	// `count(*)` from the Parquet footer has to open the file instead. On the
	// cold-cache run that cost the wide shape 70.8 MB of reads for a query
	// whose answer is a number in the metadata. Most files in an insert-only
	// mirror have no vector at all.
	gen := t.State.DVGen[dvStem(rel)]
	opts := ""
	if gen != 0 {
		opts = ", file_row_number = true"
	}
	q := fmt.Sprintf("SELECT %s FROM read_parquet(%s%s)",
		strings.Join(sel, ", "), sqlString(abs), opts)

	if gen != 0 {
		dv := t.dvPathGenAny(rel, gen)
		if dv == "" {
			// The manifest names a vector that is gone, so this manifest is
			// older than the mirror. Answering without it would resurrect every
			// row it retired.
			return "", ErrStaleManifest
		}
		q += " WHERE file_row_number NOT IN " + dvSubquery(dv)
	}
	return q, nil
}

// ViewSQL returns a SELECT producing this table's live contents.
//
// It reflects the manifest as loaded, so a caller outside the apply goroutine
// should Reload before asking, and should re-ask on ErrStaleManifest.
func (t *Table) ViewSQL() (string, error) {
	if h := t.Halted(); h != "" {
		return "", fmt.Errorf("mirror is halted and must not be served: %s", h)
	}

	// Whole rows: the base files plus every delta that is not a patch file.
	var whole []string
	for _, b := range t.State.BaseFiles {
		q, err := t.branch(filepath.Join("base", b), t.Order)
		if err != nil {
			return "", err
		}
		whole = append(whole, padBranch(q))
	}
	// Patch files, grouped by the columns they carry. A key has at most one
	// live patch (partial.go), so the groups are disjoint and each becomes one
	// LEFT JOIN rather than a window function over a union.
	patchCols := map[string][]string{}
	patchBranches := map[string][]string{}
	var sigs []string
	for _, d := range t.State.DeltaFiles {
		rel := filepath.Join("delta", d)
		cols := t.State.PartialCols[d]
		if cols == nil {
			q, err := t.branch(rel, t.Order)
			if err != nil {
				return "", err
			}
			whole = append(whole, padBranch(q))
			continue
		}
		sig := strings.Join(cols, "\x00")
		if _, seen := patchCols[sig]; !seen {
			sigs = append(sigs, sig)
			patchCols[sig] = cols
		}
		q, err := t.branch(rel, cols)
		if err != nil {
			return "", err
		}
		patchBranches[sig] = append(patchBranches[sig], q)
	}

	if len(whole) == 0 {
		// An empty mirror still has to answer with the right column names and
		// types, or a view over it fails to plan rather than returning nothing.
		sel := make([]string, len(t.Order))
		for i, c := range t.Order {
			sel[i] = fmt.Sprintf("CAST(NULL AS %s) AS %s",
				sqlType(t.typeOf(c)), sqlIdent(c))
		}
		return "SELECT " + strings.Join(sel, ", ") + " WHERE false", nil
	}

	var b strings.Builder
	b.WriteString("WITH qs_whole AS (\n  ")
	b.WriteString(strings.Join(whole, "\n  UNION ALL\n  "))
	b.WriteString("\n)")
	for i, sig := range sigs {
		fmt.Fprintf(&b, ",\nqs_patch_%d AS (\n  ", i)
		b.WriteString(strings.Join(patchBranches[sig], "\n  UNION ALL\n  "))
		b.WriteString("\n)")
	}

	// A patch supplies the columns it carries and nothing else, and its
	// presence is detected by its KEY being non-null rather than by the column
	// being non-null — a patch is entitled to set a column to NULL, and
	// COALESCE would silently fall back to the stale value when it did.
	sel := make([]string, 0, len(t.Order))
	for _, c := range t.Order {
		expr := "qs_whole." + sqlIdent(c)
		for i, sig := range sigs {
			if !containsStr(patchCols[sig], c) {
				continue
			}
			expr = fmt.Sprintf("CASE WHEN qs_patch_%d.%s IS NOT NULL THEN qs_patch_%d.%s ELSE %s END",
				i, sqlIdent(t.Key), i, sqlIdent(c), expr)
		}
		sel = append(sel, expr+" AS "+sqlIdent(c))
	}
	fmt.Fprintf(&b, "\nSELECT %s\nFROM qs_whole", strings.Join(sel, ",\n       "))
	for i := range sigs {
		fmt.Fprintf(&b, "\nLEFT JOIN qs_patch_%d ON qs_whole.%s = qs_patch_%d.%s",
			i, sqlIdent(t.Key), i, sqlIdent(t.Key))
	}
	return b.String(), nil
}

// ColumnsFromFiles recovers a table's column list and declared types from the
// mirror's own Parquet files.
//
// The source catalog is authoritative and should be preferred. This exists for
// the case where it is not reachable — which is exactly the case in which
// somebody is most likely to be querying the mirror instead.
func ColumnsFromFiles(root, schema, table string) (map[string]string, []string, error) {
	dir := filepath.Join(root, schema+"."+table)
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return nil, nil, err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, nil, err
	}
	// A base file holds whole rows in the table's column order; a partial delta
	// does not, so it is not a candidate.
	var candidates []string
	for _, f := range st.BaseFiles {
		candidates = append(candidates, filepath.Join(dir, "base", f))
	}
	for _, d := range st.DeltaFiles {
		if st.PartialCols[d] == nil {
			candidates = append(candidates, filepath.Join(dir, "delta", d))
		}
	}
	// NEWEST first. A mirror that has been upgraded holds a column one way in
	// its old files and another in its new ones (types.go), and the newest
	// file is the one written the way the current writer writes: its types
	// are the ones every other branch of the view can be cast to.
	for i := len(candidates) - 1; i >= 0; i-- {
		path := candidates[i]
		cols, order, err := schemaOf(path)
		if err == nil {
			return cols, order, nil
		}
	}
	return nil, nil, fmt.Errorf("no readable whole-row file in %s", dir)
}

func schemaOf(path string) (map[string]string, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, nil, err
	}
	defer rdr.Close()
	s := rdr.MetaData().Schema
	cols := map[string]string{}
	order := make([]string, 0, s.NumColumns())
	for i := 0; i < s.NumColumns(); i++ {
		c := s.Column(i)
		name := c.Name()
		order = append(order, name)
		// Back to a Postgres type name, which is all arrowType needs to agree
		// with what the writer used. Numerics keep their precision and scale so
		// that a round trip does not quietly widen them. A numeric stored as
		// text comes back as text: the file does not say it was ever a number.
		typ := parquetSQLType(c)
		switch typ {
		case "SMALLINT":
			cols[name] = "smallint"
		case "INTEGER":
			cols[name] = "integer"
		case "BIGINT":
			cols[name] = "bigint"
		case "FLOAT":
			cols[name] = "real"
		case "DOUBLE":
			cols[name] = "double precision"
		case "BOOLEAN":
			cols[name] = "boolean"
		case "UUID":
			cols[name] = "uuid"
		case "BLOB":
			cols[name] = "bytea"
		case "DATE":
			cols[name] = "date"
		case "TIME":
			cols[name] = "time without time zone"
		case "TIMESTAMP":
			cols[name] = "timestamp without time zone"
		case "TIMESTAMP WITH TIME ZONE":
			cols[name] = "timestamp with time zone"
		default:
			if strings.HasPrefix(typ, "DECIMAL(") {
				cols[name] = "numeric" + strings.TrimPrefix(typ, "DECIMAL")
			} else {
				cols[name] = "text"
			}
		}
	}
	return cols, order, nil
}

// dvSubquery is the SELECT a view uses to read one deletion vector.
//
// It has to match the encoding on disk, because a mirror that predates dv.go
// still has JSON vectors and will until compaction retires the files they
// describe. The Parquet form is not only faster to parse — 0.6 ms against
// 95.7 ms for 1.56M positions — it is also a plain column, so the engine reads
// it as a column rather than unnesting one enormous list value.
func dvSubquery(path string) string {
	if strings.HasSuffix(path, ".json") {
		return fmt.Sprintf("(SELECT unnest(v) FROM read_json(%s, "+
			"columns = {'v': 'BIGINT[]'}, format = 'unstructured'))", sqlString(path))
	}
	return fmt.Sprintf("(SELECT p FROM read_parquet(%s))", sqlString(path))
}

// padBranch puts a constant in front of a whole-row branch's columns.
//
// A UNION ALL has to keep at least one column per branch even when the query
// above it wants none, and the engine keeps the FIRST. For count(*) that made
// every branch read the table's first column — usually the primary key, and on
// a UUID-keyed 10M-row mirror 228 MB of it, 810 ms of a 1,190 ms count(*).
// With a constant in that position the scan reads nothing it was not asked
// for: the same count measured 250 ms. The outer SELECT names the table's own
// columns, so the pad never reaches a reader.
func padBranch(q string) string {
	return "SELECT 0::TINYINT AS " + padColumn + ", " + strings.TrimPrefix(q, "SELECT ")
}

// padColumn is not a name a mirrored table can have: ViewSQL would otherwise
// have to rename it around a collision.
const padColumn = `"__qs_pad"`

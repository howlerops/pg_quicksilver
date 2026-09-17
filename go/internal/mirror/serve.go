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
// A compaction that lands mid-query deletes files the query names, and the
// engine will say so; the caller re-asks for the SQL and re-runs, which is the
// same ErrStaleManifest contract read.go documents. Publishing a view that
// "never goes stale" would mean either pinning files forever or answering from
// a mixture of two manifests, and the second is the bug round five was about.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

// sqlType is the engine type a column reads back as, so that a file predating
// an ADD COLUMN can be given a correctly typed literal instead of erroring.
func sqlType(pgType string) string {
	if k := temporalOf(pgType); k != notTemporal {
		return temporalSQLType(k)
	}
	switch dt := arrowType(pgType).(type) {
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d,%d)", dt.Precision, dt.Scale)
	default:
		switch arrowType(pgType) {
		case arrow.PrimitiveTypes.Int64:
			return "BIGINT"
		case arrow.PrimitiveTypes.Float64:
			return "DOUBLE"
		case arrow.FixedWidthTypes.Boolean:
			return "BOOLEAN"
		}
		return "VARCHAR"
	}
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func sqlIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// fileColumns reads a Parquet file's column names from its footer.
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
	schema := rdr.MetaData().Schema
	for i := 0; i < schema.NumColumns(); i++ {
		out[schema.Column(i).Name()] = true
	}
	return out, nil
}

// branch is one file's contribution: its columns projected to `want`, with the
// deletion vector the manifest names applied.
func (t *Table) branch(rel string, want []string) (string, error) {
	abs := filepath.Join(t.Dir, rel)
	have, err := fileColumns(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrStaleManifest
		}
		return "", err
	}
	sel := make([]string, 0, len(want))
	for _, c := range want {
		if have[c] {
			// A temporal column is CAST rather than named. Files written before
			// temporal.go hold it as text, and a UNION ALL that mixes a VARCHAR
			// branch with a TIMESTAMP one either fails to plan or silently
			// degrades every branch to VARCHAR. The cast is a no-op the engine
			// elides when the types already agree, and a migration when they do
			// not.
			if k := temporalOf(t.Columns[c]); k != notTemporal {
				sel = append(sel, fmt.Sprintf("CAST(%s AS %s) AS %s",
					sqlIdent(c), temporalSQLType(k), sqlIdent(c)))
				continue
			}
			sel = append(sel, sqlIdent(c))
			continue
		}
		// This file predates an ADD COLUMN. NULL is right only when the column
		// was added without a default; otherwise PostgreSQL reads these rows
		// back as attmissingval and so must the view.
		if mv, ok := t.State.MissingVals[c]; ok {
			sel = append(sel, fmt.Sprintf("CAST(%s AS %s) AS %s",
				sqlString(mv), sqlType(t.Columns[c]), sqlIdent(c)))
		} else {
			sel = append(sel, fmt.Sprintf("CAST(NULL AS %s) AS %s",
				sqlType(t.Columns[c]), sqlIdent(c)))
		}
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
		dv := t.dvPathGen(rel, gen)
		if _, err := os.Stat(dv); err != nil {
			// The manifest names a vector that is gone, so this manifest is
			// older than the mirror. Answering without it would resurrect every
			// row it retired.
			return "", ErrStaleManifest
		}
		q += fmt.Sprintf(" WHERE file_row_number NOT IN "+
			"(SELECT unnest(v) FROM read_json(%s, columns = {'v': 'BIGINT[]'}, "+
			"format = 'unstructured'))", sqlString(dv))
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
		whole = append(whole, q)
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
			whole = append(whole, q)
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
				sqlType(t.Columns[c]), sqlIdent(c))
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
	for _, path := range candidates {
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
		// that a round trip does not quietly widen them.
		switch lt := c.LogicalType().(type) {
		case *schema.DecimalLogicalType:
			cols[name] = fmt.Sprintf("numeric(%d,%d)", lt.Precision(), lt.Scale())
		default:
			switch c.PhysicalType() {
			case parquet.Types.Int64:
				cols[name] = "bigint"
			case parquet.Types.Double:
				cols[name] = "double precision"
			case parquet.Types.Boolean:
				cols[name] = "boolean"
			default:
				cols[name] = "text"
			}
		}
	}
	return cols, order, nil
}

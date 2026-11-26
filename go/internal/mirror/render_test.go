package mirror

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The published SQL used to hold the writer's absolute paths, so a reader that
// mounts the mirror anywhere else could not evaluate it. It now holds paths
// relative to the table directory — data files and deletion vectors alike —
// and Render puts them under whatever directory the reader has.
func TestPublishedPathsAreRelativeAndRenderAnywhere(t *testing.T) {
	tbl, _ := graceTable(t, time.Minute)
	seedR(t, tbl, 50) // a base with a vector on it, and a delta
	pub := mustPublished(t, tbl)

	root := filepath.Dir(tbl.Dir)
	if strings.Contains(pub.SQL, root) {
		t.Fatalf("the published SQL names the writer's path %s:\n%s", root, pub.SQL)
	}
	raw, _ := os.ReadFile(filepath.Join(tbl.Dir, PublishedFile))
	var onDisk map[string]any
	if err := json.Unmarshal(raw, &onDisk); err != nil {
		t.Fatal(err)
	}
	if onDisk["paths"] != PathsRelative || onDisk["base_dir"] != tbl.Dir {
		t.Fatalf("snapshot.json: paths=%v base_dir=%v", onDisk["paths"], onDisk["base_dir"])
	}
	// Every file the snapshot lists is named in the SQL exactly as listed.
	for _, f := range pub.Files {
		if !strings.Contains(pub.SQL, "'"+f+"'") {
			t.Errorf("%s is listed and is not named in the SQL as that", f)
		}
	}
	if !strings.Contains(strings.Join(pub.Files, " "), "dv/") {
		t.Fatal("the snapshot under test should name a deletion vector")
	}

	// At the writer's own directory it is ViewSQL, byte for byte.
	want, err := tbl.ViewSQL()
	if err != nil {
		t.Fatal(err)
	}
	if got := pub.Render(""); got != want {
		t.Fatalf("rendered at base_dir:\n%s\n--- want\n%s", got, want)
	}
	// Somewhere else, only the directory differs.
	moved := pub.Render("/mnt/elsewhere/public.r")
	if moved != strings.ReplaceAll(want, tbl.Dir, "/mnt/elsewhere/public.r") {
		t.Fatalf("rendered elsewhere:\n%s", moved)
	}
	// The absolute form an older writer published re-roots the same way.
	old := Published{SQL: want}
	if got := old.Render("/mnt/elsewhere/public.r"); got != moved {
		t.Fatalf("an absolute-path snapshot re-rooted:\n%s", got)
	}
	if got := old.Render(""); got != want {
		t.Fatal("an absolute-path snapshot with nowhere to render to must be left alone")
	}

	// A halted snapshot's SQL is a message, not a view.
	if err := tbl.Halt("a 'quoted' reason with read_parquet('x') in it"); err != nil {
		t.Fatal(err)
	}
	halted := mustPublished(t, tbl)
	if got := halted.Render("/mnt/elsewhere/public.r"); got != halted.SQL {
		t.Fatalf("rendering changed a halt message:\n%s", got)
	}
}

// Render rewrites the argument of a read call and nothing else: not a default
// value, not a column name, however much either looks like one.
func TestRebaseSQLOnlyTouchesReadCalls(t *testing.T) {
	to := func(p string) string { return "/r/" + p }
	for _, c := range []struct{ in, want string }{
		{`SELECT a FROM read_parquet('base/1.parquet')`,
			`SELECT a FROM read_parquet('/r/base/1.parquet')`},
		{`SELECT a FROM read_parquet('base/1.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('dv/1.000000002.dv.parquet'))`,
			`SELECT a FROM read_parquet('/r/base/1.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/r/dv/1.000000002.dv.parquet'))`},
		{`(SELECT unnest(v) FROM read_json('dv/1.000000001.dv.json', columns = {'v': 'BIGINT[]'}, format = 'unstructured'))`,
			`(SELECT unnest(v) FROM read_json('/r/dv/1.000000001.dv.json', columns = {'v': 'BIGINT[]'}, format = 'unstructured'))`},
		// a default value that is the text of a call
		{`SELECT CAST('read_parquet(''base/9.parquet'')' AS VARCHAR) AS "d" FROM read_parquet('base/1.parquet')`,
			`SELECT CAST('read_parquet(''base/9.parquet'')' AS VARCHAR) AS "d" FROM read_parquet('/r/base/1.parquet')`},
		// a default value that ENDS with the call's name, followed by a literal
		{`SELECT 'x read_parquet(' || 'base/9.parquet' FROM read_parquet('base/1.parquet')`,
			`SELECT 'x read_parquet(' || 'base/9.parquet' FROM read_parquet('/r/base/1.parquet')`},
		// a column named like one
		{`SELECT "read_parquet('base/9.parquet')", "it's" FROM read_parquet('base/1.parquet')`,
			`SELECT "read_parquet('base/9.parquet')", "it's" FROM read_parquet('/r/base/1.parquet')`},
		// a path with a quote in it
		{`SELECT a FROM read_parquet('ba''se/1.parquet')`,
			`SELECT a FROM read_parquet('/r/ba''se/1.parquet')`},
		{`SELECT CAST(NULL AS BIGINT) AS "id" WHERE false`, `SELECT CAST(NULL AS BIGINT) AS "id" WHERE false`},
		{`SELECT 'unterminated`, `SELECT 'unterminated`},
	} {
		if got := rebaseSQL(c.in, to); got != c.want {
			t.Errorf("rebaseSQL(%s)\n got %s\nwant %s", c.in, got, c.want)
		}
	}
}

// A quiet table publishes nothing, so its snapshot's LSN stands still; a
// reader gating on freshness is given the LSN the watermark vouches for.
func TestReadPublishedReportsTheEffectiveLSN(t *testing.T) {
	root := t.TempDir()
	tables := wmTables(t, root, 2)
	tick(t, tables, 0x100, wmChange("t0", 1, "a"), wmChange("t1", 1, "a"))
	tick(t, tables, 0x200, wmChange("t0", 2, "b"))
	raw, err := readPublished(tables[1].Dir)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ReadPublished(root, "public", "t1")
	if err != nil {
		t.Fatal(err)
	}
	if raw.AppliedLSN != "0/100" || p.AppliedLSN != "0/200" || p.Seq != raw.Seq {
		t.Fatalf("snapshot.json says %s, ReadPublished %s (seq %d/%d)", raw.AppliedLSN, p.AppliedLSN, raw.Seq, p.Seq)
	}
}

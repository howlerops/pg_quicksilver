package mirror

// Which writer wrote each file, and which columns that makes untrustworthy.
//
// A mirror outlives the binary that wrote it, and nothing rewrites its files
// on upgrade. Most of what an older writer did differently is a different
// ENCODING of the same value, which every reader converts as it goes
// (types.go, "OLD FILES"): an integer stored wide, a uuid as text, an
// unconstrained numeric as DECIMAL(38,9). Those need nothing.
//
// Some of it is not. An earlier snapshot read the source with `col::text`,
// and for eight types a cast to text is a different function from the type's
// output function, which is what the stream delivers:
//
//	char(n)   'ab'::char(5)::text = 'ab'      output: 'ab   '
//	inet      '10.0.0.1'::inet::text = '10.0.0.1/32'   output: '10.0.0.1'
//	money, interval, timetz, cidr, macaddr, bit: likewise, each its own way
//
// So a base file from such a snapshot holds one spelling and every row the
// stream has since written holds the other, in the same column, both stored
// as text. No reader can convert, because nothing in the file says which
// spelling a value has; qs-verify reports the table DIVERGED and is right.
// The only repair is to read the source again.
//
// Until now nothing could even tell which mirrors were affected. So:
//
// THE MANIFEST RECORDS A WRITER FORMAT VERSION PER DATA FILE (State.FileFormat).
// A file with no entry is legacy: written before versions were recorded. A
// file made FROM other files — a delta merge, a base rewrite — holds their
// values as they were, so it takes the oldest version among its inputs, not
// the version of the binary that copied them. Only a snapshot, which reads
// the source afresh, and an Apply, which writes what the stream sent, start
// at the current version.
//
// WriterFormatVersion is the one constant to bump when a representation
// changes in a way a reader cannot undo, together with one entry in
// formatRules saying which columns of which older files are affected.
//
// LegacyFormatColumns is the report: per table, the columns holding data
// from a writer version whose representation of that column's type is known
// to be inconsistent, and so need the table rebuilt. Acting on it (halting,
// QS_REBUILD_TABLES, a log line) is the sidecar's decision.

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// WriterFormatVersion is what this binary records for the files it writes.
// History:
//
//	0 (absent)  every binary before versions were recorded
//	1           snapshots read the output function's text, not ::text;
//	            unconstrained numeric is text, not DECIMAL(38,9)
//	2           numeric(p,s) with p <= 18 is stored on INT64, not fixed-length
//	            bytes; delta-packed where the column is spread out; declared
//	            NOT NULL columns are REQUIRED; min/max are marked exact (see
//	            encoding.go). No rule above is about these, so no file of
//	            version 1 or 2 is flagged for rebuild.
const WriterFormatVersion = 2

// formatRule says that files from writers older than fixedIn hold some
// columns in a way no reader can repair.
type formatRule struct {
	fixedIn int
	// affects reports whether column col of the file at rel (whose footer is
	// have) is one of them.
	affects func(t *Table, col, rel string, have map[string]bool) bool
	problem string
}

var formatRules = []formatRule{
	{
		fixedIn: 1,
		affects: func(t *Table, col, rel string, _ map[string]bool) bool {
			// Only a base file can hold snapshot rows: a legacy delta was
			// written from the stream, in the stream's spelling. (A legacy
			// base may be a rewrite of such deltas and nothing else, and is
			// reported anyway: the manifest of that binary did not say.)
			return strings.HasPrefix(rel, "base"+string(filepath.Separator)) && castSpellingDiffers(t.Columns[col])
		},
		problem: "rows from a snapshot taken by an earlier binary are spelled as ::text prints this type, " +
			"and rows from the stream as its output function does; the two are stored side by side as text " +
			"and cannot be told apart, so the column reads as DIVERGED from its source",
	},
}

// What is NOT a rule, and was: an unconstrained numeric PRIMARY KEY that an
// earlier binary stored as DECIMAL(38,9). The decimal lost the display scale,
// so the key the source prints as 1.50 read back as 1.5 and the stream's 1.50
// did not find it. That was never a property of the file. It was the key index
// comparing a number as text, and it is fixed where it was wrong: the key of a
// numeric column is its canonical value (keyMode, index.go), which 1.50, 1.5
// and 1.500000000 all are. Such a table needs no rebuild.

// castSpellingDiffers reports whether a type's ::text and its output function
// can print the same value differently. Arrays of such types are included:
// array_out prints each element with the element's output function, and a
// cast of the array to text does the same, but an earlier snapshot's
// element-wise spelling is not something this code can vouch for.
func castSpellingDiffers(pgType string) bool {
	base, _ := baseType(strings.TrimSuffix(strings.TrimSpace(pgType), "[]"))
	switch base {
	case "character", "char", "bpchar",
		"inet", "cidr", "macaddr", "macaddr8",
		"bit", "money", "interval",
		"time with time zone", "timetz":
		return true
	}
	return false
}

// LegacyColumn is one column of a table that holds data no reader can make
// consistent, and why.
type LegacyColumn struct {
	Column string
	PGType string
	// WriterVersion is the oldest writer format version among the files
	// concerned; 0 is a binary from before versions were recorded.
	WriterVersion int
	// Files are those files, relative to the table directory.
	Files []string
	// Problem says what is wrong with the data, in a sentence an operator
	// can act on.
	Problem string
	// Remedy is what fixes it. Always "rebuild": re-snapshot the table
	// (QS_REBUILD_TABLES), which replaces every file with ones this binary
	// wrote from the source.
	Remedy string
}

// LegacyFormatColumns reports the columns of this table that hold data from
// a writer version whose representation of the column's type is known to be
// inconsistent, and therefore need the table rebuilt. It is empty for a
// table every file of which this binary (or a later one) wrote from the
// source or the stream, and for a table with no column of an affected type.
//
// It is derived from the files the manifest names NOW, never from history: a
// table whose legacy files are gone (a rebuild, a TRUNCATE) is clean. It reads
// the manifest in State and file footers through the footer cache; no data. A footer that cannot be read
// is reported as an error rather than as a clean table.
func (t *Table) LegacyFormatColumns() ([]LegacyColumn, error) {
	byCol := map[string]*LegacyColumn{}
	for _, rel := range t.dataFilesRel() {
		ver := t.State.FileFormat[filepath.Base(rel)]
		var have map[string]bool
		for _, rule := range formatRules {
			if ver >= rule.fixedIn {
				continue
			}
			if have == nil {
				var err error
				if have, err = t.columnsOf(filepath.Join(t.Dir, rel)); err != nil {
					return nil, fmt.Errorf("%s: %w", rel, err)
				}
			}
			for _, col := range t.Order {
				if !have[col] || !rule.affects(t, col, rel, have) {
					continue
				}
				lc := byCol[col]
				if lc == nil {
					lc = &LegacyColumn{Column: col, PGType: t.Columns[col], WriterVersion: ver,
						Problem: rule.problem, Remedy: "rebuild"}
					byCol[col] = lc
				}
				if ver < lc.WriterVersion {
					lc.WriterVersion = ver
				}
				if !containsStr(lc.Files, filepath.ToSlash(rel)) {
					lc.Files = append(lc.Files, filepath.ToSlash(rel))
				}
			}
		}
	}
	out := make([]LegacyColumn, 0, len(byCol))
	for _, lc := range byCol {
		out = append(out, *lc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Column < out[j].Column })
	return out, nil
}

// noteFormats remembers which files the manifest names and at what version,
// so that the next manifest write can tell what was added and what it was
// made from. New and Reload call it.
func (t *Table) noteFormats() {
	t.fmtPrev = map[string]int{}
	for _, rel := range t.dataFilesRel() {
		name := filepath.Base(rel)
		t.fmtPrev[name] = t.State.FileFormat[name]
	}
}

// stampFormats brings State.FileFormat up to date with the files State names,
// immediately before a manifest is written. See the top of this file for the
// rule; saveState is the only caller, which is what makes it cover every
// path that adds a file.
func (t *Table) stampFormats() {
	now := map[string]bool{}
	for _, rel := range t.dataFilesRel() {
		now[filepath.Base(rel)] = true
	}
	// What the files being added were made from: everything this manifest
	// stops naming. An Apply removes nothing and so writes at the current
	// version; a merge or a rewrite removes its inputs and inherits from them.
	// (A TRUNCATE inside an Apply removes files that are the inputs of
	// nothing, and says so by emptying fmtPrev: applyRun.truncate.)
	version := WriterFormatVersion
	if !t.freshContents {
		for name, v := range t.fmtPrev {
			if !now[name] && v < version {
				version = v
			}
		}
	}
	if legacyTypes {
		version = 0 // the test switch stands for a binary that recorded nothing
	}
	t.freshContents = false
	next := make(map[string]int, len(now))
	for name := range now {
		v, known := t.fmtPrev[name]
		if !known {
			v = version
		}
		next[name] = v
	}
	t.fmtPrev = next
	t.State.FileFormat = nil
	for name, v := range next {
		if v > 0 {
			if t.State.FileFormat == nil {
				t.State.FileFormat = map[string]int{}
			}
			t.State.FileFormat[name] = v
		}
	}
}

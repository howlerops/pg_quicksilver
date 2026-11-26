package mirror

// Opting an unconstrained numeric into DECIMAL(38,s) when the column already
// holds data.
//
// QS_NUMERIC_SCALE used to take effect the moment it was set: typeOf answered
// numeric(38,s) for the column, and everything that asks typeOf followed. For
// a table that already had rows stored as exact text, that meant the view
// cast the text to DECIMAL(38,s) — and an engine's cast ROUNDS, silently:
// 0.123456 at scale 4 read as 0.1235 — until the first write that carried
// such a value forward, which halted the table. A setting meant to make a
// column faster to aggregate made it wrong and then stopped it.
//
// The option is now a transition with a test in front of it. A column the
// operator names is exposed as the decimal only once it is known that every
// value stored fits DECIMAL(38,s) exactly:
//
//   - A table with no file holding the column in another form (a new table,
//     or one that was opted in from the start) has nothing to check, and is a
//     decimal at once.
//   - Otherwise the writer scans the column, once, at its first manifest
//     write. If every live value fits, the decision is recorded in the
//     manifest (State.NumericScales), the column is a decimal from that
//     manifest on, and a base rewrite is asked for to convert the files
//     (CompactReason: "numeric-opt-in"). Until the rewrite lands the view
//     casts the text of the old files — exactly, because that is what was
//     checked.
//   - If a value does not fit, the option is REFUSED for that column: one log
//     line naming the value and its key, and the column goes on being served
//     as the exact text. Nothing is rounded and nothing halts. The refusal is
//     not recorded, so it is looked at again when the writer next starts —
//     after the operator has corrected the row, or the scale.
//
// A reader (qs-query, qs-verify) never scans. It follows the manifest, and
// for a column the manifest has no decision on, serves text unless no file
// says otherwise.
//
// The decision is kept per table DIRECTORY, not per Table: a base rewrite
// works through a detached copy of the Table (compact_async.go), and the copy
// has to store the column the way the table it belongs to does.

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type optState int

const (
	optPending  optState = iota // files hold another form and have not been checked: text
	optAccepted                 // DECIMAL(38,scale)
	optRefused                  // a stored value does not fit: text
)

type optDecision struct {
	scale int32
	state optState
	// why is the refusal, for NumericOptIns.
	why string
	// mixed is whether some file still holds an accepted column in another
	// form, which is what the rewrite is for; seq is the manifest sequence it
	// was last looked at.
	mixed bool
	seq   int
}

// numericOpts holds every table's decisions, by table directory.
var numericOpts = struct {
	sync.Mutex
	byDir map[string]map[string]*optDecision
}{byDir: map[string]map[string]*optDecision{}}

// optScale is the scale the operator asked for, for an unconstrained numeric
// column that is not an array.
func (t *Table) optScale(col string) (int32, bool) {
	if NumericScale == "" {
		return 0, false
	}
	pg := t.Columns[col]
	if _, _, declared, ok := numericSpec(pg); !ok || declared ||
		strings.HasSuffix(strings.TrimSpace(pg), "[]") {
		return 0, false
	}
	return numericScaleFor(NumericScale, t.Qualified, col)
}

func decimalSQL(scale int32) string { return "DECIMAL(38," + strconv.Itoa(int(scale)) + ")" }

// optedIn reports whether col is to be stored and served as DECIMAL(38,scale).
func (t *Table) optedIn(col string, scale int32) bool {
	numericOpts.Lock()
	d := numericOpts.byDir[t.Dir][col]
	numericOpts.Unlock()
	if d != nil && d.scale == scale {
		return d.state == optAccepted
	}
	if t.detached {
		// A rewrite's copy has no manifest to decide from, and its table
		// decided before it began (New, saveState). No decision means the
		// table never treated the column as a decimal.
		return false
	}
	return t.decideOptIn(col, scale).state == optAccepted
}

// decideOptIn makes the part of the decision that needs no scan, and records
// it.
func (t *Table) decideOptIn(col string, scale int32) *optDecision {
	d := &optDecision{scale: scale, seq: -1}
	other, err := t.filesHoldingOtherwise(col, scale)
	switch {
	case t.State.NumericScales[col] == scale && hasKey(t.State.NumericScales, col):
		d.state, d.mixed = optAccepted, err != nil || len(other) > 0
	case err == nil && len(other) == 0:
		d.state = optAccepted
	default:
		d.state = optPending
	}
	numericOpts.Lock()
	if numericOpts.byDir[t.Dir] == nil {
		numericOpts.byDir[t.Dir] = map[string]*optDecision{}
	}
	numericOpts.byDir[t.Dir][col] = d
	numericOpts.Unlock()
	return d
}

func hasKey(m map[string]int32, k string) bool { _, ok := m[k]; return ok }

// filesHoldingOtherwise lists the manifest's files that hold col as anything
// but DECIMAL(38,scale), relative to Dir.
func (t *Table) filesHoldingOtherwise(col string, scale int32) ([]string, error) {
	want := fileTypeKey(col, decimalSQL(scale))
	var out []string
	for _, rel := range t.dataFilesRel() {
		have, err := t.columnsOf(filepath.Join(t.Dir, rel))
		if err != nil {
			return nil, err
		}
		if have[col] && !have[want] {
			out = append(out, rel)
		}
	}
	return out, nil
}

// dataFilesRel is every data file the manifest names, relative to Dir.
func (t *Table) dataFilesRel() []string {
	out := make([]string, 0, len(t.State.BaseFiles)+len(t.State.DeltaFiles))
	for _, b := range t.State.BaseFiles {
		out = append(out, filepath.Join("base", b))
	}
	for _, d := range t.State.DeltaFiles {
		out = append(out, filepath.Join("delta", d))
	}
	return out
}

// forgetOptIns drops this directory's decisions, so that the Table being
// opened decides from the manifest it has just read.
func (t *Table) forgetOptIns() {
	numericOpts.Lock()
	delete(numericOpts.byDir, t.Dir)
	numericOpts.Unlock()
}

// primeOptIns decides every opted-in column that has no decision yet. New and
// saveState call it, so that a decision always exists before a rewrite's
// detached copy asks.
func (t *Table) primeOptIns() {
	if NumericScale == "" || t.detached {
		return
	}
	for _, col := range t.Order {
		if s, ok := t.optScale(col); ok {
			t.optedIn(col, s)
		}
	}
}

// resolveOptIns is the writer's half: it scans each column still pending and
// accepts or refuses it. It runs at the first manifest write of a process,
// before the manifest is marshalled, so an acceptance is in that manifest.
func (t *Table) resolveOptIns() {
	if NumericScale == "" || t.detached {
		return
	}
	t.primeOptIns()
	for _, col := range t.Order {
		s, ok := t.optScale(col)
		if !ok {
			continue
		}
		numericOpts.Lock()
		d := numericOpts.byDir[t.Dir][col]
		numericOpts.Unlock()
		if d == nil || d.scale != s || d.state != optPending {
			if d != nil && d.scale == s && d.state == optAccepted {
				t.recordOptIn(col, s)
			}
			continue
		}
		key, value, rows, err := t.firstMisfit(col, s)
		numericOpts.Lock()
		switch {
		case err != nil:
			// Not checked is not accepted. It stays pending, and text.
			d.why = "the column could not be scanned: " + err.Error()
		case value != "":
			d.state = optRefused
			d.why = fmt.Sprintf("the value %q (%s = %s) does not fit DECIMAL(38,%d) exactly", value, t.Key, key, s)
		default:
			d.state, d.mixed, d.seq = optAccepted, true, -1
		}
		state, why := d.state, d.why
		numericOpts.Unlock()
		switch state {
		case optAccepted:
			t.recordOptIn(col, s)
			slog.Info("QS_NUMERIC_SCALE: every stored value fits, so the column is now a decimal; "+
				"a base rewrite will convert the files that hold it as text",
				"table", t.Qualified, "column", col, "type", decimalSQL(s), "values_checked", rows)
		default:
			slog.Warn("QS_NUMERIC_SCALE is REFUSED for this column: it stays exact text and nothing is rounded. "+
				"Correct the value or choose a scale that holds it, then restart",
				"table", t.Qualified, "column", col, "asked_for", decimalSQL(s), "why", why)
		}
	}
	// A decision recorded for a column that is no longer opted in at that
	// scale is not a decision about anything.
	for col, s := range t.State.NumericScales {
		if now, ok := t.optScale(col); !ok || now != s {
			delete(t.State.NumericScales, col)
		}
	}
}

func (t *Table) recordOptIn(col string, scale int32) {
	if t.State.NumericScales == nil {
		t.State.NumericScales = map[string]int32{}
	}
	t.State.NumericScales[col] = scale
}

// firstMisfit scans col in every file that holds it otherwise and returns the
// first live value DECIMAL(38,scale) cannot hold exactly, with its key, or ""
// when every one of them fits.
//
// It reads through the table's own read path while the column is still text,
// so a value an older file holds as DECIMAL(38,9) arrives as the digits it
// stands for. Rows a deletion vector has retired are not values the table
// holds, and are not looked at.
func (t *Table) firstMisfit(col string, scale int32) (key, value string, rows int, err error) {
	files, err := t.filesHoldingOtherwise(col, scale)
	if err != nil {
		return "", "", 0, err
	}
	stop := fmt.Errorf("found")
	for _, rel := range files {
		dead, derr := t.deadPositions(rel)
		if derr != nil {
			return "", "", rows, derr
		}
		cols := []string{t.Key, col}
		if col == t.Key {
			cols = cols[:1]
		}
		err := t.forEachRowGroup(filepath.Join(t.Dir, rel), dead, cols,
			func(rs []map[string]any, _ int) error {
				for _, r := range rs {
					v := r[col]
					if v == nil {
						continue
					}
					rows++
					text := textOf(plainValue(v))
					if _, ok := parseDecimalExact(text, maxDecimalPrecision, scale); !ok {
						key, value = textOf(plainValue(r[t.Key])), text
						return stop
					}
				}
				return nil
			})
		if err == stop {
			return key, value, rows, nil
		}
		if err != nil {
			return "", "", rows, err
		}
	}
	return "", "", rows, nil
}

// numericRewriteDue reports whether a base rewrite is owed to convert files
// that hold an accepted column as text. It looks at file footers only when
// the manifest has changed since it last looked, and only while a conversion
// is outstanding.
func (t *Table) numericRewriteDue() bool {
	if NumericScale == "" || t.detached {
		return false
	}
	numericOpts.Lock()
	var check []string
	for col, d := range numericOpts.byDir[t.Dir] {
		if d.state == optAccepted && d.mixed && d.seq != t.State.Seq {
			check = append(check, col)
		}
	}
	numericOpts.Unlock()
	for _, col := range check {
		s, ok := t.optScale(col)
		if !ok {
			continue
		}
		other, err := t.filesHoldingOtherwise(col, s)
		numericOpts.Lock()
		if d := numericOpts.byDir[t.Dir][col]; d != nil && d.scale == s {
			d.mixed, d.seq = err != nil || len(other) > 0, t.State.Seq
		}
		numericOpts.Unlock()
	}
	numericOpts.Lock()
	defer numericOpts.Unlock()
	for _, d := range numericOpts.byDir[t.Dir] {
		if d.state == optAccepted && d.mixed {
			return true
		}
	}
	return false
}

// NumericOptIn is where one column named by QS_NUMERIC_SCALE stands.
type NumericOptIn struct {
	Column string
	Scale  int32
	// State is "decimal" (stored and served as DECIMAL(38,Scale)), "pending"
	// (existing files hold it as text and have not been checked yet: served as
	// text) or "refused" (a stored value does not fit: served as text).
	State string
	// Why is the refusal, naming a value that does not fit.
	Why string
	// Converting is whether files written before the option still hold the
	// column as text, which the next base rewrite converts.
	Converting bool
}

// NumericOptIns reports every column QS_NUMERIC_SCALE names in this table,
// for a health endpoint or a log line. It is empty when the option is unset.
func (t *Table) NumericOptIns() []NumericOptIn {
	var out []NumericOptIn
	for _, col := range t.Order {
		s, ok := t.optScale(col)
		if !ok {
			continue
		}
		t.optedIn(col, s)
		numericOpts.Lock()
		d := numericOpts.byDir[t.Dir][col]
		o := NumericOptIn{Column: col, Scale: s, State: "pending"}
		if d != nil && d.scale == s {
			o.Why, o.Converting = d.why, d.state == optAccepted && d.mixed
			switch d.state {
			case optAccepted:
				o.State = "decimal"
			case optRefused:
				o.State = "refused"
			}
		}
		numericOpts.Unlock()
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Column < out[j].Column })
	return out
}

package mirror

// NOT NULL, as the source declares it now.
//
// A Parquet column is OPTIONAL or REQUIRED. An OPTIONAL one carries a
// definition level per value, which a reader decodes before it can touch the
// values; a REQUIRED one does not. Measured on a 10M-row orders table,
// `count(*) GROUP BY status` cost 47 ms of CPU with every column OPTIONAL and
// 33 ms with the NOT NULL ones REQUIRED, which is what DuckDB's own format
// costs.
//
// The mirror is not told nullability by the change stream, and the manifest
// does not record it. It is OPTIONAL INFORMATION SUPPLIED BY THE CALLER
// (SetNotNull), and the rules are these.
//
//  1. A column is written REQUIRED only when the caller has said it is NOT
//     NULL NOW. A table nobody called SetNotNull on writes every column
//     OPTIONAL, exactly as before. Nothing is inferred: not from the data, not
//     from the column being the key.
//
//  2. A NULL arriving for such a column is a CONTRACT VIOLATION. The batch
//     fails, loudly, with a *NotNullViolation, before anything is written
//     (the check is the conversion every write passes through, values.go). No
//     default is stored, and the table is not HALTED: the fault may be the
//     caller's (it described a constraint the stream has not reached yet).
//     The Apply fails as any failed write does, so the Table has to be
//     reopened; the caller then gives the corrected claim and applies the
//     batch again. A rewrite that meets such a NULL fails the same way and
//     leaves the table as it was.
//
//  3. Nullability is a property of a FILE, not of the table. When it changes
//     (DROP NOT NULL, SET NOT NULL, a column added later) the files already
//     written are left alone and new ones use the new setting. A table then
//     has both, and every reader copes: Parquet readers read a REQUIRED and an
//     OPTIONAL column of one type as the same column, the Go read path goes by
//     name, and DuckLake is told the column allows NULLs (it does, in some
//     file, or may again).
//
//  4. An ABSENT column is not a NULL. A patch file carries only the columns a
//     change carried (partial.go), and a file written before ADD COLUMN lacks
//     the new one; neither holds a NULL for the column it does not have, so
//     REQUIRED says nothing about them. A file that predates ADD COLUMN ...
//     NOT NULL DEFAULT x reads the default back (State.MissingVals), and a
//     rewrite that folds it in stores x, which is not NULL.
//
// The setting is not durable. After a restart the table writes OPTIONAL until
// the caller says otherwise, which is the safe direction.

import (
	"errors"
	"fmt"
	"sort"
)

// NotNullViolation is a NULL that arrived for a column the caller declared
// NOT NULL. Nothing of the batch it was in has been written.
type NotNullViolation struct {
	Table, Column string
}

func (e *NotNullViolation) Error() string {
	return fmt.Sprintf("%s: NULL in column %q, which was declared NOT NULL (SetNotNull). "+
		"Nothing of this batch was written and no default was stored in its place: either the "+
		"source does not hold this constraint at this point of the stream, or the mirror has "+
		"been given a row the source could not hold", e.Table, e.Column)
}

// IsNotNullViolation reports whether err is, or wraps, a NotNullViolation.
func IsNotNullViolation(err error) bool {
	var v *NotNullViolation
	return errors.As(err, &v)
}

// SetNotNull states which columns the source declares NOT NULL now. It
// REPLACES what was said before: a column left out is nullable from the next
// file on, and SetNotNull() with no arguments makes every column nullable.
//
// Call it from the goroutine that applies changes, before the Apply, Snapshot
// or rewrite that should use it, and only with what is true AT THE POINT OF
// THE STREAM the mirror is at: a constraint added by a later ALTER TABLE is
// not true of the rows before it, and a NULL among them fails the batch
// (NotNullViolation). A name that is not a column of the table is remembered
// and takes effect if the column is added.
//
// It changes no file already written and is not persisted; see the top of
// this file.
func (t *Table) SetNotNull(cols ...string) {
	if len(cols) == 0 {
		t.notNull = nil
		return
	}
	t.notNull = make(map[string]bool, len(cols))
	for _, c := range cols {
		t.notNull[c] = true
	}
}

// NotNull is the columns last given to SetNotNull that the table has, sorted.
func (t *Table) NotNull() []string {
	var out []string
	for c := range t.notNull {
		if _, ok := t.Columns[c]; ok {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// required reports whether a file written now holds col as REQUIRED.
func (t *Table) required(col string) bool {
	return !legacyEncoding && !legacyTypes && t.notNull[col]
}

// checkNotNull is the NOT NULL half of the check every write makes, for a
// caller that has to know before it does something it cannot undo.
func (t *Table) checkNotNull(rows []map[string]any) error {
	var req []string
	for _, c := range t.Order {
		if t.required(c) {
			req = append(req, c)
		}
	}
	if len(req) == 0 {
		return nil
	}
	for _, r := range rows {
		for _, c := range req {
			if r[c] == nil {
				return &NotNullViolation{Table: t.Qualified, Column: c}
			}
		}
	}
	return nil
}

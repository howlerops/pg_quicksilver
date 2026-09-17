package mirror

// Temporal columns, stored as instants rather than as sentences about instants.
//
// The writer maps what it does not recognise to a string, and until now that
// included every temporal type. A query engine reading the mirror therefore saw
// `VARCHAR`, which is enough to GROUP BY and enough to ORDER BY — ISO-8601 with
// a fixed offset happens to sort correctly — and not enough for the query
// people actually write:
//
//	WHERE ts > now() - interval '1 day'
//
// That needs a cast the query does not have, and row-group statistics describe
// strings rather than instants, so nothing prunes.
//
// The precondition for fixing it was docs/22. Parsing text into an instant means
// knowing which zone and which DateStyle produced it, and until the rendering
// was pinned the honest answer was "whichever session happened to render this
// row". Now every temporal value entering the mirror is ISO-8601 in UTC, and
// parsing it is a fact rather than a guess.
//
// WHAT CANNOT BE REPRESENTED. PostgreSQL's temporal range is wider than an
// Arrow timestamp's, and it has two values that are not instants at all:
//
//   - `infinity` and `-infinity`. These round-trip exactly, as the largest and
//     smallest int64, which is how PostgreSQL stores them too.
//   - years before 1 AD (`0044-03-15 ... BC`) and beyond year 294247. These do
//     not, and the mirror HALTS rather than storing something else. A halt is
//     durable, says which table, column and value caused it, and is recovered by
//     rebuilding with QS_TEMPORAL_TYPES=0 — which restores the old text
//     behaviour for the whole mirror. Storing NULL, or a silently wrapped
//     instant, is the failure mode every other line of this package exists to
//     avoid.

import (
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// TemporalTypes is the escape hatch for a table holding values Arrow cannot
// represent. Off, temporal columns are stored as the text PostgreSQL rendered,
// which is what every mirror written before this did.
var TemporalTypes = os.Getenv("QS_TEMPORAL_TYPES") != "0"

// PostgreSQL's infinity, stored the way PostgreSQL stores it: the extremes of
// the integer domain. Arrow has no notion of an infinite instant, so these are
// ordinary values to every engine — which is the correct behaviour, because
// they sort and compare exactly as they should.
const (
	posInfinity int64 = math.MaxInt64
	negInfinity int64 = math.MinInt64
)

// temporalKind is which of the four shapes a column is, or none.
type temporalKind int

const (
	notTemporal temporalKind = iota
	kindTimestampTZ
	kindTimestamp
	kindDate
	kindTime
)

// temporalOf classifies a declared PostgreSQL type.
//
// `time with time zone` is deliberately NOT here. It has no Arrow equivalent —
// an offset without a date is not a point on any timeline — and PostgreSQL's
// own documentation recommends against the type. It stays text.
func temporalOf(pgType string) temporalKind {
	if !TemporalTypes {
		return notTemporal
	}
	s := strings.ToLower(strings.TrimSpace(pgType))
	if i := strings.Index(s, "("); i >= 0 { // timestamp(3) with time zone
		if j := strings.Index(s, ")"); j > i {
			s = strings.TrimSpace(s[:i] + s[j+1:])
		}
	}
	switch {
	case s == "timestamptz", s == "timestamp with time zone":
		return kindTimestampTZ
	case s == "timestamp", s == "timestamp without time zone":
		return kindTimestamp
	case s == "date":
		return kindDate
	case s == "time", s == "time without time zone":
		return kindTime
	}
	return notTemporal
}

func temporalArrowType(k temporalKind) arrow.DataType {
	switch k {
	case kindTimestampTZ:
		// Microseconds because that is PostgreSQL's resolution exactly; UTC
		// because docs/22 pinned every session to render in it, so the stored
		// instant needs no zone of its own to be unambiguous.
		return &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	case kindTimestamp:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case kindDate:
		return arrow.FixedWidthTypes.Date32
	case kindTime:
		return arrow.FixedWidthTypes.Time64us
	}
	return nil
}

// sqlTypeOf is what a query engine should call the column.
func temporalSQLType(k temporalKind) string {
	switch k {
	case kindTimestampTZ:
		return "TIMESTAMP WITH TIME ZONE"
	case kindTimestamp:
		return "TIMESTAMP"
	case kindDate:
		return "DATE"
	case kindTime:
		return "TIME"
	}
	return ""
}

// The layouts PostgreSQL produces once DateStyle is ISO and TimeZone is UTC.
// Ordered most-likely first; Go's parser is strict, so each must be tried.
var tsTZLayouts = []string{
	"2006-01-02 15:04:05.999999-07",
	"2006-01-02 15:04:05.999999-07:00",
	"2006-01-02 15:04:05.999999Z07:00",
}

var tsLayouts = []string{
	"2006-01-02 15:04:05.999999",
	"2006-01-02T15:04:05.999999",
}

// parseTemporal turns PostgreSQL's text into the integer Arrow stores.
//
// ok is false when the value is outside what Arrow can hold, and the caller
// halts rather than guessing. That is the whole reason this returns a bool
// instead of a zero value: a zero here is 1970-01-01, a real instant, and
// would be indistinguishable from data.
func parseTemporal(k temporalKind, s string) (int64, bool) {
	s = strings.TrimSpace(s)
	switch strings.ToLower(s) {
	case "infinity":
		return posInfinity, true
	case "-infinity":
		return negInfinity, true
	}
	switch k {
	case kindTimestampTZ:
		for _, l := range tsTZLayouts {
			if t, err := time.Parse(l, s); err == nil {
				return microsOf(t)
			}
		}
	case kindTimestamp:
		for _, l := range tsLayouts {
			if t, err := time.Parse(l, s); err == nil {
				return microsOf(t)
			}
		}
	case kindDate:
		if t, err := time.Parse("2006-01-02", s); err == nil {
			// Date32 counts DAYS, and the value is midnight UTC by
			// construction, so the division is exact rather than rounded.
			return t.Unix() / 86400, true
		}
	case kindTime:
		for _, l := range []string{"15:04:05.999999", "15:04:05"} {
			if t, err := time.Parse(l, s); err == nil {
				return int64(t.Hour())*3600_000_000 + int64(t.Minute())*60_000_000 +
					int64(t.Second())*1_000_000 + int64(t.Nanosecond())/1000, true
			}
		}
		// 24:00:00 is a legal PostgreSQL time and not a legal clock reading,
		// so Go's parser rejects it.
		if s == "24:00:00" {
			return 24 * 3600_000_000, true
		}
	}
	return 0, false
}

// microsOf guards the conversion Go will happily do wrong. UnixMicro on a year
// outside the int64 microsecond range wraps silently, and a wrapped instant is
// a plausible-looking date in the wrong millennium.
func microsOf(t time.Time) (int64, bool) {
	const maxSec = math.MaxInt64 / 1_000_000
	if t.Unix() > maxSec || t.Unix() < -maxSec {
		return 0, false
	}
	us := t.UnixMicro()
	// The sentinels are reserved. An ordinary instant landing on one is
	// astronomically unlikely and would read back as "infinity", so it is
	// nudged by a microsecond rather than lied about.
	if us == posInfinity {
		us--
	} else if us == negInfinity {
		us++
	}
	return us, true
}

// renderTemporal turns the stored integer back into exactly the text
// PostgreSQL would print for it, because that text is what the checksum
// compares and what a reader of the mirror sees.
func renderTemporal(k temporalKind, v int64) string {
	switch v {
	case posInfinity:
		return "infinity"
	case negInfinity:
		return "-infinity"
	}
	switch k {
	case kindTimestampTZ:
		// "+00" and not "+00:00": PostgreSQL omits the minutes of a
		// whole-hour offset, and the mirror's text has to match the source's
		// or every row reads as a divergence.
		return time.UnixMicro(v).UTC().Format("2006-01-02 15:04:05.999999") + "+00"
	case kindTimestamp:
		return time.UnixMicro(v).UTC().Format("2006-01-02 15:04:05.999999")
	case kindDate:
		return time.Unix(v*86400, 0).UTC().Format("2006-01-02")
	case kindTime:
		d := time.Duration(v) * time.Microsecond
		h := int(d / time.Hour)
		m := int(d/time.Minute) % 60
		sec := int(d/time.Second) % 60
		us := v % 1_000_000
		if us == 0 {
			return fmt.Sprintf("%02d:%02d:%02d", h, m, sec)
		}
		return strings.TrimRight(fmt.Sprintf("%02d:%02d:%02d.%06d", h, m, sec, us), "0")
	}
	return ""
}

// unrepresentable is the halt reason. It names the value, because the only
// useful thing an operator can do with this is look at the row.
func unrepresentable(table, col, pgType, value string) string {
	return fmt.Sprintf("halting %s: column %q (%s) holds %q, which is outside "+
		"the range an Arrow timestamp can represent. Storing it as anything "+
		"else would be silently wrong. Rebuild the mirror with "+
		"QS_TEMPORAL_TYPES=0 to store temporal columns as text instead",
		table, col, pgType, value)
}

// appendTemporal is the one shape all four builders share: parse, or refuse.
//
// A value that will not parse appends NULL here and is caught by checkTemporal
// before the file is written — the two exist separately because the builder has
// no way to report an error, and losing the reason is how a halt becomes a
// blank column.
func appendTemporal[T ~int32 | ~int64](
	add func(T), _ T, k temporalKind, v any, null func(),
) {
	s, ok := v.(string)
	if !ok {
		s = fmt.Sprint(v)
	}
	n, ok := parseTemporal(k, s)
	if !ok {
		null()
		return
	}
	add(T(n))
}

// temporalKinds classifies a column list once, and says whether any of it is
// temporal at all — which for most tables is the whole answer and costs one
// pass rather than one per row.
func (t *Table) temporalKinds(cols []string) ([]temporalKind, bool) {
	if !TemporalTypes {
		return nil, false
	}
	kinds := make([]temporalKind, len(cols))
	any := false
	for i, c := range cols {
		kinds[i] = temporalOf(t.Columns[c])
		if kinds[i] != notTemporal {
			any = true
		}
	}
	return kinds, any
}

// checkOne is the per-value half, shared by both shapes of the check.
func (t *Table) checkOne(k temporalKind, col string, v any) error {
	if k == notTemporal || v == nil {
		return nil
	}
	s, ok := v.(string)
	if !ok {
		s = fmt.Sprint(v)
	}
	if _, ok := parseTemporal(k, s); ok {
		return nil
	}
	reason := unrepresentable(t.Qualified, col, t.Columns[col], s)
	_ = t.Halt(reason)
	return fmt.Errorf("%s", reason)
}

// checkTemporalValues is the column-ordered form, for the snapshot's path.
//
// It exists because the check has to cover EVERY way into the writer. It used
// to be called from writeParquetCols only, and the snapshot does not go through
// there — so bootstrap, the one path that loads data written before the mirror
// existed and therefore the one most likely to meet a date from 44 BC, was the
// path that never looked.
func (t *Table) checkTemporalValues(vals []any, cols []string) error {
	kinds, any := t.temporalKinds(cols)
	if !any {
		return nil
	}
	for i, c := range cols {
		if i >= len(vals) {
			break
		}
		if err := t.checkOne(kinds[i], c, vals[i]); err != nil {
			return err
		}
	}
	return nil
}

// checkTemporal refuses a batch containing a temporal value Arrow cannot hold.
//
// It runs before the writer rather than inside it because the writer cannot
// report which value was the problem, and "which value" is the only part an
// operator can act on.
func (t *Table) checkTemporal(rows []map[string]any, cols []string) error {
	kinds, any := t.temporalKinds(cols)
	if !any {
		return nil
	}
	for _, r := range rows {
		for i, c := range cols {
			v, present := r[c]
			if !present {
				continue
			}
			if err := t.checkOne(kinds[i], c, v); err != nil {
				return err
			}
		}
	}
	return nil
}

package mirror

// Values in the form the writer stores them, and the refusal to store anything
// else.
//
// A row reaches the writer from two directions. From PostgreSQL every value is
// text. From Parquet — a compaction, a merge, a carry-forward — it has already
// been an integer, a float, a decimal or an instant once. Both used to be
// funnelled through text: an int64 read from a file was printed and parsed to
// append it to the next one, a decimal128 was rendered to a string and parsed
// by decimal128.FromString, a timestamp was formatted by time.Format and then
// parsed twice — once to check it, once to store it. A profile of a 1M-row
// compaction put half its CPU there.
//
// So there is now ONE conversion, storable, and it runs once per value, before
// anything is written:
//
//   - text is parsed into the native form, or refused;
//   - a value that is already native passes through untouched.
//
// The read side cooperates by handing decimals and instants back as small typed
// wrappers (decimalValue, temporalValue) rather than as strings. They print as
// exactly the text the strings held — fmt.Sprint, Render and the checksum all
// see what they saw before — but the writer can append them without parsing.
// ForEachLive turns them back into plain strings before a caller outside this
// package sees a row, so the public shape of a row has not changed.
//
// WHAT IS REFUSED. appendValue used to append NULL for anything it could not
// parse, and decimal128.FromString rounds. Measured on `numeric` (stored as
// Decimal(38,9)): 0.123456789012345 was stored as 0.123456789, 1e-12 as 0, NaN
// as NULL, a 33-digit integer as NULL. Each of those is a mirror that differs
// from its source and says nothing. They now take the path temporal.go already
// had for a date from 44 BC: the batch is refused before the file is touched
// and the mirror halts with the table, column and value in the reason.

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

// decimalValue is a decimal as Parquet stores it. Comparable, so a row holding
// one can still be compared and used wherever a string could.
type decimalValue struct {
	num         decimal128.Num
	prec, scale int32
}

// String is the text arrayValue used to return for this value, exactly.
func (d decimalValue) String() string { return d.num.ToString(d.scale) }

// temporalValue is an instant, date or time of day as Parquet stores it.
type temporalValue struct {
	k temporalKind
	v int64
}

// String is the text PostgreSQL prints for this value. See renderTemporal.
func (t temporalValue) String() string { return renderTemporal(t.k, t.v) }

// plainValue turns a typed wrapper back into the string it stands for.
func plainValue(v any) any {
	switch x := v.(type) {
	case decimalValue:
		return x.String()
	case temporalValue:
		return x.String()
	case uuidValue:
		return x.String()
	case binaryValue:
		return x.String()
	}
	return v
}

// plainRow does that for a whole row, in place. Assigning to a key that is
// already present is safe during a range.
func plainRow(r map[string]any) {
	for c, v := range r {
		switch v.(type) {
		case decimalValue, temporalValue, uuidValue, binaryValue:
			r[c] = plainValue(v)
		}
	}
}

func textOf(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func temporalKindOf(dt arrow.DataType) temporalKind {
	switch tt := dt.(type) {
	case *arrow.TimestampType:
		if tt.TimeZone != "" {
			return kindTimestampTZ
		}
		return kindTimestamp
	case *arrow.Date32Type:
		return kindDate
	case *arrow.Time64Type:
		return kindTime
	}
	return notTemporal
}

// storable returns v in the form appendNative takes for a column of type dt.
//
// ok is false when the column cannot hold the value EXACTLY. There is no third
// outcome: nothing here rounds, truncates or substitutes NULL.
func storable(dt arrow.DataType, v any) (any, bool) {
	if v == nil {
		return nil, true
	}
	switch d := dt.(type) {
	case *arrow.StringType:
		switch x := v.(type) {
		case decimalValue:
			// A numeric column that an older file held as DECIMAL(38,9) and
			// this one holds as text. The decimal lost the display scale the
			// source had (1.50 was stored as 1.500000000), so the padding is
			// dropped rather than written out as though PostgreSQL printed it.
			return normalizeNumeric(x.String()), true
		case float32:
			return pgFloat(float64(x), 32), true
		case float64:
			return pgFloat(x, 64), true
		}
		return v, true // appendNative renders whatever this is
	case *arrow.Int64Type, *arrow.Int32Type, *arrow.Int16Type:
		var n int64
		switch x := v.(type) {
		case int64:
			// Already native — a value read from a file on its way to the
			// next. Handed back as it came, so the rewrite does not box it
			// again; only the width is checked.
			switch dt.ID() {
			case arrow.INT32:
				return v, x >= math.MinInt32 && x <= math.MaxInt32
			case arrow.INT16:
				return v, x >= math.MinInt16 && x <= math.MaxInt16
			}
			return v, true
		case int:
			n = int64(x)
		case int32:
			n = int64(x)
		case int16:
			n = int64(x)
		default:
			var err error
			if n, err = strconv.ParseInt(strings.TrimSpace(textOf(v)), 10, 64); err != nil {
				return nil, false
			}
		}
		// Every integer travels as an int64 and is narrowed by the builder, so
		// the range is checked here: a value the declared width cannot hold
		// is refused, not wrapped.
		switch dt.ID() {
		case arrow.INT32:
			return n, n >= math.MinInt32 && n <= math.MaxInt32
		case arrow.INT16:
			return n, n >= math.MinInt16 && n <= math.MaxInt16
		}
		return n, true
	case *arrow.Float64Type:
		switch x := v.(type) {
		case float64:
			return v, true
		case float32:
			return float64(x), true
		}
		// ParseFloat reads NaN, Infinity and -Infinity, which float8 holds and
		// so does this column. Out of range is an error, not ±Inf.
		f, err := strconv.ParseFloat(strings.TrimSpace(textOf(v)), 64)
		return f, err == nil
	case *arrow.Float32Type:
		switch x := v.(type) {
		case float32:
			return v, true
		case float64:
			// From a file that stored this `real` as a DOUBLE. The double was
			// parsed from the float4's own shortest text, so narrowing it gives
			// the float4 back; a double that is no float4's text is refused.
			f := float32(x)
			return f, float64(f) == x || math.IsNaN(x) ||
				pgFloat(float64(f), 32) == pgFloat(x, 64)
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(textOf(v)), 32)
		return float32(f), err == nil
	case *arrow.BinaryType:
		switch x := v.(type) {
		case binaryValue:
			return v, true
		case []byte:
			return binaryValue(x), true
		}
		b, ok := parseBytea(textOf(v))
		return b, ok
	case arrow.ExtensionType:
		if d.ExtensionName() == uuidArrowType.ExtensionName() {
			if _, ok := v.(uuidValue); ok {
				return v, true
			}
			u, ok := parseUUIDValue(textOf(v))
			return u, ok
		}
	case *arrow.BooleanType:
		if _, ok := v.(bool); ok {
			return v, true
		}
		// Everything PostgreSQL's boolean input accepts, and nothing else:
		// "anything unrecognised is false" stored false for garbage.
		switch strings.ToLower(strings.TrimSpace(textOf(v))) {
		case "t", "true", "1", "y", "yes", "on":
			return true, true
		case "f", "false", "0", "n", "no", "off":
			return false, true
		}
		return nil, false
	case *arrow.Decimal128Type:
		if x, ok := v.(decimalValue); ok && x.prec == d.Precision && x.scale == d.Scale {
			return v, true
		}
		if x, ok := v.(decimalValue); ok {
			// Another scale — an older file's DECIMAL(38,9), say. Its text is
			// exact, and parseDecimalExact refuses what the new scale drops.
			v = x.String()
		}
		n, ok := parseDecimalExact(textOf(v), d.Precision, d.Scale)
		return decimalValue{num: n, prec: d.Precision, scale: d.Scale}, ok
	case *arrow.MonthDayNanoIntervalType:
		if _, ok := v.(arrow.MonthDayNanoInterval); ok {
			return v, true
		}
		iv, ok := parseInterval(textOf(v))
		return iv, ok
	}
	if k := temporalKindOf(dt); k != notTemporal {
		if x, ok := v.(temporalValue); ok && x.k == k {
			return v, true
		}
		n, ok := parseTemporal(k, textOf(v))
		if ok && k == kindDate {
			n, ok = dateDays(n)
		}
		return temporalValue{k: k, v: n}, ok
	}
	return v, true // no builder knows this type; appendValue writes NULL, as it always has
}

// parseDecimalExact reads decimal text into the integer a Decimal(prec, scale)
// column stores, or reports that the column cannot hold it.
//
// decimal128.FromString is not used because it ROUNDS to the scale: asked for
// 0.123456789012345 at scale 9 it returns 0.123456789 and no error. This
// refuses instead — a digit is only ever dropped if it is a trailing zero — and
// refuses NaN, Infinity and anything wider than the precision.
func parseDecimalExact(s string, prec, scale int32) (decimal128.Num, bool) {
	var zero decimal128.Num
	s = strings.TrimSpace(s)
	if s == "" {
		return zero, false
	}
	neg := false
	switch s[0] {
	case '-':
		neg, s = true, s[1:]
	case '+':
		s = s[1:]
	}
	var buf [48]byte
	digits := buf[:0] // significant digits, the point removed
	nfrac, exp := 0, 0
	seenDot, seenDigit := false, false
scan:
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			seenDigit = true
			if seenDot {
				nfrac++
			}
			if c != '0' || len(digits) > 0 {
				digits = append(digits, c)
			}
		case c == '.' && !seenDot:
			seenDot = true
		case (c == 'e' || c == 'E') && seenDigit:
			e, err := strconv.Atoi(s[i+1:])
			if err != nil || e > 1<<20 || e < -(1<<20) {
				return zero, false
			}
			exp = e
			break scan
		default:
			return zero, false
		}
	}
	if !seenDigit {
		return zero, false
	}
	// The value is digits × 10^(exp-nfrac); the column stores value × 10^scale.
	shift := exp - nfrac + int(scale)
	for shift < 0 && len(digits) > 0 {
		if digits[len(digits)-1] != '0' {
			return zero, false // a real digit below the scale: not exact
		}
		digits = digits[:len(digits)-1]
		shift++
	}
	if len(digits) == 0 {
		return zero, true
	}
	if shift < 0 {
		shift = 0
	}
	if prec > 38 {
		prec = 38
	}
	if len(digits)+shift > int(prec) {
		return zero, false
	}
	for ; shift > 0; shift-- {
		digits = append(digits, '0')
	}
	var n decimal128.Num
	if len(digits) <= 18 {
		var u uint64
		for _, c := range digits {
			u = u*10 + uint64(c-'0')
		}
		n = decimal128.FromU64(u)
	} else {
		b, ok := new(big.Int).SetString(string(digits), 10)
		if !ok {
			return zero, false
		}
		n = decimal128.FromBigInt(b)
	}
	if neg {
		n = n.Negate()
	}
	return n, true
}

// appendNative appends a value already in the builder's own form. It reports
// false, having appended nothing, when it is not.
func appendNative(b array.Builder, v any) bool {
	switch bb := b.(type) {
	case *array.StringBuilder:
		// pgoutput delivers every value as text, so this is the hot path and
		// fmt.Sprint on an interface holding a string is pure overhead.
		switch x := v.(type) {
		case string:
			bb.Append(x)
		case float32:
			bb.Append(pgFloat(float64(x), 32))
		case float64:
			bb.Append(pgFloat(x, 64))
		default:
			bb.Append(fmt.Sprint(v))
		}
		return true
	case *array.Int64Builder:
		if n, ok := v.(int64); ok {
			bb.Append(n)
			return true
		}
	case *array.Int32Builder:
		if n, ok := v.(int64); ok && n >= math.MinInt32 && n <= math.MaxInt32 {
			bb.Append(int32(n))
			return true
		}
	case *array.Int16Builder:
		if n, ok := v.(int64); ok && n >= math.MinInt16 && n <= math.MaxInt16 {
			bb.Append(int16(n))
			return true
		}
	case *array.Float64Builder:
		if f, ok := v.(float64); ok {
			bb.Append(f)
			return true
		}
	case *array.Float32Builder:
		if f, ok := v.(float32); ok {
			bb.Append(f)
			return true
		}
	case *array.BinaryBuilder:
		if b, ok := v.(binaryValue); ok {
			bb.Append([]byte(b))
			return true
		}
	case *extensions.UUIDBuilder:
		if u, ok := v.(uuidValue); ok {
			bb.AppendBytes(u)
			return true
		}
	case *array.BooleanBuilder:
		if x, ok := v.(bool); ok {
			bb.Append(x)
			return true
		}
	case *array.Decimal128Builder:
		if d, ok := v.(decimalValue); ok {
			dt := bb.Type().(*arrow.Decimal128Type)
			if d.prec == dt.Precision && d.scale == dt.Scale {
				bb.Append(d.num)
				return true
			}
		}
	case *array.TimestampBuilder:
		if t, ok := v.(temporalValue); ok && t.k == temporalKindOf(bb.Type()) {
			bb.Append(arrow.Timestamp(t.v))
			return true
		}
	case *array.Date32Builder:
		if t, ok := v.(temporalValue); ok && t.k == kindDate {
			bb.Append(arrow.Date32(t.v))
			return true
		}
	case *array.Time64Builder:
		if t, ok := v.(temporalValue); ok && t.k == kindTime {
			bb.Append(arrow.Time64(t.v))
			return true
		}
	case *array.MonthDayNanoIntervalBuilder:
		if iv, ok := v.(arrow.MonthDayNanoInterval); ok {
			bb.Append(iv)
			return true
		}
	}
	return false
}

// appendValue appends one value to a column.
//
// Every value the writer hands it has been through storable already, so the
// first branch is the only one that runs there. The conversion below it is for
// a caller that has not, and the NULL below that is unreachable from the
// writer: a value storable refuses never gets this far.
func appendValue(b array.Builder, dt arrow.DataType, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	if appendNative(b, v) {
		return
	}
	if c, ok := storable(dt, v); ok && c != nil && appendNative(b, c) {
		return
	}
	b.AppendNull()
}

// unstorable is the halt reason for a value that is not temporal. Like
// unrepresentable, it names the value, because looking at the row is the only
// thing an operator can do with it.
func unstorable(table, col, pgType string, dt arrow.DataType, value string) string {
	return fmt.Sprintf("halting %s: column %q (%s) holds %q, which the mirror's "+
		"column type %s cannot store exactly. Storing NULL or a rounded value "+
		"instead would be silently wrong",
		table, col, pgType, value, dt)
}

// refuse halts the mirror over one value and returns the reason as an error.
func (t *Table) refuse(schema *arrow.Schema, i int, col string, v any) error {
	dt := schema.Field(i).Type
	table, pgType := "the mirror", ""
	if t != nil {
		table, pgType = t.Qualified, t.Columns[col]
	}
	var reason string
	if temporalKindOf(dt) != notTemporal {
		reason = unrepresentable(table, col, pgType, textOf(v))
	} else {
		reason = unstorable(table, col, pgType, dt, textOf(v))
	}
	if t != nil {
		_ = t.Halt(reason)
	}
	return fmt.Errorf("%s", reason)
}

// storableValues converts one row, already in column order, IN PLACE.
//
// This is the check, and it is also the parse: what it leaves in vals is what
// the builders append, so no value is read twice.
func (t *Table) storableValues(schema *arrow.Schema, cols []string, vals []any) error {
	for i := range cols {
		if i >= len(vals) {
			break
		}
		if vals[i] == nil {
			// The one thing a REQUIRED column cannot hold. Refused here, where
			// every write is checked before it is written, and NOT a halt:
			// see nullability.go.
			if !schema.Field(i).Nullable {
				q := "the mirror"
				if t != nil {
					q = t.Qualified
				}
				return &NotNullViolation{Table: q, Column: cols[i]}
			}
			continue
		}
		c, ok := storable(schema.Field(i).Type, vals[i])
		if !ok {
			return t.refuse(schema, i, cols[i], vals[i])
		}
		vals[i] = c
	}
	if t != nil && len(t.joins) > 0 {
		return t.fillCompanions(cols, vals)
	}
	return nil
}

// storableRows flattens a batch of row maps to column order, converting as it
// goes. The maps are not modified. It returns nothing on error, so a caller
// that has not yet opened a file has nothing to clean up.
func (t *Table) storableRows(schema *arrow.Schema, cols []string, rows []map[string]any) ([][]any, error) {
	out := make([][]any, len(rows))
	flat := make([]any, len(rows)*len(cols)) // one allocation, not one per row
	for ri, r := range rows {
		vals := flat[ri*len(cols) : (ri+1)*len(cols) : (ri+1)*len(cols)]
		for i, name := range cols {
			vals[i] = r[name]
		}
		if err := t.storableValues(schema, cols, vals); err != nil {
			return nil, err
		}
		out[ri] = vals
	}
	return out, nil
}

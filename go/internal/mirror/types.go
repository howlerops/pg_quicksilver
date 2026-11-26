package mirror

// How each PostgreSQL type is stored, and what a query engine is told it is.
//
// The rule is: a column is stored in the engine's NATIVE form when that form
// holds every value of the PostgreSQL type exactly, and as the text
// PostgreSQL's OUTPUT FUNCTION prints otherwise. Nothing is stored rounded and
// nothing is stored NULL in place of a value (values.go); a value the chosen
// form cannot hold halts the table.
//
//	PostgreSQL                    Parquet / Arrow                   DuckDB reads
//	----------------------------  --------------------------------  ----------------
//	smallint                      INT32 (INT 16)   / Int16          SMALLINT
//	integer                       INT32            / Int32          INTEGER
//	bigint                        INT64            / Int64          BIGINT
//	real                          FLOAT            / Float32        FLOAT
//	double precision              DOUBLE           / Float64        DOUBLE
//	boolean                       BOOLEAN          / Boolean        BOOLEAN
//	uuid                          FLBA(16) UUID    / arrow.uuid     UUID
//	bytea                         BYTE_ARRAY       / Binary         BLOB
//	numeric(p,s), p <= 38         DECIMAL(p,s)     / Decimal128     DECIMAL(p,s)
//	numeric(p,s), p > 38          BYTE_ARRAY UTF8  / String         VARCHAR
//	numeric (unconstrained)       BYTE_ARRAY UTF8  / String         VARCHAR
//	  ... opted in, QS_NUMERIC_SCALE   DECIMAL(38,s) / Decimal128   DECIMAL(38,s)
//	date                          INT32 DATE       / Date32         DATE
//	time                          INT64 TIME(us)   / Time64         TIME
//	timestamp                     INT64 TS(us)     / Timestamp      TIMESTAMP
//	timestamptz                   INT64 TS(us,UTC) / Timestamp UTC  TIMESTAMPTZ
//	everything else               BYTE_ARRAY UTF8  / String         VARCHAR
//
// "Everything else" is text, char(n), varchar(n), name, json, jsonb, xml,
// enums, arrays, ranges, interval, time with time zone, inet, cidr, macaddr,
// bit, varbit, money, oid and any type this file has not heard of.
//
// WHY EACH ONE.
//
// Integers and floats at their DECLARED width, because the width is part of
// the answer: `integer + integer` overflows where `bigint + bigint` does not,
// sum(integer) is a different type from sum(bigint), and a `real` widened to
// DOUBLE compares unequal to the float4 literal it was written from (0.1::real
// is 0.100000001490116 as a double). Every integer used to be BIGINT and every
// float DOUBLE; bench/exactness found 208 statements whose only difference from
// PostgreSQL was that.
//
// uuid as 16 bytes: it is 128 bits, the text is 36 bytes, and on a 10M-row
// table the native form made a key lookup 4x and a join 2.3x faster. It also
// makes 'A0EE…'::uuid equal 'a0ee…'::uuid, which text does not.
//
// bytea as BINARY: the text is `\x` plus two hex digits per byte, so native is
// half the bytes before compression, and an engine sees a BLOB whose
// octet_length is the value's rather than 2n+2.
//
// numeric(p,s) as a decimal when p <= 38, which is what Decimal128 holds.
// Above that, text: there is no wider exact decimal an engine reads, and
// asking Arrow for a Decimal128(50,2) is a panic, not an error.
//
// UNCONSTRAINED numeric as text. It used to be DECIMAL(38,9), and a `numeric`
// column holds whatever it is given: 0.1234567890123, 1e-12, a 40-digit
// integer, NaN. Each of those is a value DECIMAL(38,9) cannot hold, so each
// halted the table — an ordinary column, an ordinary value, and a mirror that
// stops. No fixed scale is right for a column whose scale is per value, so
// the default stores what PostgreSQL prints, exactly, and a reader is given
// exactly that: VARCHAR, through the view and through the DuckLake alike.
//
// The view used to cast it to DOUBLE, as DuckDB's own PostgreSQL scanner
// does. That made 1e400 read as inf and a 29-digit value lose twelve digits,
// silently, while the DuckLake of the same mirror said VARCHAR. The rule is
// now one per COLUMN, decided by the declared type and the operator's setting
// and by nothing in the data, so it cannot change from one file to the next:
//
//	numeric(p,s), p <= 38                      DECIMAL(p,s)
//	numeric, numeric(p,s) with p > 38          VARCHAR, every digit
//	  ... the column named in QS_NUMERIC_SCALE DECIMAL(38,s), halts on a
//	                                           value that does not fit
//
// A reader who wants arithmetic over a VARCHAR numeric casts in the query,
// to the type whose loss it has chosen: CAST(n AS DECIMAL(38,6)) or
// CAST(n AS DOUBLE). There is no setting that makes the mirror approximate.
// See docs/16-deploying.md.
//
// char(n) as text WITH its padding: that is what the output function prints
// and what the stream delivers. There is no fixed-width character type in
// Parquet whose comparison ignores trailing blanks, so bpchar's comparison
// rule is not reproduced; the stored value is.
//
// interval stays text for the reason in interval.go; timetz, inet, cidr,
// macaddr, bit and money have no native form in Parquet at all, and enum
// needs its label order, which the mirror is not given.
//
// OLD FILES. A mirror written before this holds every integer as INT64, every
// float as DOUBLE, uuid and bytea as text and unconstrained numeric as
// DECIMAL(38,9). Nothing rewrites those files on upgrade; a table has both
// kinds until its next base rewrite. So every reader goes by the DECLARED type
// and converts what the file holds: ViewSQL casts each branch (serve.go), the
// Go read path coerces on the way out of a file (coerceFor) and storable
// accepts either form on the way into the next one.

import (
	"encoding/hex"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

// legacyTypes makes the writer produce what it produced before this file
// existed. It is set only by tests, which need a mirror in the old
// representation to run the new readers over.
var legacyTypes bool

// LegacyWriterForTest switches the writer to the representation of the
// previous release, or back. It is for tests of other packages that have to
// publish a mirror an older sidecar wrote (internal/ducklake); nothing else
// may call it.
func LegacyWriterForTest(on bool) { legacyTypes = on }

var uuidArrowType = extensions.NewUUIDType()

// maxDecimalPrecision is what a Decimal128 holds.
const maxDecimalPrecision = 38

// baseType lower-cases a declared type and strips a typmod: "numeric(10,2)"
// is "numeric", "timestamp(3) with time zone" is "timestamp with time zone".
func baseType(pgType string) (base, mod string) {
	s := strings.ToLower(strings.TrimSpace(pgType))
	if i := strings.Index(s, "("); i >= 0 {
		if j := strings.Index(s, ")"); j > i {
			return strings.TrimSpace(s[:i] + s[j+1:]), s[i+1 : j]
		}
	}
	return s, ""
}

// numericSpec reads a numeric's declared precision and scale. declared is
// false for an unconstrained `numeric`.
func numericSpec(pgType string) (prec, scale int32, declared, isNumeric bool) {
	base, mod := baseType(pgType)
	if base != "numeric" && base != "decimal" {
		return 0, 0, false, false
	}
	if mod == "" {
		return 0, 0, false, true
	}
	parts := strings.SplitN(mod, ",", 2)
	p, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, false, true
	}
	sc := 0
	if len(parts) == 2 {
		if sc, err = strconv.Atoi(strings.TrimSpace(parts[1])); err != nil {
			return 0, 0, false, true
		}
	}
	return int32(p), int32(sc), true, true
}

// arrowType maps a declared PostgreSQL type to the Arrow type it is stored
// as. The table at the top of this file is this function.
func arrowType(pgType string) arrow.DataType {
	// Temporal types first: see temporal.go for why they were text until
	// docs/22 made parsing them a fact rather than a guess.
	if k := temporalOf(pgType); k != notTemporal {
		return temporalArrowType(k)
	}
	s := strings.ToLower(strings.TrimSpace(pgType))
	// An ARRAY is text, whatever it is an array of. numeric(10,2)[] and double
	// precision[] used to come out as a decimal and a float; the value
	// {1.00,2.50} is neither, and once a value that cannot be stored exactly
	// became a halt rather than a NULL, every table with such a column stopped
	// on its first row.
	if strings.HasSuffix(s, "[]") {
		return arrow.BinaryTypes.String
	}
	if prec, scale, declared, ok := numericSpec(s); ok {
		if legacyTypes {
			if !declared {
				prec, scale = 38, 9
			}
			return &arrow.Decimal128Type{Precision: prec, Scale: scale}
		}
		// Text when no Decimal128 holds the type: unconstrained, wider than 38
		// digits, or a scale outside the precision (numeric(3,-2), numeric(2,5)),
		// which PostgreSQL allows and Arrow's decimal does not describe.
		if !declared || prec < 1 || prec > maxDecimalPrecision || scale < 0 || scale > prec {
			return arrow.BinaryTypes.String
		}
		return &arrow.Decimal128Type{Precision: prec, Scale: scale}
	}
	base, _ := baseType(s)
	if legacyTypes {
		switch base {
		case "smallint", "int", "integer", "bigint", "int2", "int4", "int8":
			return arrow.PrimitiveTypes.Int64
		case "bool", "boolean":
			return arrow.FixedWidthTypes.Boolean
		case "real", "double precision", "double":
			return arrow.PrimitiveTypes.Float64
		}
		return arrow.BinaryTypes.String
	}
	switch base {
	case "smallint", "int2":
		return arrow.PrimitiveTypes.Int16
	case "int", "integer", "int4":
		return arrow.PrimitiveTypes.Int32
	case "bigint", "int8":
		return arrow.PrimitiveTypes.Int64
	case "bool", "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "real", "float4":
		return arrow.PrimitiveTypes.Float32
	case "double precision", "double", "float8":
		return arrow.PrimitiveTypes.Float64
	case "uuid":
		return uuidArrowType
	case "bytea":
		return arrow.BinaryTypes.Binary
	}
	return arrow.BinaryTypes.String
}

func isIntegerType(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT16, arrow.INT32, arrow.INT64:
		return true
	}
	return false
}

// ---- operator overrides ------------------------------------------------------

// NumericScale opts unconstrained `numeric` columns into DECIMAL(38,s).
//
//	QS_NUMERIC_SCALE=public.orders.amount=4,public.fx.rate=12
//	QS_NUMERIC_SCALE=9          every unconstrained numeric, as before this change
//
// A column named here is stored as a decimal an engine can aggregate without
// parsing, and HALTS on a value with more fractional digits than s, more than
// 38-s integer digits, NaN or Infinity. A column not named is stored as text
// and halts on nothing.
//
// Naming a column that already holds data does not take effect by itself:
// the stored values are checked first, and the option is refused for a
// column one of them does not fit. See numeric_optin.go.
var NumericScale = os.Getenv("QS_NUMERIC_SCALE")

// numericScaleFor is the opted-in scale for one column, if any.
func numericScaleFor(spec, qualified, col string) (int32, bool) {
	def, hasDef := int32(0), false
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, val := "", part
		if i := strings.LastIndex(part, "="); i >= 0 {
			name, val = strings.TrimSpace(part[:i]), strings.TrimSpace(part[i+1:])
		}
		n, err := strconv.Atoi(val)
		if err != nil || n < 0 || n > maxDecimalPrecision {
			continue
		}
		if name == "" {
			def, hasDef = int32(n), true
		} else if name == qualified+"."+col {
			return int32(n), true
		}
	}
	return def, hasDef
}

// typeOf is the declared type of a column as the mirror stores it: the
// catalog's, except that an unconstrained numeric the operator opted in, and
// whose stored values have been shown to fit (numeric_optin.go), reads as
// numeric(38,s). Everything that decides a representation asks this rather
// than t.Columns.
func (t *Table) typeOf(col string) string {
	pg := t.Columns[col]
	if NumericScale == "" {
		return pg
	}
	if s, ok := t.optScale(col); ok && t.optedIn(col, s) {
		return "numeric(38," + strconv.Itoa(int(s)) + ")"
	}
	return pg
}

// BloomColumns overrides which columns get a Parquet bloom filter.
//
//	QS_BLOOM_COLUMNS=public.orders=customer_id,sku;public.events=-
//
// By default a data file carries one on the primary-key column and on every
// other uuid column, which is the cheap stand-in for "foreign key": the
// mirror is not told the constraints, and a uuid column that is not the key is
// nearly always a reference to one. A table listed here gets exactly the
// columns listed (`-` for none); QS_BLOOM_COLUMNS=off turns them off
// everywhere.
//
// What it buys: a filter says "this row group does not hold that value" for a
// value INSIDE the group's min/max range, which for a random key is every
// group. Measured on 10M uuid-keyed rows, a key lookup through DuckDB went
// from ~600 ms to ~23 ms, 151 of 153 row groups skipped. What it costs is in
// docs/16-deploying.md: about two bytes per row per filtered column.
var BloomColumns = os.Getenv("QS_BLOOM_COLUMNS")

// bloomColumnsFor returns the columns of this table that get a bloom filter.
func (t *Table) bloomColumnsFor(cols []string) []string {
	if legacyTypes {
		return nil
	}
	spec := strings.TrimSpace(BloomColumns)
	if spec == "off" || spec == "0" || spec == "-" {
		return nil
	}
	for _, part := range strings.Split(spec, ";") {
		name, list, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || strings.TrimSpace(name) != t.Qualified {
			continue
		}
		var out []string
		for _, c := range strings.Split(list, ",") {
			if c = strings.TrimSpace(c); c != "" && c != "-" && containsStr(cols, c) {
				out = append(out, c)
			}
		}
		return out
	}
	var out []string
	for _, c := range cols {
		if c == t.Key || arrowType(t.typeOf(c)) == arrow.DataType(uuidArrowType) {
			out = append(out, c)
		}
	}
	return out
}

// bloomMaxBytes is the largest filter a column chunk gets: enough for a full
// row group of distinct values at a 1% false-positive rate (about 10.5 bits a
// value in a block-split filter), rounded up to a power of two. Smaller
// chunks get smaller filters; see newParquetWriterFor.
func bloomMaxBytes() int64 {
	want := RowGroupRows * 12 / 8
	n := int64(64)
	for n < want && n < 8<<20 {
		n <<= 1
	}
	return n
}

// ---- values that are neither a number nor text ------------------------------

// uuidValue is a uuid as the file stores it. Comparable, like every value a
// row holds, and it prints as PostgreSQL prints a uuid.
type uuidValue [16]byte

func (u uuidValue) String() string {
	hi := uint64(u[0])<<56 | uint64(u[1])<<48 | uint64(u[2])<<40 | uint64(u[3])<<32 |
		uint64(u[4])<<24 | uint64(u[5])<<16 | uint64(u[6])<<8 | uint64(u[7])
	lo := uint64(u[8])<<56 | uint64(u[9])<<48 | uint64(u[10])<<40 | uint64(u[11])<<32 |
		uint64(u[12])<<24 | uint64(u[13])<<16 | uint64(u[14])<<8 | uint64(u[15])
	return formatUUID(hi, lo)
}

func (u uuidValue) halves() (hi, lo uint64) {
	for i := 0; i < 8; i++ {
		hi = hi<<8 | uint64(u[i])
		lo = lo<<8 | uint64(u[8+i])
	}
	return hi, lo
}

// parseUUIDValue reads a uuid in any spelling PostgreSQL's input accepts for
// the digits — either case, with or without hyphens or braces. The output
// function only ever prints the canonical one; the others reach here from a
// MissingVals default or a test, and a uuid column is a uuid whatever its case.
func parseUUIDValue(s string) (uuidValue, bool) {
	var u uuidValue
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '{' && s[len(s)-1] == '}' {
		s = s[1 : len(s)-1]
	}
	n := 0
	var cur byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			d = c - 'A' + 10
		case c == '-':
			continue
		default:
			return u, false
		}
		if n >= 32 {
			return u, false
		}
		if n%2 == 0 {
			cur = d << 4
		} else {
			u[n/2] = cur | d
		}
		n++
	}
	return u, n == 32
}

// binaryValue is a bytea as the file stores it: the bytes, held in a string so
// that a row stays comparable. It prints as PostgreSQL prints a bytea under
// bytea_output=hex, which internal/pgtext pins.
type binaryValue string

func (b binaryValue) String() string {
	out := make([]byte, 2+2*len(b))
	out[0], out[1] = '\\', 'x'
	hex.Encode(out[2:], []byte(b))
	return string(out)
}

// parseBytea reads PostgreSQL's hex form. The escape form is not read: pgtext
// pins hex on every connection, and guessing at a value that does not start
// with \x would store bytes nobody wrote.
func parseBytea(s string) (binaryValue, bool) {
	if len(s) < 2 || s[0] != '\\' || s[1] != 'x' {
		return "", false
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil {
		return "", false
	}
	return binaryValue(b), true
}

// pgFloat renders a float as PostgreSQL's float8out / float4out do at
// extra_float_digits >= 1: the shortest digits that round-trip, in positional
// notation unless the decimal exponent is below -4 or at least 15 (6 for a
// float4), `Infinity` rather than +Inf.
//
// strconv's 'g' format was used here and switches to an exponent at 21
// digits, so 1000000 rendered as 1e+06 and every such value in a float8
// column read as a divergence from its source.
func pgFloat(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	e := strconv.FormatFloat(f, 'e', -1, bits)
	i := strings.LastIndexByte(e, 'e')
	exp, _ := strconv.Atoi(e[i+1:])
	limit := 15
	if bits == 32 {
		limit = 6
	}
	if exp < -4 || exp >= limit {
		return e
	}
	return strconv.FormatFloat(f, 'f', -1, bits)
}

// coerceFor returns the conversion to apply to a value read from a file
// column of type have, for a column the table declares as want — or nil when
// the file already holds the declared form.
//
// This is the read half of coexistence. An old file gives back a float8 for a
// `real`, text for a uuid or a bytea; left alone those would render
// differently from the same value read from a new file (1e+06 against
// 1000000), key differently, and compare as a divergence that is only a
// change of spelling.
func coerceFor(want, have arrow.DataType) func(any) any {
	switch want.ID() {
	case arrow.FLOAT32:
		if have.ID() == arrow.FLOAT64 {
			return func(v any) any {
				if f, ok := v.(float64); ok {
					return float32(f)
				}
				return v
			}
		}
	case arrow.EXTENSION:
		if have.ID() == arrow.STRING {
			return func(v any) any {
				if s, ok := v.(string); ok {
					if u, ok := parseUUIDValue(s); ok {
						return u
					}
				}
				return v
			}
		}
	case arrow.STRING:
		// A numeric an older file holds as DECIMAL(38,9) and the table now
		// declares as text. The decimal lost the display scale the source
		// had, so its padding is dropped HERE, for every reader: storable did
		// it only for a value that reached it still typed, and a rewrite
		// through Live handed it "1.500000000" as a string, which was then
		// stored as though PostgreSQL had printed it.
		if have.ID() == arrow.DECIMAL128 {
			return func(v any) any {
				if x, ok := v.(decimalValue); ok {
					return normalizeNumeric(x.String())
				}
				return v
			}
		}
	case arrow.BINARY:
		if have.ID() == arrow.STRING {
			return func(v any) any {
				if s, ok := v.(string); ok {
					if b, ok := parseBytea(s); ok {
						return b
					}
				}
				return v
			}
		}
	}
	return nil
}

// coercions is coerceFor for the columns of one file, or nil if none needs
// converting — which is every file the current writer produced.
func (t *Table) coercions(schema *arrow.Schema, colIdx []int, colName []string) []func(any) any {
	var out []func(any) any
	for i, name := range colName {
		if _, ok := t.Columns[name]; !ok {
			continue
		}
		if f := coerceFor(arrowType(t.typeOf(name)), schema.Field(colIdx[i]).Type); f != nil {
			if out == nil {
				out = make([]func(any) any, len(colName))
			}
			out[i] = f
		}
	}
	return out
}

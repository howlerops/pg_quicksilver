package ducklake

// What the catalog says about a data file, read from the file's own footer.
//
// The prototype showed each file to a scratch DuckLake and copied the rows it
// produced. Everything in those rows is in the Parquet footer the mirror has
// just written: the row count, and per column the compressed size, the null
// count and the min and max of every row group.
//
// The statistics are what DuckLake prunes files with, so a wrong one is a
// wrong answer with no error. The rule here is that a bound is written only
// when it is known to be one, in the text form DuckDB casts back to the
// column's type, and is otherwise left NULL — which DuckLake reads as "no
// bound". Floating point columns get none at all: Parquet's min and max leave
// NaN out, and DuckDB orders NaN above everything.
//
// The column TYPES come from here too, not from the mirror's declared
// PostgreSQL types: what a reader has to be told is what is in the file.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"os"
	"reflect"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type fileInfo struct {
	rows, size, footer, rowGroups int64
	cols                          []fileColumn
}

type fileColumn struct {
	name string
	typ  string // DuckLake type name
	columnStat
}

func (fi *fileInfo) types() map[string]string {
	out := make(map[string]string, len(fi.cols))
	for _, c := range fi.cols {
		out[c.name] = c.typ
	}
	return out
}

// readFileInfo reads a Parquet file's footer and nothing else.
func readFileInfo(path string) (*fileInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fi := &fileInfo{size: st.Size()}
	if fi.footer, err = footerSize(f, st.Size()); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	rdr, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	defer rdr.Close()
	md := rdr.MetaData()
	fi.rows, fi.rowGroups = md.NumRows, int64(md.NumRowGroups())
	sc := md.Schema
	for i := 0; i < sc.NumColumns(); i++ {
		col := sc.Column(i)
		if col.MaxRepetitionLevel() > 0 || len(col.ColumnPath()) != 1 {
			return nil, fmt.Errorf("%s: column %s is nested, which this publisher does not map", path, col.Path())
		}
		kind := kindOf(col)
		fc := fileColumn{name: col.Name(), typ: kind.typ}
		fc.values = fi.rows
		fc.nullsKnown = true
		var min, max []byte
		bounded := kind.format != nil
		for g := 0; g < md.NumRowGroups(); g++ {
			cc, err := md.RowGroup(g).ColumnChunk(i)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			fc.sizeBytes += cc.TotalCompressedSize()
			set, err := cc.StatsSet()
			if err != nil || !set {
				fc.nullsKnown, bounded = false, false
				continue
			}
			stats, err := cc.Statistics()
			if err != nil || stats == nil {
				fc.nullsKnown, bounded = false, false
				continue
			}
			if stats.HasNullCount() {
				fc.nulls += stats.NullCount()
			} else {
				fc.nullsKnown = false
			}
			if cc.NumValues() == 0 || (stats.HasNullCount() && stats.NullCount() == cc.NumValues()) {
				continue // nothing but NULLs: no bound to widen with
			}
			if !stats.HasMinMax() {
				bounded = false
				continue
			}
			if !bounded {
				continue
			}
			lo, hi := stats.EncodeMin(), stats.EncodeMax()
			if min == nil || kind.less(lo, min) {
				min = append([]byte(nil), lo...)
			}
			if max == nil || kind.less(max, hi) {
				max = append([]byte(nil), hi...)
			}
		}
		if bounded && min != nil && max != nil {
			lo, ok1 := kind.format(min)
			hi, ok2 := kind.format(max)
			if ok1 && ok2 {
				fc.min, fc.max = &lo, &hi
			}
		}
		// value_count is the values that are not NULL: for a 24-row file
		// with 5 NULLs DuckDB writes 19 and 5.
		if fc.nullsKnown {
			fc.values = fi.rows - fc.nulls
		}
		fi.cols = append(fi.cols, fc)
	}
	return fi, nil
}

// footerSize is the length of the Thrift footer: the four bytes before the
// trailing magic.
func footerSize(f io.ReaderAt, size int64) (int64, error) {
	var tail [8]byte
	if size < 12 {
		return 0, fmt.Errorf("not a Parquet file: %d bytes", size)
	}
	if _, err := f.ReadAt(tail[:], size-8); err != nil {
		return 0, err
	}
	if string(tail[4:]) != "PAR1" {
		return 0, fmt.Errorf("not a Parquet file: no trailing magic")
	}
	return int64(binary.LittleEndian.Uint32(tail[:4])), nil
}

// kind is how one Parquet column appears in the catalog: its DuckLake type,
// and how its plain-encoded statistics compare and print. format == nil means
// no bounds are written for it.
type kind struct {
	typ    string
	less   func(a, b []byte) bool
	format func(b []byte) (string, bool)
}

func kindOf(col *schema.Column) kind {
	lt := deref(col.LogicalType())
	switch col.PhysicalType() {
	case parquet.Types.Boolean:
		return kind{"boolean", func(a, b []byte) bool { return a[0] == 0 && b[0] != 0 },
			func(b []byte) (string, bool) {
				if b[0] == 0 {
					return "false", true
				}
				return "true", true
			}}
	case parquet.Types.Int32, parquet.Types.Int64:
		wide := col.PhysicalType() == parquet.Types.Int64
		val := func(b []byte) int64 {
			if wide {
				return int64(binary.LittleEndian.Uint64(b))
			}
			return int64(int32(binary.LittleEndian.Uint32(b)))
		}
		less := func(a, b []byte) bool { return val(a) < val(b) }
		num := func(b []byte) (string, bool) { return fmt.Sprint(val(b)), true }
		name := "int32"
		if wide {
			name = "int64"
		}
		switch t := lt.(type) {
		case schema.IntLogicalType:
			if !t.IsSigned() {
				mask := ^uint64(0)
				if t.BitWidth() < 64 {
					mask = uint64(1)<<uint(t.BitWidth()) - 1
				}
				uval := func(b []byte) uint64 { return uint64(val(b)) & mask }
				return kind{fmt.Sprintf("uint%d", t.BitWidth()),
					func(a, b []byte) bool { return uval(a) < uval(b) },
					func(b []byte) (string, bool) { return fmt.Sprint(uval(b)), true }}
			}
			return kind{fmt.Sprintf("int%d", t.BitWidth()), less, num}
		case schema.DecimalLogicalType:
			return kind{fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale()), less,
				func(b []byte) (string, bool) { return decimalText(big.NewInt(val(b)), t.Scale()), true }}
		case schema.DateLogicalType:
			return kind{"date", less, func(b []byte) (string, bool) {
				return timeText(time.Unix(val(b)*86400, 0), "2006-01-02", "")
			}}
		case schema.TimeLogicalType:
			return kind{typ: "time"}
		case schema.TimestampLogicalType:
			return timestampKind(t, less, val)
		}
		return kind{name, less, num}
	case parquet.Types.Int96:
		return kind{typ: "timestamp"}
	case parquet.Types.Float:
		return kind{typ: "float32"}
	case parquet.Types.Double:
		return kind{typ: "float64"}
	case parquet.Types.ByteArray, parquet.Types.FixedLenByteArray:
		unsigned := func(a, b []byte) bool { return bytes.Compare(a, b) < 0 }
		switch t := lt.(type) {
		case schema.DecimalLogicalType:
			val := func(b []byte) *big.Int {
				v := new(big.Int).SetBytes(b)
				if len(b) > 0 && b[0]&0x80 != 0 {
					v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(8*len(b))))
				}
				return v
			}
			return kind{fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale()),
				func(a, b []byte) bool { return val(a).Cmp(val(b)) < 0 },
				func(b []byte) (string, bool) { return decimalText(val(b), t.Scale()), true }}
		case schema.UUIDLogicalType:
			return kind{"uuid", unsigned, func(b []byte) (string, bool) {
				if len(b) != 16 {
					return "", false
				}
				h := hex.EncodeToString(b)
				return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], true
			}}
		case schema.StringLogicalType, schema.JSONLogicalType, schema.EnumLogicalType:
			return kind{"varchar", unsigned, func(b []byte) (string, bool) {
				return string(b), utf8.Valid(b) && !bytes.ContainsRune(b, 0)
			}}
		}
		if col.ConvertedType() == schema.ConvertedTypes.UTF8 {
			return kind{"varchar", unsigned, func(b []byte) (string, bool) {
				return string(b), utf8.Valid(b) && !bytes.ContainsRune(b, 0)
			}}
		}
		return kind{typ: "blob"}
	}
	return kind{typ: "blob"}
}

func timestampKind(t schema.TimestampLogicalType, less func(a, b []byte) bool, val func([]byte) int64) kind {
	utc := t.IsAdjustedToUTC()
	switch t.TimeUnit() {
	case schema.TimeUnitMicros:
		k := kind{"timestamp", less, func(b []byte) (string, bool) {
			us := val(b)
			sec, frac := us/1e6, us%1e6
			if frac < 0 {
				sec, frac = sec-1, frac+1e6
			}
			suffix := ""
			if utc {
				suffix = "+00"
			}
			return timeText(time.Unix(sec, frac*1000), "2006-01-02 15:04:05.000000", suffix)
		}}
		if utc {
			k.typ = "timestamptz"
		}
		return k
	case schema.TimeUnitMillis:
		if utc {
			return kind{typ: "timestamptz"}
		}
		return kind{typ: "timestamp_ms"}
	case schema.TimeUnitNanos:
		if utc {
			return kind{typ: "timestamptz"}
		}
		return kind{typ: "timestamp_ns"}
	}
	return kind{typ: "timestamp"}
}

// timeText prints a moment for DuckDB to cast back. Years DuckDB and Go print
// differently (before 1, after 9999) get no bound rather than a wrong one.
func timeText(t time.Time, layout, suffix string) (string, bool) {
	t = t.UTC()
	if y := t.Year(); y < 1 || y > 9999 {
		return "", false
	}
	return t.Format(layout) + suffix, true
}

func decimalText(v *big.Int, scale int32) string {
	neg := v.Sign() < 0
	s := new(big.Int).Abs(v).String()
	if scale > 0 {
		for int32(len(s)) <= scale {
			s = "0" + s
		}
		s = s[:len(s)-int(scale)] + "." + s[len(s)-int(scale):]
	}
	if neg {
		s = "-" + s
	}
	return s
}

// typeOfArrow is the DuckLake type of a column no file holds yet, from what
// the mirror says it will write. It has to agree with kindOf for the same
// column once a file exists; where it cannot know, widen settles it.
func typeOfArrow(t arrow.DataType) string {
	switch t := t.(type) {
	case *arrow.BooleanType:
		return "boolean"
	case *arrow.Int8Type:
		return "int8"
	case *arrow.Int16Type:
		return "int16"
	case *arrow.Int32Type:
		return "int32"
	case *arrow.Int64Type:
		return "int64"
	case *arrow.Uint8Type:
		return "uint8"
	case *arrow.Uint16Type:
		return "uint16"
	case *arrow.Uint32Type:
		return "uint32"
	case *arrow.Uint64Type:
		return "uint64"
	case *arrow.Float32Type:
		return "float32"
	case *arrow.Float64Type:
		return "float64"
	case arrow.DecimalType:
		return fmt.Sprintf("decimal(%d,%d)", t.GetPrecision(), t.GetScale())
	case *arrow.Date32Type, *arrow.Date64Type:
		return "date"
	case *arrow.Time32Type, *arrow.Time64Type:
		return "time"
	case *arrow.TimestampType:
		if t.TimeZone != "" {
			return "timestamptz"
		}
		switch t.Unit {
		case arrow.Millisecond:
			return "timestamp_ms"
		case arrow.Nanosecond:
			return "timestamp_ns"
		}
		return "timestamp"
	case *arrow.StringType, *arrow.LargeStringType:
		return "varchar"
	case *arrow.BinaryType, *arrow.LargeBinaryType:
		return "blob"
	case *arrow.FixedSizeBinaryType:
		return "blob"
	case arrow.ExtensionType:
		if t.ExtensionName() == "arrow.uuid" {
			return "uuid"
		}
		return typeOfArrow(t.StorageType())
	}
	return "varchar"
}

var intRank = map[string]int{"int8": 1, "int16": 2, "int32": 3, "int64": 4}

// widen is the one column type under which files of type a and of type b can
// both be read, if there is one. DuckLake reads a file whose integer (or
// float) is narrower than the column; the other way round it does not:
// registering a BIGINT file under an INTEGER column is refused with "Expected
// one of INTEGER, USMALLINT, SMALLINT, UTINYINT, TINYINT, found type BIGINT".
func widen(a, b string) (string, bool) {
	if a == b {
		return a, true
	}
	if ra, rb := intRank[a], intRank[b]; ra > 0 && rb > 0 {
		if ra > rb {
			return a, true
		}
		return b, true
	}
	if (a == "float32" && b == "float64") || (a == "float64" && b == "float32") {
		return "float64", true
	}
	// A uuid the mirror used to write as text, and now writes as 16 bytes.
	// DuckLake's registration function would refuse the text file under a
	// UUID column, but its reader casts it, filters included (DuckDB 2.0.1,
	// TestFilesOfDifferentPhysicalTypes), and the text bounds order as the
	// uuids do.
	if (a == "uuid" && b == "varchar") || (a == "varchar" && b == "uuid") {
		return "uuid", true
	}
	return "", false
}

// deref turns a logical type held by pointer into the value: arrow-go hands
// out both, depending on the type and on whether it was built or read.
func deref(lt schema.LogicalType) schema.LogicalType {
	v := reflect.ValueOf(lt)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		if e, ok := v.Elem().Interface().(schema.LogicalType); ok {
			return e
		}
	}
	return lt
}

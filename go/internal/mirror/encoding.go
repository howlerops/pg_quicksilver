package mirror

// How each column is ENCODED, which is a different question from how it is
// typed (types.go) and is decided per file, per column.
//
// Until this file every column was written the way arrow-go writes by default
// and the way this package asked for globally: a dictionary where one fits,
// PLAIN where it does not, zstd level 1 over the result, every column
// OPTIONAL, and a DECIMAL as a fixed-length byte array. That is the smallest
// file for text, and for numbers it is the slowest thing a reader can be
// handed: DuckDB decompresses the page, decodes an index per value, looks the
// value up, and for a decimal assembles it from big-endian bytes.
//
// THE RULE. It is a function of the column's declared type and of the rows of
// the file's FIRST ROW GROUP and of nothing else, so the same rows written
// twice produce the same file.
//
//	numeric(p,s), p <= 18      stored on INT64 (p <= 9: INT32), not on a
//	                           fixed-length byte array. Then as an integer.
//	smallint, integer, bigint, DELTA_BINARY_PACKED, UNCOMPRESSED, no
//	date, time, timestamp(tz), dictionary, when MORE THAN A QUARTER of the
//	numeric(p <= 18)           first row group's non-NULL values are distinct
//	                           (deltaMinShare). Otherwise as before:
//	                           dictionary, zstd.
//	everything else            as before: dictionary where one fits, zstd.
//	                           That is text, jsonb, bytea, uuid, boolean, the
//	                           floats and numeric wider than 18 digits.
//
// WHY. All of it was measured with DuckDB 2.0.1 on one core over 10M rows
// (the experiment this change implements, and TestEncodingCrossover for the
// threshold):
//
//   - sum(amount) over a numeric(12,2): 104 ms of CPU as a zstd-compressed
//     dictionary of byte arrays, 90 as a dictionary of INT64, 50 with that
//     dictionary uncompressed, 38 as DELTA_BINARY_PACKED uncompressed. The
//     file was 3% SMALLER: delta-packing is the compression, and zstd over
//     bit-packed integers finds almost nothing more.
//   - a timestamp that rises with the key: 83 MB as a dictionary (which
//     overflows and falls back to PLAIN), 37 MB delta-packed; a month filter
//     20 ms -> 16.
//   - a tenant id with 200 values, delta-packed: every query SLOWER (sum 104
//     -> 139 ms) and the file no smaller. A column with few values is what a
//     dictionary is for, and an engine aggregates over the dictionary's
//     indices without decoding them. Hence the threshold.
//
// WHERE THE THRESHOLD IS. TestEncodingCrossover writes 2M rows of one bigint
// column both ways at each cardinality, in random order; `wide` draws the
// values from 40 bits, `narrow` numbers them 0..d-1. Column bytes, then CPU ms
// for sum(v) and for GROUP BY v (best of 5), dictionary+zstd / delta:
//
//	distinct   in a 65,536 group   wide: MB      sum      group by    narrow: MB   sum
//	     256    0.4%               2.2 / 10.9    6 / 10    7 / 18     2.1 / 2.5     6 / 7
//	   2,048    3%                 3.3 / 10.9    8 / 10   10 / 20     3.0 / 3.3     9 / 7
//	   8,192   12%                 5.0 / 10.9    8 / 11   20 / 24     3.9 / 3.8    12 / 7
//	  16,384   25%                 6.8 / 10.9   14 / 11   22 / 27     4.7 / 4.1    15 / 8
//	  32,768   43%                 9.4 / 10.9   18 / 11   43 / 30     5.8 / 4.3    17 / 8
//	  65,536   63%                12.1 / 10.9   23 / 10   53 / 34     6.9 / 4.6    24 / 8
//	      2M   97%                16.4 / 10.9   26 / 10  117 / 91    11.5 / 5.6    38 / 8
//
// Up to a quarter of the group distinct, the dictionary is as fast to read and
// up to five times smaller when the values are spread out; from there on
// delta-packing is about twice as fast and the size is within a sixth either
// way. A quarter is where the two lines cross for the spread-out column, and
// costs the dense one 3% of its bytes at the worst point (12%).
//
// Not done, because each was measured and did not help: larger row groups
// (a single-row fetch 14 -> 66 ms), smaller pages, v2 data pages,
// BYTE_STREAM_SPLIT (DuckDB 2.0.1 refuses it on a fixed-length decimal),
// splitting the base into many files.
//
// The first row group decides for the file because the writer's properties
// are fixed when it is opened, and it is opened when its first row group is
// ready: 65,536 rows of a snapshot or a rewrite, the whole of a delta. A
// column whose character changes after that is still read correctly; it is
// only encoded the way its beginning suggested.
//
// OLD FILES are not rewritten. A table has files of both kinds until its next
// base rewrite, and to every reader they are the same columns: the logical
// type in the footer is the same DECIMAL(p,s), TIMESTAMP or INTEGER, and the
// encoding of a page is something a Parquet reader reads from the page.

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
)

// WriterFormat names the physical form of the files this writer produces, for
// whoever records it (the manifest has no per-file format field at the time
// of writing; when it has, this is the value to store).
//
//	1  dictionary + zstd everywhere, DECIMAL as FIXED_LEN_BYTE_ARRAY, every
//	   column OPTIONAL, no exactness flags on statistics
//	2  this file: integer-backed decimals, DELTA_BINARY_PACKED per the rule
//	   above, REQUIRED for declared NOT NULL, min/max marked exact
const WriterFormat = 2

// legacyEncoding makes the writer produce format 1, with the current types.
// It is set only by tests, which need a mirror the previous release wrote to
// run this release over. legacyTypes (an older release still) implies it.
var legacyEncoding bool

// LegacyEncodingForTest switches the writer to WriterFormat 1, or back. It is
// for tests, here and in internal/ducklake; nothing else may call it.
func LegacyEncodingForTest(on bool) { legacyEncoding = on }

// maxIntDecimalPrecision is the widest DECIMAL an INT64 holds.
const maxIntDecimalPrecision = 18

// deltaMinShare is the threshold between a dictionary and delta-packing: a
// column is delta-packed when its distinct values are more than one in
// deltaMinShare of its values. See the table above.
const deltaMinShare = 4

// forceEncoding overrides the rule for every integer-backed column: "delta"
// or "dict". Tests only (the crossover measurement needs both forms of one
// column).
var forceEncoding string

// integerBacked reports whether a column of this type is stored on a Parquet
// INT32 or INT64 by the current writer.
func integerBacked(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.INT16, arrow.INT32, arrow.INT64, arrow.DATE32, arrow.TIME64, arrow.TIMESTAMP:
		return true
	case arrow.DECIMAL128:
		return dt.(*arrow.Decimal128Type).Precision <= maxIntDecimalPrecision
	}
	return false
}

// deltaPacked applies the rule to one column: n non-NULL values in the first
// row group, of which `distinct` are different (counted only as far as the
// rule needs).
func deltaPacked(distinct, n int) bool {
	switch forceEncoding {
	case "delta":
		return true
	case "dict":
		return false
	}
	return distinct*deltaMinShare > n
}

// encodingProperties is the per-column part of the writer's properties for a
// file whose first row group is `sample` (rows in column order, already
// converted by storable; nil for a file with no rows).
func encodingProperties(schema *arrow.Schema, sample [][]any) []parquet.WriterProperty {
	if legacyEncoding || legacyTypes {
		return nil
	}
	opts := []parquet.WriterProperty{parquet.WithStoreDecimalAsInteger(true)}
	seen := map[any]struct{}{}
	for i, f := range schema.Fields() {
		if !integerBacked(f.Type) {
			continue
		}
		clear(seen)
		n := 0
		for _, row := range sample {
			if v := row[i]; v != nil {
				n++
				// Past the threshold the count no longer matters, and a
				// column of 65,536 different values is the common case here.
				if len(seen)*deltaMinShare <= len(sample) {
					seen[v] = struct{}{}
				}
			}
		}
		if !deltaPacked(len(seen), n) {
			continue
		}
		opts = append(opts,
			parquet.WithDictionaryFor(f.Name, false),
			parquet.WithEncodingFor(f.Name, parquet.Encodings.DeltaBinaryPacked),
			parquet.WithCompressionFor(f.Name, compress.Codecs.Uncompressed))
	}
	return opts
}

// exactStatistics reports whether the min and max arrow-go computes for a
// column chunk of this type ARE its smallest and largest value, so that the
// footer may say so (markExact).
//
// They are for every type whose statistics are the values themselves compared
// in the type's own order: integers, dates, times, timestamps, decimals
// (integer-backed and fixed-length), booleans and uuid. They are computed
// over every value of the chunk and are never truncated.
//
// NOT floats: the format leaves NaN out of min and max, and writes -0.0 and
// +0.0 as bounds rather than as values; a chunk holding a NaN has its
// statistics removed altogether (dropNaNStatistics), and the others are left
// as bounds. NOT text or bytea: arrow-go omits an over-long bound rather than
// truncating it, so what it writes is exact today, but nothing here depends
// on that staying true, and no engine answers min(text) from a footer.
func exactStatistics(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.BOOL, arrow.INT16, arrow.INT32, arrow.INT64, arrow.DATE32, arrow.TIME64,
		arrow.TIMESTAMP, arrow.DECIMAL128:
		return true
	case arrow.EXTENSION:
		return dt == arrow.DataType(uuidArrowType)
	}
	return false
}

package ducklake

// A DuckLake delete file, written straight from a dead set.
//
// The mirror's own deletion vector is one column, p. Registered as it is,
// DuckLake answers count(*) from the catalog — correctly — and fails every
// scan with "Invalid schema contained in the delete file" (go/ducklake/probe.py).
// What it reads is (file_path VARCHAR, pos BIGINT). The prototype got there by
// having DuckDB copy the vector, 40 ms of a 129 ms publish; the positions are
// already in memory as a bitmap, so here they are written once, from that.
//
// file_path is the same string on every row and is dictionary-encoded to one
// entry. The reader does not look at it (a delete file holding 'nonsense'
// reads correctly); it is written correctly anyway.

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"os"
	"sync"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type deleteInfo struct {
	count, size, footer, rowGroups int64
}

var deleteSchema = schema.MustGroup(schema.NewGroupNode("schema", parquet.Repetitions.Required, schema.FieldList{
	schema.MustPrimitive(schema.NewPrimitiveNodeLogical("file_path", parquet.Repetitions.Required,
		schema.StringLogicalType{}, parquet.Types.ByteArray, -1, -1)),
	schema.NewInt64Node("pos", parquet.Repetitions.Required, -1),
}, -1))

const (
	deleteRowGroup = 1 << 20
	deleteBatch    = 1 << 14
)

// writeDeleteFile writes the set bits of dead as positions of dataPath. It
// does NOT sync the file; the publish syncs every delete file it wrote, at
// once, before it commits (syncFiles).
func writeDeleteFile(path, dataPath string, dead []uint64, count int) (info *deleteInfo, err error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(1),
		parquet.WithDictionaryDefault(false),
		parquet.WithDictionaryFor("file_path", true),
		parquet.WithStats(false),
		parquet.WithCreatedBy("qs-mirror"),
	)
	w := file.NewParquetWriter(keepOpen{f}, deleteSchema, file.WithWriterProps(props))

	paths := make([]parquet.ByteArray, deleteBatch)
	for i := range paths {
		paths[i] = parquet.ByteArray(dataPath)
	}
	pos := make([]int64, 0, deleteBatch)
	word, cur := 0, uint64(0)
	if len(dead) > 0 {
		cur = dead[0]
	}
	// next fills pos with up to limit positions, ascending.
	next := func(limit int) {
		pos = pos[:0]
		for len(pos) < limit && word < len(dead) {
			if cur == 0 {
				word++
				if word < len(dead) {
					cur = dead[word]
				}
				continue
			}
			pos = append(pos, int64(word<<6|bits.TrailingZeros64(cur)))
			cur &= cur - 1
		}
	}
	info = &deleteInfo{count: int64(count)}
	left := count
	for left > 0 || info.rowGroups == 0 {
		n := min(left, deleteRowGroup)
		rg := w.AppendRowGroup()
		cw, err := rg.NextColumn()
		if err != nil {
			return nil, err
		}
		pw := cw.(*file.ByteArrayColumnChunkWriter)
		for done := 0; done < n; {
			k := min(n-done, deleteBatch)
			if _, err := pw.WriteBatch(paths[:k], nil, nil); err != nil {
				return nil, err
			}
			done += k
		}
		if cw, err = rg.NextColumn(); err != nil {
			return nil, err
		}
		iw := cw.(*file.Int64ColumnChunkWriter)
		for done := 0; done < n; {
			next(min(n-done, deleteBatch))
			if len(pos) == 0 {
				return nil, errDeadCount
			}
			if _, err := iw.WriteBatch(pos, nil, nil); err != nil {
				return nil, err
			}
			done += len(pos)
		}
		if err := rg.Close(); err != nil {
			return nil, err
		}
		info.rowGroups++
		left -= n
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	info.size = st.Size()
	var tail [8]byte
	if _, err := f.ReadAt(tail[:], info.size-8); err != nil {
		return nil, err
	}
	info.footer = int64(binary.LittleEndian.Uint32(tail[:4]))
	return info, nil
}

// keepOpen hides Close from the Parquet writer, which closes the sink it is
// given; the descriptor has to outlive the footer to be synced.
type keepOpen struct{ *os.File }

func (keepOpen) Close() error { return nil }

// syncFiles makes files durable, concurrently. The catalog row that names a
// delete file is durable when the publish commits, and a delete file that is
// not is every one of its rows come back.
//
// Concurrently, because an apply changes the vector of nearly every delta
// file and each fsync is its own journal commit when they are issued one
// after another: fifteen in a row were most of a publish. Issued together the
// filesystem commits them together.
func syncFiles(paths []string) error {
	errs := make([]error, len(paths))
	var wg sync.WaitGroup
	for i, path := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := os.Open(path)
			if err != nil {
				errs[i] = err
				return
			}
			errs[i] = f.Sync()
			f.Close()
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

package mirror

// A mapped index file must never change under its mapping.
//
// A lookup reads the mapping, and a page of it that is past the end of the file
// is a SIGBUS, which Go turns into the death of the process. So every path in
// this package that touches an index's NAME — a rebuild, a removal, the sweep
// of index/, a second Table opening the same directory — has to leave the
// INODE a mapping is looking at exactly as it was. They do it by writing a
// temporary and renaming, and by unlinking; this is the test that would notice
// one of them opening the name for writing instead.

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestKidxMappedFileIsNeverWrittenInPlace(t *testing.T) {
	if !DiskIndex {
		t.Skip("QS_DISK_INDEX=0")
	}
	for _, typ := range []string{"uuid", "text"} {
		t.Run(typ, func(t *testing.T) {
			tbl := kidxTableOf(t, typ)
			root := filepath.Dir(tbl.Dir)
			const n = 20_000
			lsn := 0
			var ch []changestream.Change
			for i := 0; i < n; i++ {
				ch = append(ch, kidxInsert(true, i, "a"))
			}
			kidxApply(t, tbl, &lsn, ch)
			if _, err := tbl.Compact(); err != nil {
				t.Fatal(err)
			}
			base := tbl.State.BaseFiles[0]
			path := tbl.kidxPath(base)
			if tbl.index.disk == nil || tbl.index.disk.n != n {
				t.Fatal("no mapped index to test")
			}

			// The inode the mapping is of, held open, and what it holds.
			held, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			orig, err := io.ReadAll(held)
			if err != nil {
				t.Fatal(err)
			}
			intact := func(after string) {
				t.Helper()
				st, err := held.Stat()
				if err != nil {
					t.Fatal(err)
				}
				if st.Size() != int64(len(orig)) {
					t.Fatalf("after %s the mapped file is %d bytes, was %d: it was written in place", after, st.Size(), len(orig))
				}
				now := make([]byte, len(orig))
				if _, err := held.ReadAt(now, 0); err != nil || !bytes.Equal(now, orig) {
					t.Fatalf("after %s the mapped file's bytes changed (%v)", after, err)
				}
				// And through the mapping itself, every record of it.
				for i := 0; i < n; i += 7 {
					if l, ok := tbl.index.get(tbl.keyOfString(modelKeyText(true, i))); !ok || l.File != baseID(base) {
						t.Fatalf("after %s key %d: %+v,%v", after, i, l, ok)
					}
				}
			}
			second := func() *Table {
				s, err := New(root, "public", "r", "id", tbl.Columns, tbl.Order)
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
			build := func(s *Table, finish bool) {
				kb := s.newKidxBuilder(base, n, true)
				for i := 0; i < n; i++ {
					if err := kb.add(s.keyOfString(modelKeyText(true, i)), int32(i), 0, false); err != nil {
						t.Fatal(err)
					}
				}
				if !finish {
					kb.abort()
					return
				}
				x, _, _, err := kb.finish()
				if err != nil {
					t.Fatal(err)
				}
				x.close()
			}

			steps := []struct {
				name string
				do   func()
			}{
				{"a second Table loading the same index", func() {
					s := second()
					before := kidxRebuilds.Load()
					if _, err := s.BuildIndex(); err != nil || kidxRebuilds.Load() != before {
						t.Fatalf("second open: %v", err)
					}
					s.index.close()
				}},
				{"a second Table that judges the index stale and rebuilds it", func() {
					// The base file's mtime moves, so the index no longer binds.
					p := filepath.Join(tbl.Dir, "base", base)
					st, _ := os.Stat(p)
					later := st.ModTime().Add(time.Second)
					if err := os.Chtimes(p, later, later); err != nil {
						t.Fatal(err)
					}
					s := second()
					before := kidxRebuilds.Load()
					if _, err := s.BuildIndex(); err != nil || kidxRebuilds.Load() != before+1 {
						t.Fatalf("second open did not rebuild: %v", err)
					}
					s.index.close()
				}},
				{"a rebuild under the same name in this process", func() { build(tbl, true) }},
				{"a build that is abandoned", func() { build(tbl, false) }},
				{"a build that spills runs", func() {
					defer func(r int) { kidxRunRecords = r }(kidxRunRecords)
					kidxRunRecords = 3000
					build(second(), true)
				}},
				{"dropKidx", func() { build(tbl, true); tbl.dropKidx(base) }},
				{"dropIndexJSON", func() { build(tbl, true); tbl.dropIndexJSON(base) }},
				{"the sweep of index/, keeping it", func() { build(tbl, true); sweepIndex(tbl.Dir, []string{base}, "") }},
				{"the sweep of index/, removing it", func() { sweepIndex(tbl.Dir, nil, "") }},
				{"removeDataFile's index removal", func() { build(tbl, true); second().dropIndexJSON(base) }},
			}
			for _, s := range steps {
				s.do()
				intact(s.name)
			}
			tbl.index.close()
		})
	}
}

// TestKidxSizeIsCheckedAfterMapping: a file cut short between the pass that
// verified it and the mapping must be refused, at open and when a rebuild maps
// what it has just written — not mapped and left to fault.
func TestKidxSizeIsCheckedAfterMapping(t *testing.T) {
	if !DiskIndex {
		t.Skip("QS_DISK_INDEX=0")
	}
	tbl := kidxTableOf(t, "text")
	root := filepath.Dir(tbl.Dir)
	lsn := 0
	var ch []changestream.Change
	for i := 0; i < 5000; i++ {
		ch = append(ch, kidxInsert(true, i, "a"))
	}
	kidxApply(t, tbl, &lsn, ch)
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	want := indexDump(t, tbl)
	base := tbl.State.BaseFiles[0]
	tbl.index.close()

	// Something outside this package truncates the file in the window, once.
	cuts := 0
	afterKidxMap = func(path string) {
		if cuts == 0 {
			if err := os.Truncate(path, 4096); err != nil {
				t.Error(err)
			}
		}
		cuts++
	}
	defer func() { afterKidxMap = nil }()

	if _, err := openKidx(tbl.kidxPath(base), filepath.Join(tbl.Dir, "base", base), baseID(base), kidxBytes); !errors.Is(err, errKidxResized) {
		t.Fatalf("a file truncated under its mapping was accepted: %v", err)
	}
	// The table itself: the truncated file is now simply a damaged one, is
	// rebuilt, and the rebuilt one answers as before.
	cuts = 1
	s, err := New(root, "public", "r", "id", tbl.Columns, tbl.Order)
	if err != nil {
		t.Fatal(err)
	}
	before := kidxRebuilds.Load()
	got := indexDump(t, s)
	if kidxRebuilds.Load() != before+1 || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatal("the truncated index was not rebuilt to the same answers")
	}
	s.index.close()

	// And cut again while a rebuild is mapping what it wrote: an error, not a
	// mapping.
	cuts = 0
	_ = os.Remove(tbl.kidxPath(base))
	s, _ = New(root, "public", "r", "id", tbl.Columns, tbl.Order)
	if _, err := s.BuildIndex(); !errors.Is(err, errKidxResized) {
		t.Fatalf("a rebuild mapped a file that had been cut short: %v", err)
	}
	if _, err := s.BuildIndex(); err != nil {
		t.Fatalf("the next attempt: %v", err)
	}
	if fmt.Sprint(indexDump(t, s)) != fmt.Sprint(want) {
		t.Fatal("wrong answers after the rebuild")
	}
	s.index.close()
}

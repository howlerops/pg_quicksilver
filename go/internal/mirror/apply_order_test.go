package mirror

// A key deleted and re-inserted inside ONE batch was written to the delta twice.
//
// Apply collects a batch into `upserts` (key -> row) and `order` (the order to
// write them in), and decided whether a key was already queued by looking it up
// in `upserts`:
//
//	if _, seen := upserts[k]; !seen { order = append(order, k) }
//
// A delete removes the key from `upserts` and leaves it in `order`, which is
// correct on its own — writeUpserts skips a key with no row. But a later insert
// of the same key in the same batch then finds `upserts` empty, reads that as
// "not queued yet", and appends the key to `order` a SECOND time. writeUpserts
// walks `order` and writes upserts[k] for each occurrence, so one file ends up
// holding two identical copies of the row, and the index — written last — names
// only the second.
//
// Nothing can repair that afterwards. A deletion vector addresses positions in
// one file and both positions are in this one, so a later delete or update
// tombstones the copy the index names and leaves the other live forever. The
// mirror returns the key twice, count(*) disagrees with the source, and every
// file on disk is individually valid.
//
// This is the second source of the duplicate in docs/31. The first was the
// merge, which concatenated the live rows of several files; that fix stands, and
// it was also hiding this one, because a merge that deduplicates its output
// silently repairs a file that arrived carrying a duplicate.
//
// Found by the racy test at roughly one run in a thousand, with the amplified
// hunt and physical-position diagnostics:
//
//	key 40 appears twice in the mirror
//	  index -> delta/000064.parquet#12
//	  LIVE in delta/000064.parquet#5:  v=re n=9
//	  LIVE in delta/000064.parquet#12: v=re n=9
//
// Two live copies, same file, IDENTICAL values, index on the later one. Identical
// is the tell: a race between two writers produces two different rows, and one
// row written twice from the same map produces these.

import (
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestDeleteThenReinsertInOneBatchWritesOneRow(t *testing.T) {
	// Every interleaving the apply loop can see in a single transaction. The
	// third is the one that failed; the others are here because a fix that
	// special-cases it would break them.
	for _, tc := range []struct {
		name string
		ops  []changestream.Change
		want map[string]any // nil means the key must not be in the mirror
	}{
		{
			name: "insert then delete",
			ops:  []changestream.Change{insR("7", "a", "1"), delR("7")},
			want: nil,
		},
		{
			name: "insert, delete, insert",
			ops:  []changestream.Change{insR("7", "a", "1"), delR("7"), insR("7", "re", "9")},
			want: map[string]any{"v": "re", "n": int64(9)},
		},
		{
			name: "insert, delete, insert, delete",
			ops: []changestream.Change{
				insR("7", "a", "1"), delR("7"), insR("7", "re", "9"), delR("7"),
			},
			want: nil,
		},
		{
			name: "insert, delete, insert, delete, insert",
			ops: []changestream.Change{
				insR("7", "a", "1"), delR("7"), insR("7", "re", "9"), delR("7"),
				insR("7", "third", "3"),
			},
			want: map[string]any{"v": "third", "n": int64(3)},
		},
		{
			// Two plain updates must still collapse to one row — that is the
			// behaviour the buggy lookup got right, and the reason it was written
			// that way.
			name: "update twice",
			ops:  []changestream.Change{insR("7", "a", "1"), insR("7", "b", "2")},
			want: map[string]any{"v": "b", "n": int64(2)},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := racyTable(t)
			if _, err := tbl.Apply([]changestream.Transaction{{
				CommitLSN: "0/1", NextLSN: "0/2", Changes: tc.ops,
			}}); err != nil {
				t.Fatal(err)
			}

			var got []map[string]any
			if err := tbl.ForEachLive(func(r map[string]any) error {
				if Render(r["id"]) == "7" {
					got = append(got, r)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			if tc.want == nil {
				if len(got) != 0 {
					t.Fatalf("key 7 is in the mirror %d times after it was deleted: %v",
						len(got), got)
				}
				return
			}
			if len(got) != 1 {
				whereIs(t, tbl, "7")
				t.Fatalf("key 7 appears %d times in the mirror, want exactly once: %v",
					len(got), got)
			}
			for c, w := range tc.want {
				if Render(got[0][c]) != Render(w) {
					t.Errorf("column %s: mirror=%v want=%v", c, got[0][c], w)
				}
			}
		})
	}
}

func insR(id, v, n string) changestream.Change {
	row := map[string]any{"id": id, "v": v, "n": n}
	return changestream.Change{
		Op: changestream.OpInsert, Schema: "public", Table: "r", Row: row, Key: row,
	}
}

func delR(id string) changestream.Change {
	key := map[string]any{"id": id}
	return changestream.Change{
		Op: changestream.OpDelete, Schema: "public", Table: "r", Row: nil, Key: key,
	}
}

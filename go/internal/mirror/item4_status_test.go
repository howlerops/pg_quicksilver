package mirror

// Not a test: the helper behind the status read measurement (go/ducklake/validate/item4_status.sh).
// It writes 2,000,000 rows through the storage package (New, Apply, Compact) so that the
// file a reader scans is the one the sidecar would write. Skipped unless QS_ITEM4_OUT is set:
//
//	QS_ITEM4_OUT=/var/lib/postgresql/qslake2/item4/<rev> go test ./internal/mirror -run TestItem4StatusFile -count=1 -v

import (
	"fmt"
	"os"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func TestItem4StatusFile(t *testing.T) {
	dir := os.Getenv("QS_ITEM4_OUT")
	if dir == "" {
		t.Skip("QS_ITEM4_OUT unset")
	}
	const rows, batch = 2_000_000, 100_000
	statuses := []string{"new", "paid", "shipped", "delivered", "cancelled", "refunded"}
	cols := map[string]string{"id": "bigint", "status": "text", "amount": "bigint", "note": "text"}
	order := []string{"id", "status", "amount", "note"}
	tbl, err := New(dir, "public", "t", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	lsn := 0
	for start := 1; start <= rows; start += batch {
		ops := make([]changestream.Change, 0, batch)
		for id := start; id < start+batch; id++ {
			row := map[string]any{
				"id":     fmt.Sprint(id),
				"status": statuses[id%len(statuses)],
				"amount": fmt.Sprint((id * 7919) % 100000),
				"note":   fmt.Sprintf("note-%d", id%997),
			}
			ops = append(ops, changestream.Change{Op: changestream.OpInsert, Schema: "public", Table: "t", Row: row, Key: row})
		}
		lsn++
		c := fmt.Sprintf("0/%X", lsn*16)
		if _, err := tbl.Apply([]changestream.Transaction{{CommitLSN: c, NextLSN: c, Changes: ops}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tbl.Compact(); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %d rows under %s", rows, dir)
}

package mirror

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// TestJoinDomainBuildBench writes the join benchmark's two tables through this
// package, twice: once with the domain attached and once without, under
// QS_JOIN_BENCH_DIR. Each is then rewritten so its files are one base file per
// table and, with the domain, complete. It is gated because it writes 550k rows
// and takes minutes; the DuckDB measurement reads what it leaves behind.
//
//	QS_JOIN_BENCH_DIR=/path go test -run TestJoinDomainBuildBench ./internal/mirror/
func TestJoinDomainBuildBench(t *testing.T) {
	dir := os.Getenv("QS_JOIN_BENCH_DIR")
	if dir == "" {
		t.Skip("set QS_JOIN_BENCH_DIR to build the join benchmark tables")
	}
	const (
		customers = 50000
		orders    = 500000
		batch     = 20000
	)
	for _, on := range []bool{true, false} {
		name := "off"
		if on {
			name = "on"
		}
		root := filepath.Join(dir, name)
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatal(err)
		}
		var jd *JoinDomains
		if on {
			specs, err := ParseJoinDomains(dmSpec)
			if err != nil {
				t.Fatal(err)
			}
			jd, err = OpenJoinDomains(root, specs, 0)
			if err != nil {
				t.Fatal(err)
			}
		}
		cust, ord := openDomainTables(t, root, jd)
		rng := rand.New(rand.NewSource(7))
		ids := make([]string, customers)
		for i := range ids {
			ids[i] = benchUUID(rng)
		}
		var lsn lsnSeq
		for i := 0; i < customers; i += batch {
			var ops []changestream.Change
			for j := i; j < i+batch && j < customers; j++ {
				ops = append(ops, dmChange(changestream.OpInsert, "customers", map[string]any{"id": ids[j], "name": "c"}))
			}
			applyTxn(t, cust, &lsn, ops...)
		}
		for i := 0; i < orders; i += batch {
			var ops []changestream.Change
			for j := i; j < i+batch && j < orders; j++ {
				ops = append(ops, dmChange(changestream.OpInsert, "orders", map[string]any{
					"id":          fmt.Sprint(j + 1),
					"customer_id": ids[rng.Intn(customers)],
					"amount":      fmt.Sprint(rng.Intn(10000)),
				}))
			}
			applyTxn(t, ord, &lsn, ops...)
		}
		if _, err := cust.Compact(); err != nil {
			t.Fatal(err)
		}
		if _, err := ord.Compact(); err != nil {
			t.Fatal(err)
		}
		if on {
			for _, tbl := range []*Table{cust, ord} {
				for _, dc := range tbl.DomainColumns() {
					if !dc.Complete {
						t.Fatalf("%s.%s is not complete after the rewrite", tbl.Qualified, dc.Column)
					}
				}
			}
			_ = jd.Close()
		}
		for _, tbl := range []*Table{cust, ord} {
			for _, f := range tbl.State.BaseFiles {
				t.Logf("%s %s: base/%s", name, tbl.Qualified, f)
			}
			if len(tbl.State.DeltaFiles) != 0 {
				t.Fatalf("%s: %d delta files after the rewrite", tbl.Qualified, len(tbl.State.DeltaFiles))
			}
		}
	}
}

// benchUUID is a random version-4 uuid in canonical lowercase form.
func benchUUID(rng *rand.Rand) string {
	var b [16]byte
	rng.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

package mirror

import (
	"fmt"
	"path/filepath"
	"testing"
)

// The snapshot writes rows through AppendValues, in logical column order and
// with no companion slots. The writer must fill the companions itself, and the
// file must read back with each one equal to its value's dictionary id.
func TestJoinDomainSnapshotWriterFillsCompanions(t *testing.T) {
	root := t.TempDir()
	specs, err := ParseJoinDomains(dmSpec)
	if err != nil {
		t.Fatal(err)
	}
	jd, err := OpenJoinDomains(root, specs, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer jd.Close()
	ord, err := New(root, "public", "orders", "id", dmOrdCols, dmOrdOrder)
	if err != nil {
		t.Fatal(err)
	}
	if err := ord.AttachJoinDomains(jd); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(ord.Dir, "base", "000001.parquet")
	w, err := ord.newParquetWriter(path, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	const rows = 500
	for i := 0; i < rows; i++ {
		var cid any
		if i%7 != 0 {
			cid = dmUUID(i % 30)
		}
		if err := w.AppendValues([]any{fmt.Sprint(i + 1), cid, fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	d := jd.dicts["cust"]
	if d.Len() != 30 {
		t.Fatalf("dictionary has %d keys, want 30 distinct customers", d.Len())
	}
	n, nulls := 0, 0
	err = ord.forEachRowsWant(path, nil, ord.fileCols(ord.Order), true, 0, func(rs []map[string]any, _ int) error {
		for _, r := range rs {
			n++
			comp := r[companionName("customer_id")]
			if r["customer_id"] == nil {
				nulls++
				if comp != nil {
					return fmt.Errorf("row %v: NULL member with companion %v", r["id"], comp)
				}
				continue
			}
			key, err := joinValueKey(r["customer_id"])
			if err != nil {
				return err
			}
			want, ok := d.Lookup(key)
			if !ok || comp != want {
				return fmt.Errorf("row %v: companion %v, dictionary %d (ok %v)", r["id"], comp, want, ok)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("read %d rows back, wrote %d", n, rows)
	}
	if nulls == 0 {
		t.Fatal("no NULL member was exercised")
	}
}

package mirror

// The unchanged-TOAST case, which is the one a JSONB-heavy table hits on every
// single update.
//
// PostgreSQL stores a large value out of line, and pgoutput does NOT send it
// again when a row is updated without changing it — the column arrives with
// DataType 'u' and no bytes. The decoder correctly omits it from the row map.
// What the WRITER then does with an absent column decides whether the mirror is
// correct or quietly destroys data, and there is no error either way.
//
// This is the exact shape of a table holding a document per row: a Salesforce
// object, an event payload, a rendered invoice. Every `UPDATE ... SET status =
// 'x'` on such a table is an update where the big column is absent.

import (
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func toastTable(t *testing.T) *Table {
	t.Helper()
	cols := map[string]string{
		"id": "bigint", "status": "text", "doc": "jsonb", "updated_at": "text",
	}
	order := []string{"id", "status", "doc", "updated_at"}
	tbl, err := New(t.TempDir(), "public", "records", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func txn(lsn string, changes ...changestream.Change) changestream.Transaction {
	return changestream.Transaction{CommitLSN: lsn, NextLSN: lsn, Changes: changes}
}

func change(op changestream.Op, row map[string]any) changestream.Change {
	return changestream.Change{
		Op: op, Schema: "public", Table: "records", Row: row, Key: row,
	}
}

// TestUnchangedToastIsCarriedForward is a regression test for silent data
// destruction. It fails loudly on a mirror that writes NULL where the source
// still holds a document.
func TestUnchangedToastIsCarriedForward(t *testing.T) {
	tbl := toastTable(t)
	doc := `{"Id":"001xx","Name":"` + strings.Repeat("A", 4000) + `"}`

	// INSERT: every column present, as pgoutput sends for an insert.
	if _, err := tbl.Apply([]changestream.Transaction{
		txn("0/100", change(changestream.OpInsert, map[string]any{
			"id": "1", "status": "new", "doc": doc, "updated_at": "t0",
		})),
	}); err != nil {
		t.Fatal(err)
	}

	// UPDATE touching only `status`. pgoutput marks `doc` unchanged-TOAST, so
	// the decoder leaves it out of the map entirely. THIS is the input that
	// matters — an absent key, not a nil one.
	if _, err := tbl.Apply([]changestream.Transaction{
		txn("0/200", change(changestream.OpUpdate, map[string]any{
			"id": "1", "status": "shipped", "updated_at": "t1",
			// "doc" deliberately absent
		})),
	}); err != nil {
		t.Fatal(err)
	}

	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 live row, got %d", len(rows))
	}
	got := rows[0]
	if got["status"] != "shipped" {
		t.Errorf("status = %v, want shipped", got["status"])
	}
	if got["doc"] == nil || got["doc"] == "" {
		t.Fatalf("the document was DESTROYED by an update that never touched it.\n"+
			"pgoutput does not resend an unchanged TOASTed value, so the column is "+
			"absent from the change — and an absent column must mean 'keep what is "+
			"there', not 'set to NULL'. Row counts still match and nothing errors, "+
			"which is what makes this the worst shape a bug can take.")
	}
	if got["doc"] != doc {
		t.Errorf("document changed: got %d bytes, want %d",
			len(got["doc"].(string)), len(doc))
	}
}

// TestExplicitNullIsNotConfusedWithAbsent is the other half. If absent means
// "carry forward", then setting a column to NULL on purpose must still work —
// otherwise the fix for one silent corruption introduces another.
func TestExplicitNullIsNotConfusedWithAbsent(t *testing.T) {
	tbl := toastTable(t)
	if _, err := tbl.Apply([]changestream.Transaction{
		txn("0/100", change(changestream.OpInsert, map[string]any{
			"id": "1", "status": "new", "doc": `{"a":1}`, "updated_at": "t0",
		})),
	}); err != nil {
		t.Fatal(err)
	}
	// doc present and explicitly nil: the user really did SET doc = NULL.
	if _, err := tbl.Apply([]changestream.Transaction{
		txn("0/200", change(changestream.OpUpdate, map[string]any{
			"id": "1", "status": "new", "doc": nil, "updated_at": "t1",
		})),
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := tbl.Live()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["doc"] != nil {
		t.Errorf("an explicit SET doc = NULL was ignored; got %v", rows[0]["doc"])
	}
}

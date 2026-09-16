package mirror

// Eliding a value the mirror decides is unchanged is the one judgement in this
// package made from a digest rather than from the bytes, so these tests check
// both directions: that an unchanged large column stops being rewritten, and
// that a changed one is never mistaken for an unchanged one — including when
// only its length changes, when it moves to a different column, and when a
// column pgoutput omitted makes the digest unknowable.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// inlineTable is a row whose large column is BELOW PostgreSQL's TOAST
// threshold, so pgoutput resends it on every update and partial.go never sees
// a hint. That is the case this file exists for.
func inlineTable(t *testing.T) *Table {
	t.Helper()
	cols := map[string]string{"id": "bigint", "status": "text", "blob": "text"}
	order := []string{"id", "status", "blob"}
	tbl, err := New(t.TempDir(), "public", "e", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	return tbl
}

func einsert(id, status, blob string) changestream.Change {
	row := map[string]any{"id": id, "status": status, "blob": blob}
	return changestream.Change{Op: changestream.OpInsert, Schema: "public",
		Table: "e", Row: row, Key: row}
}

func eupdate(row map[string]any) changestream.Change {
	return changestream.Change{Op: changestream.OpUpdate, Schema: "public",
		Table: "e", Row: row, Key: row}
}

func eapply(t *testing.T, tbl *Table, lsn int, changes ...changestream.Change) {
	t.Helper()
	pos := fmt.Sprintf("0/%X", lsn)
	if _, err := tbl.Apply([]changestream.Transaction{{
		CommitLSN: pos, NextLSN: pos, Changes: changes,
	}}); err != nil {
		t.Fatal(err)
	}
}

func eliveByKey(t *testing.T, tbl *Table) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	if err := tbl.ForEachLive(func(r map[string]any) error {
		out[Render(r["id"])] = r
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestUnchangedInlineValueIsNotRewritten(t *testing.T) {
	// Elision turns a change into a SHORT one; column-partial deltas are what
	// turn a short change into fewer bytes on disk. With those off the mirror
	// carries the elided columns forward and writes the row whole — correct,
	// and no saving at all.
	if !ElideUnchanged || !PartialDeltas {
		t.Skip("needs both optimisations on")
	}
	tbl := inlineTable(t)
	blob := strings.Repeat("inline-payload-", 80) // ~1.2 KB, under the TOAST threshold

	eapply(t, tbl, 1, einsert("1", "new", blob))
	// The update resends the blob unchanged, exactly as pgoutput would.
	eapply(t, tbl, 2, eupdate(map[string]any{"id": "1", "status": "done", "blob": blob}))

	if len(tbl.State.PartialCols) != 1 {
		t.Fatalf("expected the resent blob to be dropped and the change stored "+
			"as a patch; partial files: %v, delta files: %v",
			tbl.State.PartialCols, tbl.State.DeltaFiles)
	}
	for name, cols := range tbl.State.PartialCols {
		if containsStr(cols, "blob") {
			t.Errorf("patch file %s still carries the blob column (%v)", name, cols)
		}
		fi, err := os.Stat(filepath.Join(tbl.Dir, "delta", name))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() > int64(len(blob)) {
			t.Errorf("patch file is %d bytes for a %d-byte value that did not "+
				"change", fi.Size(), len(blob))
		}
	}

	got := eliveByKey(t, tbl)
	if len(got) != 1 {
		t.Fatalf("mirror has %d rows, expected 1", len(got))
	}
	if Render(got["1"]["blob"]) != blob {
		t.Errorf("blob came back as %d bytes, expected %d",
			len(Render(got["1"]["blob"])), len(blob))
	}
	if Render(got["1"]["status"]) != "done" {
		t.Errorf("status = %v, expected done", got["1"]["status"])
	}
}

// TestChangedValueIsNeverElided is the direction that costs data if it is wrong.
func TestChangedValueIsNeverElided(t *testing.T) {
	if !ElideUnchanged {
		t.Skip("QS_ELIDE_UNCHANGED=0")
	}
	blob := strings.Repeat("inline-payload-", 80)
	cases := map[string]string{
		"one byte differs":   strings.Repeat("inline-payload-", 79) + "inline-payloaD-",
		"one byte longer":    blob + "x",
		"one byte shorter":   blob[:len(blob)-1],
		"completely differs": strings.Repeat("other-payload!!-", 75),
	}
	for name, next := range cases {
		t.Run(name, func(t *testing.T) {
			tbl := inlineTable(t)
			eapply(t, tbl, 1, einsert("1", "new", blob))
			eapply(t, tbl, 2, eupdate(map[string]any{
				"id": "1", "status": "done", "blob": next,
			}))
			got := eliveByKey(t, tbl)
			if Render(got["1"]["blob"]) != next {
				t.Errorf("blob is %d bytes, expected the NEW value of %d bytes",
					len(Render(got["1"]["blob"])), len(next))
			}
		})
	}
}

// TestLargeValueMovingColumnsIsNotElided: two rows whose large columns hold the
// same bytes under DIFFERENT names must not hash alike, or swapping them would
// read as "nothing changed".
func TestLargeValueMovingColumnsIsNotElided(t *testing.T) {
	cols := map[string]string{"id": "bigint", "a": "text", "b": "text"}
	order := []string{"id", "a", "b"}
	tbl, err := New(t.TempDir(), "public", "e", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("payload-", 200)
	ch := func(op changestream.Op, row map[string]any) changestream.Change {
		return changestream.Change{Op: op, Schema: "public", Table: "e",
			Row: row, Key: row}
	}
	eapply(t, tbl, 1, ch(changestream.OpInsert,
		map[string]any{"id": "1", "a": big, "b": "small"}))
	// the same bytes, now in the other column
	eapply(t, tbl, 2, ch(changestream.OpUpdate,
		map[string]any{"id": "1", "a": "small", "b": big}))

	got := eliveByKey(t, tbl)
	if Render(got["1"]["a"]) != "small" || Render(got["1"]["b"]) != big {
		t.Errorf("after moving the payload between columns: a=%.20v b=%d bytes",
			got["1"]["a"], len(Render(got["1"]["b"])))
	}
}

// TestOmittedColumnMakesTheDigestUnknown: when pgoutput leaves a large column
// out, the mirror cannot know what the row's large columns are any more. It
// must forget the digest rather than keep a stale one — the next update that
// resends everything has to be compared against reality, not against a guess.
func TestOmittedColumnMakesTheDigestUnknown(t *testing.T) {
	if !ElideUnchanged || !PartialDeltas {
		t.Skip("needs both optimisations on")
	}
	cols := map[string]string{"id": "bigint", "n": "bigint", "a": "text", "b": "text"}
	order := []string{"id", "n", "a", "b"}
	tbl, err := New(t.TempDir(), "public", "e", "id", cols, order)
	if err != nil {
		t.Fatal(err)
	}
	ch := func(op changestream.Op, row map[string]any) changestream.Change {
		return changestream.Change{Op: op, Schema: "public", Table: "e",
			Row: row, Key: row}
	}
	a0 := strings.Repeat("aaaaaaaa", 100)
	b0 := strings.Repeat("bbbbbbbb", 100)
	eapply(t, tbl, 1, ch(changestream.OpInsert,
		map[string]any{"id": "1", "n": "0", "a": a0, "b": b0}))

	// An update that OMITS b (unchanged TOAST) and changes a. The digest of the
	// row's large columns is now partly unknown.
	a1 := strings.Repeat("AAAAAAAA", 100)
	eapply(t, tbl, 2, ch(changestream.OpUpdate,
		map[string]any{"id": "1", "n": "1", "a": a1}))
	if l, ok := tbl.index.get(tbl.keyOfString("1")); ok && l.Hash != 0 {
		t.Error("the digest survived an update that omitted a large column")
	}

	// Now everything is resent, with b changed. Nothing may be elided against
	// the stale digest.
	b1 := strings.Repeat("BBBBBBBB", 100)
	eapply(t, tbl, 3, ch(changestream.OpUpdate,
		map[string]any{"id": "1", "n": "2", "a": a1, "b": b1}))

	got := eliveByKey(t, tbl)
	if Render(got["1"]["a"]) != a1 || Render(got["1"]["b"]) != b1 ||
		Render(got["1"]["n"]) != "2" {
		t.Errorf("row came back a=%.10v... b=%.10v... n=%v",
			got["1"]["a"], got["1"]["b"], got["1"]["n"])
	}
}

// TestElisionIsSmallerThanResending prices it on the shape it was built for.
func TestElisionIsSmallerThanResending(t *testing.T) {
	if !PartialDeltas {
		t.Skip("the saving is the partial delta; see TestUnchangedInlineValueIsNotRewritten")
	}
	run := func(on bool) int64 {
		defer func(prev bool) { ElideUnchanged = prev }(ElideUnchanged)
		ElideUnchanged = on
		tbl := inlineTable(t)
		blob := strings.Repeat("inline-payload-", 80)
		var seed []changestream.Change
		for i := 1; i <= 200; i++ {
			seed = append(seed, einsert(fmt.Sprint(i), "new", blob))
		}
		eapply(t, tbl, 1, seed...)
		for round := 1; round <= 20; round++ {
			var changes []changestream.Change
			for i := 1; i <= 200; i++ {
				changes = append(changes, eupdate(map[string]any{
					"id": fmt.Sprint(i), "status": fmt.Sprint(round), "blob": blob,
				}))
			}
			eapply(t, tbl, 1+round, changes...)
		}
		got := eliveByKey(t, tbl)
		if len(got) != 200 {
			t.Fatalf("on=%v: mirror has %d rows, expected 200", on, len(got))
		}
		for i := 1; i <= 200; i++ {
			r := got[fmt.Sprint(i)]
			if Render(r["blob"]) != blob {
				t.Fatalf("on=%v: row %d lost its blob", on, i)
			}
			if Render(r["status"]) != "20" {
				t.Fatalf("on=%v: row %d has status=%v", on, i, r["status"])
			}
		}
		return dirBytes(t, tbl.Dir)
	}
	resent := run(false)
	elided := run(true)
	t.Logf("20 rounds of 200 updates resending an unchanged 1.2 KB value: "+
		"%d bytes resent, %d bytes elided (%.1fx smaller)",
		resent, elided, float64(resent)/float64(elided))
	if elided >= resent {
		t.Errorf("eliding wrote %d bytes against %d for resending", elided, resent)
	}
}

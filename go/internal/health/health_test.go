package health

import (
	"strings"
	"testing"
	"time"
)

const nan = `halting public.b: column "amount" (numeric(12,2)) holds "NaN", which the mirror's column type decimal(12, 2) cannot store exactly. Storing NULL or a rounded value instead would be silently wrong`

func TestHaltedReasonText(t *testing.T) {
	if got := HaltedReason(nil); got != "" {
		t.Errorf("no halted tables: %q", got)
	}
	got := HaltedReason(map[string]string{"public.b": nan})
	want := `1 table is halted and not mirrored: public.b (halting public.b: column "amount" (numeric(12,2)) holds "NaN", which the mirror's column type decimal(12, 2) cannot store exactly. Storing NULL or a rounded val…)`
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	// Sorted, so two probes of one state read the same; and one line.
	got = HaltedReason(map[string]string{"public.z": "late\nreason", "public.a": "early"})
	if want := "2 tables are halted and not mirrored: public.a (early); public.z (late reason)"; got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
	many := map[string]string{}
	for _, n := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		many["public."+n] = "x"
	}
	if got := HaltedReason(many); !strings.HasSuffix(got, "public.e (x); and 2 more") {
		t.Errorf("a long list is not cut: %s", got)
	}
}

func TestReadinessWithAHaltedTable(t *testing.T) {
	h := New(time.Minute)

	// Before bootstrap, with a halt read from the manifest: it says halted.
	// This is the restart that used to read "still bootstrapping".
	h.SetHalted("public.b", nan)
	ok, why := h.Ready()
	if ok || !strings.HasPrefix(why, "1 table is halted and not mirrored: public.b (") {
		t.Fatalf("ready=%v reason=%q", ok, why)
	}
	for _, not := range []string{"diverged", "bootstrapping"} {
		if strings.Contains(why, not) {
			t.Errorf("the halt reason reads as %q: %s", not, why)
		}
	}
	if !strings.Contains(why, "REBUILD") || !strings.Contains(why, "QS_SERVE_WITH_HALTED_TABLES") {
		t.Errorf("the reason does not say what to do: %s", why)
	}
	h.RecordBootstrapped()
	if ok, _ := h.Ready(); ok {
		t.Fatal("ready with a halted table and the switch off")
	}
	if s := h.Snapshot(); s.Diverged || s.HaltedTables["public.b"] != nan {
		t.Errorf("snapshot: %+v", s)
	}

	// The switch: ready on the healthy tables, and still says so.
	h.SetServeWithHalted(true)
	ok, why = h.Ready()
	if !ok || !strings.Contains(why, "DEGRADED") || !strings.Contains(why, "public.b (") {
		t.Fatalf("with the switch on: ready=%v reason=%q", ok, why)
	}
	// It relaxes the halt and nothing else.
	h2 := New(time.Minute)
	h2.SetServeWithHalted(true)
	h2.SetHalted("public.b", nan)
	if ok, why := h2.Ready(); ok || !strings.Contains(why, "bootstrapping") {
		t.Errorf("the switch made an un-bootstrapped mirror ready: %v %q", ok, why)
	}

	// And it never makes a Pod with NOTHING left ready.
	h.SetTables(2)
	if ok, _ := h.Ready(); !ok {
		t.Fatal("one of two halted, switch on: should be ready")
	}
	h.SetHalted("public.a", "x")
	if ok, why := h.Ready(); ok || !strings.Contains(why, "Every table is halted") {
		t.Errorf("every table halted, switch on: ready=%v reason=%q", ok, why)
	}
	h.ClearHalted("public.a")

	// Rebuilt.
	h.SetServeWithHalted(false)
	h.ClearHalted("public.b")
	if ok, why := h.Ready(); !ok || strings.Contains(why, "halted") {
		t.Errorf("after the rebuild: ready=%v reason=%q", ok, why)
	}
}

func TestSetHaltedReportsNewsOnce(t *testing.T) {
	h := New(time.Minute)
	if !h.SetHalted("public.b", "x") || h.SetHalted("public.b", "x") || !h.SetHalted("public.b", "y") {
		t.Error("SetHalted should report a new table or a changed reason, and nothing else")
	}
}

func TestHaltedMetrics(t *testing.T) {
	h := New(time.Minute)
	h.RecordBootstrapped()
	if m := h.Metrics(); !strings.Contains(m, "\nquicksilver_halted_tables 0\n") || strings.Contains(m, "quicksilver_table_halted{") {
		t.Errorf("no halted tables:\n%s", m)
	}
	h.SetHalted("public.b", nan)
	m := h.Metrics()
	for _, want := range []string{
		"\nquicksilver_halted_tables 1\n",
		"\nquicksilver_ready 0\n",
		`quicksilver_table_halted{table="public.b",reason="halting public.b: column \"amount\" (numeric(12,2)) holds \"NaN\", which`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics lack %q:\n%s", want, m)
		}
	}
	if !strings.HasSuffix(m, "…\"} 1\n") {
		t.Errorf("the reason label is not truncated and closed:\n%s", m[len(m)-200:])
	}
}

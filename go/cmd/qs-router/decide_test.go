package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The plans in testdata were captured from PostgreSQL 17.12 over the 1M-row
// orders table in validate/seed.sql (validate/capture_plans.sh). The decision
// is tested against what the planner said, not against what someone thought it
// would say.

var testCatalog = &Catalog{
	HeapSchema: "public",
	Mirrored:   map[string]bool{"orders": true, "customers": true},
	Unique:     map[string]bool{"orders_pkey": true, "customers_pkey": true},
}

var testThresholds = Thresholds{Rows: 50000, Cost: 10000}

func loadPlan(t *testing.T, name string) *planNode {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ParsePlan(b)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return p
}

func TestDecideOverCapturedPlans(t *testing.T) {
	cases := []struct {
		plan     string
		side     Side
		eligible bool
		stable   bool
		reason   string // substring
	}{
		{"pk_lookup", Heap, true, true, "unique-index lookup"},
		{"pk_join", Heap, true, true, "unique-index lookup"},
		{"in_list", Heap, true, false, "under both thresholds"},
		{"small_range", Heap, true, false, "under both thresholds"},
		{"customer_selective", Heap, true, false, "under both thresholds"},
		{"customer_unselective", Mirror, true, false, "Index Only Scan on orders reads an estimated 125930 rows"},
		{"count_star", Mirror, true, false, "Index Only Scan on orders"},
		{"group_by", Mirror, true, false, "Seq Scan on orders"},
		{"range_aggregate", Mirror, true, false, "Seq Scan on orders"},
		{"join", Mirror, true, false, "Seq Scan on orders"},
		// The Seq Scan node says 1,000,000 rows. The Limit above it says 10.
		{"limit_no_order", Heap, true, false, "Seq Scan on orders reads an estimated 10 rows"},
		// A Sort sits between this Limit and the scan, so the scan is read whole.
		{"limit_over_sort", Mirror, true, false, "Seq Scan on orders"},
		{"select_for_update", Heap, false, true, "plan node LockRows"},
		{"cte_write", Heap, false, true, "plan node ModifyTable"},
		{"function_call", Heap, false, true, "reads no table"},
		{"no_table", Heap, false, true, "reads no table"},
		{"function_scan", Heap, false, true, "plan node Function Scan"},
		{"tid_scan", Heap, false, true, "plan node Tid Scan"},
		{"unmirrored", Heap, false, true, "public.audit_log, which is not mirrored"},
		{"mirror_count_star", Heap, false, true, "plan node Custom Scan"},
	}
	for _, c := range cases {
		d := Decide(loadPlan(t, c.plan), testCatalog, testThresholds, false)
		if d.Side != c.side || d.Eligible != c.eligible || d.Stable != c.stable || !strings.Contains(d.Reason, c.reason) {
			t.Errorf("%s: got %s eligible=%v stable=%v %q; want %s eligible=%v stable=%v containing %q",
				c.plan, d.Side, d.Eligible, d.Stable, d.Reason, c.side, c.eligible, c.stable, c.reason)
		}
	}
}

func TestForcedMirrorStillNeedsAnEligiblePlan(t *testing.T) {
	if d := Decide(loadPlan(t, "pk_lookup"), testCatalog, testThresholds, true); d.Side != Mirror {
		t.Errorf("qs.route=mirror did not move a point lookup: %+v", d)
	}
	for _, p := range []string{"select_for_update", "cte_write", "unmirrored", "function_call", "tid_scan"} {
		if d := Decide(loadPlan(t, p), testCatalog, testThresholds, true); d.Side != Heap {
			t.Errorf("qs.route=mirror sent %s to the mirror: %+v", p, d)
		}
	}
}

func TestTableThatStoppedBeingMirrored(t *testing.T) {
	cat := &Catalog{HeapSchema: "public", Mirrored: map[string]bool{"customers": true}, Unique: map[string]bool{}}
	for _, p := range []string{"count_star", "join"} {
		if d := Decide(loadPlan(t, p), cat, testThresholds, false); d.Side != Heap || !strings.Contains(d.Reason, "not mirrored") {
			t.Errorf("%s: %+v", p, d)
		}
	}
	// Same table name in another schema is another table.
	cat = &Catalog{HeapSchema: "other", Mirrored: map[string]bool{"orders": true}, Unique: map[string]bool{}}
	if d := Decide(loadPlan(t, "count_star"), cat, testThresholds, false); d.Side != Heap {
		t.Errorf("public.orders was taken for other.orders: %+v", d)
	}
}

func TestPointLookupIsOnlyStableOnAUniqueIndex(t *testing.T) {
	cat := &Catalog{HeapSchema: "public", Mirrored: testCatalog.Mirrored, Unique: map[string]bool{}}
	if d := Decide(loadPlan(t, "pk_lookup"), cat, testThresholds, false); d.Side != Heap || d.Stable {
		t.Errorf("a one-row estimate on a non-unique index was cached by shape: %+v", d)
	}
}

func TestThresholdsAreTheOnlyThingBetweenTheSides(t *testing.T) {
	high := Thresholds{Rows: 1e9, Cost: 1e9}
	for _, p := range []string{"count_star", "group_by", "join", "customer_unselective"} {
		if d := Decide(loadPlan(t, p), testCatalog, high, false); d.Side != Heap || !d.Eligible {
			t.Errorf("%s with unreachable thresholds: %+v", p, d)
		}
	}
	// Cost alone: 72 estimated rows, cost 5.87.
	if d := Decide(loadPlan(t, "customer_selective"), testCatalog, Thresholds{Rows: 1e9, Cost: 5}, false); d.Side != Mirror || !strings.Contains(d.Reason, "total cost") {
		t.Errorf("cost threshold: %+v", d)
	}
}

func TestHeapLeak(t *testing.T) {
	if l := HeapLeak(loadPlan(t, "count_star"), testCatalog); l != "public.orders" {
		t.Errorf("leak = %q", l)
	}
	mp := loadPlan(t, "mirror_count_star")
	if l := HeapLeak(mp, testCatalog); l != "" {
		t.Errorf("the DuckDB plan was read as a heap read: %q", l)
	}
	if mp.NodeType != "Custom Scan" || mp.CustomProvider != "DuckDBScan" {
		t.Errorf("mirror-side plan root: %q %q", mp.NodeType, mp.CustomProvider)
	}
}

func TestParsePlanRejectsWhatIsNotOnePlan(t *testing.T) {
	for _, s := range []string{``, `[]`, `{}`, `[{"Plan": null}]`, `[{"Plan":{}},{"Plan":{}}]`, `not json`} {
		if _, err := ParsePlan([]byte(s)); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
}

func TestCacheExpiresAndCanBeOff(t *testing.T) {
	now := time.Unix(1000, 0)
	c := newDecisionCache(time.Minute, 2)
	c.now = func() time.Time { return now }
	c.put("a", Decision{Side: Mirror})
	if d, ok := c.get("a"); !ok || d.Side != Mirror {
		t.Fatal("miss right after put")
	}
	now = now.Add(61 * time.Second)
	if _, ok := c.get("a"); ok {
		t.Fatal("served after its TTL")
	}
	c.put("a", Decision{})
	c.put("b", Decision{})
	c.put("c", Decision{}) // over max: everything goes
	if _, ok := c.get("a"); ok {
		t.Fatal("cache grew past its bound")
	}
	off := newDecisionCache(0, 2)
	off.put("a", Decision{})
	if _, ok := off.get("a"); ok {
		t.Fatal("ttl 0 still cached")
	}
}

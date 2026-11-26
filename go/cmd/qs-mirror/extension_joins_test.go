package main

import (
	"strings"
	"testing"
)

// A complete join domain column's companion is exposed by the mirror relation
// after the heap's columns, as bigint, under its own name (docs/40, "Join keys").
func TestMirrorRelationExposesCompanionsAfterTheHeapColumns(t *testing.T) {
	cols := []extColumn{
		{Name: "id", Type: "uuid", TypeOID: 2950},
		{Name: "customer_id", Type: "uuid", TypeOID: 2950},
		{Name: "amount", Type: "numeric(12,2)", TypeOID: 1700},
	}
	joins := []extJoin{{Column: "customer_id", Domain: "cust", Companion: "__qs_d_customer_id"}}
	got := extViewSQL("quicksilver_mirror", "public", "orders", cols, joins)
	want := `CREATE VIEW "quicksilver_mirror"."public.orders" AS
SELECT (r['id'])::uuid AS "id",
       (r['customer_id'])::uuid AS "customer_id",
       (r['amount'])::numeric(12,2) AS "amount",
       (r['__qs_d_customer_id'])::bigint AS "__qs_d_customer_id"
FROM duckdb.query($qs$ SELECT * FROM lake."public"."orders" $qs$) r`
	if got != want {
		t.Fatalf("mirror relation with a companion:\n%s\nwant:\n%s", got, want)
	}
	// The companion is a column of the view and of nothing else: the heap's
	// columns are the first len(cols) of it, so position-based readers see the same.
	if strings.Count(got, "AS \"") != len(cols)+len(joins) {
		t.Error("the companion is not one column of the mirror relation")
	}
	// A different companion or none at all is a different definition.
	if extSignature(got) == extSignature(extViewSQL("quicksilver_mirror", "public", "orders", cols, nil)) {
		t.Error("exposing a companion did not change the signature: the view would not be re-created")
	}
	other := []extJoin{{Column: "customer_id", Domain: "cust", Companion: "__qs_d_other"}}
	if extSignature(got) == extSignature(extViewSQL("quicksilver_mirror", "public", "orders", cols, other)) {
		t.Error("a different companion name did not change the signature")
	}
}

// A join column the heap no longer has is not exposed.
func TestFilterJoinsKeepsOnlyColumnsTheHeapHas(t *testing.T) {
	cols := []extColumn{{Name: "id"}, {Name: "customer_id"}}
	joins := []extJoin{
		{Column: "customer_id", Domain: "cust", Companion: "__qs_d_customer_id"},
		{Column: "gone", Domain: "cust", Companion: "__qs_d_gone"},
	}
	got := filterJoins(joins, cols)
	if len(got) != 1 || got[0].Column != "customer_id" {
		t.Fatalf("filterJoins = %+v, want only customer_id", got)
	}
}

// The rows to delete are the ones the view will not expose; the rows to write
// are the ones that are new or different. A row that is already right is neither.
func TestJoinRowsChangeDeletesAndWritesOnlyWhatDiffers(t *testing.T) {
	a := extJoin{Column: "a", Domain: "d", Companion: "__qs_d_a"}
	b := extJoin{Column: "b", Domain: "d", Companion: "__qs_d_b"}
	aOther := extJoin{Column: "a", Domain: "e", Companion: "__qs_e_a"}

	cases := []struct {
		name    string
		have    map[string]extJoin
		want    []extJoin
		wantDel []string
		wantPut []extJoin
	}{
		{"nothing to do", map[string]extJoin{"a": a}, []extJoin{a}, nil, nil},
		{"new row", map[string]extJoin{}, []extJoin{a}, nil, []extJoin{a}},
		{"row leaves", map[string]extJoin{"a": a}, nil, []string{"a"}, nil},
		{"domain changed", map[string]extJoin{"a": a}, []extJoin{aOther}, nil, []extJoin{aOther}},
		{"one leaves, one arrives", map[string]extJoin{"a": a}, []extJoin{b}, []string{"a"}, []extJoin{b}},
	}
	for _, c := range cases {
		del, put := joinRowsChange(c.have, c.want)
		if strings.Join(del, ",") != strings.Join(c.wantDel, ",") {
			t.Errorf("%s: delete %v, want %v", c.name, del, c.wantDel)
		}
		if len(put) != len(c.wantPut) {
			t.Errorf("%s: write %+v, want %+v", c.name, put, c.wantPut)
			continue
		}
		for i := range put {
			if put[i] != c.wantPut[i] {
				t.Errorf("%s: write %+v, want %+v", c.name, put, c.wantPut)
			}
		}
	}
}

// A column becoming complete, or stopping, changes what observe sees, so the
// registry is brought in step at the next tick.
func TestJoinSignatureChangesWithCompleteness(t *testing.T) {
	both := map[string][]extJoin{"public.orders": {
		{Column: "customer_id", Domain: "cust", Companion: "__qs_d_customer_id"},
	}}
	none := map[string][]extJoin{}
	if joinSig(both) == joinSig(none) {
		t.Fatal("a complete column and none have the same signature")
	}
	if joinSig(both) != joinSig(map[string][]extJoin{"public.orders": {
		{Column: "customer_id", Domain: "cust", Companion: "__qs_d_customer_id"},
	}}) {
		t.Fatal("the same columns have different signatures")
	}
	if joinSig(none) != "" {
		t.Errorf("no join columns: signature %q, want empty so that an older install is not dirty", joinSig(none))
	}
}

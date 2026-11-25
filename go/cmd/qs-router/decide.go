package main

import (
	"encoding/json"
	"fmt"
)

type Side uint8

const (
	Heap Side = iota
	Mirror
)

func (s Side) String() string {
	if s == Mirror {
		return "mirror"
	}
	return "heap"
}

// Thresholds are the two numbers the decision compares the plan against.
type Thresholds struct {
	Rows float64 // a scan of a mirrored table expected to read more than this
	Cost float64 // or a plan whose total cost is above this
}

// Catalog is what the router knows about the database besides the plan.
type Catalog struct {
	HeapSchema string
	Mirrored   map[string]bool // heap tables that have a view of the same name in the mirror schema
	Unique     map[string]bool // unique indexes in the heap schema, by name
}

// Decision is the answer for one statement.
type Decision struct {
	Side   Side
	Reason string
	// Eligible: the mirror could answer this at all (only mirrored tables, only
	// plan nodes that read). A transaction pinned to the mirror needs this and
	// nothing else.
	Eligible bool
	// Reads: the plan touches at least one relation.
	Reads bool
	// Stable: the answer cannot change with the statement's constants, so it can
	// be cached under the statement's shape instead of its exact text.
	Stable bool
}

type planNode struct {
	NodeType     string `json:"Node Type"`
	RelationName string `json:"Relation Name"`
	Schema       string `json:"Schema"`
	IndexName    string `json:"Index Name"`
	// Set on a Custom Scan; pg_duckdb's is "DuckDBScan".
	CustomProvider string     `json:"Custom Plan Provider"`
	PlanRows       float64    `json:"Plan Rows"`
	TotalCost      float64    `json:"Total Cost"`
	Plans          []planNode `json:"Plans"`
}

// Node types the mirror is allowed to stand in for. A whitelist, because the
// failure of a blacklist is a new PostgreSQL node type quietly going to DuckDB.
// Not here on purpose: ModifyTable and LockRows (writes), Function Scan and
// Table Function Scan (a PostgreSQL function is not a DuckDB function), Foreign
// and Custom Scan, Sample Scan, Tid Scan and Tid Range Scan (a view has no ctid).
var readNodes = map[string]bool{
	"Seq Scan": true, "Index Scan": true, "Index Only Scan": true,
	"Bitmap Heap Scan": true, "Bitmap Index Scan": true, "BitmapAnd": true, "BitmapOr": true,
	"Aggregate": true, "Group": true, "Sort": true, "Incremental Sort": true, "Limit": true,
	"Hash": true, "Hash Join": true, "Merge Join": true, "Nested Loop": true,
	"Gather": true, "Gather Merge": true, "Materialize": true, "Memoize": true,
	"Unique": true, "Append": true, "Merge Append": true, "Subquery Scan": true,
	"CTE Scan": true, "Result": true, "WindowAgg": true, "SetOp": true, "Values Scan": true,
}

// Nodes that read all of their input before returning a row. A LIMIT above one
// of these does not shorten the scan below it.
var blockingNodes = map[string]bool{
	"Aggregate": true, "Sort": true, "Incremental Sort": true, "Hash": true,
	"Materialize": true, "SetOp": true, "WindowAgg": true, "Gather Merge": true,
}

var scanNodes = map[string]bool{
	"Seq Scan": true, "Index Scan": true, "Index Only Scan": true, "Bitmap Heap Scan": true,
}

// ParsePlan reads the single row EXPLAIN (VERBOSE, FORMAT JSON) returns.
func ParsePlan(explainJSON []byte) (*planNode, error) {
	var doc []struct {
		Plan *planNode `json:"Plan"`
	}
	if err := json.Unmarshal(explainJSON, &doc); err != nil {
		return nil, err
	}
	if len(doc) != 1 || doc[0].Plan == nil {
		return nil, fmt.Errorf("EXPLAIN returned %d plans", len(doc))
	}
	return doc[0].Plan, nil
}

type planFacts struct {
	ineligible  string  // first reason the mirror cannot answer, "" if it can
	relations   int     // relation scans in the plan
	bigScan     string  // description of the largest effective scan of a mirrored table
	bigRows     float64 // its effective row count
	allPoint    bool    // every mirrored scan is a unique-index probe of at most one row
	heapLeak    string  // a mirrored heap table read directly (used on the mirror-side plan)
	customScans int
}

func walk(n *planNode, frac float64, cat *Catalog, f *planFacts) {
	if !readNodes[n.NodeType] && f.ineligible == "" {
		f.ineligible = "plan node " + n.NodeType
	}
	if n.NodeType == "Custom Scan" {
		f.customScans++
	}
	if n.RelationName != "" {
		f.relations++
		mirrored := n.Schema == cat.HeapSchema && cat.Mirrored[n.RelationName]
		if !mirrored && f.ineligible == "" {
			f.ineligible = fmt.Sprintf("reads %s.%s, which is not mirrored", n.Schema, n.RelationName)
		}
		if mirrored {
			f.heapLeak = n.Schema + "." + n.RelationName
			if scanNodes[n.NodeType] {
				eff := n.PlanRows * frac
				if eff > f.bigRows || f.bigScan == "" {
					f.bigRows = eff
					f.bigScan = fmt.Sprintf("%s on %s", n.NodeType, n.RelationName)
				}
				point := (n.NodeType == "Index Scan" || n.NodeType == "Index Only Scan") &&
					cat.Unique[n.IndexName] && n.PlanRows <= 1
				if !point {
					f.allPoint = false
				}
			} else {
				f.allPoint = false
			}
		}
	}
	child := frac
	switch {
	case blockingNodes[n.NodeType]:
		child = 1
	case n.NodeType == "Limit" && len(n.Plans) > 0:
		// The limit's own child is the last plan; earlier ones are InitPlans.
		in := n.Plans[len(n.Plans)-1].PlanRows
		if in > 0 && n.PlanRows < in {
			child = frac * n.PlanRows / in
		}
	}
	for i := range n.Plans {
		walk(&n.Plans[i], child, cat, f)
	}
}

// Decide is the routing rule. It is a pure function of the heap-side plan.
//
// The design this implements said "sequential scan with an estimated row count
// above a threshold". Taken literally that sends `SELECT * FROM t LIMIT 10` to
// the mirror: the Seq Scan under the Limit is still labelled with the whole
// table's row count. So the row count is scaled by every non-blocking LIMIT
// above the scan before it is compared.
func Decide(root *planNode, cat *Catalog, th Thresholds, force bool) Decision {
	f := planFacts{allPoint: true}
	walk(root, 1, cat, &f)
	switch {
	case f.ineligible != "":
		return Decision{Side: Heap, Reason: f.ineligible, Reads: f.relations > 0, Stable: true}
	case f.relations == 0:
		return Decision{Side: Heap, Reason: "reads no table", Stable: true}
	}
	d := Decision{Eligible: true, Reads: true}
	switch {
	case force:
		d.Side, d.Reason, d.Stable = Mirror, "qs.route=mirror", true
	case f.bigRows > th.Rows:
		d.Side = Mirror
		d.Reason = fmt.Sprintf("%s reads an estimated %.0f rows, over %.0f", f.bigScan, f.bigRows, th.Rows)
	case root.TotalCost > th.Cost:
		d.Side = Mirror
		d.Reason = fmt.Sprintf("total cost %.0f, over %.0f", root.TotalCost, th.Cost)
	case f.allPoint:
		d.Reason = "unique-index lookup of at most one row"
		d.Stable = true
	default:
		d.Reason = fmt.Sprintf("%s reads an estimated %.0f rows, cost %.0f: under both thresholds",
			f.bigScan, f.bigRows, root.TotalCost)
	}
	return d
}

// HeapLeak inspects the plan the SAME statement gets with the mirror schema in
// front. If a mirrored heap table is still being read, changing search_path did
// not move the statement (a view over the table, a function body) and sending
// it "to the mirror" would be two round trips for nothing.
func HeapLeak(root *planNode, cat *Catalog) string {
	f := planFacts{allPoint: true}
	walk(root, 1, cat, &f)
	return f.heapLeak
}

package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

// The registry is configuration (docs/40, "What the sidecar does for the
// extension"): these tests pin the decisions the sync makes from it. The
// writes themselves need a primary; what they may touch is decided here.

// A halted table keeps its registry row: the registry wants it as it wants a
// healthy one, and the halt is recorded as this node's report only.
func TestHaltedTableKeepsItsRegistryRow(t *testing.T) {
	root := t.TempDir()
	events := newTable(t, root, "events")
	accounts := newTable(t, root, "accounts")
	if err := accounts.Halt(nanReason); err != nil {
		t.Fatal(err)
	}
	tables := map[string]*mirror.Table{"public.events": events, "public.accounts": accounts}

	want := registryWanted(tables)
	if !want["public.accounts"] || !want["public.events"] || len(want) != 2 {
		t.Fatalf("registry wanted %v: a halted table must keep its row", want)
	}

	// A halt is not a DDL-level change of the registry: it does not make the
	// registry dirty, so no sync is asked for and no row is written.
	e := newExtLink(quiet(), options{tables: []string{"public.events", "public.accounts"}})
	e.dirty, e.checkedAt = false, time.Now()
	e.observe(tables)
	if e.dirty {
		t.Fatal("a halt marked the registry dirty: the registry would be written for node state")
	}

	// The node's own answer is the report: 0/0 for the halted table, and nothing
	// said for the healthy one (nothing is known about it yet on this process).
	got := e.lsns()
	if len(got) != 1 || got["public.accounts"] != "0/0" {
		t.Fatalf("this node's reports %v; want only the halted table as 0/0", got)
	}
}

// Two sidecars share one registry. The sidecar of a node whose accounts table
// is halted syncs; the row another node's healthy sidecar relies on survives.
func TestHealthySidecarRowSurvivesHaltedSidecarSync(t *testing.T) {
	root := t.TempDir()
	healthy := map[string]*mirror.Table{
		"public.events":   newTable(t, root, "events"),
		"public.accounts": newTable(t, root, "accounts"),
	}
	haltedNode := map[string]*mirror.Table{
		"public.events":   newTable(t, t.TempDir(), "events"),
		"public.accounts": newTable(t, t.TempDir(), "accounts"),
	}
	if err := haltedNode["public.accounts"].Halt(nanReason); err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{"public.events": true, "public.accounts": true}

	if !reflect.DeepEqual(registryWanted(healthy), registryWanted(haltedNode)) {
		t.Fatal("a halted node and a healthy one configure different registry rows")
	}
	if gone := registryUnregistrations(registryWanted(haltedNode), present); len(gone) != 0 {
		t.Fatalf("the halted node's sync would unregister %v, which the healthy node's sidecar relies on", gone)
	}
}

// Two sidecars whose table lists differ: a row for a table this sidecar does
// not mirror is never removed by it, even when that table's heap is gone from
// the source it sees. The registry is shared; the heap decides, and only for
// the tables this sidecar is configured for.
func TestSidecarNeverUnregistersTablesItIsNotConfiguredFor(t *testing.T) {
	root := t.TempDir()
	configured := registryWanted(map[string]*mirror.Table{"public.events": newTable(t, root, "events")})
	present := map[string]bool{"public.events": true, "public.orders": false}
	if got := registryUnregistrations(configured, present); len(got) != 0 {
		t.Fatalf("a sidecar that does not mirror public.orders unregistered %v", got)
	}
}

// A configured table whose heap relation is gone is cleaned; one still there
// is not, whatever its state. The decision is over the configuration.
func TestTableRemovedFromHeapIsUnregistered(t *testing.T) {
	root := t.TempDir()
	configured := registryWanted(map[string]*mirror.Table{
		"public.events":   newTable(t, root, "events"),
		"public.accounts": newTable(t, root, "accounts"),
	})
	present := map[string]bool{"public.events": true, "public.accounts": false}
	if got := registryUnregistrations(configured, present); !reflect.DeepEqual(got, []string{"public.accounts"}) {
		t.Fatalf("unregistered %v; want only the table whose heap is gone", got)
	}
	// Nothing is gone when every heap is there.
	if got := registryUnregistrations(configured, map[string]bool{"public.events": true, "public.accounts": true}); len(got) != 0 {
		t.Fatalf("unregistered %v with every heap present", got)
	}
}

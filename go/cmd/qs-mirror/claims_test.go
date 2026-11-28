package main

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

func heaps(rs []regRow) []string {
	var out []string
	for _, r := range rs {
		out = append(out, r.Heap)
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var (
	t0     = time.Unix(3_000_000, 0)
	rowsT1 = []regRow{{Heap: "public.t1"}, {Heap: "public.t2"}}
)

// Two sidecars with different table lists: a table one of them still names is
// never an orphan, whichever of the two is up.
func TestOrphansTwoSidecarsDifferentLists(t *testing.T) {
	a := claimRow{Node: "pod-a", Tables: []string{"public.t1", "public.t2"}, Seen: t0}
	b := claimRow{Node: "pod-b", Tables: []string{"public.t1"}, Seen: t0}
	if got := registryOrphans(rowsT1, []claimRow{a, b}, t0, time.Hour); len(got) != 0 {
		t.Fatalf("both live: orphans %v", heaps(got))
	}
	// pod-a drops t2 from its configuration: pod-b never named it, pod-a no
	// longer does, so t2 is still named by nobody live once its claim is seen.
	a.Tables = []string{"public.t1"}
	if got := heaps(registryOrphans(rowsT1, []claimRow{a, b}, t0, time.Hour)); !eq(got, []string{"public.t2"}) {
		t.Fatalf("dropped from the one that named it: orphans %v", got)
	}
	// t1 is still named by pod-b: removing it from pod-a alone keeps it.
	b.Tables = nil
	a.Tables = nil
	if got := registryOrphans(rowsT1, []claimRow{a, b}, t0, time.Hour); len(got) != 2 {
		t.Fatalf("dropped from every sidecar: orphans %v", heaps(got))
	}
}

// A table dropped from one sidecar's QS_TABLES only is still claimed by the other.
func TestOrphansDroppedFromOneSidecarOnly(t *testing.T) {
	a := claimRow{Node: "pod-a", Tables: []string{"public.t1"}, Seen: t0}
	b := claimRow{Node: "pod-b", Tables: []string{"public.t1", "public.t2"}, Seen: t0}
	if got := heaps(registryOrphans(rowsT1, []claimRow{a, b}, t0, time.Hour)); len(got) != 0 {
		t.Fatalf("t2 is still configured on pod-b, but orphans say %v", got)
	}
}

// A table that leaves every QS_TABLES is cleaned once its last claim is older than
// the grace, and not before: the grace is the window in which a restart or a
// rolling update must not cost a registration.
func TestOrphansAfterTheGrace(t *testing.T) {
	grace := time.Hour
	a := claimRow{Node: "pod-a", Tables: []string{"public.t2"}, Seen: t0}
	rows := []regRow{{Heap: "public.t2"}}
	if got := registryOrphans(rows, []claimRow{a}, t0.Add(grace-time.Second), grace); len(got) != 0 {
		t.Fatalf("inside the grace: orphans %v", heaps(got))
	}
	if got := heaps(registryOrphans(rows, []claimRow{a}, t0.Add(grace+time.Second), grace)); !eq(got, []string{"public.t2"}) {
		t.Fatalf("after the grace: orphans %v", got)
	}
}

// A sidecar down for an hour: its claim is no longer live after the grace, so its
// rows go, and it registers them again when it comes back. Within the grace its
// tables are kept.
func TestOrphansSidecarDownForAnHour(t *testing.T) {
	down := claimRow{Node: "pod-down", Tables: []string{"public.t1"}, Seen: t0}
	rows := []regRow{{Heap: "public.t1"}}
	if got := registryOrphans(rows, []claimRow{down}, t0.Add(59*time.Minute), time.Hour); len(got) != 0 {
		t.Fatalf("down 59 minutes: orphans %v", heaps(got))
	}
	if got := heaps(registryOrphans(rows, []claimRow{down}, t0.Add(61*time.Minute), time.Hour)); !eq(got, []string{"public.t1"}) {
		t.Fatalf("down 61 minutes: orphans %v", got)
	}
}

// Scale-down: a deleted pod's claim goes stale; another sidecar that still mirrors
// the table keeps the row, whatever the deleted pod had.
func TestOrphansScaleDownKeepsLiveClaimants(t *testing.T) {
	gone := claimRow{Node: "pod-gone", Tables: []string{"public.t1"}, Seen: t0}
	live := claimRow{Node: "pod-live", Tables: []string{"public.t1"}, Seen: t0.Add(2 * time.Hour)}
	rows := []regRow{{Heap: "public.t1"}}
	if got := registryOrphans(rows, []claimRow{gone, live}, t0.Add(2*time.Hour+time.Minute), time.Hour); len(got) != 0 {
		t.Fatalf("a live claimant remains: orphans %v", heaps(got))
	}
}

// Promotion: the promoted pod stops claiming and its claim ages out; the rows of
// the tables it had are kept only by a claimant that is still there.
func TestOrphansPromotedPodClaimAgesOut(t *testing.T) {
	promoted := claimRow{Node: "pod-promoted", Tables: []string{"public.t1"}, Seen: t0}
	rows := []regRow{{Heap: "public.t1"}}
	if got := registryOrphans(rows, []claimRow{promoted}, t0.Add(10*time.Minute), time.Hour); len(got) != 0 {
		t.Fatalf("within the grace the promoted claim still counts: orphans %v", heaps(got))
	}
	if got := heaps(registryOrphans(rows, []claimRow{promoted}, t0.Add(2*time.Hour), time.Hour)); !eq(got, []string{"public.t1"}) {
		t.Fatalf("after the grace nothing claims it: orphans %v", got)
	}
}

// A row unclaimed at one sweep is removed only once it has been unclaimed for the
// grace, and a row that is claimed again in between starts over.
func TestSettleWaitsOutTheGrace(t *testing.T) {
	saved := claimGrace
	claimGrace = time.Hour
	defer func() { claimGrace = saved }()
	j := &janitor{}
	orphan := []regRow{{Heap: "public.t2"}}
	if got := j.settle(orphan, t0); len(got) != 0 {
		t.Fatalf("first sight removed the row: %v", heaps(got))
	}
	if got := j.settle(orphan, t0.Add(59*time.Minute)); len(got) != 0 {
		t.Fatalf("inside the grace removed the row: %v", heaps(got))
	}
	// Claimed again: the clock stops.
	if got := j.settle(nil, t0.Add(59*time.Minute+30*time.Second)); len(got) != 0 {
		t.Fatalf("nothing unclaimed, nothing removed: %v", heaps(got))
	}
	if got := j.settle(orphan, t0.Add(61*time.Minute)); len(got) != 0 {
		t.Fatalf("a claim that came back must restart the clock: %v", heaps(got))
	}
	if got := heaps(j.settle(orphan, t0.Add(61*time.Minute+time.Hour))); !eq(got, []string{"public.t2"}) {
		t.Fatalf("after the grace again: %v", got)
	}
}

// A heartbeat inside its interval touches nothing: the connection is nil here, so
// a write would panic.
func TestClaimHeartbeatIsRateLimited(t *testing.T) {
	c := &claimer{log: slog.New(slog.NewTextHandler(os.Stderr, nil)), node: "pod", tables: []string{"public.t1"}, at: time.Now()}
	c.heartbeat(context.Background(), nil)
	if c.absent || !c.at.After(time.Now().Add(-time.Minute)) {
		t.Fatal("heartbeat changed state inside its interval")
	}
}

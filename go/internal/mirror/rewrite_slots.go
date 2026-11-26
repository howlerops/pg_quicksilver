package mirror

// A process-wide limit on base rewrites in flight.
//
// A rewrite decodes the table it rewrites a row group at a time, and while it
// runs it holds that group's columns, its key index builder and its writer
// buffers. Each table's rewrite is bounded by its own row group, so one rewrite
// is cheap. Several at once are not: the memory is per table, the tables run
// on one process, and a DuckLake base rewrite (lakeRewrites) asks for one on
// every table at the same time. Measured with four tables under churn, three
// rewrites overlapped and the sidecar's RSS went from ~250 MB to 1.25 GB while
// the Go heap in use was about 0.5 GB; the rest was the collector's headroom and
// memory not yet returned to the OS.
//
// So the number of rewrites in flight is capped process-wide. A table that asks
// while the slots are taken is not an error: BeginCompaction returns nil, the
// caller tries again on its next tick, and in the meantime that table keeps its
// deltas bounded by merging them, which the apply loop already does for a table
// that is not compacting. Rewrites therefore queue behind one another and the
// peak is that of the largest single rewrite, not the sum.

// rewriteSlotsMax is QS_MAX_CONCURRENT_REWRITES, default 1 (envInt ignores
// zero and negative values).
var rewriteSlotsMax = int(envInt("QS_MAX_CONCURRENT_REWRITES", 1))

// rewriteSlots holds one token per rewrite in flight.
var rewriteSlots = make(chan struct{}, rewriteSlotsMax)

// tryAcquireRewriteSlot takes a slot without waiting. False means the process
// already has the maximum number of rewrites running.
func tryAcquireRewriteSlot() bool {
	select {
	case rewriteSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

// releaseRewriteSlot returns a slot taken by tryAcquireRewriteSlot. It is
// called exactly once per successful acquire, by the rewrite goroutine as it
// ends, whether or not the rewrite was aborted before the swap.
func releaseRewriteSlot() {
	<-rewriteSlots
}

// rewriteSlotsInUse reports how many rewrites hold a slot now.
func rewriteSlotsInUse() int { return len(rewriteSlots) }

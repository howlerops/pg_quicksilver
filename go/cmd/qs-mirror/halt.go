package main

// One table that cannot be mirrored is not a Pod that cannot mirror.
//
// A halt used to stop everything: pump returned the Apply error, the next
// attempt found the halt in the manifest and refused to start, and supervise
// dropped the slot. So a NaN in one numeric column cost every other table on
// the Pod its stream, and a fresh snapshot each when an operator arrived. This
// file holds the three decisions that make a halt cost one table instead:
//
//	classifyTableError  is this error "this table cannot continue", or is it
//	                    the process that cannot?
//	planHalted          at start, which halted tables has an operator asked to
//	                    have rebuilt, and which stay out of service?
//	noteHalted          tell readiness, once, and count what is left.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/howlerops/pg_quicksilver/go/internal/health"
	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

type verdict int

const (
	// stopProcess: return the error, let supervise back off and retry. Nothing
	// is recorded, so the retry resumes where the manifests say.
	stopProcess verdict = iota
	// haltTable: this table is out of service, durably; the others carry on.
	haltTable
)

// classifyTableError draws the line between an error that takes ONE TABLE out
// of service and one that stops the process, and it is the only place that
// line is drawn. err is what Table.Apply or Table.Snapshot returned;
// manifestHalt is that table's halt reason after the call.
//
// The line is drawn conservatively, which here means: when in doubt, stop the
// process. A wrong "stop" costs a retry with backoff and loses nothing — the
// slot holds what was not confirmed. A wrong "halt" discards a healthy table's
// changes until an operator rebuilds it, over what may have been a full disk.
//
// So a table halt needs BOTH of:
//
//  1. the mirror package already decided it. Table.refuse records the halt in
//     the manifest before it returns, for exactly the errors that mean "the
//     source holds something this table cannot represent" (a value the column
//     type cannot store exactly, a temporal out of range). Nothing here second-
//     guesses which errors those are; an error the mirror did not halt for is
//     never promoted to a halt.
//  2. the error IS that halt: its text carries the recorded reason. A table can
//     be halted and then fail for an unrelated reason (a rebuild of a halted
//     table whose snapshot hits ENOSPC), and that must not read as "the rebuild
//     was refused".
//
// and NONE of: a cancelled or expired context, a filesystem error, an errno.
// Those are the process's problem whatever the manifest says.
func classifyTableError(err error, manifestHalt string) verdict {
	if err == nil || manifestHalt == "" {
		return stopProcess
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return stopProcess
	}
	var pathErr *fs.PathError
	var linkErr *os.LinkError
	var errno syscall.Errno
	if errors.As(err, &pathErr) || errors.As(err, &linkErr) || errors.As(err, &errno) {
		return stopProcess
	}
	if !strings.Contains(err.Error(), manifestHalt) {
		return stopProcess
	}
	return haltTable
}

// haltOrFail applies classifyTableError to one table's error. It returns nil
// when the table is now durably halted and the caller should carry on with the
// others, and the error to stop the process with otherwise.
func haltOrFail(log *slog.Logger, h *health.Health, qualified string, t *mirror.Table, err error) error {
	reason := t.Halted()
	if classifyTableError(err, reason) != haltTable {
		return err
	}
	// Table.refuse ignores the error from writing the halt. A halt that is in
	// memory and not on disk is undone by the next restart, which then resumes
	// a table that is missing whatever was discarded while it was "halted". So
	// it is written again here, and failing to write it IS a process error.
	//
	// "Again" is safe only because Table.Halt knows the Apply failed: it adds
	// the halt to the manifest ON DISK, the last one that Apply's predecessor
	// committed, and does not save the Table's memory — which names the failed
	// transaction's chunks with none of their tombstones. The Table is not
	// used for anything else from here on: every loop steps over a halted
	// table, and it would refuse (mirror.ErrNeedsReopen) if one did not.
	if herr := t.Halt(reason); herr != nil {
		return fmt.Errorf("%s must halt but the halt cannot be made durable: %w (halt reason: %s)",
			qualified, herr, reason)
	}
	recordHalt(log, h, qualified, t)
	return nil
}

// recordHalt tells readiness and the log about a halted table, once per reason.
func recordHalt(log *slog.Logger, h *health.Health, qualified string, t *mirror.Table) {
	if !h.SetHalted(qualified, t.Halted()) {
		return
	}
	// A rewrite in flight would never be collected: maintain skips this table.
	t.AbortCompaction()
	log.Error("table halted; it is out of service and its changes are discarded "+
		"until it is rebuilt. The other tables are still mirrored",
		"table", qualified, "reason", t.Halted(),
		"rebuild", "create "+filepath.Join(t.Dir, rebuildMarker)+" and restart the sidecar")
}

// noteHalted reconciles readiness with the tables and returns how many are
// still in service. It runs every tick because a halt can also be recorded
// where this loop does not see the error: a background rewrite that meets an
// unstorable value halts the table from FinishCompaction.
func noteHalted(log *slog.Logger, h *health.Health, tables map[string]*mirror.Table) (live int) {
	for q, t := range tables {
		if t.Halted() == "" {
			live++
			continue
		}
		recordHalt(log, h, q, t)
	}
	return live
}

func errAllHalted(tables map[string]*mirror.Table) error {
	names := make([]string, 0, len(tables))
	for q := range tables {
		names = append(names, q)
	}
	sort.Strings(names)
	return fmt.Errorf("%w: every mirrored table is halted (%s); none will resume without an operator",
		ErrHalted, strings.Join(names, ", "))
}

// ---- rebuilding one halted table ------------------------------------------

// rebuildMarker is the file an operator creates in a halted table's mirror
// directory to ask for that table to be rebuilt from a snapshot:
//
//	touch <QS_MIRROR_PATH>/public.orders/REBUILD     then restart the sidecar
//
// A file rather than an environment variable (QS_REBUILD_TABLES was the other
// candidate), for three reasons:
//
//   - It is a REQUEST, and a request is spent when it is honoured. The marker
//     is removed once the rebuild succeeds. An env var stays in the Pod spec
//     for as long as someone forgets it, and every later halt of that table —
//     a halt exists to make a human look — would be rebuilt away on the next
//     restart without one.
//   - It is per Pod. The halt is a fact about one Pod's mirror directory; an
//     env var set through the plugin reaches every instance and restarts them
//     all to deliver it.
//   - It lives where the halt lives: same directory, same volume as the
//     manifest that records it, so the two cannot be separated by a restore.
const rebuildMarker = "REBUILD"

func rebuildRequested(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, rebuildMarker))
	return err == nil
}

// haltedPlan is what to do at start with the tables whose manifests are halted.
type haltedPlan struct {
	Rebuild []string // halted AND requested: re-snapshot, then clear the halt
	Stay    []string // halted, no request: out of service, as before the restart
	Stale   []string // requested but NOT halted: the request is discarded
}

// planHalted decides it. halted maps every configured table to its manifest's
// halt reason ("" when it is not halted); requested holds the tables whose
// directory carries the marker.
//
// A request for a table that is not halted is discarded rather than kept or
// honoured. Honoured, a stray file re-snapshots a healthy table. Kept, it
// waits in the directory and rebuilds that table unattended the next time it
// halts, which is the one thing a halt must never do.
func planHalted(halted map[string]string, requested map[string]bool) haltedPlan {
	var p haltedPlan
	for table, reason := range halted {
		switch {
		case reason != "" && requested[table]:
			p.Rebuild = append(p.Rebuild, table)
		case reason != "":
			p.Stay = append(p.Stay, table)
		case requested[table]:
			p.Stale = append(p.Stale, table)
		}
	}
	sort.Strings(p.Rebuild)
	sort.Strings(p.Stay)
	sort.Strings(p.Stale)
	return p
}

// finishRebuild puts a rebuilt table back in service. The order is the point:
// the snapshot is already durable (Snapshot wrote the manifest, halt and all),
// then the halt is cleared, then the request is removed. A crash between any
// two leaves either a halted table with its request still standing — rebuilt
// again at the next start — or a healthy table with a stale request, which
// planHalted discards.
func finishRebuild(log *slog.Logger, h *health.Health, qualified string, t *mirror.Table) error {
	if err := t.ClearHalt(); err != nil {
		return fmt.Errorf("rebuilt %s but could not clear its halt: %w", qualified, err)
	}
	if err := os.Remove(filepath.Join(t.Dir, rebuildMarker)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Warn("rebuilt the table but could not remove the rebuild request; "+
			"it will be discarded at the next start", "table", qualified, "err", err)
	}
	h.ClearHalted(qualified)
	log.Info("halted table rebuilt from a snapshot and back in service", "table", qualified)
	return nil
}

// serveWithHalted reads QS_SERVE_WITH_HALTED_TABLES. Anything but an explicit
// yes is no: the default is that a Pod with a halted table leaves the read
// endpoint, so a reader never gets a partial set of mirrors without having
// been told.
func serveWithHalted() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QS_SERVE_WITH_HALTED_TABLES"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

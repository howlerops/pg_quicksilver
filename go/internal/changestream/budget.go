package changestream

// How much disk the spool may take.
//
// Nothing bounded it. A spool lives under the mirror path (cmd/qs-mirror
// points SpoolDir at <mirror>/.spool), which is the volume the data files are
// on, so one very large transaction — or one left open for a day while others
// streamed past it — grew until a write failed with ENOSPC. Which write is
// not the spool's to choose: it was as likely to be the Parquet file an Apply
// was half-way through as the spool itself.
//
// So every spool file of a Streaming counts its bytes into one total, and the
// total has a budget. Two different things happen at the budget, because two
// different things can be over it:
//
// A CONFIRMATION WOULD GIVE BYTES BACK. Some of the total belongs to
// transactions that have committed and are waiting to be applied and
// confirmed (their files are deleted at Confirm). Then the receiver stops
// reading the socket, exactly as it does for a full in-memory buffer and with
// the same consequences: status updates go on being sent so the walsender
// does not time the connection out, and the backlog stays in PostgreSQL's WAL
// behind the slot. The apply loop is not paused — it is what frees the bytes
// — and its Confirm wakes the receiver.
//
// NONE WOULD. Everything spooled belongs to transactions still arriving.
// Pausing cannot help: the server sends the rest of a streamed transaction
// only to a reader, and nothing here can be applied before its commit. The
// stream fails with *SpoolBudgetError, which says how many bytes the
// transactions had reached, what the budget was and what to change. Nothing
// of them was applied, no data file was being written on their account, and
// the slot has not moved, so a restart with more room receives them again.
// That is the "single transaction larger than the budget" case, and also the
// rarer one of several open transactions that only together exceed it.
//
// The check runs between messages, so the total can pass the budget by one
// change before it is acted on. The default budget leaves three quarters of
// what was free to the data files, which is what makes that slack harmless.

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/howlerops/pg_quicksilver/go/internal/diskfree"
)

// SpoolMaxBytesEnv names the environment variable that fixes the budget, in
// bytes. Unset or 0 derives it; a negative number or "off" removes the bound.
const SpoolMaxBytesEnv = "QS_SPOOL_MAX_BYTES"

const (
	// spoolFreeFraction is the share of the volume's free space, measured when
	// the stream starts, that the default budget takes.
	spoolFreeFraction = 4
	// SpoolBudgetCap is the most the default budget is ever set to, however
	// large the volume: a spool is replayed from the slot after any restart,
	// and beyond this the right answer is a smaller transaction.
	SpoolBudgetCap = int64(64) << 30
	// spoolBudgetUnknown is the default when the free space cannot be read.
	spoolBudgetUnknown = int64(8) << 30
)

// spoolFree is the free-space probe, replaced by tests.
var spoolFree diskfree.Probe = diskfree.Free

// DefaultSpoolBudget is the budget a Streaming whose SpoolMaxBytes is zero
// gives itself at Start: QS_SPOOL_MAX_BYTES if set, otherwise a quarter of the
// free space on the volume holding dir, at most SpoolBudgetCap. A negative
// result means unbounded.
func DefaultSpoolBudget(dir string) int64 {
	if v := strings.TrimSpace(os.Getenv(SpoolMaxBytesEnv)); v != "" {
		if strings.EqualFold(v, "off") {
			return -1
		}
		if n, err := strconv.ParseFloat(v, 64); err == nil && n != 0 {
			return int64(n)
		}
	}
	dir = spoolDirOrTemp(dir)
	// The directory may not exist yet; its volume is its nearest ancestor's.
	for probe := dir; ; {
		if free, ok := spoolFree(probe); ok {
			if b := free / spoolFreeFraction; b < SpoolBudgetCap {
				return max(b, 1)
			}
			return SpoolBudgetCap
		}
		parent := parentDir(probe)
		if parent == probe {
			return spoolBudgetUnknown
		}
		probe = parent
	}
}

func parentDir(p string) string {
	p = strings.TrimRight(p, string(os.PathSeparator))
	i := strings.LastIndexByte(p, os.PathSeparator)
	if i <= 0 {
		return string(os.PathSeparator)
	}
	return p[:i]
}

func (s *Streaming) resolveSpoolBudget() {
	if s.SpoolMaxBytes == 0 {
		s.SpoolMaxBytes = DefaultSpoolBudget(s.SpoolDir)
	}
}

// SpoolBudgetError is why a stream stopped: the transactions it was still
// receiving had spooled more than the budget, and no confirmation could give
// any of it back.
type SpoolBudgetError struct {
	Spooled int64  // bytes the unfinished transactions had reached
	Budget  int64  // the budget they were held to
	Open    int    // how many transactions those bytes belong to
	Dir     string // where the spool is
}

func (e *SpoolBudgetError) Error() string {
	what := "a transaction still being received has"
	if e.Open > 1 {
		what = fmt.Sprintf("%d transactions still being received have", e.Open)
	}
	return fmt.Sprintf("spool budget exceeded: %s spooled %d bytes under %s, and the budget is %d "+
		"(%s; unset, it is 1/%d of the free space on that volume when the stream starts, at most %d). "+
		"It needs MORE than %d bytes — how much more is not known until it commits. "+
		"Nothing of it was applied and the slot has not moved, so it will be sent again: "+
		"free space on the volume or set %s above its size, then restart",
		what, e.Spooled, e.Dir, e.Budget, SpoolMaxBytesEnv, spoolFreeFraction, SpoolBudgetCap,
		e.Spooled, SpoolMaxBytesEnv)
}

// spoolVerdict is the budget check: whether to stop reading until a
// confirmation frees spool bytes, or to fail because none will. Caller holds
// mu.
func (s *Streaming) spoolVerdict() (pause bool, err error) {
	if s.SpoolMaxBytes <= 0 {
		return false, nil
	}
	total := s.spoolBytes.Load()
	if total < s.SpoolMaxBytes {
		return false, nil
	}
	if s.heldBytes > 0 {
		return true, nil
	}
	open := len(s.streams)
	if s.cur != nil && s.cur.txn.spool != nil {
		open++
	}
	return false, &SpoolBudgetError{
		Spooled: total, Budget: s.SpoolMaxBytes, Open: open, Dir: spoolDirOrTemp(s.SpoolDir),
	}
}

// SpoolBytes is how many bytes of spool files this stream has on disk now:
// transactions still arriving plus committed ones not yet confirmed. It is
// the number to export as a metric beside BufferedChanges.
func (s *Streaming) SpoolBytes() int64 { return s.spoolBytes.Load() }

// SpoolBudgetBytes is the bound SpoolBytes is held to (zero before Start has
// derived it, negative for none).
func (s *Streaming) SpoolBudgetBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.SpoolMaxBytes
}

// SpoolPaused reports whether the receiver has stopped reading the socket
// because the spool is at its budget and is waiting for a Confirm to free
// some of it.
func (s *Streaming) SpoolPaused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spoolPaused
}

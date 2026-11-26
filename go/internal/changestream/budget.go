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
// total has a budget. Three different things happen at the budget, because
// three different things can be over it:
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
// NONE WOULD, AND ONE TRANSACTION IS THE WHOLE BUDGET. Everything spooled
// belongs to transactions still arriving, and one of them has by itself
// reached the budget: it does not fit even with every other byte drained.
// Pausing cannot help: the server sends the rest of a streamed transaction
// only to a reader, and nothing here can be applied before its commit. The
// stream fails with *SpoolBudgetError, which says how many bytes the
// transaction had reached, what the budget was and what to change. Nothing
// of it was applied, no data file was being written on its account, and
// the slot has not moved, so a restart with more room receives it again.
//
// NONE WOULD, BUT EACH TRANSACTION STILL FITS. Several transactions the
// server is streaming at once have only TOGETHER reached the budget. This
// used to fail like the case above, and that was wrong twice over: none of
// them is too large, and a restart replays the same interleaving into the
// same failure, for ever. Pausing is wrong too — nothing is complete, so no
// Confirm would ever come. The receiver READS ON: the first of them to
// commit becomes a transaction a confirmation gives back, which is the first
// case, and the receiver pauses there until the applier has drained it. The
// price is stated plainly: while this lasts the total is over the budget, by
// less than one budget per further transaction open at once. The default
// budget is a quarter of what was free, for that reason among others.
//
// The check runs between messages, so one transaction can pass the budget by
// one change before it is acted on.

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

// SpoolBudgetError is why a stream stopped: one transaction it was still
// receiving had by itself spooled as much as the budget, so it could not fit
// whatever a confirmation gave back.
type SpoolBudgetError struct {
	Spooled int64  // bytes that one unfinished transaction had reached
	Budget  int64  // the budget it was held to
	Open    int    // how many spooled transactions were still arriving, it among them
	Total   int64  // bytes of all of those together
	Dir     string // where the spool is
}

func (e *SpoolBudgetError) Error() string {
	others := ""
	if e.Open > 1 {
		others = fmt.Sprintf(" (%d transactions were arriving at once, %d bytes between them; "+
			"the others are not the reason)", e.Open, e.Total)
	}
	return fmt.Sprintf("spool budget exceeded: a transaction still being received has spooled %d bytes under %s, "+
		"and the budget is %d%s "+
		"(%s; unset, it is 1/%d of the free space on that volume when the stream starts, at most %d). "+
		"It needs MORE than %d bytes — how much more is not known until it commits. "+
		"Nothing of it was applied and the slot has not moved, so it will be sent again: "+
		"free space on the volume or set %s above its size, then restart",
		e.Spooled, e.Dir, e.Budget, others, SpoolMaxBytesEnv, spoolFreeFraction, SpoolBudgetCap,
		e.Spooled, SpoolMaxBytesEnv)
}

// spoolVerdict is the budget check: whether to stop reading until a
// confirmation frees spool bytes, or to fail because one transaction cannot
// fit whatever is freed. Neither means read on. Caller holds mu and is the
// receiving goroutine, which is the only writer of an assembling spool.
func (s *Streaming) spoolVerdict() (pause bool, err error) {
	if s.SpoolMaxBytes <= 0 {
		return false, nil
	}
	total := s.spoolBytes.Load()
	if total < s.SpoolMaxBytes {
		return false, nil
	}
	// A complete transaction is spooled and the consumer can drain it: wait
	// for that, whatever else is open.
	if s.heldBytes > 0 {
		return true, nil
	}
	// Everything counted is still arriving. Only a transaction that is the
	// whole budget by itself is one no draining could make room for.
	open, largest, arriving := s.openSpools()
	if largest < s.SpoolMaxBytes {
		return false, nil
	}
	return false, &SpoolBudgetError{
		Spooled: largest, Budget: s.SpoolMaxBytes, Open: open, Total: arriving,
		Dir: spoolDirOrTemp(s.SpoolDir),
	}
}

// openSpools counts the transactions still arriving that have a spool file:
// how many, the bytes of the largest and of all of them. Caller holds mu and
// is the receiving goroutine.
func (s *Streaming) openSpools() (n int, largest, total int64) {
	note := func(a *assembly) {
		if a == nil || a.txn.spool == nil {
			return
		}
		b := a.txn.spool.budgeted()
		n++
		total += b
		largest = max(largest, b)
	}
	for _, a := range s.streams {
		note(a)
	}
	note(s.cur)
	return n, largest, total
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

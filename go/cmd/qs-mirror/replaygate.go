package main

// The replay gate: a commit becomes visible in the mirror only once the local
// standby has replayed it.
//
// The sidecar reads the PRIMARY's WAL and the standby reads its own. They are
// not in step. With replay paused at R, the sidecar keeps applying commits
// past R, so the mirror can hold effects the standby's heap does not yet have:
// a session routed to the mirror at one moment and to the heap a moment later
// read newer data and then older data. The extension's freshness check had a
// lower bound only (applied >= replay), so nothing stopped that.
//
// The invariant, and where each part of it is enforced:
//
//	A reported applied LSN X for a table means: the mirror contains exactly the
//	effects of every commit with end LSN <= X on that table, and X <= the local
//	replay LSN at the time it was published.
//
//   - Apply (this file, gatedPrefix, called from pump): a transaction whose
//     commit ends at E = NextLSN is applied only when E <= replay. A tick takes
//     the longest such PREFIX, so commit order is kept and a streamed
//     transaction is held whole until its commit, the same as any other.
//   - Confirm (pump): only what was applied is confirmed. A held transaction is
//     not confirmed, so the slot keeps it and a restart replays it.
//   - Idle advance (pump): the idle position P is capped at replay+1, so the
//     marker that writes applied_lsn and the confirmation never run past it.
//   - Report (extension.go, reportFor): a table whose published LSN is past the
//     replay LSN is reported 0/0 (not readable on this node: the snapshot at S
//     of a table is not served until replay reaches S), and the idle position
//     reported for it is clamped to the replay LSN.
//   - Read (pg_quicksilver.c, qs_fresh): the mirror is routed only when it is
//     at or past the statement's replay position (lower bound) and not past it
//     (upper bound, which falls back to the heap).
//
// Why E is NextLSN. A commit record starts at CommitLSN and ends after it.
// pg_last_wal_replay_lsn() is always the END of a replayed record, so
// replay > CommitLSN means the commit record has been replayed completely.
// NextLSN is CommitLSN+1 (changestream/pgoutput.go), so "NextLSN <= replay" is
// exactly that test.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

// gateOnReplay is QS_GATE_ON_REPLAY. On by default. Off restores the old
// behaviour, for a mirror that has no local Postgres to replay.
var gateOnReplay = envBool("QS_GATE_ON_REPLAY", true)

// replayPoll is how often the pump asks the standby for its replay position
// while it holds transactions back. Short, because it is the latency added to
// a commit that the standby has only just replayed.
var replayPoll = envDuration("QS_REPLAY_POLL", 20*time.Millisecond)

// idleTarget is the position an idle advance may confirm and mark, given the
// stream's idle position p. The marker writes applied_lsn = target-1 and
// confirms target, so neither may pass what the standby has replayed: with the
// gate on, the target is at most replay+1. A replay position not yet read (0)
// advances nothing. With the gate off, p stands.
func idleTarget(p, replay uint64, gated bool) uint64 {
	if !gated {
		return p
	}
	if replay == 0 {
		return 0
	}
	if p > replay+1 {
		return replay + 1
	}
	return p
}

// pollReplay is the wait between two looks at the standby's replay position
// while transactions are held. It ends early when the stream has something,
// because the stream's signal is the other reason to look.
func pollReplay(ctx context.Context, ready <-chan struct{}) error {
	t := time.NewTimer(replayPoll)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	case <-ready:
	}
	return nil
}

// lsnString is the LSN spelling the rest of the sidecar uses ("X/Y").
func lsnString(v uint64) string {
	return fmt.Sprintf("%X/%X", uint32(v>>32), uint32(v))
}

// envBool reads an on/off switch in the spellings the sidecar accepts. An
// unrecognised value is the default, as it is for the other switches.
func envBool(key string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "on", "1", "true", "yes":
		return true
	case "off", "0", "false", "no":
		return false
	}
	return def
}

// gatedPrefix returns how many of the leading transactions the standby has
// replayed through replay. Transactions are in commit order, so the answer is
// a prefix: the first one not yet replayed holds back everything after it.
func gatedPrefix(txns []changestream.Transaction, replay uint64) int {
	for i, t := range txns {
		if changestream.ParseLSN(t.NextLSN) > replay {
			return i
		}
	}
	return len(txns)
}

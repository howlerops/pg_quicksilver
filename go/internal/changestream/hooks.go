package changestream

// What must be durable before a position is confirmed, beyond what the caller
// of Confirm wrote itself.
//
// Confirm's contract has always been "call only after the mirror write is
// fsynced". The mirror keeps one piece of state that is not written by any
// table's Apply — the per-root watermark that stands in for the manifests of
// tables a batch did not touch (internal/mirror/watermark.go) — and it has to
// reach the disk at exactly this point: after every table has applied the
// batch, before the server is told. Rather than have every caller remember a
// second call, whoever owns such state registers it here and Streaming.Confirm
// runs it first. A hook that fails fails the Confirm, and the position is not
// advanced.

import "sync"

var beforeConfirm struct {
	sync.RWMutex
	fns []func(lsn string) error
}

// BeforeConfirm registers fn to run at the start of every Streaming.Confirm
// that advances the confirmed position, with the LSN being confirmed THROUGH
// (a transaction's NextLSN). It runs on the caller's goroutine, before
// anything is recorded.
func BeforeConfirm(fn func(lsn string) error) {
	beforeConfirm.Lock()
	beforeConfirm.fns = append(beforeConfirm.fns, fn)
	beforeConfirm.Unlock()
}

// RunBeforeConfirm runs the registered hooks. Streaming.Confirm and
// Logical.Confirm call it themselves; it is exported for a Stream implemented
// elsewhere, which must call it before it confirms lsn.
func RunBeforeConfirm(lsn string) error { return runBeforeConfirm(lsn) }

func runBeforeConfirm(lsn string) error {
	beforeConfirm.RLock()
	fns := beforeConfirm.fns
	beforeConfirm.RUnlock()
	for _, fn := range fns {
		if err := fn(lsn); err != nil {
			return err
		}
	}
	return nil
}

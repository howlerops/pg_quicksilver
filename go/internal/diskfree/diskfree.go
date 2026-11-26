// Package diskfree answers one question: how many bytes can still be written
// to the volume a path is on. Two budgets are derived from it — the spool's
// (internal/changestream) and the retention grace's (internal/mirror) — and
// both are tested with a probe of their own, which is why Probe is a type.
package diskfree

// Probe reports the bytes available to an unprivileged writer on the volume
// holding path. ok is false when it cannot be known (the path does not exist,
// or the platform has no statfs); callers then fall back to a fixed number.
type Probe func(path string) (free int64, ok bool)

// Free is the real probe.
func Free(path string) (int64, bool) { return free(path) }

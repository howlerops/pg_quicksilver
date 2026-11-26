//go:build unix

package diskfree

import (
	"math"
	"syscall"
)

func free(path string) (int64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, false
	}
	// Bavail, not Bfree: the blocks reserved for root are not this process's
	// to plan with, even when it runs as root.
	n := uint64(st.Bavail) * uint64(st.Bsize)
	if n > math.MaxInt64 {
		return math.MaxInt64, true
	}
	return int64(n), true
}

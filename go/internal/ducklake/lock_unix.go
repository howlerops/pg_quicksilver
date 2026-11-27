//go:build unix

package ducklake

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes the lake for this process. The catalog has one writer by
// construction — ids are handed out from counters this process reads once per
// transaction, and expiry unlinks files — and a second sidecar pointed at the
// same lake would break both. The lock is advisory and dies with the process,
// so a kill -9 leaves nothing to clean up.
func lockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("ducklake: %s is being published by another process (%w)", dir, err)
	}
	return f, nil
}

func unlockDir(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}

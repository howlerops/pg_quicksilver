//go:build unix

package mirror

import (
	"os"
	"syscall"
)

// linkedElsewhere reports whether the file has another name: a DuckLake's
// hard link (internal/ducklake) is the one that matters. Removing this name
// then frees no disk.
func linkedElsewhere(fi os.FileInfo) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	return ok && st.Nlink > 1
}

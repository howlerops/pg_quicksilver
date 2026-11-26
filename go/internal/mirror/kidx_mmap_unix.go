//go:build unix

package mirror

import (
	"os"
	"syscall"
)

// mapFile maps the first n bytes of a file read-only.
//
// MADV_RANDOM because that is what a key lookup is: without it the kernel reads
// around every fault, and a sidecar that looked up a few thousand keys would
// find most of the file resident for no reason.
func mapFile(path string, n int) ([]byte, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	data, err := syscall.Mmap(int(f.Fd()), 0, n, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	_ = syscall.Madvise(data, syscall.MADV_RANDOM)
	return data, func() { _ = syscall.Munmap(data) }, nil
}

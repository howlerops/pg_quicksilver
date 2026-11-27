//go:build unix

package mirror

import (
	"os"
	"syscall"
)

// mapFile maps the first n bytes of an open file read-only. The mapping
// outlives the descriptor.
//
// MADV_RANDOM because that is what a key lookup is: without it the kernel reads
// around every fault, and a sidecar that looked up a few thousand keys would
// find most of the file resident for no reason.
func mapFile(f *os.File, n int) ([]byte, func(), error) {
	data, err := syscall.Mmap(int(f.Fd()), 0, n, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	_ = syscall.Madvise(data, syscall.MADV_RANDOM)
	return data, func() { _ = syscall.Munmap(data) }, nil
}

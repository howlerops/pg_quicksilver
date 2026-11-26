//go:build !unix

package mirror

import (
	"errors"
	"io"
	"os"
)

// mapFile, where there is no mmap, reads the file into memory. That gives up
// the point of the exercise but not its correctness.
func mapFile(f *os.File, n int) ([]byte, func(), error) {
	data := make([]byte, n)
	if _, err := io.ReadFull(io.NewSectionReader(f, 0, int64(n)), data); err != nil {
		return nil, nil, errors.New("kidx: file shorter than its header says")
	}
	return data, func() {}, nil
}

//go:build !unix

package mirror

import (
	"errors"
	"os"
)

// mapFile, where there is no mmap, reads the file into memory. That gives up
// the point of the exercise but not its correctness.
func mapFile(path string, n int) ([]byte, func(), error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	if len(data) < n {
		return nil, nil, errors.New("kidx: file shorter than its header says")
	}
	return data[:n], func() {}, nil
}

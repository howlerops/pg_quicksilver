//go:build !unix

package ducklake

import "os"

func lockDir(string) (*os.File, error) { return nil, nil }

func unlockDir(*os.File) {}

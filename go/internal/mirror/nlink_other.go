//go:build !unix

package mirror

import "os"

func linkedElsewhere(os.FileInfo) bool { return false }

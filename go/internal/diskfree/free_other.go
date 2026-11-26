//go:build !unix

package diskfree

func free(string) (int64, bool) { return 0, false }

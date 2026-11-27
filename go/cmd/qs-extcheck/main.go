// qs-extcheck holds the pgx integration tests for the pg_quicksilver extension
// (ext/pg_quicksilver). There is nothing to run here but the tests:
//
//	QS_EXT_DSN='host=/tmp port=5541 dbname=app user=postgres' go test ./cmd/qs-extcheck/
package main

import "fmt"

func main() {
	fmt.Println("run: QS_EXT_DSN=... go test ./cmd/qs-extcheck/")
}

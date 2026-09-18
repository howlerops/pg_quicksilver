//go:build !race

package mirror

// raceEnabled is whether this binary was built with -race.
//
// It exists for one test: the race detector adds shadow memory and per-
// allocation bookkeeping, so heap accounting under it is not comparable to
// heap accounting without it. A memory test that does not know the difference
// reports a false failure on the race build and sends whoever reads it looking
// for a leak that is not there.
//
// See race_on.go for the other half.
const raceEnabled = false

package main

// The buffer defaults follow the memory the sidecar can actually use.
//
// The defaults were written for a machine with room: 250,000 row-changes per
// tick and four times that pending. Measured at a 256Mi limit, a drain of a
// backlog was OOM-killed while those were in memory; with QS_MAX_TICK_CHANGES=
// 20000 the same drain survived. So the default tick size is derived from the
// memory limit, and so is the GC target, which otherwise lets the heap grow to
// twice the live set before it is collected.
//
// The limit is GOMEMLIMIT when it is set, else the cgroup's memory limit (v2
// memory.max, then v1 memory/memory.limit_in_bytes). With no limit at all the
// old default stands. Every QS_* override still wins over these.

import (
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
)

const (
	// bytesPerTickChange is the sizing rule: one row-change in a tick costs
	// about 12 KB of heap at peak once it is decoded, applied and freed.
	bytesPerTickChange = 12_000
	minDefaultTick     = 5_000
	maxDefaultTick     = 250_000
	// gcShareOfLimit is the GOMEMLIMIT set when the operator has not set one,
	// as a fraction of the cgroup limit: the rest is for the stacks, the
	// mapped key index and the page cache the kernel charges to the cgroup.
	gcShareNum, gcShareDen = 8, 10
)

// memoryLimit is what the sizing was derived from.
type memoryLimit struct {
	Bytes  int64  // 0: none known
	Source string // "GOMEMLIMIT", "cgroup v2 memory.max", "cgroup v1 ...", or ""
}

// cgroupRoot is where the cgroup files are. A variable so the test can point it
// at fake files.
var cgroupRoot = "/sys/fs/cgroup"

// selfCgroupFile is where the process finds the cgroup it is in. A variable so
// the test can leave the test runner's own cgroup out of it; "" leaves it out.
var selfCgroupFile = "/proc/self/cgroup"

// detectMemory reads the limit. gomemlimit is the GOMEMLIMIT value (empty if
// unset), root the cgroup filesystem root.
//
// The limit is the smallest finite one among the root's memory files and the
// process's own cgroup's (ownCgroupFiles). The root alone is not enough: outside
// a container namespace the root's limit is the machine's, and a sidecar placed
// in a limited cgroup below it would otherwise size itself for the whole host.
func detectMemory(root, gomemlimit string) memoryLimit {
	if b, ok := parseGoMemLimit(gomemlimit); ok {
		return memoryLimit{Bytes: b, Source: "GOMEMLIMIT"}
	}
	candidates := []struct{ path, source string }{
		{filepath.Join(root, "memory.max"), "cgroup v2 memory.max"},
		{filepath.Join(root, "memory", "memory.limit_in_bytes"), "cgroup v1 memory.limit_in_bytes"},
	}
	candidates = append(candidates, ownCgroupFiles(root)...)
	var best memoryLimit
	for _, c := range candidates {
		raw, err := os.ReadFile(c.path)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(raw))
		if s == "max" {
			continue // v2: explicitly unlimited
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n <= 0 || n >= 1<<60 {
			continue // v1 writes a huge sentinel for "no limit"
		}
		if best.Bytes == 0 || n < best.Bytes {
			best = memoryLimit{Bytes: n, Source: c.source}
		}
	}
	return best
}

// ownCgroupFiles are the memory limit files of the cgroup this process is in,
// found from selfCgroupFile: the v2 line "0::/path" gives root/path/memory.max,
// and the v1 line "N:memory:/path" gives root/memory/path/memory.limit_in_bytes.
func ownCgroupFiles(root string) []struct{ path, source string } {
	if selfCgroupFile == "" {
		return nil
	}
	raw, err := os.ReadFile(selfCgroupFile)
	if err != nil {
		return nil
	}
	var out []struct{ path, source string }
	for _, line := range strings.Split(string(raw), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		dir := filepath.Clean(parts[2])
		switch {
		case parts[1] == "": // cgroup v2 unified hierarchy
			out = append(out, struct{ path, source string }{
				filepath.Join(root, dir, "memory.max"), "cgroup v2 memory.max (own cgroup)"})
		case slices.Contains(strings.Split(parts[1], ","), "memory"): // cgroup v1 memory controller
			out = append(out, struct{ path, source string }{
				filepath.Join(root, "memory", dir, "memory.limit_in_bytes"), "cgroup v1 memory.limit_in_bytes (own cgroup)"})
		}
	}
	return out
}

// parseGoMemLimit accepts what the runtime accepts for GOMEMLIMIT: a byte count
// or a number with a B, KiB, MiB, GiB or TiB suffix. "off" and anything else is
// not a limit here.
func parseGoMemLimit(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "off" {
		return 0, false
	}
	units := []struct {
		suffix string
		mult   float64
	}{
		{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1},
	}
	mult := 1.0
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSuffix(s, u.suffix), u.mult
			break
		}
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || f <= 0 {
		return 0, false
	}
	return int64(f * mult), true
}

// defaultTickChanges is limit/12,000 clamped to [5,000, 250,000]; with no
// known limit, the old default.
func defaultTickChanges(limit int64) int64 {
	if limit <= 0 {
		return maxDefaultTick
	}
	n := limit / bytesPerTickChange
	if n < minDefaultTick {
		return minDefaultTick
	}
	if n > maxDefaultTick {
		return maxDefaultTick
	}
	return n
}

// gcLimit is the GOMEMLIMIT to set, or 0 for none: 80% of the cgroup limit when
// the operator has not set GOMEMLIMIT. A GOMEMLIMIT from the environment is the
// operator's own and is left alone.
func gcLimit(m memoryLimit, gomemlimitSet bool) int64 {
	if gomemlimitSet || m.Bytes <= 0 || m.Source == "GOMEMLIMIT" {
		return 0
	}
	return m.Bytes / gcShareDen * gcShareNum
}

// memSizing is read once, at start, and everything below derives from it.
var memSizing = detectMemory(cgroupRoot, os.Getenv("GOMEMLIMIT"))

// logMemorySizing applies the GC limit and logs what was chosen, once.
func logMemorySizing(log *slog.Logger) {
	set := os.Getenv("GOMEMLIMIT") != ""
	if g := gcLimit(memSizing, set); g > 0 {
		debug.SetMemoryLimit(g)
		set = true
	}
	source := memSizing.Source
	if source == "" {
		source = "none"
	}
	log.Info("memory sizing",
		"limit_bytes", memSizing.Bytes, "limit_source", source,
		"max_tick_changes", maxTickChanges, "max_pending_changes", maxPendingChanges,
		"gomemlimit_set", set, "gomemlimit_bytes", debug.SetMemoryLimit(-1))
}

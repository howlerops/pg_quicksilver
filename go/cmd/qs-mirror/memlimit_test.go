package main

import (
	"os"
	"path/filepath"
	"testing"
)

// fakeCgroup writes the given files under a fresh root and returns the root.
func fakeCgroup(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const mi = 1 << 20

func TestDetectMemoryFromCgroupFiles(t *testing.T) {
	// The fake root is the only cgroup this test looks at, not the test runner's own.
	defer func(s string) { selfCgroupFile = s }(selfCgroupFile)
	selfCgroupFile = ""
	cases := []struct {
		name  string
		files map[string]string
		gomem string
		want  memoryLimit
	}{
		{"v2 limit", map[string]string{"memory.max": "268435456\n"}, "",
			memoryLimit{Bytes: 256 * mi, Source: "cgroup v2 memory.max"}},
		{"v2 unlimited", map[string]string{"memory.max": "max\n"}, "", memoryLimit{}},
		{"v1 limit", map[string]string{"memory/memory.limit_in_bytes": "1073741824\n"}, "",
			memoryLimit{Bytes: 1024 * mi, Source: "cgroup v1 memory.limit_in_bytes"}},
		{"v1 sentinel for no limit", map[string]string{"memory/memory.limit_in_bytes": "9223372036854771712\n"}, "",
			memoryLimit{}},
		{"v2 wins over v1 when both are present",
			map[string]string{"memory.max": "536870912", "memory/memory.limit_in_bytes": "1073741824"}, "",
			memoryLimit{Bytes: 512 * mi, Source: "cgroup v2 memory.max"}},
		{"no cgroup files", map[string]string{}, "", memoryLimit{}},
		{"GOMEMLIMIT beats the cgroup", map[string]string{"memory.max": "268435456"}, "512MiB",
			memoryLimit{Bytes: 512 * mi, Source: "GOMEMLIMIT"}},
		{"GOMEMLIMIT off falls through to the cgroup", map[string]string{"memory.max": "268435456"}, "off",
			memoryLimit{Bytes: 256 * mi, Source: "cgroup v2 memory.max"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := fakeCgroup(t, c.files)
			if got := detectMemory(root, c.gomem); got != c.want {
				t.Errorf("detectMemory = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestParseGoMemLimit(t *testing.T) {
	cases := map[string]int64{
		"":        0,
		"off":     0,
		"junk":    0,
		"-1":      0,
		"1048576": 1048576,
		"512MiB":  512 * mi,
		"1GiB":    1 << 30,
		"1.5GiB":  3 << 29,
		"64KiB":   64 << 10,
		"2TiB":    2 << 40,
		"100B":    100,
	}
	for in, want := range cases {
		got, ok := parseGoMemLimit(in)
		if want == 0 {
			if ok {
				t.Errorf("parseGoMemLimit(%q) = %d, want no limit", in, got)
			}
			continue
		}
		if !ok || got != want {
			t.Errorf("parseGoMemLimit(%q) = %d, %v; want %d", in, got, ok, want)
		}
	}
}

func TestDefaultTickChangesFromLimit(t *testing.T) {
	cases := []struct {
		name  string
		limit int64
		want  int64
	}{
		{"no limit keeps the old default", 0, 250_000},
		{"tiny limit clamps to the floor", 16 * mi, 5_000},
		{"256Mi gives about 22,000", 256 * mi, 256 * mi / 12_000},
		{"1Gi", 1024 * mi, 1024 * mi / 12_000},
		{"3 GiB reaches the old default", 3 << 30, 250_000},
		{"a large limit stays at the old default", 64 << 30, 250_000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := defaultTickChanges(c.limit); got != c.want {
				t.Errorf("defaultTickChanges(%d) = %d, want %d", c.limit, got, c.want)
			}
		})
	}
	if got := defaultTickChanges(256 * mi); got < 20_000 || got > 24_000 {
		t.Errorf("256Mi should give about 22,000, got %d", got)
	}
}

func TestGCLimitIsEightyPercentUnlessSetByOperator(t *testing.T) {
	m := memoryLimit{Bytes: 1000, Source: "cgroup v2 memory.max"}
	if got := gcLimit(m, false); got != 800 {
		t.Errorf("gcLimit with a cgroup limit = %d, want 800", got)
	}
	if got := gcLimit(m, true); got != 0 {
		t.Errorf("a GOMEMLIMIT already set must not be replaced, got %d", got)
	}
	if got := gcLimit(memoryLimit{}, false); got != 0 {
		t.Errorf("no known limit should set nothing, got %d", got)
	}
	if got := gcLimit(memoryLimit{Bytes: 1000, Source: "GOMEMLIMIT"}, false); got != 0 {
		t.Errorf("a limit that came from GOMEMLIMIT is not re-applied, got %d", got)
	}
}

// The process's own cgroup counts, and a larger limit above it does not: a
// sidecar placed in a 1 GiB cgroup below an 8 GiB root must size for 1 GiB.
func TestDetectMemoryUsesOwnCgroupBelowRoot(t *testing.T) {
	const gib = 1 << 30
	saved := selfCgroupFile
	defer func() { selfCgroupFile = saved }()

	root := fakeCgroup(t, map[string]string{
		"memory/memory.limit_in_bytes":       "8589934592\n",
		"memory/qsmem/memory.limit_in_bytes": "1073741824\n",
	})
	self := filepath.Join(t.TempDir(), "cgroup")
	if err := os.WriteFile(self, []byte("9:cpu,cpuacct:/\n2:memory:/qsmem\n1:name=systemd:/\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	selfCgroupFile = self
	want := memoryLimit{Bytes: gib, Source: "cgroup v1 memory.limit_in_bytes (own cgroup)"}
	if got := detectMemory(root, ""); got != want {
		t.Errorf("v1 own cgroup: detectMemory = %+v, want %+v", got, want)
	}

	// v2: the unified line names the cgroup, and its memory.max is the limit.
	root2 := fakeCgroup(t, map[string]string{
		"apps/pod/memory.max": "1073741824\n",
		"memory.max":          "max\n",
	})
	if err := os.WriteFile(self, []byte("0::/apps/pod\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	want2 := memoryLimit{Bytes: gib, Source: "cgroup v2 memory.max (own cgroup)"}
	if got := detectMemory(root2, ""); got != want2 {
		t.Errorf("v2 own cgroup: detectMemory = %+v, want %+v", got, want2)
	}
}

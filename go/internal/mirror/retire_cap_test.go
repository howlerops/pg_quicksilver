package mirror

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeVolume is a free-space probe a test sets.
type fakeVolume struct {
	free  int64
	known bool
	asked []string
}

func (v *fakeVolume) probe(path string) (int64, bool) {
	v.asked = append(v.asked, path)
	return v.free, v.known
}

func oldBase(t *testing.T, tbl *Table) string {
	t.Helper()
	if len(tbl.State.BaseFiles) != 1 {
		t.Fatalf("base files %v", tbl.State.BaseFiles)
	}
	return filepath.Join(tbl.Dir, "base", tbl.State.BaseFiles[0])
}

func exists(path string) bool { _, err := os.Stat(path); return err == nil }

// The defect: a base file larger than the cap was unlinked the moment a
// rewrite superseded it, so a query running across the rewrite lost its
// table. With room on the volume the superseded base now waits out the grace
// whatever the cap is — and only the newest one does.
func TestSupersededBaseKeepsItsGraceWhenTheVolumeHasRoom(t *testing.T) {
	tbl, clk := graceTable(t, time.Minute)
	p := tbl.publisher()
	vol := &fakeVolume{free: 1 << 40, known: true}
	p.maxBytes, p.free = 1, vol.probe // every base is "larger than the cap"
	seedR(t, tbl, 500)

	first := oldBase(t, tbl)
	before := mustPublished(t, tbl)
	if before.GraceUnprotected != "" {
		t.Fatalf("the volume has room, and the snapshot says its files are unprotected: %s", before.GraceUnprotected)
	}
	swapCompaction(t, tbl)
	if !exists(first) {
		t.Fatal("the superseded base was unlinked at once although the volume has room for it; " +
			"a query that started on the previous snapshot has lost its table")
	}
	if len(vol.asked) == 0 || vol.asked[0] != tbl.Dir {
		t.Fatalf("the probe was asked about %v, want the table directory", vol.asked)
	}
	// Everything else the previous snapshot named is still held to the cap.
	if gone := missing(tbl.Dir, before.Files); len(gone) == 0 {
		t.Fatal("a cap of one byte evicted nothing")
	}

	// A second rewrite: its base is now the newest, and the first one's
	// exemption ends.
	second := oldBase(t, tbl)
	applyR(t, tbl, "0/9", rChange("update", 7, "again", 7))
	swapCompaction(t, tbl)
	if exists(first) {
		t.Fatal("two superseded bases are being held; only the most recent is exempt from the cap")
	}
	if !exists(second) {
		t.Fatal("the most recently superseded base was unlinked")
	}
	// And it is a grace, not a reprieve.
	clk.at = clk.at.Add(2 * time.Minute)
	reapNow(tbl)
	if exists(second) {
		t.Fatal("the exempt base outlived its grace")
	}

	// Without the probe this is the behaviour being replaced.
	p.free = nil
	third := oldBase(t, tbl)
	applyR(t, tbl, "0/A", rChange("update", 8, "again", 8))
	if got := mustPublished(t, tbl).GraceUnprotected; got == "" {
		t.Fatal("a base over the cap with no known room was published as protected")
	}
	swapCompaction(t, tbl)
	if exists(third) {
		t.Fatal("with no way to know the volume has room, a base over the cap must still go at once")
	}
}

// When there is not room, the base goes at once — and the snapshot that named
// it said so while it was still current.
func TestSnapshotSaysWhenItsBaseHasNoGrace(t *testing.T) {
	tbl, _ := graceTable(t, time.Minute)
	p := tbl.publisher()
	vol := &fakeVolume{free: 1 << 40, known: true}
	p.maxBytes, p.free = 1, vol.probe
	seedR(t, tbl, 500)
	base := oldBase(t, tbl)
	fi, err := os.Stat(base)
	if err != nil {
		t.Fatal(err)
	}

	// The volume fills to one byte short of the base's size.
	vol.free = fi.Size() - 1
	applyR(t, tbl, "0/9", rChange("update", 7, "again", 7))
	pub := mustPublished(t, tbl)
	if pub.GraceUnprotected == "" {
		t.Fatal("the base cannot be held across a rewrite and the snapshot does not say so")
	}
	for _, want := range []string{"QS_RETIRE_MAX_BYTES", "without the grace"} {
		if !strings.Contains(pub.GraceUnprotected, want) {
			t.Errorf("the reason does not mention %q: %s", want, pub.GraceUnprotected)
		}
	}
	if pub.GraceSeconds != 60 {
		t.Fatalf("grace %v", pub.GraceSeconds)
	}
	swapCompaction(t, tbl)
	if exists(base) {
		t.Fatal("no room on the volume, and the superseded base was kept anyway")
	}
	// Exactly enough room is room.
	vol.free = 1 << 40
	applyR(t, tbl, "0/A", rChange("update", 8, "again", 8))
	if got := mustPublished(t, tbl).GraceUnprotected; got != "" {
		t.Fatalf("room came back and the snapshot still says %q", got)
	}
	// A base under the cap needs no room at all.
	p.maxBytes, vol.free = 1<<30, 0
	applyR(t, tbl, "0/B", rChange("update", 9, "again", 9))
	if got := mustPublished(t, tbl).GraceUnprotected; got != "" {
		t.Fatalf("a base under the cap was published as unprotected: %s", got)
	}
	// A halted snapshot names no files and says nothing about them.
	if err := tbl.Halt("why"); err != nil {
		t.Fatal(err)
	}
	if got := mustPublished(t, tbl).GraceUnprotected; got != "" {
		t.Fatalf("a halted snapshot: %q", got)
	}
}

// The default cap is a fraction of the volume with the old fixed value as its
// floor; a configured one is the number configured.
func TestDefaultRetireCapScalesWithTheVolume(t *testing.T) {
	tbl := racyTable(t)
	p := tbl.publisher()
	vol := &fakeVolume{known: true}
	p.free = vol.probe
	p.maxBytes = autoCap

	vol.free = 400 << 30
	if got := p.capNow(); got != 100<<30 {
		t.Fatalf("400 GiB free: cap %d, want a quarter", got)
	}
	vol.free = 2 << 30
	if got := p.capNow(); got != RetireMaxBytes {
		t.Fatalf("2 GiB free: cap %d, want the floor %d", got, RetireMaxBytes)
	}
	vol.known = false
	if got := p.capNow(); got != RetireMaxBytes {
		t.Fatalf("unknown free space: cap %d, want the floor", got)
	}
	vol.known, vol.free = true, 400<<30
	p.maxBytes = 5 << 30
	if got := p.capNow(); got != 5<<30 {
		t.Fatalf("a configured cap of 5 GiB became %d", got)
	}
	p.maxBytes = 0
	if got := p.capNow(); got != 0 {
		t.Fatalf("no cap became %d", got)
	}
	// What a re-snapshot may keep on disk meanwhile follows the same number.
	p.maxBytes = autoCap
	big := filepath.Join(tbl.Dir, "base", "big")
	if err := os.WriteFile(big, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(big, 3<<30); err != nil { // sparse: 3 GiB, no blocks
		t.Skip("cannot make a sparse file here: ", err)
	}
	if !tbl.canHold([]string{big}) {
		t.Fatal("3 GiB of previous contents on a volume with 400 GiB free must be kept during a re-snapshot")
	}
	vol.free = 2 << 30
	if tbl.canHold([]string{big}) {
		t.Fatal("3 GiB of previous contents were kept with 2 GiB free")
	}
}

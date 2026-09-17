package mirror

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDVRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.000000001.dv.parquet")

	// Sizes that straddle the row-group boundary, because a vector written in
	// several groups and read back as one is exactly the kind of thing that
	// works until a mirror gets large.
	for _, n := range []int{0, 1, 1000, dvRowGroup + 7} {
		want := make([]int, n)
		for i := range want {
			want[i] = i * 3
		}
		if err := writeDV(path, want); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		got, err := readDVFile(path)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if len(got) != len(want) {
			t.Fatalf("n=%d: wrote %d positions, read back %d", n, len(want), len(got))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("n=%d: position %d came back as %d, want %d", n, i, got[i], want[i])
			}
		}
	}
}

// A vector written before deletion vectors became Parquet must still be read,
// and the failure if it is not is the worst kind this code has: a vector that
// cannot be found does not raise anything, it reads as "nothing in that file is
// dead" and every row it retired comes back to life.
func TestJSONDeletionVectorsStillApply(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "dv"), 0o755)
	tbl := &Table{Dir: dir}

	rel := "base/000001.parquet"
	b, _ := json.Marshal([]int{3, 9, 27})
	legacy := filepath.Join(dir, "dv", "000001.000000004.dv.json")
	if err := os.WriteFile(legacy, b, 0o644); err != nil {
		t.Fatal(err)
	}

	if got := tbl.dvPathGenAny(rel, 4); got != legacy {
		t.Fatalf("a JSON vector on disk was not found: got %q", got)
	}
	tbl.State.DVGen = map[string]int{"000001": 4}
	dead, err := tbl.deadPositions(rel)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []int{3, 9, 27} {
		if !dead[p] {
			t.Fatalf("position %d is dead in the JSON vector but the mirror read it as live", p)
		}
	}
	if len(dead) != 3 {
		t.Fatalf("read %d dead positions from a 3-position vector", len(dead))
	}

	// And the view has to read it with the reader that matches, not the one the
	// current format uses.
	if sub := dvSubquery(legacy); !strings.Contains(sub, "read_json") {
		t.Fatalf("the view would read a JSON vector with the wrong reader: %s", sub)
	}
	if sub := dvSubquery(tbl.dvPathGen(rel, 5)); !strings.Contains(sub, "read_parquet") {
		t.Fatalf("the view would read a Parquet vector with the wrong reader: %s", sub)
	}
}

// When both encodings of a generation exist — the one changeover where a mirror
// has a JSON vector and has just written its Parquet successor — the newest has
// to win. A lexical sort over whole filenames does not give that, because
// ".json" sorts after ".parquet".
func TestNewestVectorWinsAcrossEncodings(t *testing.T) {
	dir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dir, "dv"), 0o755)
	tbl := &Table{Dir: dir}
	rel := "base/000001.parquet"

	b, _ := json.Marshal([]int{1})
	if err := os.WriteFile(filepath.Join(dir, "dv", "000001.000000009.dv.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeDV(filepath.Join(dir, "dv", "000001.000000010.dv.parquet"), []int{1, 2}); err != nil {
		t.Fatal(err)
	}

	got := tbl.dvGlob(rel)
	if len(got) != 2 {
		t.Fatalf("expected both encodings, got %v", got)
	}
	if !strings.HasSuffix(got[len(got)-1], "000000010.dv.parquet") {
		t.Fatalf("generation 10 is newer than 9, but the glob put %q last", got[len(got)-1])
	}

	dead, err := tbl.deadPositionsLatest(rel)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 2 {
		t.Fatalf("read the older generation: %d positions, want 2", len(dead))
	}

	// dropDV has to take both, or a retired data file leaves a vector behind
	// that the next file to reuse the name would inherit.
	tbl.State.DVGen = map[string]int{"000001": 10}
	tbl.dropDV(rel)
	if left := tbl.dvGlob(rel); len(left) != 0 {
		t.Fatalf("dropDV left %v behind", left)
	}
}

func TestDVGenOf(t *testing.T) {
	for in, want := range map[string]int{
		"000001.000000007.dv.parquet": 7,
		"000001.000000007.dv.json":    7,
		"000123.000000000.dv.parquet": 0,
		"nonsense":                    -1,
	} {
		if got := dvGenOf(in); got != want {
			t.Errorf("dvGenOf(%q) = %d, want %d", in, got, want)
		}
	}
}

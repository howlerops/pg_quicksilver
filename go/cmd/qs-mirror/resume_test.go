package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/howlerops/pg_quicksilver/go/internal/changestream"
)

func tablesOf(plan []resnapshot) []string {
	var out []string
	for _, r := range plan {
		out = append(out, r.Table)
	}
	return out
}

func TestPlanResume(t *testing.T) {
	confirmed := changestream.ParseLSN("0/5000")
	for _, c := range []struct {
		name    string
		applied map[string]string
		want    []string
	}{
		// What every healthy restart looks like: the pump confirms commit+1.
		{"caught up", map[string]string{"public.a": "0/4FFF"}, nil},
		// Fresh bootstrap that never applied a transaction: both are the
		// slot's consistent point.
		{"equal", map[string]string{"public.a": "0/5000"}, nil},
		// The slot is behind; the floor in Table.Apply discards the replay.
		{"ahead", map[string]string{"public.a": "0/9000"}, nil},
		// The lost tick: the slot confirmed what the mirror does not hold.
		{"behind", map[string]string{"public.a": "0/4000"}, []string{"public.a"}},
		// One byte short of healthy is still a place a commit could be.
		{"behind by two", map[string]string{"public.a": "0/4FFE"}, []string{"public.a"}},
		{"never snapshotted", map[string]string{"public.a": ""}, []string{"public.a"}},
		{"unreadable", map[string]string{"public.a": "garbage"}, []string{"public.a"}},
		// Per TABLE, not per slot: only the ones that need it, in a stable order.
		{"mixed", map[string]string{
			"public.z": "0/1", "public.ok": "0/4FFF", "public.new": "", "public.far": "0/9000",
		}, []string{"public.new", "public.z"}},
	} {
		got := planResume(c.applied, confirmed)
		if !reflect.DeepEqual(tablesOf(got), c.want) {
			t.Errorf("%s: re-snapshot %v, want %v", c.name, tablesOf(got), c.want)
		}
		for _, r := range got {
			if r.Reason == "" {
				t.Errorf("%s: %s is re-snapshotted without saying why", c.name, r.Table)
			}
		}
	}
}

// The reason is the only thing an operator sees, so it has to carry both LSNs.
func TestPlanResumeSaysHowFar(t *testing.T) {
	got := planResume(map[string]string{"public.a": "0/4000"}, changestream.ParseLSN("1/5000"))
	if len(got) != 1 || !strings.Contains(got[0].Reason, "1/5000") ||
		!strings.Contains(got[0].Reason, "0/4000") {
		t.Fatalf("reason does not name both positions: %+v", got)
	}
}

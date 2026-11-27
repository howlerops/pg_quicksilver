package main

import (
	"strings"
	"testing"
	"time"
)

func TestDecideSlots(t *testing.T) {
	const base = "quicksilver_app"
	const limit = int64(4 << 30)
	const period = 6 * time.Hour
	logical := func(name string, active bool, retained int64, inactive time.Duration) slotInfo {
		return slotInfo{Name: name, Logical: true, Active: active, Retained: retained, InactiveFor: inactive}
	}
	physical := func(name string) slotInfo { return slotInfo{Name: name, InactiveFor: -1} }
	longPod := "app-with-a-very-long-instance-name-that-does-not-fit-in-a-slot-7"

	cases := []struct {
		name      string
		slots     []slotInfo
		connected []string
		maxWAL    int64
		period    time.Duration
		want      map[string]string // slot -> "drop:<rule>" or "keep:<rule>"
	}{
		{
			name: "an active slot is never dropped, whatever else is true of it",
			slots: []slotInfo{
				logical(base+"_app_2", true, 100<<30, 0),
				logical(base, true, 100<<30, 0),
			},
			want: map[string]string{base + "_app_2": "keep:active", base: "keep:active"},
		},
		{
			name:  "inactive, old, and nobody by that name: the orphan",
			slots: []slotInfo{logical(base+"_app_9", false, 1<<20, 7*time.Hour)},
			want:  map[string]string{base + "_app_9": "drop:orphaned"},
		},
		{
			name:  "inactive but not for long enough",
			slots: []slotInfo{logical(base+"_app_9", false, 1<<20, 5*time.Hour)},
			want:  map[string]string{base + "_app_9": "keep:recently_inactive"},
		},
		{
			name:  "exactly at the period counts as over it",
			slots: []slotInfo{logical(base+"_app_9", false, 0, period)},
			want:  map[string]string{base + "_app_9": "drop:orphaned"},
		},
		{
			name:  "inactive for an unknown time is kept",
			slots: []slotInfo{logical(base+"_app_9", false, 1<<20, -1)},
			want:  map[string]string{base + "_app_9": "keep:inactivity_unknown"},
		},
		{
			name:      "its Pod is streaming: kept however old",
			slots:     []slotInfo{logical(base+"_app_2", false, 1<<20, 48*time.Hour)},
			connected: []string{"app-2"},
			want:      map[string]string{base + "_app_2": "keep:pod_connected"},
		},
		{
			name:      "its Pod is streaming: kept even over the WAL limit",
			slots:     []slotInfo{logical(base+"_app_2", false, limit+1, 48*time.Hour)},
			connected: []string{"app-1", "app-2"},
			want:      map[string]string{base + "_app_2": "keep:pod_connected_over_wal_limit"},
		},
		{
			name: "CNPG's physical slot for the instance: a member that is down is kept",
			slots: []slotInfo{
				logical(base+"_app_3", false, 1<<20, 48*time.Hour),
				physical("_cnpg_app_3"),
			},
			want: map[string]string{base + "_app_3": "keep:member"},
		},
		{
			name: "a physical slot without the prefix counts too",
			slots: []slotInfo{
				logical(base+"_app_3", false, 1<<20, 48*time.Hour),
				physical("app_3"),
			},
			want: map[string]string{base + "_app_3": "keep:member"},
		},
		{
			name: "a member that is down is still dropped at the WAL limit",
			slots: []slotInfo{
				logical(base+"_app_3", false, limit, time.Minute),
				physical("_cnpg_app_3"),
			},
			want: map[string]string{base + "_app_3": "drop:over_wal_limit"},
		},
		{
			name:  "over the WAL limit, inactive for a second, no Pod: dropped",
			slots: []slotInfo{logical(base+"_app_9", false, limit+5, time.Second)},
			want:  map[string]string{base + "_app_9": "drop:over_wal_limit"},
		},
		{
			name:  "just under the WAL limit and recent: kept",
			slots: []slotInfo{logical(base+"_app_9", false, limit-1, time.Second)},
			want:  map[string]string{base + "_app_9": "keep:recently_inactive"},
		},
		{
			name:  "the legacy shared slot is a candidate under its exact name",
			slots: []slotInfo{logical(base, false, 1<<20, 7*time.Hour)},
			want:  map[string]string{base: "drop:orphaned"},
		},
		{
			name: "other people's slots are not mentioned at all",
			slots: []slotInfo{
				logical("quicksilver_application_x_1", false, 100<<30, 99*time.Hour), // another cluster's
				logical("quicksilver_ap", false, 100<<30, 99*time.Hour),
				logical("debezium", false, 100<<30, 99*time.Hour),
				physical("_cnpg_app_2"),
				physical(base + "_x"), // a physical slot is never a candidate
			},
			want: map[string]string{},
		},
		{
			name: "a temporary slot (the re-snapshot's) is not a candidate",
			slots: []slotInfo{
				{Name: base + "_app_2_resnap", Logical: true, Temporary: true, Retained: 100 << 30, InactiveFor: 99 * time.Hour},
			},
			want: map[string]string{},
		},
		{
			name:   "no WAL rule: only inactivity decides",
			slots:  []slotInfo{logical(base+"_app_9", false, 100<<30, time.Hour)},
			maxWAL: -1,
			want:   map[string]string{base + "_app_9": "keep:recently_inactive"},
		},
		{
			name:   "no inactivity rule: only the WAL limit decides",
			slots:  []slotInfo{logical(base+"_app_9", false, 1<<20, 999*time.Hour), logical(base+"_app_8", false, limit, 0)},
			period: -1,
			want:   map[string]string{base + "_app_9": "keep:no_inactivity_rule", base + "_app_8": "drop:over_wal_limit"},
		},
		{
			// The slot name of a long Pod name carries a hash of the real name,
			// so the only way to recognise it is to compute it forwards.
			name:      "a hashed slot name is matched through the connected Pod's name",
			slots:     []slotInfo{logical(podSlotName(base, longPod), false, 1<<20, 48*time.Hour)},
			connected: []string{longPod},
			want:      map[string]string{podSlotName(base, longPod): "keep:pod_connected"},
		},
		{
			name: "and through the physical slot by the part before the hash",
			slots: []slotInfo{
				logical(podSlotName(base, longPod), false, 1<<20, 48*time.Hour),
				physical("_cnpg_" + sanitiseSlotPart(longPod)),
			},
			want: map[string]string{podSlotName(base, longPod): "keep:member"},
		},
		{
			name: "a different long name does not vouch for it",
			slots: []slotInfo{
				logical(podSlotName(base, longPod), false, 1<<20, 48*time.Hour),
				physical("_cnpg_another_long_instance_name_that_does_not_fit_in_a_slot_at_all_8"),
			},
			want: map[string]string{podSlotName(base, longPod): "drop:orphaned"},
		},
		{
			name:      "an application_name that is not a Pod of ours vouches for nothing",
			slots:     []slotInfo{logical(base+"_app_9", false, 1<<20, 48*time.Hour)},
			connected: []string{"walreceiver", "pg_basebackup", "", "app-2"},
			want:      map[string]string{base + "_app_9": "drop:orphaned"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := janitorInput{Base: base, Slots: c.slots, Connected: c.connected,
				MaxWAL: limit, MaxInactive: period, PhysicalPrefix: "_cnpg_"}
			if c.maxWAL != 0 {
				in.MaxWAL = c.maxWAL
			}
			if c.period != 0 {
				in.MaxInactive = c.period
			}
			got := map[string]string{}
			for _, d := range decideSlots(in) {
				verb := "keep:"
				if d.Drop {
					verb = "drop:"
				}
				if d.Detail == "" {
					t.Errorf("%s: a decision with nothing to log", d.Slot)
				}
				got[d.Slot] = verb + d.Rule
			}
			if len(got) != len(c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
			for slot, want := range c.want {
				if got[slot] != want {
					t.Errorf("%s: got %q, want %q", slot, got[slot], want)
				}
			}
		})
	}
}

// The two properties that are not allowed to depend on a case table: no input
// makes the janitor drop an active slot, or the slot of a Pod that is connected.
func TestDecideSlotsNeverDropsActiveOrConnected(t *testing.T) {
	const base = "qs"
	pods := []string{"a-1", "a-2", "a-3"}
	for mask := 0; mask < 1<<6; mask++ {
		bit := func(i int) bool { return mask&(1<<i) != 0 }
		var in janitorInput
		in.Base, in.MaxWAL, in.MaxInactive, in.PhysicalPrefix = base, 1000, time.Hour, "_cnpg_"
		for i, p := range pods {
			s := slotInfo{Name: podSlotName(base, p), Logical: true, Active: bit(i),
				Retained: 5000, InactiveFor: 100 * time.Hour}
			in.Slots = append(in.Slots, s)
			if bit(3 + i) {
				in.Connected = append(in.Connected, p)
			}
		}
		for i, d := range decideSlots(in) {
			if d.Drop && (bit(i) || bit(3+i)) {
				t.Fatalf("mask %06b: dropped %s (active %v, connected %v)", mask, d.Slot, bit(i), bit(3+i))
			}
			if !d.Drop && !bit(i) && !bit(3+i) {
				t.Fatalf("mask %06b: kept %s, which is inactive, over the limit and has no Pod", mask, d.Slot)
			}
		}
	}
}

func TestDecideSlotsLogsTheOverLimitConnectedCase(t *testing.T) {
	d := decideSlots(janitorInput{Base: "qs", MaxWAL: 10, MaxInactive: time.Hour,
		Slots:     []slotInfo{{Name: "qs_a_1", Logical: true, Retained: 11, InactiveFor: time.Minute}},
		Connected: []string{"a-1"}})
	if len(d) != 1 || d[0].Drop || d[0].Rule != ruleConnectedOver || !strings.Contains(d[0].Detail, "over the") {
		t.Fatalf("%+v", d)
	}
}

func TestJanitorSwitchDefaultsOn(t *testing.T) {
	t.Setenv("QS_SLOT_JANITOR", "")
	if !janitorOn() {
		t.Fatal("the janitor must default to on")
	}
	for _, v := range []string{"off", "0", "false", "no", " OFF "} {
		t.Setenv("QS_SLOT_JANITOR", v)
		if janitorOn() {
			t.Fatalf("QS_SLOT_JANITOR=%q did not turn it off", v)
		}
	}
}

// Two clusters on one server whose names differ by a suffix: the janitor of
// "quicksilver" must not take "quicksilver_a"'s slots for its own orphans.
func TestJanitorOwnsOnlyItsOwnClustersSlots(t *testing.T) {
	for _, c := range []struct {
		name, base, cluster string
		want                bool
	}{
		{"quicksilver_app", "quicksilver_app", "app", true},        // the legacy shared slot
		{"quicksilver_app_app_1", "quicksilver_app", "app", true},  // Pod app-1
		{"quicksilver_app_app_12", "quicksilver_app", "app", true}, // Pod app-12
		{"quicksilver_app_a", "quicksilver_app", "app", false},     // cluster "app_a"'s legacy slot
		{"quicksilver_app_a_app_a_1", "quicksilver_app", "app", false},
		{"quicksilver_app_app_a_1", "quicksilver_app", "app", false}, // not an ordinal
		{"quicksilver_app_app_", "quicksilver_app", "app", false},
		{"quicksilver_other_other_1", "quicksilver_app", "app", false},
		{"custom_app_2", "custom", "app", true},
		{"custom_x_2", "custom", "", true}, // cluster unknown: the wide rule
	} {
		if got := janitorOwns(c.name, c.base, c.cluster); got != c.want {
			t.Errorf("janitorOwns(%q, %q, %q) = %v, want %v", c.name, c.base, c.cluster, got, c.want)
		}
	}
	// A name cut to 63 bytes with a hash tag is still recognised by its prefix.
	long := "quicksilver_" + strings.Repeat("c", 40)
	cut := podSlotName(long, strings.Repeat("c", 40)+"-3")
	if len(cut) != maxSlotName || !janitorOwns(cut, long, strings.Repeat("c", 40)) {
		t.Errorf("cut name %q (len %d) not recognised as this cluster's", cut, len(cut))
	}
}

package main

import (
	"regexp"
	"strings"
	"testing"
)

var validSlot = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)

func TestPodSlotNameIsPerPodValidAndStable(t *testing.T) {
	for _, c := range []struct{ base, pod, want string }{
		{"quicksilver_app", "app-1", "quicksilver_app_app_1"},
		{"quicksilver_app", "app-2", "quicksilver_app_app_2"},
		// No Pod name: a sidecar run by hand keeps the configured slot.
		{"qs_e2e", "", "qs_e2e"},
	} {
		got := podSlotName(c.base, c.pod)
		if got != c.want {
			t.Errorf("podSlotName(%q, %q) = %q, want %q", c.base, c.pod, got, c.want)
		}
		if got != podSlotName(c.base, c.pod) {
			t.Errorf("podSlotName(%q, %q) is not deterministic", c.base, c.pod)
		}
	}
}

// Two Pods must never land on one slot, including when the name has to be cut
// to fit and when the sanitiser would fold their names together.
func TestPodSlotNameStaysUniqueWhenCut(t *testing.T) {
	base := "quicksilver_" + strings.Repeat("c", 45)
	seen := map[string]string{}
	for _, pod := range []string{
		"cluster-with-a-long-name-1", "cluster-with-a-long-name-2",
		"cluster-with-a-long-name-10", "a-b", "a.b", "a_b",
	} {
		for _, b := range []string{base, "quicksilver_app"} {
			got := podSlotName(b, pod)
			if !validSlot.MatchString(got) {
				t.Errorf("podSlotName(%q, %q) = %q (%d bytes) is not a valid slot name",
					b, pod, got, len(got))
			}
			if got != podSlotName(b, pod) {
				t.Errorf("podSlotName(%q, %q) is not deterministic", b, pod)
			}
			key := b + "|" + pod
			if prev, dup := seen[got]; dup {
				t.Errorf("%s and %s share slot %q", prev, key, got)
			}
			seen[got] = key
		}
	}
	if a, b := podSlotName(base, "pod-1"), podSlotName(base, "pod-2"); a == b {
		t.Errorf("a cut name lost the Pod: both are %q", a)
	}
}

func TestSnapshotSlotNameFitsAndDiffers(t *testing.T) {
	for _, slot := range []string{"quicksilver_app_app_1", strings.Repeat("s", 63)} {
		got := snapshotSlotName(slot)
		if !validSlot.MatchString(got) || got == slot {
			t.Errorf("snapshotSlotName(%q) = %q", slot, got)
		}
	}
}

// loadOptions is where the derivation is applied, and dropSlot reads the slot
// from options and nowhere else, so this is also what bounds what it can drop.
func TestLoadOptionsDerivesThePodsOwnSlot(t *testing.T) {
	t.Setenv("QS_SLOT", "quicksilver_app")
	t.Setenv("QS_POD_NAME", "app-2")
	o := loadOptions()
	if o.slot != "quicksilver_app_app_2" || o.slotBase != "quicksilver_app" {
		t.Errorf("slot = %q (configured %q), want quicksilver_app_app_2", o.slot, o.slotBase)
	}
}

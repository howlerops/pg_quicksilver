package plugin

import "testing"

// The plugin cannot name a slot per Pod: it patches one template and every
// instance gets the same environment. The sidecar derives its own slot from
// QS_SLOT and QS_POD_NAME, so the contract the plugin has to keep is that both
// arrive, and that the second really is the Pod's name. When QS_POD_NAME was
// only ever logged, dropping it would have changed nothing; now it would put
// every sidecar back on one shared slot, where the second one to start silently
// misses rows.
func TestSidecarGetsWhatASlotPerPodNeeds(t *testing.T) {
	cfg := ParseConfig("app", map[string]string{"tables": "public.t"})
	var slot, podField string
	for _, e := range MirrorSidecar("app", cfg).Env {
		switch e.Name {
		case "QS_SLOT":
			slot = e.Value
		case "QS_POD_NAME":
			if e.ValueFrom != nil && e.ValueFrom.FieldRef != nil {
				podField = e.ValueFrom.FieldRef.FieldPath
			}
		}
	}
	if slot != cfg.SlotName {
		t.Errorf("QS_SLOT = %q, want the configured name %q", slot, cfg.SlotName)
	}
	if podField != "metadata.name" {
		t.Errorf("QS_POD_NAME comes from %q, want metadata.name: "+
			"it is what makes each Pod's slot its own", podField)
	}
}

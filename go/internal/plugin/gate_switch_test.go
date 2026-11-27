package plugin

import "testing"

// The replay gate is on unless the operator turns it off, and the container
// says which (QS_GATE_ON_REPLAY), so a Pod is never on a different setting
// than the parameter it was given.
func TestGateOnReplayIsOnByDefaultAndReachesTheSidecar(t *testing.T) {
	c := MirrorSidecar("app", ParseConfig("app", okParams()))
	if got, ok := envValue(c.Env, "QS_GATE_ON_REPLAY"); !ok || got != "on" {
		t.Fatalf("default: QS_GATE_ON_REPLAY = %q (present %v), want on", got, ok)
	}

	params := okParams()
	params["gateOnReplay"] = "false"
	c = MirrorSidecar("app", ParseConfig("app", params))
	if got, ok := envValue(c.Env, "QS_GATE_ON_REPLAY"); !ok || got != "off" {
		t.Fatalf("gateOnReplay=false: QS_GATE_ON_REPLAY = %q (present %v), want off", got, ok)
	}
}

func TestGateOnReplayRefusesANonBoolean(t *testing.T) {
	params := okParams()
	params["gateOnReplay"] = "sometimes"
	refused := false
	validateSwitches(params, func(param, value, msg string) {
		if param == "gateOnReplay" {
			refused = true
		}
	})
	if !refused {
		t.Fatal("a non-boolean gateOnReplay was not refused")
	}
}

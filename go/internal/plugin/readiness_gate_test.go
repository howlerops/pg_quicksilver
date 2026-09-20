package plugin

import "testing"

// A native sidecar's readiness probe decides the whole POD's readiness, and a
// Pod that is not ready leaves every Service that selects it.
//
// Kubernetes states it plainly for restartable init containers, which is what
// InjectPluginSidecarInitContainer produces: "If a readinessProbe is specified
// for this init container, its result will be used to determine the ready state
// of the Pod."
//
// So wiring /readyz — which reports not-ready when the mirror is behind its
// freshness SLO — onto the sidecar in `shadow` mode means a lagging MIRROR
// evicts a perfectly healthy PostgreSQL REPLICA from <cluster>-ro and
// <cluster>-r. That inverts what shadow mode is for. docs/05 defines shadow as
// "-ro untouched", and the failure is not hypothetical: docs/28 measured the
// mirror draining at roughly half the rate a heavy writer produced, which is
// exactly the condition that fails this probe on every replica at once and
// empties the read endpoint.
//
// The probe is correct in `takeover`, where -ro IS answered from the mirror and
// a stale mirror must leave the endpoint. So the probe's presence is precisely
// the semantic difference between the two modes, and it is the mode that
// decides it, not the sidecar.
func TestFreshnessGatesTheEndpointOnlyInTakeover(t *testing.T) {
	base := map[string]string{"tables": "public.m"}

	t.Run("shadow does not gate the Pod", func(t *testing.T) {
		cfg := ParseConfig("c", base)
		if cfg.Mode != ModeShadow {
			t.Fatalf("expected shadow to be the default, got %q", cfg.Mode)
		}
		if p := MirrorSidecar("c", cfg).ReadinessProbe; p != nil {
			t.Errorf("shadow mode wires a readiness probe (%v), so a mirror that falls "+
				"behind takes the PostgreSQL replica out of -ro with it", p.HTTPGet)
		}
	})

	t.Run("takeover gates the Pod", func(t *testing.T) {
		cfg := ParseConfig("c", map[string]string{
			"tables": "public.m", "mode": "takeover", AcknowledgeParam: "true",
		})
		p := MirrorSidecar("c", cfg).ReadinessProbe
		if p == nil {
			t.Fatal("takeover serves -ro from the mirror, so a stale mirror MUST leave " +
				"the endpoint; without a readiness probe nothing enforces the freshness SLO")
		}
		if p.HTTPGet == nil || p.HTTPGet.Path != "/readyz" {
			t.Errorf("readiness probe does not check /readyz: %+v", p.HTTPGet)
		}
	})

	// Liveness is unconditional in both modes. It deliberately does not check
	// freshness: restarting a mirror that is behind discards its progress and
	// makes it further behind.
	t.Run("liveness is never conditional", func(t *testing.T) {
		for _, mode := range []string{"shadow", "takeover"} {
			params := map[string]string{"tables": "public.m", "mode": mode}
			if mode == "takeover" {
				params[AcknowledgeParam] = "true"
			}
			c := MirrorSidecar("c", ParseConfig("c", params))
			if c.LivenessProbe == nil || c.LivenessProbe.HTTPGet.Path != "/healthz" {
				t.Errorf("%s: liveness probe is not /healthz", mode)
			}
		}
	})
}

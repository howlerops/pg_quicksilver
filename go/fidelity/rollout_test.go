package fidelity

import (
	"strings"
	"testing"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
)

// Does CloudNativePG notice when the sidecar changes?
//
// internal/plugin/lifecycle.go carries a long comment arguing that declaring
// EVALUATE is not optional, because specs.NewInstance asks with EVALUATE to
// compute what a running Pod *should* look like, that evaluated spec is what
// lands in the pod-spec annotation, and checkPodSpecIsOutdated compares the
// annotation against a freshly evaluated spec to decide whether to roll an
// instance. Omit EVALUATE and the sidecar is absent from BOTH sides of that
// comparison, so changing sidecarImage would never roll the Pods.
//
// That argument was entirely reasoned from reading the operator's source. It
// is also exactly the class of claim that is wrong in a way nobody notices for
// a year, because everything keeps working — the Pods just never update.
//
// It does not need a kubelet to check. checkPodSpecIsOutdated is unexported,
// but the decision it delegates to, specs.ComparePodSpecs, is public and is
// what actually answers the question. These tests drive CNPG 1.30's own
// comparison over Pods built by CNPG's own specs.NewInstance, patched by our
// real hook.
func TestChangingTheSidecarImageRollsThePods(t *testing.T) {
	cluster := quicksilverCluster()

	// What the annotation on a running Pod would hold: the evaluated spec at
	// the time it was created.
	running := runHook(t, cluster, realInstancePod(t, cluster, 1),
		lifecycle.OperatorOperationType_TYPE_EVALUATE)
	if running == nil {
		t.Fatal("no patch on EVALUATE: the annotation would never contain the sidecar")
	}

	// Nothing has changed yet. The comparison MUST match, or the operator would
	// roll every instance on every reconcile — a Pod-recreation loop that looks
	// like flapping rather than like a bug in a plugin.
	same := runHook(t, cluster, realInstancePod(t, cluster, 1),
		lifecycle.OperatorOperationType_TYPE_EVALUATE)
	if match, diff := specs.ComparePodSpecs(running.Spec, same.Spec); !match {
		t.Errorf("an unchanged Cluster compares as outdated in %s: the operator would "+
			"roll the instances continuously", diff)
	}

	// Now pin a different sidecar image, which is the whole reason the plugin
	// reads image.mirror at all.
	upgraded := quicksilverCluster()
	params := upgraded.Spec.Plugins[0].Parameters
	params["sidecarImage"] = "ghcr.io/howlerops/pg_quicksilver-mirror:9.9.9"
	target := runHook(t, upgraded, realInstancePod(t, upgraded, 1),
		lifecycle.OperatorOperationType_TYPE_EVALUATE)
	if target == nil {
		t.Fatal("no patch for the upgraded cluster")
	}

	match, diff := specs.ComparePodSpecs(running.Spec, target.Spec)
	if match {
		t.Fatal("CloudNativePG sees NO difference after sidecarImage changed, so the " +
			"Pods would never be rolled and the old mirror would run forever")
	}
	t.Logf("CNPG would roll the instances: %s", diff)
	// The diff must name the init-containers, because the sidecar is injected
	// as a native sidecar. A difference reported anywhere else would mean the
	// comparison noticed something other than what changed.
	if !strings.Contains(diff, "init-containers") {
		t.Errorf("the rollout reason does not mention init-containers, so the sidecar "+
			"is not what CNPG noticed: %s", diff)
	}
}

// The other half of the same argument, stated as a test rather than a comment:
// a spec that never went through the hook does not carry the sidecar, so the
// comparison is blind to it. This is what omitting EVALUATE would produce.
func TestWithoutTheHookTheSidecarIsInvisibleToTheComparison(t *testing.T) {
	cluster := quicksilverCluster()
	upgraded := quicksilverCluster()
	upgraded.Spec.Plugins[0].Parameters["sidecarImage"] =
		"ghcr.io/howlerops/pg_quicksilver-mirror:9.9.9"

	// Both sides unpatched — the operator's own spec, as if the plugin had not
	// declared EVALUATE and so contributed nothing to the evaluated Pod.
	bare := realInstancePod(t, cluster, 1)
	bareUpgraded := realInstancePod(t, upgraded, 1)

	match, diff := specs.ComparePodSpecs(bare.Spec, bareUpgraded.Spec)
	if !match {
		t.Fatalf("expected the unpatched specs to be identical, got %s — this test "+
			"no longer isolates the hook's contribution", diff)
	}
	// Match == true is the FAILURE MODE being demonstrated: sidecarImage moved
	// and CloudNativePG cannot tell. The assertion above is what makes the
	// previous test meaningful, by showing the difference it detects comes from
	// the hook and from nothing else.
	t.Log("confirmed: without the hook's contribution, a sidecarImage change is " +
		"invisible to CNPG's comparison")
}

// mode: off injects nothing, so a Cluster that turns the mirror off must still
// compare cleanly against itself. Otherwise turning the mirror off would leave
// the operator rolling Pods forever trying to reach a state it already has.
func TestModeOffIsStableUnderComparison(t *testing.T) {
	off := quicksilverCluster()
	off.Spec.Plugins[0].Parameters["mode"] = "off"

	a := realInstancePod(t, off, 1)
	b := realInstancePod(t, off, 1)
	if p := runHook(t, off, a, lifecycle.OperatorOperationType_TYPE_EVALUATE); p != nil {
		a = p
	}
	if p := runHook(t, off, b, lifecycle.OperatorOperationType_TYPE_EVALUATE); p != nil {
		b = p
	}
	if match, diff := specs.ComparePodSpecs(a.Spec, b.Spec); !match {
		t.Errorf("mode: off does not compare equal to itself (%s), so the operator "+
			"would roll instances forever", diff)
	}
}

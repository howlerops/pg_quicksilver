package fidelity

// An invalid Quicksilver configuration, against the Pod CloudNativePG 1.30
// really builds. The operator never calls the plugin's ValidateClusterCreate or
// ValidateClusterChange (no caller in internal/webhook or internal/controller),
// so a misspelled parameter is admitted and reaches the lifecycle hook. These
// tests pin what the hook then does, for the cases in
// go/internal/plugin/lifecycle.go invalidConfigResponse.

import (
	"context"
	"encoding/json"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"

	qsplugin "github.com/howlerops/pg_quicksilver/go/internal/plugin"
)

// hookOutcome calls the hook directly, so an error is a result to assert on,
// not a test failure as in runHook.
func hookOutcome(t *testing.T, cluster *cnpgv1.Cluster, podJSON []byte,
	op lifecycle.OperatorOperationType_Type,
) ([]byte, error) {
	t.Helper()
	clusterJSON, err := json.Marshal(cluster)
	must(t, err)
	res, err := qsplugin.LifecycleImpl{}.LifecycleHook(context.Background(),
		&lifecycle.OperatorLifecycleRequest{
			OperationType:     &lifecycle.OperatorOperationType{Type: op},
			ClusterDefinition: clusterJSON,
			ObjectDefinition:  podJSON,
		})
	if err != nil {
		return nil, err
	}
	return res.GetJsonPatch(), nil
}

func withParams(params map[string]string, running bool) *cnpgv1.Cluster {
	c := quicksilverCluster()
	c.Spec.Plugins[0].Parameters = params
	if running {
		c.Status.Instances = 3
		c.Status.InstanceNames = []string{"app-1", "app-2", "app-3"}
	}
	return c
}

func TestMisspelledParameterOnNewClusterBuildsPodWithoutMirror(t *testing.T) {
	cluster := withParams(map[string]string{"tables": "public.events", "tabels": "x"}, false)
	pod := realInstancePod(t, cluster, 1)
	if patched := runHook(t, cluster, pod, lifecycle.OperatorOperationType_TYPE_CREATE); patched != nil {
		t.Errorf("a misspelled parameter injected a mirror into a real Pod: %v",
			containerNames(patched.Spec.InitContainers))
	}
}

func TestTakeoverWithoutAckNeverGetsTheReadinessGate(t *testing.T) {
	cluster := withParams(map[string]string{"mode": "takeover", "tables": "public.events"}, false)
	pod := realInstancePod(t, cluster, 1)
	if patched := runHook(t, cluster, pod, lifecycle.OperatorOperationType_TYPE_CREATE); patched != nil {
		t.Errorf("mode: takeover without acknowledgeOLTPRegression injected into a real Pod: %v",
			containerNames(patched.Spec.InitContainers))
	}
}

// A running cluster must not get a changed Pod spec from an invalid config: the
// rollout check evaluates with EVALUATE, and an error there is skipped by the
// operator, so the running instances are not restarted.
func TestInvalidConfigOnRunningClusterIsRefusedForEvaluate(t *testing.T) {
	cluster := withParams(map[string]string{"tabels": "x", "tables": "public.events"}, true)
	pod := realInstancePod(t, cluster, 2)
	podJSON, err := json.Marshal(pod)
	must(t, err)
	if _, err := hookOutcome(t, cluster, podJSON, lifecycle.OperatorOperationType_TYPE_EVALUATE); err == nil {
		t.Error("EVALUATE on a running cluster with an invalid config returned no error; " +
			"the rollout check would compare against a Pod without the mirror and restart every instance")
	}
}

// PATCH carries the role labels the operator writes on failover. An invalid
// config must not turn that write into an error.
func TestInvalidConfigDoesNotBlockPodPatch(t *testing.T) {
	cluster := withParams(map[string]string{"tabels": "x", "tables": "public.events"}, true)
	pod := realInstancePod(t, cluster, 2)
	podJSON, err := json.Marshal(pod)
	must(t, err)
	patch, err := hookOutcome(t, cluster, podJSON, lifecycle.OperatorOperationType_TYPE_PATCH)
	if err != nil {
		t.Fatalf("PATCH on a running Pod was blocked by an invalid config, which would block failover "+
			"label updates: %v", err)
	}
	if len(patch) != 0 {
		t.Errorf("PATCH with an invalid config changed the Pod: %s", patch)
	}
}

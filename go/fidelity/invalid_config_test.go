package fidelity

// An invalid Quicksilver configuration, against the Pod CloudNativePG 1.30
// really builds. The operator never calls the plugin's ValidateClusterCreate or
// ValidateClusterChange (no caller in internal/webhook or internal/controller),
// so a misspelled parameter is admitted and reaches the lifecycle hook.
//
// The hook tells a Pod being built from a Pod that runs by the operator's
// status.instancesStatus, not by status.instanceNames: the latter is filled
// from PVCs, so a fresh instance with a PVC and no Pod is listed there. See
// invalidConfigResponse in go/internal/plugin/lifecycle.go.

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

// withInstanceStatus sets the operator's two views of the instances: pvcs is
// status.instanceNames (PVC-derived), listed is status.instancesStatus (Pods).
func withInstanceStatus(params map[string]string, pvcs, listed []string) *cnpgv1.Cluster {
	c := quicksilverCluster()
	c.Spec.Plugins[0].Parameters = params
	c.Status.InstanceNames = pvcs
	c.Status.Instances = len(pvcs)
	if len(listed) > 0 {
		c.Status.InstancesStatus = map[cnpgv1.PodStatus][]string{cnpgv1.PodHealthy: listed}
	}
	return c
}

var (
	misspelledParams = map[string]string{"tables": "public.events", "tabels": "x"}
	allPVCs          = []string{"app-1", "app-2", "app-3"}
)

// A fresh Cluster: every instance has a PVC and none has a Pod yet. Each Pod
// must be built WITHOUT the mirror, with no error, so the reconcile completes.
func TestFreshClusterWithPVCsButNoPodsBuildsWithoutMirror(t *testing.T) {
	cluster := withInstanceStatus(misspelledParams, allPVCs, nil)
	for _, serial := range []int{1, 2, 3} {
		pod := realInstancePod(t, cluster, serial)
		podJSON, err := json.Marshal(pod)
		must(t, err)
		for _, op := range []lifecycle.OperatorOperationType_Type{
			lifecycle.OperatorOperationType_TYPE_CREATE,
			lifecycle.OperatorOperationType_TYPE_EVALUATE,
		} {
			patch, err := hookOutcome(t, cluster, podJSON, op)
			if err != nil {
				t.Fatalf("serial %d, %v: a fresh Pod was refused, which stops the reconcile "+
					"before the status is written: %v", serial, op, err)
			}
			if len(patch) != 0 {
				t.Errorf("serial %d, %v: a fresh Pod was patched with an invalid config: %s", serial, op, patch)
			}
		}
	}
}

// A replica joining a running primary: the primary is listed, the replica's
// Pod is not yet.
func TestReplicaBeingBuiltNextToRunningPrimaryHasNoMirror(t *testing.T) {
	cluster := withInstanceStatus(misspelledParams, allPVCs, []string{"app-1"})
	pod := realInstancePod(t, cluster, 2)
	podJSON, err := json.Marshal(pod)
	must(t, err)
	patch, err := hookOutcome(t, cluster, podJSON, lifecycle.OperatorOperationType_TYPE_EVALUATE)
	if err != nil {
		t.Fatalf("a replica being built was refused: %v", err)
	}
	if len(patch) != 0 {
		t.Errorf("a replica being built got a patch from an invalid config: %s", patch)
	}
}

func TestMisspelledParameterOnNewClusterBuildsPodWithoutMirror(t *testing.T) {
	cluster := withInstanceStatus(misspelledParams, nil, nil)
	pod := realInstancePod(t, cluster, 1)
	if patched := runHook(t, cluster, pod, lifecycle.OperatorOperationType_TYPE_CREATE); patched != nil {
		t.Errorf("a misspelled parameter injected a mirror into a real Pod: %v",
			containerNames(patched.Spec.InitContainers))
	}
}

func TestTakeoverWithoutAckNeverGetsTheReadinessGate(t *testing.T) {
	cluster := withInstanceStatus(map[string]string{"mode": "takeover", "tables": "public.events"}, nil, nil)
	pod := realInstancePod(t, cluster, 1)
	if patched := runHook(t, cluster, pod, lifecycle.OperatorOperationType_TYPE_CREATE); patched != nil {
		t.Errorf("mode: takeover without acknowledgeOLTPRegression injected into a real Pod: %v",
			containerNames(patched.Spec.InitContainers))
	}
}

// The rollout check of a running instance: its Pod is listed. An error here is
// skipped by the operator's rollout check, so the instance is not rolled to drop
// a mirror it runs with.
func TestRunningInstanceOnInvalidConfigIsRefusedForEvaluate(t *testing.T) {
	cluster := withInstanceStatus(misspelledParams, allPVCs, allPVCs)
	pod := realInstancePod(t, cluster, 2)
	podJSON, err := json.Marshal(pod)
	must(t, err)
	if _, err := hookOutcome(t, cluster, podJSON, lifecycle.OperatorOperationType_TYPE_EVALUATE); err == nil {
		t.Error("EVALUATE of a running instance with an invalid config returned no error; " +
			"the rollout check would compare against a Pod without the mirror and restart it")
	}
}

// PATCH carries the role labels the operator writes on failover. An invalid
// config must not turn that write into an error.
func TestInvalidConfigDoesNotBlockPodPatch(t *testing.T) {
	cluster := withInstanceStatus(misspelledParams, allPVCs, allPVCs)
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

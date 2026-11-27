package plugin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The operator never asks plugins to validate at admission, so every case here
// is an ordinary Cluster that reaches the plugin the way the real operator sends
// it: as serialised JSON, through LifecycleHook (CREATE, EVALUATE, PATCH, UPDATE)
// and SetStatusInCluster.
//
// What tells a Pod being built from a Pod that already runs is the operator's
// status.instancesStatus (see invalidConfigResponse). status.instanceNames is
// filled from PVCs and is NOT that signal: an instance whose PVC exists but whose
// Pod does not yet is listed there. Each case states both lists.

var (
	validParams = map[string]string{
		"mode": "shadow", "tables": "public.m", "credentialsSecret": "qs-creds",
	}
	takeoverNoAck = map[string]string{
		"mode": "takeover", "tables": "public.m", "credentialsSecret": "qs-creds",
	}
	takeoverAck = map[string]string{
		"mode": "takeover", "tables": "public.m", "credentialsSecret": "qs-creds",
		AcknowledgeParam: "true",
	}
	misspelled = map[string]string{
		"mode": "shadow", "tables": "public.m", "credentialsSecret": "qs-creds",
		"tabels": "public.m",
	}
	latestImage = map[string]string{
		"mode": "shadow", "tables": "public.m", "credentialsSecret": "qs-creds",
		"sidecarImage": "ghcr.io/howlerops/pg_quicksilver-mirror:latest",
	}
)

// Status shapes, as the operator writes them.
var (
	// A fresh Cluster before any Pod: nothing has a PVC or a Pod.
	noInstances = struct{ pvcs, listed []string }{nil, nil}
	// PVCs exist for both instances, but no Pod has been created for either:
	// the operator's PVC-derived instanceNames already lists both.
	pvcsNoPods = struct{ pvcs, listed []string }{[]string{"pg-1", "pg-2"}, nil}
	// The primary runs (listed as a Pod); the replica's PVC exists and its Pod
	// is about to be built.
	replicaBeingBuilt = struct{ pvcs, listed []string }{[]string{"pg-1", "pg-2"}, []string{"pg-1"}}
	// Both instances run.
	bothRunning = struct{ pvcs, listed []string }{[]string{"pg-1", "pg-2"}, []string{"pg-1", "pg-2"}}
)

// testCluster builds a Cluster with the plugin enabled and the given instance
// status. The Pod under test is always pg-2.
func testCluster(params map[string]string, shape struct{ pvcs, listed []string }) *apiv1.Cluster {
	enabled := true
	c := &apiv1.Cluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns"},
	}
	c.Spec.Instances = 2
	c.Spec.Plugins = []apiv1.PluginConfiguration{{
		Name: PluginName, Enabled: &enabled, Parameters: params,
	}}
	c.Status.InstanceNames = shape.pvcs
	c.Status.Instances = len(shape.pvcs)
	if len(shape.listed) > 0 {
		c.Status.InstancesStatus = map[apiv1.PodStatus][]string{apiv1.PodHealthy: shape.listed}
		c.Status.ReadyInstances = len(shape.listed)
	}
	return c
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func testPod() *corev1.Pod {
	return &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "pg-2", Namespace: "ns"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "postgres", Image: "pg:17"}}},
	}
}

func TestLifecycleHookDecisionForInvalidConfig(t *testing.T) {
	type tc struct {
		name        string
		params      map[string]string
		shape       struct{ pvcs, listed []string }
		op          lifecycle.OperatorOperationType_Type
		wantErr     bool
		wantSidecar bool
		wantGate    bool // the readiness gate that takes a replica out of -ro
	}
	create := lifecycle.OperatorOperationType_TYPE_CREATE
	evaluate := lifecycle.OperatorOperationType_TYPE_EVALUATE
	patch := lifecycle.OperatorOperationType_TYPE_PATCH
	update := lifecycle.OperatorOperationType_TYPE_UPDATE

	cases := []tc{
		{"valid shadow, create of a new Pod", validParams, noInstances, create, false, true, false},
		{"valid shadow, evaluate of a running Pod", validParams, bothRunning, evaluate, false, true, false},
		{"valid takeover with ack, create", takeoverAck, noInstances, create, false, true, true},

		// The fresh-cluster case that failed in CI: PVCs exist, no Pod yet.
		// The Pod must be built WITHOUT the mirror, and nothing may error.
		{"misspelled, fresh cluster, PVCs but no Pod: built without mirror", misspelled, pvcsNoPods, evaluate, false, false, false},
		{"misspelled, fresh cluster, PVCs but no Pod, create: built without mirror", misspelled, pvcsNoPods, create, false, false, false},
		// A replica joining a running primary: its Pod is not listed yet.
		{"misspelled, replica being built next to a running primary: no mirror", misspelled, replicaBeingBuilt, evaluate, false, false, false},
		{"misspelled, no instances at all: built without mirror", misspelled, noInstances, create, false, false, false},

		// The rollout check of a running Pod: it is listed, so the error keeps the
		// operator from comparing it with a Pod that has no mirror.
		{"misspelled, evaluate of a running Pod: refused, no rollout", misspelled, bothRunning, evaluate, true, false, false},
		{"misspelled, create of an already-listed Pod: refused", misspelled, bothRunning, create, true, false, false},
		{"misspelled, patch (role label) of a running Pod: never blocked", misspelled, bothRunning, patch, false, false, false},
		{"misspelled, update of a running Pod: never blocked", misspelled, bothRunning, update, false, false, false},

		{"takeover without ack, fresh cluster: no takeover", takeoverNoAck, pvcsNoPods, create, false, false, false},
		{"takeover without ack, evaluate of a running Pod: refused, no rollout", takeoverNoAck, bothRunning, evaluate, true, false, false},
		{"takeover without ack, patch of a running Pod: no takeover injected", takeoverNoAck, bothRunning, patch, false, false, false},

		// Not a validation error today: the sidecar image is not checked against a
		// tag policy, and the default image itself is :latest. Pinned so that a
		// future rule is a deliberate change to this table.
		{"sidecarImage :latest is accepted (no rule yet)", latestImage, noInstances, create, false, true, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := &lifecycle.OperatorLifecycleRequest{
				OperationType:     &lifecycle.OperatorOperationType{Type: c.op},
				ClusterDefinition: mustJSON(t, testCluster(c.params, c.shape)),
				ObjectDefinition:  mustJSON(t, testPod()),
			}
			resp, err := LifecycleImpl{}.LifecycleHook(context.Background(), req)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want an error, got patch %s", resp.GetJsonPatch())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			patch := string(resp.GetJsonPatch())
			if got := strings.Contains(patch, MirrorContainerName); got != c.wantSidecar {
				t.Errorf("sidecar injected = %v, want %v (patch %s)", got, c.wantSidecar, patch)
			}
			if got := strings.Contains(patch, "/readyz"); got != c.wantGate {
				t.Errorf("readiness gate injected = %v, want %v", got, c.wantGate)
			}
		})
	}
}

func TestSetStatusInClusterPublishesConfigErrors(t *testing.T) {
	cases := []struct {
		name       string
		params     map[string]string
		wantState  string
		wantParam  string // a configErrors entry with this parameter, or "" for none
		wantInText string // substring of that entry's message
	}{
		{"valid", validParams, StateValid, "", ""},
		{"valid takeover with ack", takeoverAck, StateValid, "", ""},
		{"misspelled parameter", misspelled, StateInvalidConfig, "tabels", "unknown Quicksilver parameter"},
		{"takeover without ack", takeoverNoAck, StateInvalidConfig, "mode", AcknowledgeParam},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := OperatorImpl{}.SetStatusInCluster(context.Background(),
				&operator.SetStatusInClusterRequest{Cluster: mustJSON(t, testCluster(c.params, bothRunning))})
			if err != nil {
				t.Fatalf("SetStatusInCluster: %v", err)
			}
			var st Status
			if err := json.Unmarshal(raw.GetJsonStatus(), &st); err != nil {
				t.Fatalf("status is not JSON: %v", err)
			}
			if st.State != c.wantState {
				t.Errorf("state = %q, want %q", st.State, c.wantState)
			}
			if c.wantParam == "" {
				if len(st.ConfigErrors) != 0 {
					t.Errorf("valid config reported errors: %+v", st.ConfigErrors)
				}
				return
			}
			found := false
			for _, e := range st.ConfigErrors {
				if e.Parameter == c.wantParam && strings.Contains(e.Message, c.wantInText) {
					found = true
				}
			}
			if !found {
				t.Errorf("no configErrors entry for %q containing %q: %+v", c.wantParam, c.wantInText, st.ConfigErrors)
			}
		})
	}
}

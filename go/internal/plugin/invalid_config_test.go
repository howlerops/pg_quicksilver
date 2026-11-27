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

// testCluster builds a Cluster with the plugin enabled. running reports whether
// the operator has already built instance Pods for it.
func testCluster(params map[string]string, running bool) *apiv1.Cluster {
	enabled := true
	c := &apiv1.Cluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "pg", Namespace: "ns"},
	}
	c.Spec.Instances = 2
	c.Spec.Plugins = []apiv1.PluginConfiguration{{
		Name: PluginName, Enabled: &enabled, Parameters: params,
	}}
	if running {
		c.Status.Instances = 2
		c.Status.InstanceNames = []string{"pg-1", "pg-2"}
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
		running     bool
		op          lifecycle.OperatorOperationType_Type
		wantErr     bool
		wantSidecar bool
		wantGate    bool // the readiness gate that takes a replica out of -ro
	}
	cases := []tc{
		{"valid shadow, create of a new Pod", validParams, false,
			lifecycle.OperatorOperationType_TYPE_CREATE, false, true, false},
		{"valid shadow, evaluate on a running cluster", validParams, true,
			lifecycle.OperatorOperationType_TYPE_EVALUATE, false, true, false},
		{"valid takeover with ack, create", takeoverAck, false,
			lifecycle.OperatorOperationType_TYPE_CREATE, false, true, true},

		{"misspelled tabels, create, new cluster: built without the mirror", misspelled, false,
			lifecycle.OperatorOperationType_TYPE_CREATE, false, false, false},
		{"misspelled tabels, evaluate, new cluster: built without the mirror", misspelled, false,
			lifecycle.OperatorOperationType_TYPE_EVALUATE, false, false, false},
		{"misspelled tabels, evaluate, running cluster: refused, no rollout", misspelled, true,
			lifecycle.OperatorOperationType_TYPE_EVALUATE, true, false, false},
		{"misspelled tabels, create, running cluster: refused", misspelled, true,
			lifecycle.OperatorOperationType_TYPE_CREATE, true, false, false},
		{"misspelled tabels, patch (role label): never blocked", misspelled, true,
			lifecycle.OperatorOperationType_TYPE_PATCH, false, false, false},
		{"misspelled tabels, update: never blocked", misspelled, true,
			lifecycle.OperatorOperationType_TYPE_UPDATE, false, false, false},

		{"takeover without ack, create, new cluster: no takeover", takeoverNoAck, false,
			lifecycle.OperatorOperationType_TYPE_CREATE, false, false, false},
		{"takeover without ack, evaluate, running: refused, no rollout", takeoverNoAck, true,
			lifecycle.OperatorOperationType_TYPE_EVALUATE, true, false, false},
		{"takeover without ack, patch, running: no takeover injected", takeoverNoAck, true,
			lifecycle.OperatorOperationType_TYPE_PATCH, false, false, false},

		// Not a validation error today: the sidecar image is not checked against
		// a tag policy, and the default image itself is :latest. Pinned so that a
		// future rule is a deliberate change to this table.
		{"sidecarImage :latest is accepted (no rule yet)", latestImage, false,
			lifecycle.OperatorOperationType_TYPE_CREATE, false, true, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := &lifecycle.OperatorLifecycleRequest{
				OperationType:     &lifecycle.OperatorOperationType{Type: c.op},
				ClusterDefinition: mustJSON(t, testCluster(c.params, c.running)),
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
				&operator.SetStatusInClusterRequest{Cluster: mustJSON(t, testCluster(c.params, true))})
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

package plugin

// These tests go through the actual gRPC request and response shapes, including
// the JSON patches, rather than calling the helpers directly. The patch is the
// contract with the operator: a hook can compute a perfectly correct object and
// still emit a patch that does not apply, and a unit test on the helper would
// never notice.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/cnpg-i/pkg/postgres"
)

func clusterJSON(t *testing.T, c *apiv1.Cluster) []byte {
	t.Helper()
	c = c.DeepCopy()
	c.TypeMeta = metav1.TypeMeta{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster"}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// instancePod is a stripped-down version of what CNPG actually creates: the
// container must be named "postgres" and carry the data volume mount, because
// that is what the sidecar injector copies.
func instancePod() *corev1.Pod {
	return &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "app-1", Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "postgres",
				Image: "ghcr.io/cloudnative-pg/postgresql:17.2",
				VolumeMounts: []corev1.VolumeMount{
					{Name: "pgdata", MountPath: "/var/lib/postgresql/data"},
				},
			}},
			Volumes: []corev1.Volume{{Name: "pgdata"}},
		},
	}
}

func podJSON(t *testing.T, p *corev1.Pod) []byte {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// applyPatch applies an RFC 6902 patch the way the operator would, so a patch
// that cannot be applied fails the test instead of passing silently.
func applyPatch(t *testing.T, original, patch []byte) []byte {
	t.Helper()
	if len(patch) == 0 {
		return original
	}
	p, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		t.Fatalf("emitted patch does not decode: %v\npatch: %s", err, patch)
	}
	out, err := p.Apply(original)
	if err != nil {
		t.Fatalf("emitted patch does not apply: %v\npatch: %s", err, patch)
	}
	return out
}

func TestIdentityCapabilitiesMatchRegisteredServices(t *testing.T) {
	res, err := IdentityImpl{}.GetPluginCapabilities(context.Background(),
		&identity.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[identity.PluginCapability_Service_Type]bool{}
	for _, c := range res.GetCapabilities() {
		got[c.GetService().GetType()] = true
	}
	// Declaring a capability we do not serve makes the operator call a method
	// that answers Unimplemented, which surfaces as a reconcile error and not
	// as anything that names this plugin.
	want := []identity.PluginCapability_Service_Type{
		identity.PluginCapability_Service_TYPE_OPERATOR_SERVICE,
		identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE,
		identity.PluginCapability_Service_TYPE_POSTGRES,
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("missing declared capability %v", w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("declared %d capabilities, registered %d services", len(got), len(want))
	}
	// Specifically: we do not implement the WAL service, so we must not claim it.
	if got[identity.PluginCapability_Service_TYPE_WAL_SERVICE] {
		t.Error("WAL service declared but not implemented")
	}
}

func TestValidateClusterCreateOverGRPC(t *testing.T) {
	c := newCluster(map[string]string{"mode": "takeover", "tables": "public.events"})
	res, err := OperatorImpl{}.ValidateClusterCreate(context.Background(),
		&operator.OperatorValidateClusterCreateRequest{Definition: clusterJSON(t, c)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetValidationErrors()) == 0 {
		t.Fatal("unacknowledged takeover accepted over gRPC")
	}
	pc := res.GetValidationErrors()[0].GetPathComponents()
	if len(pc) == 0 || pc[0] != "spec" {
		t.Errorf("validation error path does not start at spec: %v", pc)
	}
}

func TestPluginNotConfiguredIsIgnored(t *testing.T) {
	c := newCluster(nil)
	c.Spec.Plugins = nil
	res, err := OperatorImpl{}.ValidateClusterCreate(context.Background(),
		&operator.OperatorValidateClusterCreateRequest{Definition: clusterJSON(t, c)})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetValidationErrors()) != 0 {
		t.Errorf("a cluster that does not use this plugin was validated against it: %v",
			res.GetValidationErrors())
	}
}

func TestMutateClusterPatchAppliesAndFillsDefaults(t *testing.T) {
	c := newCluster(map[string]string{"tables": "public.events"})
	orig := clusterJSON(t, c)
	res, err := OperatorImpl{}.MutateCluster(context.Background(),
		&operator.OperatorMutateClusterRequest{Definition: orig})
	if err != nil {
		t.Fatal(err)
	}
	patched := applyPatch(t, orig, res.GetJsonPatch())

	var out apiv1.Cluster
	if err := json.Unmarshal(patched, &out); err != nil {
		t.Fatal(err)
	}
	params := PluginParameters(&out)
	for k, want := range map[string]string{
		"mode": "shadow", "ingest": "logical", "freshnessSLO": "30s",
		"slotName": "quicksilver_app", "publication": "quicksilver_app",
		"mirrorPath": DefaultMirrorPath,
	} {
		if params[k] != want {
			t.Errorf("after mutation %s = %q, want %q", k, params[k], want)
		}
	}
	if params["tables"] != "public.events" {
		t.Errorf("mutation clobbered tables: %q", params["tables"])
	}
}

func TestMutateClusterLeavesExplicitValuesAlone(t *testing.T) {
	c := newCluster(map[string]string{
		"tables": "public.events", "freshnessSLO": "5m", "slotName": "custom_slot",
	})
	orig := clusterJSON(t, c)
	res, err := OperatorImpl{}.MutateCluster(context.Background(),
		&operator.OperatorMutateClusterRequest{Definition: orig})
	if err != nil {
		t.Fatal(err)
	}
	var out apiv1.Cluster
	if err := json.Unmarshal(applyPatch(t, orig, res.GetJsonPatch()), &out); err != nil {
		t.Fatal(err)
	}
	p := PluginParameters(&out)
	if p["freshnessSLO"] != "5m" || p["slotName"] != "custom_slot" {
		t.Errorf("mutation overrode explicit values: %v", p)
	}
}

func TestLifecycleInjectsSidecarAndPatchApplies(t *testing.T) {
	c := newCluster(map[string]string{"tables": "public.events,public.orders"})
	pod := instancePod()
	origPod := podJSON(t, pod)

	res, err := LifecycleImpl{}.LifecycleHook(context.Background(), &lifecycle.OperatorLifecycleRequest{
		OperationType:     &lifecycle.OperatorOperationType{Type: lifecycle.OperatorOperationType_TYPE_CREATE},
		ClusterDefinition: clusterJSON(t, c),
		ObjectDefinition:  origPod,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetJsonPatch()) == 0 {
		t.Fatal("no sidecar was injected into an instance Pod")
	}

	var out corev1.Pod
	if err := json.Unmarshal(applyPatch(t, origPod, res.GetJsonPatch()), &out); err != nil {
		t.Fatal(err)
	}

	var sidecar *corev1.Container
	for i := range out.Spec.InitContainers {
		if out.Spec.InitContainers[i].Name == MirrorContainerName {
			sidecar = &out.Spec.InitContainers[i]
		}
	}
	if sidecar == nil {
		t.Fatalf("sidecar not present after applying the patch; init containers: %+v",
			out.Spec.InitContainers)
	}
	// A native sidecar is an init container with restartPolicy: Always. Without
	// it the Pod never leaves Init and the instance never starts.
	if sidecar.RestartPolicy == nil || *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Errorf("sidecar restartPolicy = %v, want Always", sidecar.RestartPolicy)
	}
	// It must receive the data volume, or it has nowhere to write the mirror.
	found := false
	for _, vm := range sidecar.VolumeMounts {
		if vm.Name == "pgdata" {
			found = true
		}
	}
	if !found {
		t.Errorf("sidecar has no pgdata mount: %v", sidecar.VolumeMounts)
	}

	env := map[string]string{}
	for _, e := range sidecar.Env {
		env[e.Name] = e.Value
	}
	if env["QS_TABLES"] != "public.events,public.orders" {
		t.Errorf("QS_TABLES = %q", env["QS_TABLES"])
	}
	// The one that matters after a failover: the sidecar must follow the -rw
	// service, never a Pod name.
	if env["QS_PRIMARY_HOST"] != "app-rw" {
		t.Errorf("QS_PRIMARY_HOST = %q, want app-rw", env["QS_PRIMARY_HOST"])
	}
	for _, name := range []string{"app-1", "app-2", "app-3"} {
		if strings.Contains(env["QS_PRIMARY_HOST"], name) {
			t.Errorf("sidecar is pinned to a Pod name (%q); after a promotion that "+
				"node no longer accepts a replication connection", env["QS_PRIMARY_HOST"])
		}
	}
	if sidecar.ReadinessProbe == nil {
		t.Error("no readiness probe: nothing would remove a stale mirror from the endpoint")
	}
}

func TestLifecycleIsIdempotent(t *testing.T) {
	c := newCluster(map[string]string{"tables": "public.events"})
	pod := instancePod()
	cur := podJSON(t, pod)
	for i := 0; i < 3; i++ {
		res, err := LifecycleImpl{}.LifecycleHook(context.Background(), &lifecycle.OperatorLifecycleRequest{
			OperationType:     &lifecycle.OperatorOperationType{Type: lifecycle.OperatorOperationType_TYPE_PATCH},
			ClusterDefinition: clusterJSON(t, c),
			ObjectDefinition:  cur,
		})
		if err != nil {
			t.Fatal(err)
		}
		cur = applyPatch(t, cur, res.GetJsonPatch())
	}
	var out corev1.Pod
	if err := json.Unmarshal(cur, &out); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, ct := range out.Spec.InitContainers {
		if ct.Name == MirrorContainerName {
			n++
		}
	}
	if n != 1 {
		t.Errorf("sidecar injected %d times across repeated reconciles, want 1", n)
	}
}

func TestLifecycleSkipsWhenModeOff(t *testing.T) {
	c := newCluster(map[string]string{"mode": "off"})
	res, err := LifecycleImpl{}.LifecycleHook(context.Background(), &lifecycle.OperatorLifecycleRequest{
		OperationType:     &lifecycle.OperatorOperationType{Type: lifecycle.OperatorOperationType_TYPE_CREATE},
		ClusterDefinition: clusterJSON(t, c),
		ObjectDefinition:  podJSON(t, instancePod()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetJsonPatch()) != 0 {
		t.Errorf("mode: off still injected a sidecar: %s", res.GetJsonPatch())
	}
}

func TestLifecycleIgnoresNonPods(t *testing.T) {
	c := newCluster(map[string]string{"tables": "public.events"})
	svc, _ := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]string{"name": "app-rw"},
	})
	res, err := LifecycleImpl{}.LifecycleHook(context.Background(), &lifecycle.OperatorLifecycleRequest{
		OperationType:     &lifecycle.OperatorOperationType{Type: lifecycle.OperatorOperationType_TYPE_CREATE},
		ClusterDefinition: clusterJSON(t, c),
		ObjectDefinition:  svc,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.GetJsonPatch()) != 0 {
		t.Errorf("patched a Service: %s", res.GetJsonPatch())
	}
}

func TestEnrichConfiguration(t *testing.T) {
	c := newCluster(map[string]string{"tables": "public.events"})
	res, err := PostgresImpl{}.EnrichConfiguration(context.Background(),
		&postgres.EnrichConfigurationRequest{
			ClusterDefinition: clusterJSON(t, c),
			Configs:           map[string]string{"shared_buffers": "256MB"},
			OperationType:     &postgres.OperationType{Type: postgres.OperationType_TYPE_RECONCILE},
		})
	if err != nil {
		t.Fatal(err)
	}
	got := res.GetConfigs()
	if got["wal_level"] != "logical" {
		t.Errorf("wal_level = %q, want logical", got["wal_level"])
	}
	if got["shared_buffers"] != "256MB" {
		t.Error("enrichment dropped an existing parameter")
	}
	if got["sync_replication_slots"] != "on" {
		t.Errorf("sync_replication_slots = %q on a PG17 cluster, want on", got["sync_replication_slots"])
	}
	// The one that must NOT be set here: it names a physical slot that will not
	// exist on the new primary after a promotion, and logical decoding then
	// blocks indefinitely with no error to the client (docs/15).
	if _, ok := got["synchronized_standby_slots"]; ok {
		t.Errorf("synchronized_standby_slots was set statically (%q); after a failover "+
			"it names a slot that does not exist and decoding hangs silently",
			got["synchronized_standby_slots"])
	}
	if got["max_replication_slots"] != "7" { // 3 instances + 4 headroom
		t.Errorf("max_replication_slots = %q, want 7", got["max_replication_slots"])
	}
}

func TestEnrichConfigurationDoesNotLowerExistingLimits(t *testing.T) {
	c := newCluster(map[string]string{"tables": "public.events"})
	res, err := PostgresImpl{}.EnrichConfiguration(context.Background(),
		&postgres.EnrichConfigurationRequest{
			ClusterDefinition: clusterJSON(t, c),
			Configs:           map[string]string{"max_replication_slots": "50"},
		})
	if err != nil {
		t.Fatal(err)
	}
	if res.GetConfigs()["max_replication_slots"] != "50" {
		t.Errorf("an operator-chosen higher limit was lowered to %q",
			res.GetConfigs()["max_replication_slots"])
	}
}

func TestEnrichConfigurationLeavesClusterAloneWhenOff(t *testing.T) {
	for _, params := range []map[string]string{
		{"mode": "off"},
		nil,
	} {
		c := newCluster(params)
		if params == nil {
			c.Spec.Plugins = nil
		}
		res, err := PostgresImpl{}.EnrichConfiguration(context.Background(),
			&postgres.EnrichConfigurationRequest{
				ClusterDefinition: clusterJSON(t, c),
				Configs:           map[string]string{"shared_buffers": "256MB"},
			})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := res.GetConfigs()["wal_level"]; ok {
			t.Errorf("params=%v: wal_level was forced on a cluster that builds no mirror; "+
				"that is a +70%% WAL bill for nothing", params)
		}
	}
}

func TestSetStatusInCluster(t *testing.T) {
	c := newCluster(map[string]string{"tables": "public.events", "mode": "takeover",
		AcknowledgeParam: "true"})
	c.Status.InstanceNames = []string{"app-1", "app-2", "app-3"}
	c.Status.CurrentPrimary = "app-1"

	res, err := OperatorImpl{}.SetStatusInCluster(context.Background(),
		&operator.SetStatusInClusterRequest{Cluster: clusterJSON(t, c)})
	if err != nil {
		t.Fatal(err)
	}
	var st Status
	if err := json.Unmarshal(res.GetJsonStatus(), &st); err != nil {
		t.Fatalf("status is not valid JSON: %v (%s)", err, res.GetJsonStatus())
	}
	if st.Mode != "takeover" || st.Slot != "quicksilver_app" {
		t.Errorf("status = %+v", st)
	}
	// The primary must never be listed as a mirroring node: its latency is the
	// thing the whole design exists to protect.
	for _, p := range st.MirroringPods {
		if p == "app-1" {
			t.Error("the current primary was reported as building the mirror")
		}
	}
	if len(st.MirroringPods) != 2 {
		t.Errorf("mirroringPods = %v, want the two replicas", st.MirroringPods)
	}
	if !strings.Contains(st.Message, "-ro") {
		t.Errorf("takeover status does not say the -ro service changed: %q", st.Message)
	}
}

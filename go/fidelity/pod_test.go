package fidelity

// The lifecycle hook, run against the Pod CloudNativePG actually builds.
//
// The plugin package tests the hook against a hand-written Pod fixture, which
// proves the logic and nothing about the shape of the real input. Here the Pod
// comes from specs.NewInstance — the operator's own builder, imported — so the
// container names, the volume mounts, the annotations and the init containers
// are whatever CloudNativePG 1.30 really produces.
//
// This is also where the operation-type contract gets checked. CNPG skips a
// lifecycle hook whose declared OperationTypes do not contain the verb it is
// asking for, and it asks with four different verbs in three different code
// paths. A verb we forget to declare is not an error anywhere; it is a hook that
// quietly never runs.

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	jsonpatch "github.com/evanphx/json-patch/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	qsplugin "github.com/howlerops/pg_quicksilver/go/internal/plugin"
)

// The operator carries its OWN copy of the Cluster type
// (cloudnative-pg/api/v1), distinct from the cloudnative-pg/api module that
// cnpg-i-machinery's decoder uses. They are structurally identical and share a
// JSON encoding, which is the entire point of the CNPG-I contract: the wire
// format is the interface, not the Go type. Building the Pod therefore uses the
// operator's type and the hook is handed the JSON, exactly as in production.
func quicksilverCluster() *cnpgv1.Cluster {
	return &cnpgv1.Cluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default"},
		Spec: cnpgv1.ClusterSpec{
			Instances:            3,
			ImageName:            "ghcr.io/cloudnative-pg/postgresql:17.2-standard-bookworm",
			StorageConfiguration: cnpgv1.StorageConfiguration{Size: "20Gi"},
			Plugins: []cnpgv1.PluginConfiguration{{
				Name: qsplugin.PluginName,
				Parameters: map[string]string{
					"tables":       "public.events,public.orders",
					"freshnessSLO": "30s",
				},
			}},
		},
	}
}

// realInstancePod builds the Pod CloudNativePG would create. The context
// carries no plugin client, so NewInstance returns the unpatched Pod — which is
// exactly the input our hook receives.
func realInstancePod(t *testing.T, cluster *cnpgv1.Cluster, serial int) *corev1.Pod {
	t.Helper()
	pod, err := specs.NewInstance(context.Background(), *cluster, serial, true)
	must(t, err)
	if pod.Kind == "" {
		pod.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	}
	return pod
}

func runHook(t *testing.T, cluster *cnpgv1.Cluster, pod *corev1.Pod,
	op lifecycle.OperatorOperationType_Type,
) *corev1.Pod {
	t.Helper()
	clusterJSON, err := json.Marshal(cluster)
	must(t, err)
	podJSON, err := json.Marshal(pod)
	must(t, err)

	res, err := qsplugin.LifecycleImpl{}.LifecycleHook(context.Background(),
		&lifecycle.OperatorLifecycleRequest{
			OperationType:     &lifecycle.OperatorOperationType{Type: op},
			ClusterDefinition: clusterJSON,
			ObjectDefinition:  podJSON,
		})
	must(t, err)
	if len(res.GetJsonPatch()) == 0 {
		return nil
	}
	p, err := jsonpatch.DecodePatch(res.GetJsonPatch())
	if err != nil {
		t.Fatalf("patch does not decode against a real CNPG Pod: %v\n%s", err, res.GetJsonPatch())
	}
	out, err := p.Apply(podJSON)
	if err != nil {
		t.Fatalf("patch does not apply to a real CNPG Pod: %v\n%s", err, res.GetJsonPatch())
	}
	var patched corev1.Pod
	must(t, json.Unmarshal(out, &patched))
	return &patched
}

func sidecarOf(pod *corev1.Pod) *corev1.Container {
	if pod == nil {
		return nil
	}
	for i := range pod.Spec.InitContainers {
		if pod.Spec.InitContainers[i].Name == qsplugin.MirrorContainerName {
			return &pod.Spec.InitContainers[i]
		}
	}
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == qsplugin.MirrorContainerName {
			return &pod.Spec.Containers[i]
		}
	}
	return nil
}

func TestSidecarInjectionIntoARealCNPGPod(t *testing.T) {
	cluster := quicksilverCluster()
	pod := realInstancePod(t, cluster, 1)

	t.Logf("CNPG built Pod %q: containers=%v initContainers=%v volumes=%d",
		pod.Name, containerNames(pod.Spec.Containers),
		containerNames(pod.Spec.InitContainers), len(pod.Spec.Volumes))

	patched := runHook(t, cluster, pod, lifecycle.OperatorOperationType_TYPE_CREATE)
	if patched == nil {
		t.Fatal("no patch emitted for a real instance Pod")
	}
	sc := sidecarOf(patched)
	if sc == nil {
		t.Fatalf("sidecar absent after patching a real CNPG Pod; initContainers=%v containers=%v",
			containerNames(patched.Spec.InitContainers), containerNames(patched.Spec.Containers))
	}

	// The data volume is the reason this is a sidecar and not a Deployment.
	// CNPG names it "pgdata"; if that ever changes, the mirror has nowhere to
	// write and this is where we find out.
	var mounts []string
	for _, vm := range sc.VolumeMounts {
		mounts = append(mounts, vm.Name+":"+vm.MountPath)
	}
	t.Logf("sidecar mounts: %v", mounts)
	if !slices.ContainsFunc(sc.VolumeMounts, func(vm corev1.VolumeMount) bool {
		return vm.Name == "pgdata"
	}) {
		t.Errorf("sidecar did not receive CNPG's data volume: %v", mounts)
	}

	// The mirror path must land inside a mounted volume, or the mirror is
	// written to the container's ephemeral filesystem and lost on every restart
	// — while appearing to work perfectly until one happens.
	cfg := qsplugin.ParseConfig(cluster.Name, cluster.Spec.Plugins[0].Parameters)
	covered := false
	for _, vm := range sc.VolumeMounts {
		if len(cfg.MirrorPath) >= len(vm.MountPath) && cfg.MirrorPath[:len(vm.MountPath)] == vm.MountPath {
			covered = true
			t.Logf("mirrorPath %s is inside volume %s at %s", cfg.MirrorPath, vm.Name, vm.MountPath)
		}
	}
	if !covered {
		t.Errorf("mirrorPath %q is not inside any mounted volume; the mirror would be "+
			"written to ephemeral container storage and lost on restart", cfg.MirrorPath)
	}

	// The instance's own container must survive intact.
	if sidecarOf(patched) != nil && len(patched.Spec.Containers) < len(pod.Spec.Containers) {
		t.Error("the patch removed a container CNPG had put there")
	}
	if !slices.Contains(containerNames(patched.Spec.Containers), "postgres") {
		t.Error("the postgres container is gone after patching")
	}
}

// TestEveryDeclaredVerbActuallyInjects is the test that caught a real bug.
//
// CloudNativePG asks the Pod hook with four verbs from three code paths, and
// skips any plugin that has not declared the verb being asked. EVALUATE was
// missing from our capability list, which is invisible: no error, no log, just a
// hook that never runs on the path that computes what a Pod *should* look like.
func TestEveryDeclaredVerbActuallyInjects(t *testing.T) {
	res, err := qsplugin.LifecycleImpl{}.GetCapabilities(context.Background(),
		&lifecycle.OperatorLifecycleCapabilitiesRequest{})
	must(t, err)

	var declared []lifecycle.OperatorOperationType_Type
	for _, c := range res.GetLifecycleCapabilities() {
		if c.GetKind() != "Pod" {
			continue
		}
		for _, o := range c.GetOperationTypes() {
			declared = append(declared, o.GetType())
		}
	}
	t.Logf("declared Pod operation types: %v", declared)

	// EVALUATE specifically: specs.NewInstance uses it to build the spec that
	// checkPodSpecIsOutdated compares a running Pod against. Without it the
	// sidecar is absent from both sides of that comparison, so a change to
	// sidecarImage would never roll the instances and the pod-spec annotation
	// would describe a Pod that does not exist.
	if !slices.Contains(declared, lifecycle.OperatorOperationType_TYPE_EVALUATE) {
		t.Error("TYPE_EVALUATE is not declared; specs.NewInstance would skip this hook, " +
			"and sidecar changes would never trigger a rollout")
	}

	cluster := quicksilverCluster()
	for _, op := range declared {
		pod := realInstancePod(t, cluster, 1)
		patched := runHook(t, cluster, pod, op)
		if sidecarOf(patched) == nil {
			t.Errorf("verb %v is declared but injects nothing; CNPG would call it and "+
				"get a Pod without the mirror", op)
		}
	}
}

func TestModeOffLeavesARealPodUntouched(t *testing.T) {
	cluster := quicksilverCluster()
	cluster.Spec.Plugins[0].Parameters = map[string]string{"mode": "off"}
	pod := realInstancePod(t, cluster, 1)
	if patched := runHook(t, cluster, pod, lifecycle.OperatorOperationType_TYPE_CREATE); patched != nil {
		t.Errorf("mode: off patched a real instance Pod: %v",
			containerNames(patched.Spec.InitContainers))
	}
}

func containerNames(cs []corev1.Container) []string {
	out := make([]string, 0, len(cs))
	for _, c := range cs {
		out = append(out, c.Name)
	}
	return out
}

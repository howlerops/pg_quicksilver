package plugin

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/object"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
)

// MirrorContainerName is the injected sidecar's container name. It is also the
// key the injector uses to stay idempotent, so it must not change between
// releases without a migration.
const MirrorContainerName = "quicksilver-mirror"

// LifecycleImpl injects the mirror builder into instance Pods.
//
// The mirror is a sidecar on the ordinary CNPG instance Pod rather than a
// separate Deployment, for one structural reason: it needs the instance's data
// volume. Sharing the Pod means sharing the PVC, so the Parquet mirror lands
// beside PGDATA on storage that is already provisioned, already backed up with
// the node, and already deleted with it. A separate Deployment would need its
// own volume and its own lifecycle, and the two could drift apart in exactly
// the failure case (a node being rebuilt) where they must not.
//
// Measured sizing, from docs/11: an 8.7 GB heap produced a 251 MB Parquet
// mirror, so the extra demand on the data PVC is a few percent, not a doubling.
type LifecycleImpl struct {
	lifecycle.UnimplementedOperatorLifecycleServer
}

func (LifecycleImpl) GetCapabilities(
	context.Context, *lifecycle.OperatorLifecycleCapabilitiesRequest,
) (*lifecycle.OperatorLifecycleCapabilitiesResponse, error) {
	return &lifecycle.OperatorLifecycleCapabilitiesResponse{
		LifecycleCapabilities: []*lifecycle.OperatorLifecycleCapabilities{
			{
				Group: "",
				Kind:  "Pod",
				OperationTypes: []*lifecycle.OperatorOperationType{
					{Type: lifecycle.OperatorOperationType_TYPE_CREATE},
					{Type: lifecycle.OperatorOperationType_TYPE_PATCH},
					{Type: lifecycle.OperatorOperationType_TYPE_UPDATE},
					// EVALUATE is not optional, and leaving it out fails
					// silently. CloudNativePG skips any hook whose declared
					// operation types lack the one it is asking for
					// (internal/cnpi/plugin/client/lifecycle.go), and
					// specs.NewInstance asks with EVALUATE to compute what a
					// running Pod *should* look like. That evaluated spec is
					// what lands in the pod-spec annotation and what
					// checkPodSpecIsOutdated compares against to decide whether
					// to roll an instance.
					//
					// Omit EVALUATE and the sidecar is absent from both sides of
					// that comparison, so it is invisible to it: changing
					// sidecarImage would never roll the Pods, and the annotation
					// would describe a Pod that does not exist.
					{Type: lifecycle.OperatorOperationType_TYPE_EVALUATE},
				},
			},
		},
	}, nil
}

func (LifecycleImpl) LifecycleHook(
	_ context.Context, req *lifecycle.OperatorLifecycleRequest,
) (*lifecycle.OperatorLifecycleResponse, error) {
	kind, err := object.GetKind(req.GetObjectDefinition())
	if err != nil {
		return nil, err
	}
	if kind != "Pod" {
		return &lifecycle.OperatorLifecycleResponse{}, nil
	}

	cluster, err := decoder.DecodeClusterLenient(req.GetClusterDefinition())
	if err != nil {
		return nil, err
	}
	pod, err := decoder.DecodePodJSON(req.GetObjectDefinition())
	if err != nil {
		return nil, err
	}

	cfg := ParseConfig(cluster.Name, PluginParameters(cluster))
	if !Enabled(cluster) || cfg.Mode == ModeOff {
		// Nothing to inject. Returning an empty patch (rather than one that
		// strips the container) is deliberate: flipping to mode: off should
		// stop the mirror on the next rollout, not evict running Pods.
		return &lifecycle.OperatorLifecycleResponse{}, nil
	}

	mutated := pod.DeepCopy()
	sidecar := MirrorSidecar(cluster.Name, cfg)
	if err := object.InjectPluginSidecarInitContainer(mutated, sidecar, true); err != nil {
		return nil, err
	}

	patch, err := object.CreatePatch(mutated, pod)
	if err != nil {
		return nil, err
	}
	return &lifecycle.OperatorLifecycleResponse{JsonPatch: patch}, nil
}

// MirrorSidecar builds the container spec. Exported so the tests can assert on
// it directly rather than through a JSON patch.
func MirrorSidecar(clusterName string, cfg Config) *corev1.Container {
	return &corev1.Container{
		Name:  MirrorContainerName,
		Image: cfg.SidecarImage,
		Env: []corev1.EnvVar{
			{Name: "QS_CLUSTER", Value: clusterName},
			{Name: "QS_MODE", Value: string(cfg.Mode)},
			{Name: "QS_INGEST", Value: string(cfg.Ingest)},
			{Name: "QS_TABLES", Value: strings.Join(cfg.Tables, ",")},
			{Name: "QS_SLOT", Value: cfg.SlotName},
			{Name: "QS_PUBLICATION", Value: cfg.Publication},
			{Name: "QS_MIRROR_PATH", Value: cfg.MirrorPath},
			{Name: "QS_FRESHNESS_SLO", Value: cfg.FreshnessSLO.String()},
			{
				Name: "QS_POD_NAME",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			},
			{
				Name: "QS_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				},
			},
			// The sidecar reaches the primary through the cluster's -rw service,
			// which follows a failover on its own. It must NOT be pinned to a
			// Pod name: after a promotion that name is a node that no longer
			// accepts a replication connection, and the stream would stall
			// silently rather than fail (docs/15).
			{Name: "QS_PRIMARY_HOST", Value: fmt.Sprintf("%s-rw", clusterName)},
			{Name: "QS_DATABASE", Value: cfg.Database},
			{
				Name: "QS_PGUSER",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: cfg.CredentialsSecret},
						Key:                  "username",
					},
				},
			},
			{
				Name: "QS_PGPASSWORD",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: cfg.CredentialsSecret},
						Key:                  "password",
					},
				},
			},
		},
		Ports: []corev1.ContainerPort{
			{Name: "qs-health", ContainerPort: HealthPort},
		},
		// Readiness is the mechanism that keeps a stale mirror out of the
		// endpoint, so it is a real dependency of correctness, not a nicety.
		// /readyz reports not-ready when the mirror is behind the freshness SLO,
		// and distinguishes "behind" from "idle" — an idle source is fresh.
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/readyz",
					Port: intOrString(HealthPort),
				},
			},
			PeriodSeconds:    5,
			FailureThreshold: 2,
		},
		// Liveness deliberately does NOT check freshness. A mirror that is
		// behind should be taken out of service, not restarted: restarting it
		// discards in-memory progress and makes it further behind.
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/healthz",
					Port: intOrString(HealthPort),
				},
			},
			PeriodSeconds:    10,
			FailureThreshold: 6,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: boolPtr(false),
			ReadOnlyRootFilesystem:   boolPtr(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
}

// HealthPort is where the sidecar serves /readyz, /healthz and /metrics.
const HealthPort = 9187

func boolPtr(b bool) *bool { return &b }

package plugin

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/object"
	"github.com/cloudnative-pg/cnpg-i/pkg/lifecycle"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
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

	// The operator never asks plugins to validate at admission (see
	// Validate's callers in docs/16), so an invalid configuration arrives
	// here as an ordinary Cluster. It must not reach MirrorSidecar: a
	// misspelled parameter would be ignored, and mode: takeover without the
	// acknowledgement would inject the readiness gate that takes replicas out
	// of -ro. Decide per operation, see invalidConfigResponse.
	if faults := Validate(cluster, PluginParameters(cluster)); len(faults) > 0 {
		return invalidConfigResponse(req.GetOperationType().GetType(), cluster, faults)
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

// invalidConfigResponse is what LifecycleHook does for a Pod when the Cluster's
// Quicksilver configuration fails Validate. It is a deliberate trade between
// two things the operator does with a hook result, both checked against the
// operator source (CloudNativePG 1.26.0 and 1.30.0 behave the same here):
//
//   - An error from EVALUATE is how the operator's rollout check learns what a
//     Pod should look like (internal/controller/cluster_upgrade.go,
//     isPodNeedingRollout). That check logs a checker error and skips it, so an
//     error here means "no rollout", and a running primary is not restarted.
//     Returning an empty patch instead would evaluate the Pod WITHOUT the
//     sidecar, the stored Pod spec would then differ, and every instance would
//     be rolled to drop a mirror it had been running.
//
//   - An error from CREATE, or from EVALUATE while building a new instance
//     (internal/controller/cluster_create.go, ensureInstancesAreCreated), stops
//     that reconcile before finalizeReconciliation. The status that reports the
//     fault is written there, so a refusal that aborts the reconcile would hide
//     the reason. That is why a Cluster with no instances yet is built WITHOUT
//     the mirror rather than refused: PostgreSQL comes up, the reconcile
//     completes, and .status.pluginStatus says why there is no mirror.
//
// So, per operation:
//
//   - CREATE and EVALUATE, Cluster with no instances: empty patch. The Pod runs
//     PostgreSQL with no mirror. Nothing takes over.
//   - CREATE and EVALUATE, Cluster that already has instances: an error. No
//     new instance Pod is built and no rollout is planned, so the running
//     database is left alone and the operator logs the reason on every pass.
//   - PATCH and UPDATE: empty patch. The operator patches instance Pods for
//     role labels on failover and switchover, and an error there would block
//     the failover. Existing Pods keep whatever they already have.
//
// The one thing this never does is inject the sidecar, so mode: takeover
// without its acknowledgement cannot reach the readiness gate.
func invalidConfigResponse(
	op lifecycle.OperatorOperationType_Type,
	cluster *apiv1.Cluster,
	faults []*operator.ValidationError,
) (*lifecycle.OperatorLifecycleResponse, error) {
	switch op {
	case lifecycle.OperatorOperationType_TYPE_CREATE, lifecycle.OperatorOperationType_TYPE_EVALUATE:
		if clusterHasInstances(cluster) {
			return nil, fmt.Errorf("quicksilver: not building an instance Pod for cluster %s/%s, "+
				"its configuration is invalid: %s", cluster.Namespace, cluster.Name, describeFaults(faults))
		}
		return &lifecycle.OperatorLifecycleResponse{}, nil
	default:
		return &lifecycle.OperatorLifecycleResponse{}, nil
	}
}

// clusterHasInstances reports whether the operator has already built instance
// Pods for this Cluster. Before the first instance exists the operator's
// status says so, and the check is only used to choose between the two
// invalid-config outcomes above.
func clusterHasInstances(cluster *apiv1.Cluster) bool {
	return cluster.Status.Instances > 0 || len(cluster.Status.InstanceNames) > 0
}

// describeFaults renders validation errors as one line for an error message.
func describeFaults(faults []*operator.ValidationError) string {
	parts := make([]string, 0, len(faults))
	for _, f := range faults {
		name := strings.Join(f.GetPathComponents(), ".")
		parts = append(parts, fmt.Sprintf("%s (%s): %s", name, f.GetValue(), f.GetMessage()))
	}
	return strings.Join(parts, "; ")
}

// MirrorSidecar builds the container spec. Exported so the tests can assert on
// it directly rather than through a JSON patch.
func MirrorSidecar(clusterName string, cfg Config) *corev1.Container {
	c := &corev1.Container{
		Name:  MirrorContainerName,
		Image: cfg.SidecarImage,
		Env: []corev1.EnvVar{
			{Name: "QS_CLUSTER", Value: clusterName},
			{Name: "QS_MODE", Value: string(cfg.Mode)},
			{Name: "QS_INGEST", Value: string(cfg.Ingest)},
			{Name: "QS_TABLES", Value: strings.Join(cfg.Tables, ",")},
			// The same on every Pod, so it is not the slot: the sidecar derives
			// its own from this and QS_POD_NAME below. Both are load-bearing —
			// without the Pod name two sidecars share one slot and the second
			// silently misses rows. TestSidecarGetsWhatASlotPerPodNeeds.
			{Name: "QS_SLOT", Value: cfg.SlotName},
			{Name: "QS_PUBLICATION", Value: cfg.Publication},
			{Name: "QS_MIRROR_PATH", Value: cfg.MirrorPath},
			{Name: "QS_FRESHNESS_SLO", Value: cfg.FreshnessSLO.String()},
			// A soft heap ceiling derived from the container's memory limit, so
			// that a mirror too large for its limit gets SLOWER rather than
			// killed. See sidecarResources for the measured cliff this is
			// sized to stay clear of.
			{Name: "GOMEMLIMIT", Value: goMemLimit(sidecarMemory(cfg))},
			// Always set, in the sidecar's own spelling, so that what a Pod
			// runs with can be read off its spec rather than inferred from an
			// absence.
			{Name: "QS_DUCKLAKE", Value: onOff(cfg.DuckLake)},
			{Name: "QS_SERVE_WITH_HALTED_TABLES", Value: onOff(cfg.ServeWithHaltedTables)},
			{Name: "QS_EXTENSION", Value: extensionSetting(cfg)},
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
		// endpoint, so under takeover it is a real dependency of correctness.
		// /readyz reports not-ready when the mirror is behind the freshness SLO,
		// and distinguishes "behind" from "idle" — an idle source is fresh.
		//
		// It is wired ONLY in takeover. This is a native sidecar, and for a
		// restartable init container Kubernetes uses the readiness probe's
		// result "to determine the ready state of the Pod" — so the probe does
		// not gate the mirror, it gates the whole instance Pod, PostgreSQL
		// included. In shadow that is backwards: a mirror nobody is querying
		// would evict a healthy replica from -ro and -r, and it would do it to
		// every replica at once, because they all fall behind the same writer.
		// docs/05 defines shadow as "-ro untouched"; a probe here is the one
		// thing that would touch it.
		ReadinessProbe: readinessProbe(cfg),
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
		Resources: sidecarResources(cfg),
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: boolPtr(false),
			ReadOnlyRootFilesystem:   boolPtr(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	// Only when asked for: unset, the sidecar's own default (5m) applies, and
	// that default is the sidecar's to change.
	if cfg.DuckLake && cfg.DuckLakeRetention > 0 {
		c.Env = append(c.Env, corev1.EnvVar{Name: "QS_DUCKLAKE_RETENTION", Value: cfg.DuckLakeRetention.String()})
	}
	c.Env = append(c.Env, switchEnv(cfg)...)
	return c
}

// HealthPort is where the sidecar serves /readyz, /healthz and /metrics.
//
// NOT 9187, which is what this used to be. Every container in a Pod shares one
// network namespace, and CloudNativePG's instance manager serves its own
// metrics on :9187. The mirror is a NATIVE sidecar, so it starts first, binds
// the port, and then the instance manager cannot:
//
//	{"level":"error","msg":"Error while running the web server",
//	 "address":":9187","error":"listen tcp :9187: bind: address already in use"}
//	Error: unretryable: listen tcp :9187: bind: address already in use
//
// It calls that unretryable and exits, so PostgreSQL never starts and the Pod
// crash-loops. In shadow mode. On every instance. The plugin whose entire
// premise is that it does not disturb the database was stopping it from
// booting.
//
// Nothing here could have caught it: the sidecar's port only collides inside a
// real CNPG instance Pod, and until bench/scripts/cluster_e2e.sh there was
// never one. 9188 is adjacent, free in CNPG's layout (5432, 8000, 8010, 9187),
// and deliberately not another round number someone else has claimed.
const HealthPort = 9188

func boolPtr(b bool) *bool { return &b }

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// extensionSetting falls back to auto for a value Validate would have refused:
// an already-admitted Cluster must still get a Pod.
func extensionSetting(cfg Config) string {
	if cfg.Extension == ExtensionOff {
		return ExtensionOff
	}
	return ExtensionAuto
}

// sidecarResources sets the mirror sidecar's memory request AND limit to the
// same value, and derives GOMEMLIMIT from it.
//
// Request == limit puts the sidecar in the Guaranteed QoS class. The old spec
// set a 256Mi request and no limit, which is Burstable, and the Pod that makes
// an eviction candidate is the one running PostgreSQL — so the mirror, which is
// an optimisation, could get the database evicted under node memory pressure.
// That is the same principle as the slot guard in docs/28: the mirror must
// never be the reason the primary goes down.
//
// GOMEMLIMIT is a soft ceiling: past it Go collects harder rather than growing,
// which turns "the container is killed" into "the mirror runs slower". That is
// the right trade for a sidecar next to a database, but it is a real trade and
// it has a cliff. Measured against the 20.5M-row mirror, whose live heap is
// 901 MB, draining one 20,000-row UPDATE:
//
//	setting                peak RSS   drain
//	default (GOGC=100)      1854 MB     12s
//	GOMEMLIMIT 2000MiB      1860 MB     12s   above the natural peak; not binding
//	GOMEMLIMIT 1600MiB      1536 MB     12s   binding, and free
//	GOMEMLIMIT 1200MiB      1174 MB    125s   THRASHING — ten times slower
//	GOGC 50                 1392 MB     12s
//	GOGC 25                 2013 MB    183s   WORSE ON BOTH AXES
//
// A ceiling close to the live heap makes the collector run continuously. So the
// fraction below is deliberately generous: at 80% of the limit, an operator who
// sizes the limit by the documented rule (~90 bytes of RSS per live row) lands
// around 1.7x the live heap — 1600MiB against 901 MB live is 1.78x, which is
// the row that was free. An operator who undersizes it gets a slow mirror
// instead of a killed one, which is the failure this is chosen to produce.
//
// GOGC is deliberately NOT the knob, and GOGC=25 is why. Collecting harder
// slows the apply loop, a slower apply loop leaves more changes in flight, and
// changes in flight are heap — so tightening the ratio cost 15x the drain time
// and finished with MORE resident memory than the default. A ratio cannot
// distinguish "this heap is one big long-lived index" from "this heap is
// garbage", and here it is almost entirely the former. GOMEMLIMIT can, because
// it is an absolute ceiling rather than a multiple of the live set.
// readinessProbe returns the freshness gate, and only in the mode that wants
// one.
//
// Returning nil is not "no probe available" — /readyz is served in every mode
// and stays the way to ask whether a mirror is fresh, from a dashboard, from
// qs-verify, or by hand. What nil means is that the answer does not decide
// whether the instance Pod receives PostgreSQL traffic. Only takeover routes
// reads through the mirror, so only takeover has a reason to make freshness a
// condition of serving anything.
func readinessProbe(cfg Config) *corev1.Probe {
	if cfg.Mode != ModeTakeover {
		return nil
	}
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/readyz",
				Port: intOrString(HealthPort),
			},
		},
		PeriodSeconds:    5,
		FailureThreshold: 2,
	}
}

func sidecarResources(cfg Config) corev1.ResourceRequirements {
	mem := sidecarMemory(cfg)
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: mem,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: mem,
		},
	}
}

// goMemLimit is the soft heap ceiling for a container limited to `mem`, as a
// GOMEMLIMIT value. See sidecarResources for why it is 80% and not 95%.
func goMemLimit(mem resource.Quantity) string {
	return strconv.FormatInt(mem.Value()/5*4, 10)
}

// sidecarMemory parses the configured limit, falling back to the default rather
// than failing: an unparseable value must not stop a Pod from being admitted.
func sidecarMemory(cfg Config) resource.Quantity {
	mem, err := resource.ParseQuantity(cfg.SidecarMemory)
	if err != nil {
		return resource.MustParse(DefaultSidecarMemory)
	}
	return mem
}

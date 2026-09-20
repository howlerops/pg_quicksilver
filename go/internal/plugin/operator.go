package plugin

import (
	"context"
	"encoding/json"
	"fmt"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/object"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
)

// OperatorImpl is the webhook-side half of the plugin: it validates Cluster
// resources on create and change, fills in defaults, and publishes what the
// mirror is doing into .status.plugins.
type OperatorImpl struct {
	operator.UnimplementedOperatorServer
}

func (OperatorImpl) GetCapabilities(
	context.Context, *operator.OperatorCapabilitiesRequest,
) (*operator.OperatorCapabilitiesResult, error) {
	rpcs := []operator.OperatorCapability_RPC_Type{
		operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CREATE,
		operator.OperatorCapability_RPC_TYPE_VALIDATE_CLUSTER_CHANGE,
		operator.OperatorCapability_RPC_TYPE_MUTATE_CLUSTER,
		operator.OperatorCapability_RPC_TYPE_SET_STATUS_IN_CLUSTER,
	}
	caps := make([]*operator.OperatorCapability, 0, len(rpcs))
	for _, r := range rpcs {
		caps = append(caps, &operator.OperatorCapability{
			Type: &operator.OperatorCapability_Rpc{
				Rpc: &operator.OperatorCapability_RPC{Type: r},
			},
		})
	}
	return &operator.OperatorCapabilitiesResult{Capabilities: caps}, nil
}

func (OperatorImpl) ValidateClusterCreate(
	_ context.Context, req *operator.OperatorValidateClusterCreateRequest,
) (*operator.OperatorValidateClusterCreateResult, error) {
	cluster, err := decoder.DecodeClusterLenient(req.GetDefinition())
	if err != nil {
		return nil, err
	}
	if !Enabled(cluster) {
		return &operator.OperatorValidateClusterCreateResult{}, nil
	}
	return &operator.OperatorValidateClusterCreateResult{
		ValidationErrors: Validate(cluster, PluginParameters(cluster)),
	}, nil
}

func (OperatorImpl) ValidateClusterChange(
	_ context.Context, req *operator.OperatorValidateClusterChangeRequest,
) (*operator.OperatorValidateClusterChangeResult, error) {
	oldCluster, err := decoder.DecodeClusterLenient(req.GetOldCluster())
	if err != nil {
		return nil, err
	}
	newCluster, err := decoder.DecodeClusterLenient(req.GetNewCluster())
	if err != nil {
		return nil, err
	}
	if !Enabled(newCluster) {
		// Removing the plugin is always allowed; Deregister does the cleanup.
		return &operator.OperatorValidateClusterChangeResult{}, nil
	}
	if !Enabled(oldCluster) {
		return &operator.OperatorValidateClusterChangeResult{
			ValidationErrors: Validate(newCluster, PluginParameters(newCluster)),
		}, nil
	}
	return &operator.OperatorValidateClusterChangeResult{
		ValidationErrors: ValidateChange(oldCluster, newCluster),
	}, nil
}

// MutateCluster writes back the defaults the plugin resolved, so that what an
// operator reads with `kubectl get cluster -o yaml` is what the mirror is
// actually doing. Leaving them implicit means the slot name and the freshness
// SLO live only in this binary, and change silently when it is upgraded.
func (OperatorImpl) MutateCluster(
	_ context.Context, req *operator.OperatorMutateClusterRequest,
) (*operator.OperatorMutateClusterResult, error) {
	cluster, err := decoder.DecodeClusterLenient(req.GetDefinition())
	if err != nil {
		return nil, err
	}
	if !Enabled(cluster) {
		return &operator.OperatorMutateClusterResult{}, nil
	}
	mutated := cluster.DeepCopy()

	idx := -1
	for i, p := range mutated.Spec.Plugins {
		if p.Name == PluginName {
			idx = i
		}
	}
	if idx < 0 {
		return &operator.OperatorMutateClusterResult{}, nil
	}

	cfg := ParseConfig(cluster.Name, mutated.Spec.Plugins[idx].Parameters)
	params := mutated.Spec.Plugins[idx].Parameters
	if params == nil {
		params = map[string]string{}
	}
	setDefault(params, "mode", string(cfg.Mode))
	setDefault(params, "ingest", string(cfg.Ingest))
	setDefault(params, "freshnessSLO", cfg.FreshnessSLO.String())
	setDefault(params, "sidecarImage", cfg.SidecarImage)
	setDefault(params, "slotName", cfg.SlotName)
	setDefault(params, "publication", cfg.Publication)
	setDefault(params, "mirrorPath", cfg.MirrorPath)
	setDefault(params, "database", cfg.Database)
	setDefault(params, "credentialsSecret", cfg.CredentialsSecret)
	mutated.Spec.Plugins[idx].Parameters = params

	patch, err := object.CreatePatch(mutated, cluster)
	if err != nil {
		return nil, err
	}
	return &operator.OperatorMutateClusterResult{JsonPatch: patch}, nil
}

func setDefault(m map[string]string, k, v string) {
	if _, ok := m[k]; !ok {
		m[k] = v
	}
}

// Status is what lands in .status.plugins["quicksilver.howlerops.io"].
//
// It reports the plugin's own view of the cluster, not the mirror's freshness:
// the operator reconcile loop has no connection to the mirror, and inventing a
// freshness number here would be worse than omitting it. Per-node freshness is
// exported by the sidecar on /metrics and surfaced through the Pod's readiness
// gate (internal/health), which is what actually removes a stale node from the
// endpoint.
type Status struct {
	Mode          string   `json:"mode"`
	Ingest        string   `json:"ingest"`
	Tables        []string `json:"tables,omitempty"`
	FreshnessSLO  string   `json:"freshnessSLO"`
	Slot          string   `json:"slot"`
	Publication   string   `json:"publication"`
	MajorVersion  int      `json:"postgresMajorVersion,omitempty"`
	MirroringPods []string `json:"mirroringPods,omitempty"`
	Message       string   `json:"message,omitempty"`
}

func (OperatorImpl) SetStatusInCluster(
	_ context.Context, req *operator.SetStatusInClusterRequest,
) (*operator.SetStatusInClusterResponse, error) {
	cluster, err := decoder.DecodeClusterLenient(req.GetCluster())
	if err != nil {
		return nil, err
	}
	if !Enabled(cluster) {
		return &operator.SetStatusInClusterResponse{}, nil
	}
	cfg := ParseConfig(cluster.Name, PluginParameters(cluster))
	major, _ := MajorVersion(cluster)

	st := Status{
		Mode:         string(cfg.Mode),
		Ingest:       string(cfg.Ingest),
		Tables:       cfg.Tables,
		FreshnessSLO: cfg.FreshnessSLO.String(),
		Slot:         cfg.SlotName,
		Publication:  cfg.Publication,
		MajorVersion: major,
	}
	if cfg.Mode == ModeOff {
		st.Message = "mode: off — no mirror is being built"
	} else {
		st.MirroringPods = mirroringPods(cluster)
		if cfg.Mode == ModeTakeover {
			// This used to say the -ro service "is served by the columnar
			// mirror", which was never true and cannot become true under this
			// architecture: the mirror is a sidecar in the instance Pod, so
			// -ro already selects the same nodes and one PostgreSQL answers
			// both engines. Nothing is retargeted anywhere. What takeover
			// actually changes is that mirror freshness now gates the Pod.
			st.Message = fmt.Sprintf(
				"mirror freshness gates the %s-ro endpoint: a replica whose mirror is "+
					"behind freshnessSLO (%s) is removed from it, PostgreSQL included. "+
					"OLTP-shaped queries answered from the mirror are far slower than on "+
					"a hot standby", cluster.Name, cfg.FreshnessSLO)
		}
	}

	raw, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	return &operator.SetStatusInClusterResponse{JsonStatus: raw}, nil
}

// mirroringPods lists the replicas that carry the mirror. The primary never
// does: it is the one node whose latency the whole design exists to protect.
func mirroringPods(cluster *apiv1.Cluster) []string {
	var out []string
	for _, name := range cluster.Status.InstanceNames {
		if name != cluster.Status.CurrentPrimary {
			out = append(out, name)
		}
	}
	return out
}

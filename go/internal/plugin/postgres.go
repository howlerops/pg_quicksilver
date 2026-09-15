package plugin

import (
	"context"
	"strconv"

	"github.com/cloudnative-pg/cnpg-i-machinery/pkg/pluginhelper/decoder"
	"github.com/cloudnative-pg/cnpg-i/pkg/postgres"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// PostgresImpl turns on the server settings the mirror needs, so that enabling
// Quicksilver is one plugin block rather than a plugin block plus a list of
// GUCs the operator has to remember.
type PostgresImpl struct {
	postgres.UnimplementedPostgresServer
}

func (PostgresImpl) GetCapabilities(
	context.Context, *postgres.PostgresCapabilitiesRequest,
) (*postgres.PostgresCapabilitiesResult, error) {
	return &postgres.PostgresCapabilitiesResult{
		Capabilities: []*postgres.PostgresCapability{
			{
				Type: &postgres.PostgresCapability_Rpc{
					Rpc: &postgres.PostgresCapability_RPC{
						Type: postgres.PostgresCapability_RPC_TYPE_ENRICH_CONFIGURATION,
					},
				},
			},
		},
	}, nil
}

// Slots the mirror needs beyond whatever the cluster already uses: one logical
// slot per mirroring replica, plus headroom for the slot being recreated while
// the old one is still being dropped.
const slotHeadroom = 4

func (PostgresImpl) EnrichConfiguration(
	_ context.Context, req *postgres.EnrichConfigurationRequest,
) (*postgres.EnrichConfigurationResult, error) {
	cluster, err := decoder.DecodeClusterLenient(req.GetClusterDefinition())
	if err != nil {
		return nil, err
	}

	configs := map[string]string{}
	for k, v := range req.GetConfigs() {
		configs[k] = v
	}

	cfg := ParseConfig(cluster.Name, PluginParameters(cluster))
	if !Enabled(cluster) || cfg.Mode == ModeOff || cfg.Ingest != IngestLogical {
		return &postgres.EnrichConfigurationResult{Configs: configs}, nil
	}

	// wal_level=logical costs +70% WAL volume against the same workload
	// (measured, docs/11). That is the price of admission for ingest: logical
	// and it is charged on the primary, so it belongs in the docs and in the
	// capacity plan — not hidden behind a default nobody reads.
	configs["wal_level"] = "logical"

	need := int(cluster.Spec.Instances) + slotHeadroom
	raise(configs, "max_replication_slots", need)
	raise(configs, "max_wal_senders", need)

	// PostgreSQL 17 slot synchronisation. `failover` is set per slot by the
	// sidecar when it creates one; these two are cluster-wide.
	//
	// sync_replication_slots is safe to leave on everywhere: it does nothing on
	// a primary and starts the sync worker on a standby, which is exactly the
	// behaviour we want as instances change role.
	if major, known := MajorVersion(cluster); !known || major >= MinimumMajorVersion {
		configs["sync_replication_slots"] = "on"
		configs["hot_standby_feedback"] = "on"
	}

	// synchronized_standby_slots is deliberately NOT set here. It names the
	// physical slot of a specific standby, it is written into
	// postgresql.auto.conf and copied to that standby by pg_basebackup, and
	// after a promotion it names a slot that does not exist on the new primary.
	// Logical decoding then waits forever, with a warning in the log and no
	// error to the client (docs/15). It has to be maintained against the live
	// topology and cleared on promotion, which is the sidecar's job, not a
	// static config's.

	return &postgres.EnrichConfigurationResult{Configs: configs}, nil
}

// raise sets key to at least want, leaving a larger operator-chosen value alone.
func raise(configs map[string]string, key string, want int) {
	if cur, err := strconv.Atoi(configs[key]); err == nil && cur >= want {
		return
	}
	configs[key] = strconv.Itoa(want)
}

func intOrString(port int32) intstr.IntOrString {
	return intstr.FromInt32(port)
}

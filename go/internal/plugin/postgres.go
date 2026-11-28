package plugin

// The PostgreSQL settings the mirror needs. The plugin CHECKS them and reports;
// it does not write them.
//
// This file used to implement CNPG-I's EnrichConfiguration, which would have
// written them into postgresql.conf. That never happened on any operator this
// project ships to. The instance manager calls EnrichConfiguration only for
// plugins it can reach from inside the instance Pod, that is plugins that
// declare TYPE_INSTANCE_SIDECAR_INJECTION and serve a socket in the Pod's plugin
// directory, and this plugin is a Deployment reached by the operator. Checked
// against CloudNativePG 1.27.0 and 1.30.2 in go/fidelity (docs/16, "The
// PostgreSQL settings"). So the settings are REQUIRED in the Cluster's
// spec.postgresql.parameters, and this file says which are missing or wrong,
// from the same spec the operator gives the plugin.

import (
	"strconv"
	"strings"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Slots the mirror needs beyond whatever the cluster already uses: one logical
// slot per mirroring replica, plus headroom for the slot being recreated while
// the old one is still being dropped.
const slotHeadroom = 4

// Defaults PostgreSQL and CloudNativePG apply when a setting is absent from the
// Cluster. CloudNativePG 1.26.0 and 1.30 default max_replication_slots to 32
// (pkg/postgres/configuration.go); PostgreSQL's own default for
// max_wal_senders is 10, and CloudNativePG does not set it.
const (
	pgDefaultMaxReplicationSlots = 32
	pgDefaultMaxWalSenders       = 10
)

// requiredSettings are the spec.postgresql.parameters a Cluster needs for the
// mirror to behave as documented. A missing or wrong one is reported in the
// status; see settingWarnings.
func requiredSettings(cluster *apiv1.Cluster, cfg Config) []ConfigError {
	if !Enabled(cluster) || cfg.Mode == ModeOff || cfg.Ingest != IngestLogical {
		return nil
	}
	params := cluster.Spec.PostgresConfiguration.Parameters
	instances := int(cluster.Spec.Instances)
	var out []ConfigError

	// Failover slot synchronisation. Without it a failover rebuilds every mirror
	// from a full snapshot instead of resuming (docs/14, docs/15).
	if major, known := MajorVersion(cluster); !known || major >= MinimumMajorVersion {
		for _, name := range []string{"sync_replication_slots", "hot_standby_feedback"} {
			if v := strings.TrimSpace(params[name]); v != "on" {
				out = append(out, ConfigError{
					Parameter: name,
					Value:     v,
					Message: "must be \"on\" in spec.postgresql.parameters: PostgreSQL's default is off, " +
						"and the plugin cannot set it for you (docs/16, \"The PostgreSQL settings\")",
				})
			}
		}
	}

	need := instances + slotHeadroom
	if _, ok := intParam(params, "max_replication_slots"); !ok {
		if need > pgDefaultMaxReplicationSlots {
			out = append(out, ConfigError{Parameter: "max_replication_slots",
				Message: "not set, and PostgreSQL's default of 32 is below the " + strconv.Itoa(need) +
					" the mirror needs (instances + 4): set it in spec.postgresql.parameters"})
		}
	}
	// A value below need is an error, not a warning: see Validate.

	// Each mirroring standby is a logical walsender on the primary, and each
	// standby is also a physical one: two per standby, not one.
	wantSenders := 2*instances + slotHeadroom
	if v, ok := intParam(params, "max_wal_senders"); !ok {
		if wantSenders > pgDefaultMaxWalSenders {
			out = append(out, ConfigError{Parameter: "max_wal_senders",
				Message: "not set, and PostgreSQL's default of 10 is below the " + strconv.Itoa(wantSenders) +
					" the cluster needs (two per standby, plus headroom): set it in spec.postgresql.parameters"})
		}
	} else if v < wantSenders {
		out = append(out, ConfigError{Parameter: "max_wal_senders", Value: strconv.Itoa(v),
			Message: "is below " + strconv.Itoa(wantSenders) + " (two per standby, plus headroom): " +
				"connections beyond it are refused, and a mirror can then stop"})
	}
	return out
}

// intParam reads an integer setting from the spec.
func intParam(params map[string]string, name string) (int, bool) {
	raw, ok := params[name]
	if !ok || strings.TrimSpace(raw) == "" {
		return 0, false
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, false
	}
	return v, true
}

func intOrString(port int32) intstr.IntOrString {
	return intstr.FromInt32(port)
}

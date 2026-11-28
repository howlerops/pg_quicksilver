package fidelity

// The PostgreSQL settings the mirror needs, against the CloudNativePG operator
// code this module pins (v1.30.0), and the question the e2e answered on 1.27.0:
// does anything the plugin does reach an instance's postgresql.conf?
//
// It does not. The plugin used to implement EnrichConfiguration, which the
// instance manager calls only for plugins it can dial inside the instance Pod.
// These tests pin the operator facts that decide that, and the plugin's side of
// them: what it advertises, and what it reports for a Cluster's spec.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	pgconf "github.com/cloudnative-pg/cloudnative-pg/pkg/postgres"
	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"

	qsplugin "github.com/howlerops/pg_quicksilver/go/internal/plugin"
)

// advertised returns the capability names the plugin declares, in the form
// CloudNativePG stores them in Cluster.status.pluginStatus.
func advertised(t *testing.T) []string {
	t.Helper()
	res, err := qsplugin.IdentityImpl{}.GetPluginCapabilities(context.Background(),
		&identity.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, c := range res.GetCapabilities() {
		out = append(out, c.GetService().GetType().String())
	}
	return out
}

func pluginCluster(params map[string]string, caps []string) *cnpgv1.Cluster {
	enabled := true
	c := &cnpgv1.Cluster{}
	c.Name = "app"
	c.Namespace = "ns"
	c.Spec.Instances = 3
	c.Spec.ImageName = "ghcr.io/cloudnative-pg/postgresql:17.2-standard-bookworm"
	c.Spec.Plugins = []cnpgv1.PluginConfiguration{{
		Name: qsplugin.PluginName, Enabled: &enabled, Parameters: params,
	}}
	c.Status.PluginStatus = []cnpgv1.PluginStatus{{Name: qsplugin.PluginName, Capabilities: caps}}
	return c
}

// The instance manager dials only the plugins this returns. The operator's own
// function decides it, so the test calls the operator's own function.
func TestTheInstanceManagerDoesNotDialThePlugin(t *testing.T) {
	c := pluginCluster(map[string]string{"tables": "public.events"}, advertised(t))
	if got := c.GetInstanceEnabledPluginNames(); len(got) != 0 {
		t.Fatalf("the instance manager would dial %v: the plugin is not reachable from the Pod, "+
			"so its postgres settings are never applied", got)
	}

	// The control: with the capability the instance manager DOES select the
	// plugin, which is why the plugin must not advertise it (identity.go).
	withSidecar := append(advertised(t), identity.PluginCapability_Service_TYPE_INSTANCE_SIDECAR_INJECTION.String())
	c = pluginCluster(map[string]string{"tables": "public.events"}, withSidecar)
	if got := c.GetInstanceEnabledPluginNames(); len(got) != 1 {
		t.Fatalf("control failed: with instance sidecar injection the plugin is %v, not selected", got)
	}
}

// What the plugin advertises must not include the services the instance manager
// would call, and the plugin must serve what it advertises (grpc_test.go).
func TestAdvertisedServicesAreOnlyTheOperatorAndLifecycle(t *testing.T) {
	want := map[string]bool{
		identity.PluginCapability_Service_TYPE_OPERATOR_SERVICE.String():  true,
		identity.PluginCapability_Service_TYPE_LIFECYCLE_SERVICE.String(): true,
	}
	for _, c := range advertised(t) {
		if !want[c] {
			t.Errorf("the plugin advertises %s", c)
		}
	}
}

// The settings are read from the spec and reported. Given every required setting
// in the spec, the status names none of them.
func TestRequiredSettingsInTheSpecSilenceTheStatus(t *testing.T) {
	params := map[string]string{
		"tables": "public.events", "ducklake": "true",
		"sidecarImage": "ghcr.io/howlerops/pg_quicksilver-mirror:0.0.4",
	}
	c := pluginCluster(params, advertised(t))
	c.Spec.PostgresConfiguration.Parameters = map[string]string{
		"sync_replication_slots":     "on",
		"hot_standby_feedback":       "on",
		"max_replication_slots":      "32",
		"max_wal_senders":            "10",
		"duckdb.allowed_directories": "/var/lib/postgresql/data/quicksilver/",
		"duckdb.extension_directory": "/usr/lib/quicksilver/duckdb-extensions",
	}
	c.Spec.PostgresConfiguration.AdditionalLibraries = []string{"pg_duckdb", "pg_quicksilver"}
	st := pluginStatusOf(t, c)
	if st.State != qsplugin.StateValid {
		t.Fatalf("state = %q, want valid: %+v", st.State, st.ConfigErrors)
	}
	if len(st.ConfigWarnings) != 0 {
		t.Errorf("a complete spec still has warnings: %+v", st.ConfigWarnings)
	}

	// And with none of them, every one is named.
	c.Spec.PostgresConfiguration = cnpgv1.PostgresConfiguration{}
	st = pluginStatusOf(t, c)
	named := map[string]bool{}
	for _, w := range st.ConfigWarnings {
		named[w.Parameter] = true
	}
	for _, want := range []string{"sync_replication_slots", "hot_standby_feedback", "duckdb.allowed_directories"} {
		if !named[want] {
			t.Errorf("%s is missing from the spec and not in configWarnings: %+v", want, st.ConfigWarnings)
		}
	}
}

// The operator writes exactly what the spec says. A setting the plugin would
// once have added is absent from the file unless the Cluster names it.
func TestTheOperatorRendersOnlyWhatTheSpecNames(t *testing.T) {
	c := pluginCluster(map[string]string{"tables": "public.events"}, advertised(t))
	c.Spec.PostgresConfiguration.Parameters = map[string]string{"sync_replication_slots": "on"}
	conf := renderedConf(t, c)
	if !strings.Contains(conf, "sync_replication_slots = 'on'") {
		t.Fatalf("the spec's setting is not in the rendered file:\n%s", conf)
	}
	if strings.Contains(conf, "hot_standby_feedback") || strings.Contains(conf, "duckdb.allowed_directories") {
		t.Errorf("a setting the plugin no longer writes appears in the file:\n%s", conf)
	}
}

// renderedConf renders the operator's postgresql.conf for a Cluster, the way the
// instance manager does, with no plugin in the loop.
func renderedConf(t *testing.T, cluster *cnpgv1.Cluster) string {
	t.Helper()
	info := pgconf.ConfigurationInfo{
		Settings:           pgconf.CnpgConfigurationSettings,
		MajorVersion:       17,
		UserSettings:       cluster.Spec.PostgresConfiguration.Parameters,
		IncludingMandatory: true,
	}
	conf := pgconf.CreatePostgresqlConfiguration(info)
	file, _ := pgconf.CreatePostgresqlConfFile(conf)
	return file
}

func pluginStatusOf(t *testing.T, c *cnpgv1.Cluster) qsplugin.Status {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	res, err := qsplugin.OperatorImpl{}.SetStatusInCluster(context.Background(),
		&operator.SetStatusInClusterRequest{Cluster: raw})
	if err != nil {
		t.Fatal(err)
	}
	var st qsplugin.Status
	if err := json.Unmarshal(res.GetJsonStatus(), &st); err != nil {
		t.Fatal(err)
	}
	return st
}

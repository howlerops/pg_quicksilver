package fidelity

// What the plugin's routing settings look like in postgresql.conf, as the
// CloudNativePG 1.30 operator renders them. EnrichConfiguration is the one
// place the plugin can write a PostgreSQL parameter, and the operator takes its
// map as the final configuration (pkg/postgres/plugin/config.go): these tests
// run the plugin's output through the operator's own CreatePostgresqlConfFile,
// so what is asserted is the file a PostgreSQL instance reads.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	cnpgv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	pgconf "github.com/cloudnative-pg/cloudnative-pg/pkg/postgres"
	"github.com/cloudnative-pg/cnpg-i/pkg/postgres"

	qsplugin "github.com/howlerops/pg_quicksilver/go/internal/plugin"
)

// renderedConf enriches the operator's configuration for a Cluster through the
// plugin, then renders the file the way the instance manager does.
func renderedConf(t *testing.T, cluster *cnpgv1.Cluster, userSettings map[string]string) string {
	t.Helper()
	def, err := json.Marshal(cluster)
	if err != nil {
		t.Fatal(err)
	}
	info := pgconf.ConfigurationInfo{
		Settings:           pgconf.CnpgConfigurationSettings,
		MajorVersion:       17,
		UserSettings:       userSettings,
		IncludingMandatory: true,
	}
	base := pgconf.CreatePostgresqlConfiguration(info).GetConfigurationParameters()
	res, err := qsplugin.PostgresImpl{}.EnrichConfiguration(context.Background(),
		&postgres.EnrichConfigurationRequest{
			ClusterDefinition: def,
			Configs:           base,
			OperationType:     &postgres.OperationType{Type: postgres.OperationType_TYPE_RECONCILE},
		})
	if err != nil {
		t.Fatal(err)
	}
	conf := pgconf.CreatePostgresqlConfiguration(info)
	conf.SetConfigurationParameters(res.GetConfigs())
	file, _ := pgconf.CreatePostgresqlConfFile(conf)
	return file
}

func routedCluster(params map[string]string) *cnpgv1.Cluster {
	enabled := true
	c := &cnpgv1.Cluster{}
	c.Name = "app"
	c.Namespace = "ns"
	c.Spec.Instances = 3
	c.Spec.ImageName = "ghcr.io/howlerops/pg_quicksilver-postgres:17-0.0.4"
	c.Spec.Plugins = []cnpgv1.PluginConfiguration{{
		Name: qsplugin.PluginName, Enabled: &enabled, Parameters: params,
	}}
	return c
}

// The line the operator writes for a plugin-derived value, with the trailing
// slash that the routing check (docs/40) requires.
func TestRoutingDirectoryReachesPostgresqlConf(t *testing.T) {
	c := routedCluster(map[string]string{"tables": "public.events", "ducklake": "true"})
	conf := renderedConf(t, c, nil)
	want := "duckdb.allowed_directories = '/var/lib/postgresql/data/quicksilver/'"
	if !strings.Contains(conf, want) {
		t.Fatalf("the rendered postgresql.conf does not carry %q; the operator dropped or "+
			"rewrote the plugin's parameter:\n%s", want, conf)
	}
}

// A value the user set reaches the file unchanged, and nothing the plugin
// derives replaces it.
func TestUserAllowedDirectoriesReachesPostgresqlConfUnchanged(t *testing.T) {
	c := routedCluster(map[string]string{"tables": "public.events", "ducklake": "true"})
	c.Spec.PostgresConfiguration.Parameters = map[string]string{
		"duckdb.allowed_directories": "/var/lib/postgresql/data/",
	}
	conf := renderedConf(t, c, c.Spec.PostgresConfiguration.Parameters)
	if !strings.Contains(conf, "duckdb.allowed_directories = '/var/lib/postgresql/data/'") {
		t.Fatalf("the user's value is not the one in the file:\n%s", conf)
	}
	if strings.Contains(conf, "quicksilver/'") {
		t.Error("the plugin's derived value was written next to the user's")
	}
}

// Without the lake the file has no routing line at all.
func TestNoRoutingLineWithoutTheLake(t *testing.T) {
	c := routedCluster(map[string]string{"tables": "public.events"})
	if conf := renderedConf(t, c, nil); strings.Contains(conf, "duckdb.allowed_directories") {
		t.Errorf("duckdb.allowed_directories written with ducklake off:\n%s", conf)
	}
}

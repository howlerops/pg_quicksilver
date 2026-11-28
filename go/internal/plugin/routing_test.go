package plugin

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
	"github.com/cloudnative-pg/cnpg-i/pkg/postgres"
)

// enrich runs EnrichConfiguration on a Cluster with the operator's own values.
func enrich(t *testing.T, c *apiv1.Cluster, configs map[string]string) map[string]string {
	t.Helper()
	res, err := PostgresImpl{}.EnrichConfiguration(context.Background(),
		&postgres.EnrichConfigurationRequest{
			ClusterDefinition: clusterJSON(t, c),
			Configs:           configs,
			OperationType:     &postgres.OperationType{Type: postgres.OperationType_TYPE_RECONCILE},
		})
	if err != nil {
		t.Fatal(err)
	}
	return res.GetConfigs()
}

func routedParams() map[string]string {
	return map[string]string{"tables": "public.events", "ducklake": "true"}
}

func TestRoutingDerivesAllowedDirectoriesFromMirrorPath(t *testing.T) {
	got := enrich(t, newCluster(routedParams()), map[string]string{})
	if got["duckdb.allowed_directories"] != "/var/lib/postgresql/data/quicksilver/" {
		t.Errorf("duckdb.allowed_directories = %q, want the mirror directory with a trailing slash",
			got["duckdb.allowed_directories"])
	}

	p := routedParams()
	p["mirrorPath"] = "/data/mirror/"
	got = enrich(t, newCluster(p), map[string]string{})
	if got["duckdb.allowed_directories"] != "/data/mirror/" {
		t.Errorf("a custom mirrorPath gave %q, want /data/mirror/", got["duckdb.allowed_directories"])
	}
}

// Nothing is derived without a lake, when the extension is off, or with no
// mirror: the setting would be a directory nothing reads, and an extension
// that is switched off must not touch PostgreSQL.
func TestRoutingDerivesNothingWithoutALake(t *testing.T) {
	cases := map[string]map[string]string{
		"no ducklake":   {"tables": "public.events"},
		"extension off": {"tables": "public.events", "ducklake": "true", "extension": "off"},
		"mode off":      {"mode": "off", "ducklake": "true"},
	}
	for name, p := range cases {
		got := enrich(t, newCluster(p), map[string]string{})
		if _, ok := got["duckdb.allowed_directories"]; ok {
			t.Errorf("%s: duckdb.allowed_directories = %q, want it unset", name, got["duckdb.allowed_directories"])
		}
	}
}

// A value the user set is the user's. Enrichment must not replace it, even one
// that does not name the mirror: Validate reports that, and the operator's
// settings are the user's to change.
func TestRoutingNeverOverridesAUserValue(t *testing.T) {
	c := newCluster(routedParams())
	c.Spec.PostgresConfiguration.Parameters = map[string]string{
		"duckdb.allowed_directories": "/elsewhere/",
	}
	got := enrich(t, c, map[string]string{"duckdb.allowed_directories": "/elsewhere/"})
	if got["duckdb.allowed_directories"] != "/elsewhere/" {
		t.Errorf("a user value was replaced: %q", got["duckdb.allowed_directories"])
	}
}

func TestAllowedDirectoriesCover(t *testing.T) {
	cases := []struct {
		value, dir string
		want       bool
	}{
		{"/var/lib/q/", "/var/lib/q", true},
		{"/var/lib/q", "/var/lib/q/", true},
		{"/var/lib", "/var/lib/q", true},
		{"/var/lib/quicksilver", "/var/lib/q", false},
		{"/var/lib/q/other", "/var/lib/q", false},
		{"/tmp, /var/lib/q/", "/var/lib/q", true},
		{"/", "/var/lib/q", true},
		{"", "/var/lib/q", false},
	}
	for _, c := range cases {
		if got := allowedDirectoriesCover(c.value, c.dir); got != c.want {
			t.Errorf("allowedDirectoriesCover(%q, %q) = %v, want %v", c.value, c.dir, got, c.want)
		}
	}
}

func TestUserAllowedDirectoriesThatMissTheMirrorIsAWarningNotAnError(t *testing.T) {
	c := newCluster(routedParams())
	c.Spec.PostgresConfiguration.Parameters = map[string]string{"duckdb.allowed_directories": "/elsewhere/"}

	// The mirror must not depend on routing: no validation error, so the
	// instance Pods are still built with the mirror.
	for _, e := range Validate(c, PluginParameters(c)) {
		t.Errorf("a routing setting made the configuration invalid: %s", e.GetMessage())
	}

	ws := routingWarnings(c, ParseConfig(c.Name, PluginParameters(c)))
	found := false
	for _, w := range ws {
		if w.Parameter == "duckdb.allowed_directories" {
			found = true
			if !strings.Contains(w.Message, "/var/lib/postgresql/data/quicksilver/") ||
				!strings.Contains(w.Message, "mirror itself is unaffected") {
				t.Errorf("the warning does not say what is wrong and what is not: %s", w.Message)
			}
		}
	}
	if !found {
		t.Fatalf("a user allowed_directories that misses the mirror was not warned about: %v", ws)
	}

	// One that covers the mirror is fine.
	c.Spec.PostgresConfiguration.Parameters["duckdb.allowed_directories"] = "/var/lib/postgresql/data/"
	for _, w := range routingWarnings(c, ParseConfig(c.Name, PluginParameters(c))) {
		if w.Parameter == "duckdb.allowed_directories" {
			t.Errorf("a covering value was warned about: %s", w.Message)
		}
	}

	// Without a lake there is nothing to route, so nothing to warn about.
	off := newCluster(map[string]string{"tables": "public.events"})
	off.Spec.PostgresConfiguration.Parameters = map[string]string{"duckdb.allowed_directories": "/elsewhere/"}
	if ws := routingWarnings(off, ParseConfig(off.Name, PluginParameters(off))); len(ws) != 0 {
		t.Errorf("without a lake the setting is not routing's business, got %v", ws)
	}
}

func TestSidecarImageWithAMutableTagIsWarnedNotRefused(t *testing.T) {
	cases := map[string]bool{ // image -> warned
		"ghcr.io/howlerops/pg_quicksilver-mirror:latest":          true,
		"ghcr.io/howlerops/pg_quicksilver-mirror:dev":             true,
		"ghcr.io/howlerops/pg_quicksilver-mirror":                 true,
		"localhost:5000/mirror":                                   true,
		"ghcr.io/howlerops/pg_quicksilver-mirror:0.0.4":           false,
		"ghcr.io/howlerops/pg_quicksilver-mirror:v0.0.4":          false,
		"ghcr.io/howlerops/pg_quicksilver-mirror:latest@sha256:0": false,
		"ghcr.io/howlerops/pg_quicksilver-mirror@sha256:abcdef":   false,
	}
	for image, warned := range cases {
		p := routedParams()
		p["sidecarImage"] = image
		c := newCluster(p)
		if errs := Validate(c, p); len(errs) != 0 {
			t.Errorf("%s: Validate refused it (%v); a mutable tag must not block", image, errs)
		}
		got := sidecarImageWarnings(ParseConfig(c.Name, p))
		if (len(got) == 1) != warned {
			t.Errorf("%s: warned=%v, want %v (%v)", image, len(got) == 1, warned, got)
		}
	}
}

func TestRoutingWarningsNameWhatIsMissing(t *testing.T) {
	c := newCluster(routedParams())
	ws := routingWarnings(c, ParseConfig(c.Name, PluginParameters(c)))
	if len(ws) != 2 {
		t.Fatalf("no preload and no extension directory should give two warnings, got %v", ws)
	}

	c.Spec.PostgresConfiguration.AdditionalLibraries = []string{"pg_quicksilver", "pg_duckdb"}
	c.Spec.PostgresConfiguration.Parameters = map[string]string{"duckdb.extension_directory": "/usr/lib/x"}
	ws = routingWarnings(c, ParseConfig(c.Name, PluginParameters(c)))
	if len(ws) != 1 || !strings.Contains(ws[0].Message, "must come before") {
		t.Fatalf("the wrong order should be the one warning, got %v", ws)
	}

	c.Spec.PostgresConfiguration.AdditionalLibraries = []string{"pg_duckdb", "pg_quicksilver"}
	if ws = routingWarnings(c, ParseConfig(c.Name, PluginParameters(c))); len(ws) != 0 {
		t.Errorf("a complete configuration has no warnings, got %v", ws)
	}

	// Routing off: none of this applies.
	off := newCluster(map[string]string{"tables": "public.events"})
	if ws = routingWarnings(off, ParseConfig(off.Name, PluginParameters(off))); len(ws) != 0 {
		t.Errorf("without a lake there is nothing to warn about, got %v", ws)
	}
}

// The status carries the warnings whether or not the configuration is valid,
// and a valid configuration with a mutable tag is still valid.
func TestStatusPublishesWarningsAlongsideValidState(t *testing.T) {
	p := routedParams()
	p["sidecarImage"] = "ghcr.io/howlerops/pg_quicksilver-mirror:latest"
	c := newCluster(p)
	c.Spec.PostgresConfiguration.AdditionalLibraries = []string{"pg_duckdb", "pg_quicksilver"}
	c.Spec.PostgresConfiguration.Parameters = map[string]string{"duckdb.extension_directory": "/usr/lib/x"}
	res, err := OperatorImpl{}.SetStatusInCluster(context.Background(),
		&operator.SetStatusInClusterRequest{Cluster: clusterJSON(t, c)})
	if err != nil {
		t.Fatal(err)
	}
	var st Status
	if err := json.Unmarshal(res.GetJsonStatus(), &st); err != nil {
		t.Fatal(err)
	}
	if st.State != StateValid {
		t.Errorf("state = %q, want valid", st.State)
	}
	if len(st.ConfigWarnings) != 1 || st.ConfigWarnings[0].Parameter != "sidecarImage" {
		t.Errorf("configWarnings = %+v, want the one sidecarImage warning", st.ConfigWarnings)
	}
}

// Each mirroring standby is a logical and a physical walsender, so the plugin's
// walsender count must cover two per standby, not one.
func TestWalSendersCoverTwoPerStandby(t *testing.T) {
	for _, n := range []int{3, 7, 8} {
		c := newCluster(map[string]string{"tables": "public.events"})
		c.Spec.Instances = n
		got := enrich(t, c, map[string]string{})
		want := 2*n + slotHeadroom
		if got["max_wal_senders"] != strconv.Itoa(want) {
			t.Errorf("%d instances: max_wal_senders = %q, want %d", n, got["max_wal_senders"], want)
		}
	}
}

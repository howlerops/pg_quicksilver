package plugin

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/identity"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
)

func routedParams() map[string]string {
	return map[string]string{"tables": "public.events", "ducklake": "true", "sidecarImage": "ghcr.io/howlerops/pg_quicksilver-mirror:0.0.4"}
}

// requiredCluster is a routed Cluster with every required setting in place.
func requiredCluster() *apiv1.Cluster {
	c := newCluster(routedParams())
	c.Spec.Instances = 3
	c.Spec.PostgresConfiguration.Parameters = map[string]string{
		"sync_replication_slots":     "on",
		"hot_standby_feedback":       "on",
		"max_replication_slots":      "32",
		"max_wal_senders":            "10",
		"duckdb.allowed_directories": "/var/lib/postgresql/data/quicksilver/",
		"duckdb.extension_directory": "/usr/lib/quicksilver/duckdb-extensions",
	}
	c.Spec.PostgresConfiguration.AdditionalLibraries = []string{"pg_duckdb", "pg_quicksilver"}
	return c
}

// warnedParams is the set of Parameter names in configWarnings.
func warnedParams(c *apiv1.Cluster) map[string]string {
	out := map[string]string{}
	for _, w := range configWarnings(c, ParseConfig(c.Name, PluginParameters(c))) {
		out[w.Parameter] = w.Message
	}
	return out
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

// A complete, required configuration has no warnings about the settings.
func TestRequiredSettingsCleanWhenPresent(t *testing.T) {
	if w := warnedParams(requiredCluster()); len(w) != 0 {
		t.Errorf("a complete configuration is warned about: %v", w)
	}
}

// Each required setting that is missing is named in the status, and one that is
// set to the wrong value too. The plugin writes none of them.
func TestRequiredSettingsMissingOrWrongAreReported(t *testing.T) {
	c := requiredCluster()
	c.Spec.PostgresConfiguration.Parameters["sync_replication_slots"] = "off"
	delete(c.Spec.PostgresConfiguration.Parameters, "hot_standby_feedback")
	delete(c.Spec.PostgresConfiguration.Parameters, "duckdb.allowed_directories")
	w := warnedParams(c)
	for _, name := range []string{"sync_replication_slots", "hot_standby_feedback", "duckdb.allowed_directories"} {
		if _, ok := w[name]; !ok {
			t.Errorf("%s is missing or wrong and was not reported: %v", name, w)
		}
	}
	if !strings.Contains(w["sync_replication_slots"], "must be \"on\"") {
		t.Errorf("the sync_replication_slots warning does not say what to set: %s", w["sync_replication_slots"])
	}
}

// Without the mirror's slot count, the cluster needs the operator default raised.
func TestMaxReplicationSlotsAbsentAboveTheDefaultIsReported(t *testing.T) {
	c := requiredCluster()
	c.Spec.Instances = 30 // needs 34 slots; PostgreSQL and CloudNativePG default to 32
	delete(c.Spec.PostgresConfiguration.Parameters, "max_replication_slots")
	if _, ok := warnedParams(c)["max_replication_slots"]; !ok {
		t.Error("30 instances with no max_replication_slots, above the default of 32, was not reported")
	}
}

// A max_replication_slots SET below what the mirror needs is an invalid
// configuration: the mirror cannot create its slots.
func TestMaxReplicationSlotsSetTooLowIsInvalid(t *testing.T) {
	c := requiredCluster()
	c.Spec.PostgresConfiguration.Parameters["max_replication_slots"] = "5"
	found := false
	for _, e := range Validate(c, PluginParameters(c)) {
		comps := e.GetPathComponents()
		if len(comps) > 0 && comps[len(comps)-1] == "max_replication_slots" {
			found = true
		}
	}
	if !found {
		t.Error("max_replication_slots below instances + 4 was not an error")
	}
}

// A user's duckdb.allowed_directories that misses the mirror is a routing
// warning, never a configuration error: the mirror does not depend on routing.
func TestUserAllowedDirectoriesThatMissTheMirrorIsAWarningNotAnError(t *testing.T) {
	c := requiredCluster()
	c.Spec.PostgresConfiguration.Parameters["duckdb.allowed_directories"] = "/elsewhere/"
	for _, e := range Validate(c, PluginParameters(c)) {
		t.Errorf("a routing setting made the configuration invalid: %s", e.GetMessage())
	}
	msg, ok := warnedParams(c)["duckdb.allowed_directories"]
	if !ok {
		t.Fatal("a user allowed_directories that misses the mirror was not warned about")
	}
	if !strings.Contains(msg, "/var/lib/postgresql/data/quicksilver/") || !strings.Contains(msg, "mirror itself is unaffected") {
		t.Errorf("the warning does not say what is wrong and what is not: %s", msg)
	}

	// Without a lake there is no routing to warn about.
	off := newCluster(map[string]string{"tables": "public.events"})
	off.Spec.PostgresConfiguration.Parameters = map[string]string{"duckdb.allowed_directories": "/elsewhere/"}
	if _, ok := warnedParams(off)["duckdb.allowed_directories"]; ok {
		t.Error("without a lake the setting is not routing's business, but it was warned about")
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
	if len(ws) != 3 {
		t.Fatalf("no preload, no extension directory and no allowed_directories should give three warnings, got %v", ws)
	}

	c.Spec.PostgresConfiguration.AdditionalLibraries = []string{"pg_quicksilver", "pg_duckdb"}
	c.Spec.PostgresConfiguration.Parameters = map[string]string{
		"duckdb.extension_directory": "/usr/lib/x",
		"duckdb.allowed_directories": "/var/lib/postgresql/data/quicksilver/",
	}
	ws = routingWarnings(c, ParseConfig(c.Name, PluginParameters(c)))
	if len(ws) != 1 || !strings.Contains(ws[0].Message, "must come before") {
		t.Fatalf("the wrong order should be the one warning, got %v", ws)
	}

	c.Spec.PostgresConfiguration.AdditionalLibraries = []string{"pg_duckdb", "pg_quicksilver"}
	if ws = routingWarnings(c, ParseConfig(c.Name, PluginParameters(c))); len(ws) != 0 {
		t.Errorf("a complete configuration has no routing warnings, got %v", ws)
	}

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
	sawSidecar := false
	for _, w := range st.ConfigWarnings {
		if w.Parameter == "sidecarImage" {
			sawSidecar = true
		}
	}
	if !sawSidecar {
		t.Errorf("configWarnings = %+v, want the sidecarImage warning", st.ConfigWarnings)
	}
}

// Each mirroring standby is a logical and a physical walsender, so the required
// walsender count is two per standby, not one. Nothing is written for it: it is
// reported when the spec does not carry it.
func TestWalSendersRequiredTwoPerStandby(t *testing.T) {
	for _, n := range []int{3, 7, 8} {
		c := requiredCluster()
		c.Spec.Instances = n
		c.Spec.PostgresConfiguration.Parameters["max_wal_senders"] = strconv.Itoa(2*n + slotHeadroom - 1)
		if _, ok := warnedParams(c)["max_wal_senders"]; !ok {
			t.Errorf("%d instances with max_wal_senders=%d (need %d) was not reported",
				n, 2*n+slotHeadroom-1, 2*n+slotHeadroom)
		}
		c.Spec.PostgresConfiguration.Parameters["max_wal_senders"] = strconv.Itoa(2*n + slotHeadroom)
		if _, ok := warnedParams(c)["max_wal_senders"]; ok {
			t.Errorf("%d instances with the required max_wal_senders was reported", n)
		}
	}
}

// The plugin must not advertise what the operator would then call. Neither the
// postgres service nor instance-side sidecar injection: the instance manager
// only dials plugins that advertise the latter, and in 1.27.0 a missing socket
// fails the whole instance reconcile, promotion included (identity.go).
func TestPluginDoesNotAdvertiseInstanceSideServices(t *testing.T) {
	res, err := IdentityImpl{}.GetPluginCapabilities(context.Background(),
		&identity.GetPluginCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range res.GetCapabilities() {
		switch c.GetService().GetType() {
		case identity.PluginCapability_Service_TYPE_POSTGRES:
			t.Error("the plugin advertises the postgres service, which the instance manager never calls")
		case identity.PluginCapability_Service_TYPE_INSTANCE_SIDECAR_INJECTION:
			t.Error("the plugin advertises instance sidecar injection, which puts it in every instance reconcile")
		}
	}
}

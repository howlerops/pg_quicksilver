package plugin

import (
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	corev1 "k8s.io/api/core/v1"
)

func envValue(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

// A switch the operator never set must not appear in the container at all, so
// the sidecar's own default stands and existing Pod specs do not change.
func TestUnsetSwitchesAreNotInTheContainer(t *testing.T) {
	c := MirrorSidecar("app", ParseConfig("app", okParams()))
	for _, name := range []string{"QS_SLOT_JANITOR", "QS_SLOT_JANITOR_AFTER",
		"QS_BLOOM_COLUMNS", "QS_CLUSTER_BY", "QS_NUMERIC_SCALE"} {
		if v, ok := envValue(c.Env, name); ok {
			t.Errorf("%s is %q in the container though the operator did not set it", name, v)
		}
	}
	// The boolean one is always stated, as QS_DUCKLAKE is.
	if v, ok := envValue(c.Env, "QS_AUTO_REBUILD_LEGACY"); !ok || v != "off" {
		t.Errorf("QS_AUTO_REBUILD_LEGACY = %q, %v; want off, stated", v, ok)
	}
}

// Each switch set on the plugin reaches the sidecar's environment under the
// name the sidecar reads.
func TestSwitchesReachTheSidecar(t *testing.T) {
	params := okParams()
	params["slotJanitor"] = "off"
	params["slotJanitorAfter"] = "2h"
	params["autoRebuildLegacy"] = "true"
	params["bloomColumns"] = "public.events=-;public.orders=customer_id"
	params["clusterBy"] = "public.orders=o_date,public.events=none"
	params["numericScale"] = "public.orders.amount=4,9"
	params["ducklake"] = "true"
	params["serveWithHaltedTables"] = "true"
	cfg := ParseConfig("app", params)
	c := MirrorSidecar("app", cfg)

	want := map[string]string{
		"QS_SLOT_JANITOR":             "off",
		"QS_SLOT_JANITOR_AFTER":       "2h0m0s",
		"QS_AUTO_REBUILD_LEGACY":      "on",
		"QS_BLOOM_COLUMNS":            "public.events=-;public.orders=customer_id",
		"QS_CLUSTER_BY":               "public.orders=o_date,public.events=none",
		"QS_NUMERIC_SCALE":            "public.orders.amount=4,9",
		"QS_DUCKLAKE":                 "on",
		"QS_SERVE_WITH_HALTED_TABLES": "on",
	}
	for name, value := range want {
		if got, ok := envValue(c.Env, name); !ok || got != value {
			t.Errorf("%s = %q (present %v), want %q", name, got, ok, value)
		}
	}
}

func TestSlotJanitorSpellingFollowsTheSidecar(t *testing.T) {
	// The sidecar turns the janitor off for these four and on for anything else.
	for in, want := range map[string]string{
		"off": "off", "0": "off", "false": "off", "no": "off",
		"on": "on", "1": "on", "true": "on", "yes": "on", "OFF": "off",
	} {
		if got := slotJanitorEnv(in); got != want {
			t.Errorf("slotJanitorEnv(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSwitchValidation(t *testing.T) {
	cases := []struct {
		name  string
		param string
		value string
		bad   bool
	}{
		{"janitor on", "slotJanitor", "on", false},
		{"janitor maybe", "slotJanitor", "maybe", true},
		{"janitor window", "slotJanitorAfter", "6h", false},
		{"janitor window too short", "slotJanitorAfter", "10s", true},
		{"janitor window not a duration", "slotJanitorAfter", "soon", true},
		{"auto rebuild true", "autoRebuildLegacy", "true", false},
		{"auto rebuild yes", "autoRebuildLegacy", "yes please", true},
		{"bloom off", "bloomColumns", "off", false},
		{"bloom per table", "bloomColumns", "public.orders=customer_id,sku;public.events=-", false},
		{"bloom without a table", "bloomColumns", "customer_id", true},
		{"cluster by column", "clusterBy", "public.orders=o_date", false},
		{"cluster by none", "clusterBy", "public.events=none", false},
		{"cluster by unqualified", "clusterBy", "orders=o_date", true},
		{"cluster by a column with a space", "clusterBy", "public.orders=o date", true},
		{"numeric scale alone", "numericScale", "9", false},
		{"numeric scale per column", "numericScale", "public.orders.amount=4,public.fx.rate=12", false},
		{"numeric scale above 38", "numericScale", "39", true},
		{"numeric scale not a number", "numericScale", "many", true},
		{"numeric column without a table", "numericScale", "amount=4", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			params := okParams()
			params[c.param] = c.value
			flagged := flaggedParams(newCluster(params))
			if c.bad && !flagged[c.param] {
				t.Errorf("%s=%q was accepted; refused parameters: %v", c.param, c.value, flagged)
			}
			if !c.bad && flagged[c.param] {
				t.Errorf("%s=%q was refused", c.param, c.value)
			}
		})
	}
}

// flaggedParams is the set of plugin parameters Validate refuses.
func flaggedParams(cluster *apiv1.Cluster) map[string]bool {
	out := map[string]bool{}
	for _, e := range Validate(cluster, PluginParameters(cluster)) {
		if pc := e.PathComponents; len(pc) > 0 {
			out[pc[len(pc)-1]] = true
		}
	}
	return out
}

func TestSwitchesAreKnownParameters(t *testing.T) {
	params := okParams()
	params["slotJanitor"] = "on"
	for _, e := range Validate(newCluster(params), PluginParameters(newCluster(params))) {
		if strings.Contains(e.Message, "unknown") {
			t.Errorf("a documented switch was rejected as unknown: %q", e.Message)
		}
	}
	if got := ParseConfig("app", params).SlotJanitor; got != "on" {
		t.Errorf("slotJanitor was not read: %q", got)
	}
	if d := ParseConfig("app", map[string]string{"slotJanitorAfter": "6h"}).SlotJanitorAfter; d != 6*time.Hour {
		t.Errorf("slotJanitorAfter was not read: %v", d)
	}
}

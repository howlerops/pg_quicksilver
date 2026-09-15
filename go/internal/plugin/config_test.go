package plugin

import (
	"strings"
	"testing"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newCluster builds a Cluster that passes validation, so each test can change
// exactly one thing and attribute the result to it.
func newCluster(params map[string]string) *apiv1.Cluster {
	return &apiv1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default"},
		Spec: apiv1.ClusterSpec{
			Instances: 3,
			ImageName: "ghcr.io/cloudnative-pg/postgresql:17.2-standard-bookworm",
			Plugins: []apiv1.PluginConfiguration{
				{Name: PluginName, Parameters: params},
			},
		},
	}
}

func okParams() map[string]string {
	return map[string]string{"tables": "public.events,public.orders"}
}

// messages joins the validation messages so a test can assert on substance
// rather than on ordering.
func messages(errs []interface{ Error() string }) string { return "" }

func joinErrs(t *testing.T, cluster *apiv1.Cluster) string {
	t.Helper()
	var sb strings.Builder
	for _, e := range Validate(cluster, PluginParameters(cluster)) {
		sb.WriteString(e.GetMessage())
		sb.WriteString(" | ")
	}
	return sb.String()
}

func TestDefaultsAreShadowAndLogical(t *testing.T) {
	cfg := ParseConfig("app", okParams())
	if cfg.Mode != ModeShadow {
		t.Errorf("default mode = %q, want shadow: the safe default must never be takeover", cfg.Mode)
	}
	if cfg.Ingest != IngestLogical {
		t.Errorf("default ingest = %q, want logical", cfg.Ingest)
	}
	if cfg.FreshnessSLO != DefaultFreshnessSLO {
		t.Errorf("default freshnessSLO = %v, want %v", cfg.FreshnessSLO, DefaultFreshnessSLO)
	}
	if cfg.SlotName != "quicksilver_app" || cfg.Publication != "quicksilver_app" {
		t.Errorf("derived names = %q/%q, want quicksilver_app", cfg.SlotName, cfg.Publication)
	}
}

func TestDerivedNamesAreSanitised(t *testing.T) {
	// Kubernetes object names allow hyphens; PostgreSQL slot names do not.
	cfg := ParseConfig("my-app-01", nil)
	if cfg.SlotName != "quicksilver_my_app_01" {
		t.Errorf("slot name = %q, want quicksilver_my_app_01", cfg.SlotName)
	}
	if errs := Validate(newCluster(map[string]string{"tables": "public.t"}), map[string]string{
		"tables": "public.t", "slotName": cfg.SlotName,
	}); len(errs) != 0 {
		t.Errorf("sanitised slot name rejected: %v", errs)
	}
}

func TestValidClusterPasses(t *testing.T) {
	if got := joinErrs(t, newCluster(okParams())); got != "" {
		t.Errorf("valid cluster rejected: %s", got)
	}
}

func TestTakeoverRequiresAcknowledgement(t *testing.T) {
	p := okParams()
	p["mode"] = "takeover"
	errs := Validate(newCluster(p), p)
	if len(errs) == 0 {
		t.Fatal("mode: takeover was accepted without acknowledgement; the measured " +
			"935x OLTP regression must not be reachable by a one-word edit")
	}
	if !strings.Contains(errs[0].GetMessage(), "935") {
		t.Errorf("rejection message does not quote the measured regression: %q", errs[0].GetMessage())
	}

	p[AcknowledgeParam] = "true"
	if errs := Validate(newCluster(p), p); len(errs) != 0 {
		t.Errorf("acknowledged takeover rejected: %v", errs)
	}
}

func TestLogicalIngestRequiresPG17(t *testing.T) {
	for _, tc := range []struct {
		name    string
		image   string
		major   int
		wantErr bool
	}{
		{name: "pg16 image", image: "ghcr.io/cloudnative-pg/postgresql:16.6", wantErr: true},
		{name: "pg17 image", image: "ghcr.io/cloudnative-pg/postgresql:17.2", wantErr: false},
		{name: "pg18 image", image: "ghcr.io/cloudnative-pg/postgresql:18.0", wantErr: false},
		{name: "catalog major 16", major: 16, wantErr: true},
		{name: "catalog major 17", major: 17, wantErr: false},
		// An image we cannot read must not be treated as too old. Refusing a
		// cluster because its tag is unusual is a worse failure than the risk.
		{name: "unreadable image", image: "internal/pg:stable", wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(okParams())
			c.Spec.ImageName = tc.image
			if tc.major > 0 {
				c.Spec.ImageName = ""
				c.Spec.ImageCatalogRef = &apiv1.ImageCatalogRef{Major: tc.major}
			}
			got := joinErrs(t, c)
			if tc.wantErr && !strings.Contains(got, "requires PostgreSQL") {
				t.Errorf("expected a PG version rejection, got %q", got)
			}
			if !tc.wantErr && strings.Contains(got, "requires PostgreSQL") {
				t.Errorf("unexpected PG version rejection: %q", got)
			}
		})
	}
}

func TestMajorFromImage(t *testing.T) {
	for _, tc := range []struct {
		image string
		want  int
		ok    bool
	}{
		{"ghcr.io/cloudnative-pg/postgresql:17.2-standard-bookworm", 17, true},
		{"ghcr.io/cloudnative-pg/postgresql:17", 17, true},
		{"postgres:16.1", 16, true},
		// a registry host with a port must not be mistaken for a tag
		{"localhost:5000/pg:17", 17, true},
		{"localhost:5000/pg", 0, false},
		{"ghcr.io/x/pg:17.2@sha256:abc", 17, true},
		{"ghcr.io/x/pg:latest", 0, false},
		{"", 0, false},
	} {
		got, ok := majorFromImage(tc.image)
		if got != tc.want || ok != tc.ok {
			t.Errorf("majorFromImage(%q) = (%d,%v), want (%d,%v)", tc.image, got, ok, tc.want, tc.ok)
		}
	}
}

func TestUnknownParametersAreRejected(t *testing.T) {
	p := okParams()
	p["tabels"] = "public.events"
	got := joinErrs(t, newCluster(p))
	if !strings.Contains(got, "unknown Quicksilver parameter") {
		t.Errorf("a misspelled parameter was silently ignored; that produces a mirror "+
			"that never mirrors anything with no symptom. got %q", got)
	}
}

func TestTablesAreRequiredUnlessOff(t *testing.T) {
	if got := joinErrs(t, newCluster(map[string]string{})); !strings.Contains(got, "at least one table") {
		t.Errorf("missing tables accepted: %q", got)
	}
	if got := joinErrs(t, newCluster(map[string]string{"mode": "off"})); got != "" {
		t.Errorf("mode: off should not require tables, got %q", got)
	}
}

func TestTableNameValidation(t *testing.T) {
	for _, tc := range []struct {
		table string
		ok    bool
	}{
		{"public.events", true},
		{"analytics.daily_rollup", true},
		{"events", false},              // unqualified
		{"public.events.extra", false}, // three parts
		{"Public.Events", false},       // needs quoting we do not implement
		{"public.\"weird name\"", false},
		{"public.orders; drop table x", false},
	} {
		p := map[string]string{"tables": tc.table}
		got := joinErrs(t, newCluster(p))
		bad := strings.Contains(got, "schema.table")
		if tc.ok && bad {
			t.Errorf("table %q rejected: %q", tc.table, got)
		}
		if !tc.ok && !bad {
			t.Errorf("table %q accepted, want rejection", tc.table)
		}
	}
}

func TestSingleInstanceClusterRejected(t *testing.T) {
	c := newCluster(okParams())
	c.Spec.Instances = 1
	if got := joinErrs(t, c); !strings.Contains(got, "single-instance") {
		t.Errorf("single-instance cluster accepted: %q", got)
	}
	c.Spec.Instances = 1
	c.Spec.Plugins[0].Parameters = map[string]string{"mode": "off"}
	if got := joinErrs(t, c); got != "" {
		t.Errorf("mode: off on a single instance should be fine, got %q", got)
	}
}

func TestConflictingWalLevelRejected(t *testing.T) {
	c := newCluster(okParams())
	c.Spec.PostgresConfiguration.Parameters = map[string]string{"wal_level": "replica"}
	got := joinErrs(t, c)
	if !strings.Contains(got, "wal_level=logical") {
		t.Errorf("a pinned wal_level=replica was accepted; the cluster would look "+
			"configured and never stream. got %q", got)
	}
	c.Spec.PostgresConfiguration.Parameters = map[string]string{"wal_level": "logical"}
	if got := joinErrs(t, c); got != "" {
		t.Errorf("an explicit wal_level=logical should be fine, got %q", got)
	}
}

func TestFreshnessSLOValidation(t *testing.T) {
	for raw, wantErr := range map[string]bool{
		"30s": false, "2m": false, "1s": false,
		"500ms": true, "": false, "soon": true, "-5s": true,
	} {
		p := okParams()
		if raw != "" {
			p["freshnessSLO"] = raw
		}
		got := joinErrs(t, newCluster(p))
		bad := strings.Contains(got, "freshnessSLO") || strings.Contains(got, "duration") ||
			strings.Contains(got, "at least 1s")
		if bad != wantErr {
			t.Errorf("freshnessSLO=%q: rejected=%v want %v (%q)", raw, bad, wantErr, got)
		}
	}
	if cfg := ParseConfig("app", map[string]string{"freshnessSLO": "90s"}); cfg.FreshnessSLO != 90*time.Second {
		t.Errorf("freshnessSLO not parsed: %v", cfg.FreshnessSLO)
	}
}

func TestPhysicalIngestIsRefusedNotIgnored(t *testing.T) {
	p := okParams()
	p["ingest"] = "physical"
	if got := joinErrs(t, newCluster(p)); !strings.Contains(got, "not implemented") {
		t.Errorf("ingest: physical should be refused while unimplemented, got %q", got)
	}
}

func TestImmutableParametersOnChange(t *testing.T) {
	oldC := newCluster(map[string]string{"tables": "public.events", "ingest": "logical"})
	newC := newCluster(map[string]string{"tables": "public.events", "mirrorPath": "/data/elsewhere"})
	errs := ValidateChange(oldC, newC)
	found := false
	for _, e := range errs {
		if strings.Contains(e.GetMessage(), "mirrorPath cannot be changed") {
			found = true
		}
	}
	if !found {
		t.Errorf("mirrorPath change accepted; the old mirror would be orphaned on disk. got %v", errs)
	}

	// changing the table list IS allowed: a new table bootstraps by snapshot.
	newC2 := newCluster(map[string]string{"tables": "public.events,public.orders"})
	for _, e := range ValidateChange(oldC, newC2) {
		t.Errorf("adding a table was rejected: %s", e.GetMessage())
	}
}

func TestRemovingThePluginIsAllowed(t *testing.T) {
	oldC := newCluster(okParams())
	newC := newCluster(okParams())
	newC.Spec.Plugins = nil
	if Enabled(newC) {
		t.Fatal("Enabled reported true for a cluster with no plugins")
	}
	_ = oldC
}

func TestExplicitlyDisabledPluginIsNotEnabled(t *testing.T) {
	c := newCluster(okParams())
	no := false
	c.Spec.Plugins[0].Enabled = &no
	if Enabled(c) {
		t.Error("a plugin with enabled: false was reported as enabled")
	}
}

var _ = messages

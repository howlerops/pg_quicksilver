package plugin

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func envOf(c *corev1.Container) map[string]string {
	out := map[string]string{}
	for _, e := range c.Env {
		out[e.Name] = e.Value
	}
	return out
}

// The settings that decide whether pg_quicksilver has anything to route to
// have to reach the container, and in the spelling the sidecar reads. The
// sidecar treats anything it does not recognise as off, so a parameter that
// was parsed and not passed on is a lake that is never built, with no error.
func TestLakeAndExtensionSettingsReachTheSidecar(t *testing.T) {
	def := envOf(MirrorSidecar("c", ParseConfig("c", map[string]string{"tables": "public.m"})))
	for name, want := range map[string]string{
		"QS_DUCKLAKE": "off", "QS_SERVE_WITH_HALTED_TABLES": "off", "QS_EXTENSION": "auto",
	} {
		if got, ok := def[name]; !ok || got != want {
			t.Errorf("by default %s is %q (set: %v), want %q", name, got, ok, want)
		}
	}
	if _, ok := def["QS_DUCKLAKE_RETENTION"]; ok {
		t.Error("QS_DUCKLAKE_RETENTION is set although nobody asked: the sidecar's own default should apply")
	}

	on := envOf(MirrorSidecar("c", ParseConfig("c", map[string]string{
		"tables": "public.m", "ducklake": "true", "ducklakeRetention": "10m",
		"serveWithHaltedTables": "TRUE", "extension": "Off",
	})))
	for name, want := range map[string]string{
		"QS_DUCKLAKE": "on", "QS_DUCKLAKE_RETENTION": "10m0s",
		"QS_SERVE_WITH_HALTED_TABLES": "on", "QS_EXTENSION": "off",
	} {
		if on[name] != want {
			t.Errorf("%s reached the container as %q, want %q", name, on[name], want)
		}
	}

	// A retention without a lake is refused by Validate; if it gets here
	// anyway (an already-admitted Cluster) it must not be passed on.
	stray := envOf(MirrorSidecar("c", ParseConfig("c", map[string]string{"tables": "public.m", "ducklakeRetention": "10m"})))
	if _, ok := stray["QS_DUCKLAKE_RETENTION"]; ok {
		t.Error("QS_DUCKLAKE_RETENTION was passed on with ducklake off")
	}
	// And a value Validate would refuse must still give a Pod its sidecar.
	odd := envOf(MirrorSidecar("c", ParseConfig("c", map[string]string{"tables": "public.m", "extension": "sideways"})))
	if odd["QS_EXTENSION"] != "auto" {
		t.Errorf("an unknown extension setting reached the container as %q, want auto", odd["QS_EXTENSION"])
	}
}

func TestLakeAndExtensionSettingsAreValidated(t *testing.T) {
	base := func(extra map[string]string) map[string]string {
		p := map[string]string{"tables": "public.m"}
		for k, v := range extra {
			p[k] = v
		}
		return p
	}
	good := []map[string]string{
		{"ducklake": "true"},
		{"ducklake": "false"},
		{"ducklake": "true", "ducklakeRetention": "5m"},
		{"serveWithHaltedTables": "true"},
		{"extension": "auto"},
		{"extension": "off"},
		{"sidecarMemory": "4Gi"},
	}
	for _, g := range good {
		if errs := Validate(newCluster(base(g)), base(g)); len(errs) != 0 {
			t.Errorf("%v was refused: %s", g, errs[0].Message)
		}
	}
	bad := []struct {
		params map[string]string
		param  string
		says   string
	}{
		{map[string]string{"ducklake": "on please"}, "ducklake", "true or false"},
		{map[string]string{"serveWithHaltedTables": "maybe"}, "serveWithHaltedTables", "true or false"},
		{map[string]string{"ducklake": "true", "ducklakeRetention": "soon"}, "ducklakeRetention", "duration"},
		{map[string]string{"ducklake": "true", "ducklakeRetention": "1s"}, "ducklakeRetention", "at least 10s"},
		{map[string]string{"ducklakeRetention": "5m"}, "ducklakeRetention", "no effect without ducklake"},
		{map[string]string{"extension": "on"}, "extension", "auto, off"},
		{map[string]string{"sidecarMemory": "lots"}, "sidecarMemory", "quantity"},
		{map[string]string{"sidecarMemory": "16Mi"}, "sidecarMemory", "at least 128Mi"},
	}
	for _, b := range bad {
		errs := Validate(newCluster(base(b.params)), base(b.params))
		found := false
		for _, e := range errs {
			if e.PathComponents[len(e.PathComponents)-1] == b.param && strings.Contains(e.Message, b.says) {
				found = true
			}
		}
		if !found {
			t.Errorf("%v was not refused on %s with a message containing %q (got %d errors)", b.params, b.param, b.says, len(errs))
		}
	}
}

// 256Mi was sized from the key index at readiness. The measured peak is
// during catch-up with a base rewrite: 3,040 MB at 10.5M rows, 290 bytes per
// row (bench/engine_compare/merged_build/out/p2_summary.txt). The default has
// to hold a few million rows through that, and the old one held under 900k.
func TestDefaultSidecarMemoryHoldsAMirrorThroughARewrite(t *testing.T) {
	const peakBytesPerRow = 3040 << 20 / 10_500_000 // measured, rounded down
	def := resource.MustParse(DefaultSidecarMemory)
	if rows := def.Value() / peakBytesPerRow; rows < 3_000_000 {
		t.Errorf("the default sidecar memory %s holds %d rows at the measured peak of %d bytes per row; "+
			"want at least 3M", DefaultSidecarMemory, rows, peakBytesPerRow)
	}
	res := sidecarResources(ParseConfig("c", map[string]string{"tables": "public.m"}))
	req, lim := res.Requests[corev1.ResourceMemory], res.Limits[corev1.ResourceMemory]
	if req.Cmp(def) != 0 || lim.Cmp(def) != 0 {
		t.Errorf("the default did not reach the container: request %s, limit %s, want %s", req.String(), lim.String(), def.String())
	}
}

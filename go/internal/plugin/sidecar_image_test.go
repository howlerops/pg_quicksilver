package plugin

// The chart must be able to pin the mirror image, and :latest must not be what
// a deployed plugin injects.
//
// values.yaml declared image.mirror from the beginning and no template used it.
// An operator who pinned the mirror version got nothing and every Cluster was
// injected with :latest — an image that changes under you, running as a sidecar
// next to a database. The chart advertised a version it could not deliver.

import (
	"os"
	"strings"
	"testing"
)

func TestChartCanPinTheSidecarImage(t *testing.T) {
	t.Setenv("QS_DEFAULT_SIDECAR_IMAGE", "ghcr.io/howlerops/pg_quicksilver-mirror:0.1.0")
	cfg := ParseConfig("c", map[string]string{"tables": "public.m"})
	if cfg.SidecarImage != "ghcr.io/howlerops/pg_quicksilver-mirror:0.1.0" {
		t.Errorf("the plugin ignored QS_DEFAULT_SIDECAR_IMAGE and used %q; the "+
			"chart's image.mirror would be dead configuration again", cfg.SidecarImage)
	}

	// A Cluster that names one still wins — the env var is a default, not a
	// policy, and a per-Cluster override is how a canary gets a newer sidecar.
	cfg = ParseConfig("c", map[string]string{"sidecarImage": "example.com/mine:9"})
	if cfg.SidecarImage != "example.com/mine:9" {
		t.Errorf("a Cluster's sidecarImage was overridden by the environment: %q",
			cfg.SidecarImage)
	}

	// And with nothing set at all, the fallback is what it says it is.
	_ = os.Unsetenv("QS_DEFAULT_SIDECAR_IMAGE")
	cfg = ParseConfig("c", nil)
	if cfg.SidecarImage != FallbackSidecarImage {
		t.Errorf("fallback is %q, want %q", cfg.SidecarImage, FallbackSidecarImage)
	}
	if !strings.HasSuffix(FallbackSidecarImage, ":latest") {
		t.Log("the fallback is no longer :latest; update the comment that " +
			"explains why the chart overrides it")
	}
}

package plugin

import (
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// The sidecar's memory is a function of ROW COUNT, which the plugin cannot see,
// so it has to be settable — and it has to be a limit as well as a request.
//
// The original spec asked for 256Mi and set no limit. That is Burstable QoS,
// and the Pod it makes an eviction candidate is the one running PostgreSQL: the
// mirror is an optimisation and the database is the database. It was also wrong
// by sevenfold at twenty million rows, where the measured peak is 1,854 MB.
func TestSidecarMemoryIsConfigurableAndGuaranteed(t *testing.T) {
	cfg := ParseConfig("c", map[string]string{"tables": "public.m"})
	if cfg.SidecarMemory != DefaultSidecarMemory {
		t.Fatalf("default sidecar memory is %q, want %q", cfg.SidecarMemory, DefaultSidecarMemory)
	}

	big := ParseConfig("c", map[string]string{"tables": "public.m", "sidecarMemory": "2Gi"})
	if big.SidecarMemory != "2Gi" {
		t.Fatalf("sidecarMemory was not read: %q", big.SidecarMemory)
	}

	res := sidecarResources(big)
	req, lim := res.Requests[corev1.ResourceMemory], res.Limits[corev1.ResourceMemory]
	if lim.IsZero() {
		t.Error("the sidecar has no memory LIMIT, so it is Burstable and can get " +
			"the PostgreSQL Pod evicted under node memory pressure")
	}
	if req.Cmp(lim) != 0 {
		t.Errorf("request %s != limit %s, so the sidecar is not Guaranteed QoS",
			req.String(), lim.String())
	}

	// GOMEMLIMIT must be below the limit, or it does not bound anything before
	// the kernel does; and not so far below that the collector thrashes. The
	// measured cliff is at ~1.33x the live heap and the free row is ~1.78x.
	got := goMemLimit(lim)
	want := int64(2 << 30 / 5 * 4)
	if got != strconv.FormatInt(want, 10) {
		t.Errorf("GOMEMLIMIT for a 2Gi limit is %s, want %d (80%%)", got, want)
	}

	// And it has to actually reach the container.
	c := MirrorSidecar("c", big)
	var found string
	for _, e := range c.Env {
		if e.Name == "GOMEMLIMIT" {
			found = e.Value
		}
	}
	if found != got {
		t.Errorf("GOMEMLIMIT reached the container as %q, want %q — without it "+
			"the sidecar is OOM-killed at its limit instead of slowing down", found, got)
	}

	// An unparseable value must not stop a Pod being admitted.
	bad := ParseConfig("c", map[string]string{"sidecarMemory": "not-a-quantity"})
	if m := sidecarMemory(bad); m.String() != DefaultSidecarMemory {
		t.Errorf("an unparseable sidecarMemory gave %s, want the default %s",
			m.String(), DefaultSidecarMemory)
	}
}

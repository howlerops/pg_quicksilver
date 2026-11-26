package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestJoinDomainSettingsDefaultOff(t *testing.T) {
	t.Setenv("QS_JOIN_DOMAINS", "")
	t.Setenv("QS_JOIN_DOMAIN_MAX_KEYS", "")
	specs, max, err := joinDomainSettings()
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 0 {
		t.Fatalf("unset QS_JOIN_DOMAINS gave %d domains", len(specs))
	}
	if max != defaultJoinDomainMaxKeys {
		t.Fatalf("default cap %d, want %d", max, defaultJoinDomainMaxKeys)
	}
}

// With no domain configured nothing is opened, nothing is attached, and no
// _domains directory appears under the mirror root.
func TestJoinDomainsOffCreatesNothing(t *testing.T) {
	root := t.TempDir()
	jd, err := openJoinDomains(quietLog(), root, nil, defaultJoinDomainMaxKeys)
	if err != nil {
		t.Fatal(err)
	}
	if jd != nil {
		t.Fatal("no domains configured, yet a JoinDomains was opened")
	}
	if _, err := os.Stat(filepath.Join(root, "_domains")); !os.IsNotExist(err) {
		t.Fatalf("_domains exists with the feature off: %v", err)
	}
	if got := joinDomainMetrics.Metrics(); got != "" {
		t.Fatalf("metrics with no domain open: %q", got)
	}
}

func TestJoinDomainSettingsRejects(t *testing.T) {
	for _, c := range []struct{ domains, max string }{
		{"cust=public.customers", ""},
		{"cust=public.customers.id", "0"},
		{"cust=public.customers.id", "-3"},
		{"cust=public.customers.id", "lots"},
	} {
		t.Setenv("QS_JOIN_DOMAINS", c.domains)
		t.Setenv("QS_JOIN_DOMAIN_MAX_KEYS", c.max)
		if _, _, err := joinDomainSettings(); err == nil {
			t.Errorf("domains=%q max=%q accepted", c.domains, c.max)
		}
	}
}

func TestJoinDomainSettingsAndMetrics(t *testing.T) {
	t.Setenv("QS_JOIN_DOMAINS", "cust=public.customers.id,public.orders.customer_id")
	t.Setenv("QS_JOIN_DOMAIN_MAX_KEYS", "100")
	specs, max, err := joinDomainSettings()
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 || len(specs[0].Members) != 2 || max != 100 {
		t.Fatalf("parsed %+v, cap %d", specs, max)
	}
	root := t.TempDir()
	jd, err := openJoinDomains(quietLog(), root, specs, max)
	if err != nil {
		t.Fatal(err)
	}
	defer jd.Close()
	out := joinDomainMetrics.Metrics()
	for _, want := range []string{
		`quicksilver_join_domain_keys{domain="cust"} 0`,
		`quicksilver_join_domain_disabled{domain="cust"} 0`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("metrics lack %q:\n%s", want, out)
		}
	}
	joinDomainMetrics.set(nil)
}

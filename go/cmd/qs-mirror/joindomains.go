package main

// Join domains, EXPERIMENTAL and off by default. QS_JOIN_DOMAINS names the
// domains (see mirror.ParseJoinDomains); unset or empty, the sidecar does
// exactly what it did before and creates nothing under _domains.
//
// QS_JOIN_DOMAIN_MAX_KEYS caps each domain's dictionary (default 5,000,000).
// A domain past its cap is disabled durably, its tables report it incomplete,
// and the mirror keeps running.

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/howlerops/pg_quicksilver/go/internal/mirror"
)

const defaultJoinDomainMaxKeys = 5000000

// joinDomainSettings reads QS_JOIN_DOMAINS and QS_JOIN_DOMAIN_MAX_KEYS. It is
// called once, in main, so a malformed value stops the sidecar at start.
func joinDomainSettings() ([]mirror.JoinDomainSpec, int64, error) {
	specs, err := mirror.ParseJoinDomains(os.Getenv("QS_JOIN_DOMAINS"))
	if err != nil {
		return nil, 0, fmt.Errorf("QS_JOIN_DOMAINS: %w", err)
	}
	maxKeys := int64(defaultJoinDomainMaxKeys)
	if v := strings.TrimSpace(os.Getenv("QS_JOIN_DOMAIN_MAX_KEYS")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			return nil, 0, fmt.Errorf("QS_JOIN_DOMAIN_MAX_KEYS=%q: want a positive integer", v)
		}
		maxKeys = n
	}
	return specs, maxKeys, nil
}

// openJoinDomains opens the configured domains under the mirror root, logging
// each one. It returns nil when none are configured.
func openJoinDomains(log *slog.Logger, mirrorPath string, specs []mirror.JoinDomainSpec, maxKeys int64) (*mirror.JoinDomains, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	jd, err := mirror.OpenJoinDomains(mirrorPath, specs, maxKeys)
	if err != nil {
		return nil, err
	}
	for _, s := range jd.Specs() {
		members := make([]string, 0, len(s.Members))
		for _, m := range s.Members {
			members = append(members, m.Table+"."+m.Column)
		}
		log.Info("join domain (experimental)", "domain", s.Name,
			"members", strings.Join(members, ","), "max_keys", maxKeys)
	}
	joinDomainMetrics.set(jd)
	return jd, nil
}

// joinDomainGauge is the domains currently open, for /metrics.
type joinDomainGauge struct {
	mu sync.Mutex
	jd *mirror.JoinDomains
}

// joinDomainMetrics is the one gauge the sidecar exposes.
var joinDomainMetrics = &joinDomainGauge{}

// joinDomainSpecs and joinDomainMax are read from the environment in main.
var (
	joinDomainSpecs []mirror.JoinDomainSpec
	joinDomainMax   int64
)

func (g *joinDomainGauge) set(jd *mirror.JoinDomains) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.jd = jd
}

// Metrics renders quicksilver_join_domain_keys and quicksilver_join_domain_disabled,
// one series per domain. Nothing is rendered when no domain is open.
func (g *joinDomainGauge) Metrics() string {
	g.mu.Lock()
	jd := g.jd
	g.mu.Unlock()
	st := jd.Status()
	if len(st) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("# HELP quicksilver_join_domain_keys Values with a dictionary id in each join domain (experimental).\n")
	b.WriteString("# TYPE quicksilver_join_domain_keys gauge\n")
	for _, s := range st {
		fmt.Fprintf(&b, "quicksilver_join_domain_keys{domain=%q} %d\n", s.Name, s.Keys)
	}
	b.WriteString("# HELP quicksilver_join_domain_disabled 1 for a join domain past its key cap, whose tables are incomplete (experimental).\n")
	b.WriteString("# TYPE quicksilver_join_domain_disabled gauge\n")
	for _, s := range st {
		v := 0
		if s.Disabled {
			v = 1
		}
		fmt.Fprintf(&b, "quicksilver_join_domain_disabled{domain=%q} %d\n", s.Name, v)
	}
	return b.String()
}

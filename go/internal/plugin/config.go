// Package plugin implements the CNPG-I services that make Quicksilver
// deployable: the operator-side validation, mutation and status hooks, the
// lifecycle hook that injects the mirror sidecar into instance Pods, and the
// EnrichConfiguration hook that turns on the PostgreSQL settings the mirror
// needs.
//
// Everything this package refuses to do is as important as what it does. Two
// measured findings from the study are encoded here as hard gates rather than
// documentation:
//
//   - A columnar mirror is median 935x SLOWER on OLTP-shaped queries
//     (docs/11). `mode: takeover` therefore cannot be enabled by a typo; it
//     requires a second, explicitly-named acknowledgement parameter.
//   - Logical slots do not survive failover before PostgreSQL 17 (docs/14,
//     docs/15). `ingest: logical` therefore requires PG >= 17, so that a
//     failover is a resume rather than a full rebuild.
package plugin

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	"github.com/cloudnative-pg/cnpg-i/pkg/operator"
)

// PluginName is the name a Cluster uses in .spec.plugins[].name.
const PluginName = "quicksilver.howlerops.io"

// MinimumMajorVersion is the oldest PostgreSQL we will run `ingest: logical`
// against. See docs/15: on 16 a failover loses the replication slot outright and
// the only recovery is an O(table size) re-snapshot, during which the mirror is
// behind and readiness-gated out of service.
const MinimumMajorVersion = 17

// Mode decides what the mirror is allowed to serve.
type Mode string

const (
	// ModeOff keeps the plugin registered but builds nothing. Useful for
	// staging a rollout, and the only safe state during an incident.
	ModeOff Mode = "off"
	// ModeShadow builds the mirror and exposes it on a separate `-olap`
	// endpoint. Nothing that exists today changes. This is the default and the
	// only mode Phase 1 recommends.
	ModeShadow Mode = "shadow"
	// ModeTakeover points the cluster's `-ro` service at the mirror. Measured
	// median 935x regression on OLTP-shaped queries; see AcknowledgeParam.
	ModeTakeover Mode = "takeover"
)

// Ingest selects how changes reach the mirror.
type Ingest string

const (
	// IngestLogical decodes pgoutput from a logical replication slot on the
	// primary. Implemented and measured.
	IngestLogical Ingest = "logical"
	// IngestPhysical decodes the physical WAL on a standby against that
	// standby's own heap (Architecture C, docs/03 path 3c). Not implemented.
	IngestPhysical Ingest = "physical"
)

// AcknowledgeParam is the parameter an operator must set to `true` alongside
// `mode: takeover`. It is deliberately long and deliberately not a boolean flag
// named "force": whoever types it should have read what it means.
const AcknowledgeParam = "acknowledgeOLTPRegression"

// Config is the parsed, validated form of .spec.plugins[].parameters.
type Config struct {
	Mode         Mode
	Ingest       Ingest
	Tables       []string
	FreshnessSLO time.Duration
	SidecarImage string
	SlotName     string
	Publication  string
	MirrorPath   string
	Acknowledged bool

	// Database is the one database whose tables are mirrored. CNPG's default
	// application database is "app". One mirror follows one database: a second
	// would need a second slot and a second decoder, and nothing in the design
	// shares work between them.
	Database string
	// CredentialsSecret names a Secret with `username` and `password` keys for
	// a role with REPLICATION, SELECT on the mirrored tables, and CREATE on the
	// database (for the publication and the DDL event trigger).
	CredentialsSecret string

	// SidecarMemory is the memory request AND limit for the mirror sidecar.
	//
	// This has to be settable, because the right value is a function of
	// something the plugin cannot see: how many ROWS the mirrored tables hold.
	// The sidecar's heap is dominated by one structure — the key index, one
	// entry per live row — and a profile against a 20.5M-row mirror put it at
	// 95.76% of the live heap:
	//
	//	862.64MB 95.76%  mirror.newKeyIndex
	//	 18.80MB  2.09%  mirror.forEachRowGroup
	//
	// That is 42 bytes per live row resident, and roughly twice that in RSS,
	// because Go's collector targets a heap around twice the live set by
	// default. Measured peak for that mirror: 1,854 MB.
	//
	// The old value — 256Mi, request only, no limit — is correct up to about
	// three million rows and wrong by sevenfold at twenty. Being wrong here is
	// not a sidecar problem: a container that requests far less than it uses is
	// Burstable, and the Pod it makes an eviction candidate is the one running
	// PostgreSQL.
	SidecarMemory string
}

// Defaults that apply when a parameter is absent.
const (
	DefaultFreshnessSLO = 30 * time.Second
	DefaultMirrorPath   = "/var/lib/postgresql/data/quicksilver"
	DefaultDatabase     = "app"
	// FallbackSidecarImage is used only when neither the Cluster nor the
	// plugin's own environment names one. It is :latest, which is the wrong
	// thing to run beside a database — an image that changes under you is not
	// a version — so both the chart and any sensible deployment override it.
	FallbackSidecarImage = "ghcr.io/howlerops/pg_quicksilver-mirror:latest"

	// DefaultSidecarMemory suits a mirror up to roughly three million rows.
	// Past that it is wrong, and see Config.SidecarMemory for the arithmetic:
	// budget about 90 bytes of RSS per live row across the mirrored tables.
	DefaultSidecarMemory = "256Mi"
)

// DefaultSidecarImage is the mirror image injected when a Cluster does not name
// one, read from the plugin's own environment so the CHART can pin it.
//
// charts/quicksilver/values.yaml has always declared `image.mirror`, and until
// this existed no template used it: an operator who pinned the mirror version
// got nothing, and every Cluster was injected with :latest regardless. The
// chart advertised a version it could not deliver.
//
// The plugin is the only thing that knows which image to inject, so the chart
// tells it once, on its own Deployment, rather than every Cluster repeating it.
func DefaultSidecarImage() string {
	if v := strings.TrimSpace(os.Getenv("QS_DEFAULT_SIDECAR_IMAGE")); v != "" {
		return v
	}
	return FallbackSidecarImage
}

var (
	// A qualified table name, conservatively: unquoted lowercase identifiers.
	// Quoted and mixed-case identifiers are a real gap, not an oversight — they
	// need the same quoting discipline everywhere in the pipeline, and refusing
	// them is better than mangling them.
	tablePattern = regexp.MustCompile(`^[a-z_][a-z0-9_$]*\.[a-z_][a-z0-9_$]*$`)
	slotPattern  = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	// Leading numeric component of an image tag: "17.2-standard-bookworm" -> 17.
	tagMajor = regexp.MustCompile(`^v?(\d+)`)
)

// ParseConfig turns raw plugin parameters into a Config, applying defaults.
// It does not validate; Validate does, so that callers that only need the
// defaults (the lifecycle hook on an already-admitted Cluster) do not have to
// handle validation errors they cannot act on.
func ParseConfig(clusterName string, params map[string]string) Config {
	get := func(k, def string) string {
		if v, ok := params[k]; ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}

	cfg := Config{
		Mode:         Mode(get("mode", string(ModeShadow))),
		Ingest:       Ingest(get("ingest", string(IngestLogical))),
		SidecarImage: get("sidecarImage", DefaultSidecarImage()),
		SlotName:     get("slotName", defaultObjectName(clusterName)),
		Publication:  get("publication", defaultObjectName(clusterName)),
		MirrorPath:   get("mirrorPath", DefaultMirrorPath),
		FreshnessSLO: DefaultFreshnessSLO,

		Database:          get("database", DefaultDatabase),
		CredentialsSecret: get("credentialsSecret", clusterName+"-superuser"),
		SidecarMemory:     get("sidecarMemory", DefaultSidecarMemory),
	}

	for _, t := range strings.Split(get("tables", ""), ",") {
		if t = strings.TrimSpace(t); t != "" {
			cfg.Tables = append(cfg.Tables, t)
		}
	}
	if d, err := time.ParseDuration(get("freshnessSLO", "")); err == nil {
		cfg.FreshnessSLO = d
	}
	if b, err := strconv.ParseBool(get(AcknowledgeParam, "false")); err == nil {
		cfg.Acknowledged = b
	}
	return cfg
}

func defaultObjectName(cluster string) string {
	s := strings.ToLower(cluster)
	s = regexp.MustCompile(`[^a-z0-9_]`).ReplaceAllString(s, "_")
	return "quicksilver_" + s
}

// Validate checks a Cluster and its Quicksilver parameters, returning one
// ValidationError per problem so the webhook can report them all at once.
func Validate(cluster *apiv1.Cluster, params map[string]string) []*operator.ValidationError {
	var errs []*operator.ValidationError
	add := func(param, value, msg string) {
		errs = append(errs, &operator.ValidationError{
			PathComponents: []string{"spec", "plugins", PluginName, param},
			Value:          value,
			Message:        msg,
		})
	}

	cfg := ParseConfig(cluster.Name, params)

	// unknown parameters are rejected rather than ignored: a silently-dropped
	// "tabels:" is a mirror that never mirrors anything, with no symptom.
	known := map[string]bool{
		"mode": true, "ingest": true, "tables": true, "freshnessSLO": true,
		"sidecarImage": true, "slotName": true, "publication": true,
		"mirrorPath": true, AcknowledgeParam: true,
		"database": true, "credentialsSecret": true, "sidecarMemory": true,
	}
	for k := range params {
		if !known[k] {
			add(k, params[k], "unknown Quicksilver parameter")
		}
	}

	switch cfg.Mode {
	case ModeOff, ModeShadow:
	case ModeTakeover:
		if !cfg.Acknowledged {
			add("mode", string(cfg.Mode), fmt.Sprintf(
				"mode: takeover points the cluster's -ro service at the columnar mirror, "+
					"which measured a median 935x slowdown (up to 3445x) on OLTP-shaped "+
					"queries such as indexed point lookups and LIMIT-ordered fetches. "+
					"Set %s: \"true\" to confirm this cluster's read traffic is analytical.",
				AcknowledgeParam))
		}
	default:
		add("mode", string(cfg.Mode), "must be one of: off, shadow, takeover")
	}

	switch cfg.Ingest {
	case IngestLogical:
		if major, known := MajorVersion(cluster); known && major < MinimumMajorVersion {
			add("ingest", string(cfg.Ingest), fmt.Sprintf(
				"ingest: logical requires PostgreSQL >= %d (cluster is %d). Before 17 a "+
					"logical replication slot does not survive a failover, and recovery is a "+
					"full re-snapshot of every mirrored table.",
				MinimumMajorVersion, major))
		}
	case IngestPhysical:
		add("ingest", string(cfg.Ingest),
			"ingest: physical is not implemented yet (Phase 3, Architecture C)")
	default:
		add("ingest", string(cfg.Ingest), "must be one of: logical, physical")
	}

	if cfg.Mode != ModeOff {
		if len(cfg.Tables) == 0 {
			add("tables", "", "at least one table is required unless mode is off")
		}
		if cluster.Spec.Instances < 2 {
			errs = append(errs, &operator.ValidationError{
				PathComponents: []string{"spec", "instances"},
				Value:          strconv.Itoa(int(cluster.Spec.Instances)),
				Message: "Quicksilver builds the mirror on replicas; a single-instance " +
					"cluster would run it alongside the primary and compete with OLTP traffic",
			})
		}
	}
	for _, t := range cfg.Tables {
		if !tablePattern.MatchString(t) {
			add("tables", t, "must be schema.table using unquoted lowercase identifiers")
		}
	}

	if raw, ok := params["freshnessSLO"]; ok && strings.TrimSpace(raw) != "" {
		d, err := time.ParseDuration(strings.TrimSpace(raw))
		switch {
		case err != nil:
			add("freshnessSLO", raw, "must be a Go duration, for example 30s or 2m")
		case d < time.Second:
			add("freshnessSLO", raw, "must be at least 1s")
		}
	}

	for param, v := range map[string]string{"slotName": cfg.SlotName, "publication": cfg.Publication} {
		if !slotPattern.MatchString(v) {
			add(param, v, "must match [a-z_][a-z0-9_]*")
		}
	}
	if !strings.HasPrefix(cfg.MirrorPath, "/") {
		add("mirrorPath", cfg.MirrorPath, "must be an absolute path")
	}
	if !slotPattern.MatchString(cfg.Database) {
		add("database", cfg.Database, "must match [a-z_][a-z0-9_]*")
	}
	if cfg.Mode != ModeOff && cfg.CredentialsSecret == "" {
		add("credentialsSecret", "",
			"a Secret with username/password keys is required for a role with "+
				"REPLICATION and SELECT on the mirrored tables")
	}

	// A wal_level the user pinned themselves wins over our EnrichConfiguration,
	// so a conflict has to be a rejection and not a silent override. Getting
	// this wrong produces a cluster that looks configured and never streams.
	if cfg.Ingest == IngestLogical && cfg.Mode != ModeOff {
		if wl, ok := cluster.Spec.PostgresConfiguration.Parameters["wal_level"]; ok && wl != "logical" {
			errs = append(errs, &operator.ValidationError{
				PathComponents: []string{"spec", "postgresql", "parameters", "wal_level"},
				Value:          wl,
				Message:        "ingest: logical requires wal_level=logical; remove this parameter and Quicksilver will set it",
			})
		}
	}

	return errs
}

// ValidateChange rejects edits that the running mirror cannot follow.
func ValidateChange(oldCluster, newCluster *apiv1.Cluster) []*operator.ValidationError {
	newParams := PluginParameters(newCluster)
	errs := Validate(newCluster, newParams)

	oldCfg := ParseConfig(oldCluster.Name, PluginParameters(oldCluster))
	newCfg := ParseConfig(newCluster.Name, newParams)

	immutable := []struct {
		param    string
		from, to string
		why      string
	}{
		{"ingest", string(oldCfg.Ingest), string(newCfg.Ingest),
			"the two paths build the mirror from different sources and cannot hand over mid-stream"},
		{"mirrorPath", oldCfg.MirrorPath, newCfg.MirrorPath,
			"the existing mirror would be orphaned on disk and silently rebuilt from scratch"},
		{"database", oldCfg.Database, newCfg.Database,
			"the mirror follows one database; pointing it at another means every mirrored table is a different table"},
	}
	for _, im := range immutable {
		if im.from != im.to {
			errs = append(errs, &operator.ValidationError{
				PathComponents: []string{"spec", "plugins", PluginName, im.param},
				Value:          im.to,
				Message: fmt.Sprintf("%s cannot be changed on an existing cluster (%s -> %s): %s",
					im.param, im.from, im.to, im.why),
			})
		}
	}
	return errs
}

// PluginParameters returns the Quicksilver parameters from a Cluster, or nil if
// the plugin is not configured on it.
func PluginParameters(cluster *apiv1.Cluster) map[string]string {
	for _, p := range cluster.Spec.Plugins {
		if p.Name == PluginName {
			return p.Parameters
		}
	}
	return nil
}

// Enabled reports whether the plugin is present and not explicitly disabled.
func Enabled(cluster *apiv1.Cluster) bool {
	for _, p := range cluster.Spec.Plugins {
		if p.Name != PluginName {
			continue
		}
		return p.Enabled == nil || *p.Enabled
	}
	return false
}

// MajorVersion resolves the cluster's PostgreSQL major version, and reports
// whether it could be determined at all. On a create the status is empty and
// only the spec is available; an unparseable custom image name yields
// (0, false), and callers must not treat that as "too old" — refusing a cluster
// because we could not read its tag would be worse than the risk.
func MajorVersion(cluster *apiv1.Cluster) (int, bool) {
	if info := cluster.Status.PGDataImageInfo; info != nil && info.MajorVersion > 0 {
		return info.MajorVersion, true
	}
	if ref := cluster.Spec.ImageCatalogRef; ref != nil && ref.Major > 0 {
		return ref.Major, true
	}
	for _, img := range []string{cluster.Spec.ImageName, cluster.Status.Image} {
		if m, ok := majorFromImage(img); ok {
			return m, true
		}
	}
	return 0, false
}

func majorFromImage(image string) (int, bool) {
	if image == "" {
		return 0, false
	}
	// Strip any digest, then take the last path element so that a registry
	// host:port ("localhost:5000/pg:17") is not mistaken for a tag.
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	last := image
	if i := strings.LastIndex(image, "/"); i >= 0 {
		last = image[i+1:]
	}
	i := strings.Index(last, ":")
	if i < 0 {
		return 0, false
	}
	m := tagMajor.FindStringSubmatch(last[i+1:])
	if m == nil {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n < 9 || n > 99 {
		return 0, false
	}
	return n, true
}

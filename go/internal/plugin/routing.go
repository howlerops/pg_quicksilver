package plugin

// The PostgreSQL settings that routing needs, and that the plugin can derive
// from its own configuration.
//
// Routing (docs/40) answers a read from the mirror inside PostgreSQL, through
// pg_duckdb. Four things have to agree, and the one that is a function of the
// Cluster's plugin parameters is the directory DuckDB may read: the lake's
// catalog lives under mirrorPath, so duckdb.allowed_directories must name
// mirrorPath with a trailing slash, or the lake is not attached on that node
// and the statement is answered from the heap. Without the lake (ducklake
// off) there is nothing to attach, so nothing is derived.
//
// What the plugin does NOT derive, and why:
//
//   - duckdb.postgres_role names the role whose members may run DuckDB
//     directly. Naming one is a security decision (docs/40 warns that a member
//     reads every mirrored table through DuckDB), and it needs a grant the
//     operator makes. It stays a hand-written line.
//   - duckdb.extension_directory is where the OPERAND IMAGE keeps DuckDB's
//     extensions. The plugin does not know the image's contents, and a
//     directory that does not exist would make pg_duckdb fetch the extensions
//     instead. It stays a hand-written line, reported when missing.
//   - shared_preload_libraries is managed by the operator from
//     spec.postgresql.shared_preload_libraries and is a fixed parameter. The
//     plugin checks its order and reports, and does not write it.
//   - quicksilver.attach_lakes_for_all is a statement that the DuckDB build is
//     the confined one. Only the image can make that statement.
//
// The derivation runs in EnrichConfiguration (postgres.go), which exists from
// CloudNativePG 1.27. On 1.26 the operator never calls it, and nothing here is
// applied; docs/16 says so.

import (
	"regexp"
	"strings"

	apiv1 "github.com/cloudnative-pg/api/pkg/api/v1"
)

const (
	gucDuckDBAllowedDirectories = "duckdb.allowed_directories"
	gucDuckDBExtensionDirectory = "duckdb.extension_directory"

	libPgDuckDB      = "pg_duckdb"
	libPgQuicksilver = "pg_quicksilver"
)

// routingEnabled reports whether this Cluster asks for routing to be possible:
// a lake to route to (ducklake), the extension not switched off, and a mirror
// at all.
func routingEnabled(cfg Config) bool {
	return cfg.DuckLake && cfg.Extension != ExtensionOff && cfg.Mode != ModeOff
}

// lakeDirectory is the value duckdb.allowed_directories takes for a mirror
// path: the directory itself, with a trailing slash.
func lakeDirectory(mirrorPath string) string {
	return strings.TrimRight(mirrorPath, "/") + "/"
}

// allowedDirectoriesCover reports whether a duckdb.allowed_directories value
// (a comma-separated list, as DuckDB reads it) contains dir or one of its
// parents. A trailing slash on either side is not significant; "/" covers
// everything.
func allowedDirectoriesCover(value, dir string) bool {
	want := strings.TrimRight(dir, "/")
	for _, raw := range strings.Split(value, ",") {
		entry := strings.TrimSpace(raw)
		if entry == "" {
			continue
		}
		if entry == "/" {
			return true
		}
		entry = strings.TrimRight(entry, "/")
		if want == entry || strings.HasPrefix(want, entry+"/") {
			return true
		}
	}
	return false
}

// addRoutingSettings sets the derivable settings in a configuration map that
// already holds the operator's values and the user's. A value the user set is
// left alone: Validate reports the ones that conflict with the mirror.
func addRoutingSettings(configs map[string]string, cfg Config) {
	if strings.TrimSpace(configs[gucDuckDBAllowedDirectories]) == "" {
		configs[gucDuckDBAllowedDirectories] = lakeDirectory(cfg.MirrorPath)
	}
}

// userAllowedDirectoriesConflict returns the user's duckdb.allowed_directories
// and whether it is set and does not cover the mirror, which is a
// configuration error.
func userAllowedDirectoriesConflict(cluster *apiv1.Cluster, cfg Config) (string, bool) {
	raw, ok := cluster.Spec.PostgresConfiguration.Parameters[gucDuckDBAllowedDirectories]
	if !ok || strings.TrimSpace(raw) == "" {
		return "", false
	}
	return raw, !allowedDirectoriesCover(raw, cfg.MirrorPath)
}

// routingWarnings reports what is missing for routing to work, without making
// the configuration invalid: the mirror itself is fine, only routing is not.
func routingWarnings(cluster *apiv1.Cluster, cfg Config) []ConfigError {
	if !routingEnabled(cfg) {
		return nil
	}
	var out []ConfigError
	libs := cluster.Spec.PostgresConfiguration.AdditionalLibraries
	duck, quick := indexOf(libs, libPgDuckDB), indexOf(libs, libPgQuicksilver)
	switch {
	case duck < 0 || quick < 0:
		out = append(out, ConfigError{
			Parameter: "spec.postgresql.shared_preload_libraries",
			Message: "routing needs pg_duckdb and pg_quicksilver preloaded, pg_duckdb first " +
				"(docs/40); without them the extension cannot route and reads come from the heap",
		})
	case duck > quick:
		out = append(out, ConfigError{
			Parameter: "spec.postgresql.shared_preload_libraries",
			Value:     strings.Join(libs, ","),
			Message:   "pg_duckdb must come before pg_quicksilver: pg_quicksilver calls into pg_duckdb when it loads",
		})
	}
	if strings.TrimSpace(cluster.Spec.PostgresConfiguration.Parameters[gucDuckDBExtensionDirectory]) == "" {
		out = append(out, ConfigError{
			Parameter: gucDuckDBExtensionDirectory,
			Message: "not set: unless the operand image carries DuckDB's extensions in a directory set here, " +
				"pg_duckdb fetches them into PGDATA the first time a backend routes (docs/16)",
		})
	}
	return out
}

// indexOf returns the position of name in libs, or -1.
func indexOf(libs []string, name string) int {
	for i, l := range libs {
		if strings.TrimSpace(l) == name {
			return i
		}
	}
	return -1
}

// versionTag is the spelling of a tag that names a release: a leading digit,
// optionally after a v. latest, dev, main and the like move.
var versionTag = regexp.MustCompile(`^v?\d`)

// sidecarImageWarnings reports a sidecar image that can change under a running
// Cluster. It is a warning, not an error: the default is :latest, and refusing
// it would stop every Cluster that has never named an image from getting a
// mirror. A digest is never mutable.
func sidecarImageWarnings(cfg Config) []ConfigError {
	if cfg.Mode == ModeOff || cfg.SidecarImage == "" || strings.Contains(cfg.SidecarImage, "@") {
		return nil
	}
	last := cfg.SidecarImage
	if i := strings.LastIndex(last, "/"); i >= 0 {
		last = last[i+1:]
	}
	tag := ""
	if i := strings.Index(last, ":"); i >= 0 {
		tag = last[i+1:]
	}
	if versionTag.MatchString(tag) {
		return nil
	}
	shown := tag
	if shown == "" {
		shown = "latest (no tag)"
	}
	return []ConfigError{{
		Parameter: "sidecarImage",
		Value:     cfg.SidecarImage,
		Message: "uses the mutable tag " + shown + ": pods of one cluster can run different builds " +
			"as it moves, and a restart can change the mirror under you. Pin a version tag or a digest",
	}}
}

// configWarnings is everything SetStatusInCluster reports that does not make
// the configuration invalid.
func configWarnings(cluster *apiv1.Cluster, cfg Config) []ConfigError {
	out := sidecarImageWarnings(cfg)
	return append(out, routingWarnings(cluster, cfg)...)
}

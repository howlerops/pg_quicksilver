package plugin

// The PostgreSQL settings routing needs in the Cluster's spec.postgresql.parameters.
//
// Routing (docs/40) answers a read from the mirror inside PostgreSQL, through
// pg_duckdb. The one that depends on this plugin's configuration is the
// directory DuckDB may read: the lake's catalog lives under mirrorPath, so
// duckdb.allowed_directories must name mirrorPath with a trailing slash, or the
// lake is not attached on that node and the statement is answered from the
// heap. With ducklake off there is no lake, so nothing is required.
//
// The plugin does not write this setting. It cannot: the operator does not ask
// a Deployment plugin for PostgreSQL parameters (postgres.go). So the Cluster
// names it, and the plugin reports it missing or pointing elsewhere
// (routingWarnings). The routing mistake is a warning: the mirror does not
// depend on routing, and must keep being built either way.
//
// What is never the plugin's to write, and why:
//
//   - duckdb.postgres_role names the role whose members may run DuckDB
//     directly. Naming one is a security decision (docs/40 warns that a member
//     reads every mirrored table through DuckDB), and it needs a grant the
//     operator makes.
//   - duckdb.extension_directory is where the OPERAND IMAGE keeps DuckDB's
//     extensions; the plugin does not know the image's contents.
//   - shared_preload_libraries is managed by the operator from
//     spec.postgresql.shared_preload_libraries. The plugin checks its order.
//   - quicksilver.attach_lakes_for_all is a statement that the DuckDB build is
//     the confined one. Only the image can make that statement.

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

// lakeDirectory is the value duckdb.allowed_directories must take for a mirror
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
	if strings.TrimSpace(cluster.Spec.PostgresConfiguration.Parameters[gucDuckDBAllowedDirectories]) == "" {
		out = append(out, ConfigError{
			Parameter: gucDuckDBAllowedDirectories,
			Message: "not set in spec.postgresql.parameters: routing cannot attach the lake without it. " +
				"Set it to " + lakeDirectory(cfg.MirrorPath) + " (the plugin does not write it; docs/16)",
		})
	}
	if raw, conflict := userAllowedDirectoriesConflict(cluster, cfg); conflict {
		out = append(out, ConfigError{
			Parameter: gucDuckDBAllowedDirectories,
			Value:     raw,
			Message: "does not include the mirror directory " + lakeDirectory(cfg.MirrorPath) +
				", so the lake is not attached on any node and routed reads come from the heap. " +
				"The mirror itself is unaffected. Add that directory to the list, or remove the parameter " +
				"and Quicksilver will set it",
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
	out = append(out, requiredSettings(cluster, cfg)...)
	return append(out, routingWarnings(cluster, cfg)...)
}

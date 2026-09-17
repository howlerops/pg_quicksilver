// Package pgtext pins how PostgreSQL renders values as text.
//
// The mirror stores what PostgreSQL hands it, and for every type the writer
// does not map to a native Arrow type — timestamps, dates, intervals, arrays,
// jsonb, bytea — what it hands over is TEXT. That text is not a property of the
// value. It is a property of the SESSION that rendered it:
//
//	TimeZone=UTC               2026-01-15 12:00:00+00
//	TimeZone=America/New_York  2026-01-15 07:00:00-05     the same instant
//	DateStyle=German,DMY       15.01.2026                 the same date
//
// Three different sessions render values into this mirror — the bootstrap
// snapshot, the walsender behind the replication stream, and the verifier
// comparing the two — and nothing made them agree. Today they happen to agree,
// because a default PostgreSQL is `Etc/UTC, ISO, MDY` and every session in the
// benchmarks inherits it. Change the server's timezone between bootstrap and
// streaming and the mirror holds one instant spelled two ways: a GROUP BY
// splits it into two groups, a join against it misses, and the verifier reports
// a divergence that is really a disagreement about spelling.
//
// So every connection the mirror opens pins the rendering. UTC because it is
// the only zone that is the same everywhere; ISO because it is the only
// DateStyle that sorts; extra_float_digits=3 because anything less loses bits
// of a float8 on the way through text.
//
// The point is not which spelling. It is that there is exactly one.
package pgtext

import (
	"net/url"
	"os"
	"strings"
)

// Pinned is turned off by QS_PIN_RENDERING=0, so that the bug this prevents can
// be demonstrated rather than asserted. See bench/scripts/rendering_fidelity.sh.
var Pinned = os.Getenv("QS_PIN_RENDERING") != "0"

// settings are applied to every connection, in the order libpq will see them.
//
// A value here must never change once mirrors exist: an existing mirror holds
// text rendered under the OLD setting, and the stream would start appending
// text rendered under the new one — which is the exact failure this package
// exists to prevent, caused by the fix for it.
var settings = []string{
	"TimeZone=UTC",
	"DateStyle=ISO,MDY",
	"IntervalStyle=postgres",
	"extra_float_digits=3",
	"bytea_output=hex",
	"client_encoding=UTF8",
}

// marker identifies a DSN this package has already pinned, so that pinning
// twice — which happens, because the replication dialler pins defensively on a
// DSN the caller has usually pinned already — does not append the settings
// again.
//
// It must be looked for in the DECODED options, never in the raw DSN: a URI
// carries the settings percent-encoded, so `TimeZone=UTC` is spelled
// `TimeZone%3DUTC` there and a raw substring check silently never matches.
// That is not hypothetical — it is what the first version of this did, and the
// idempotence test caught it doubling every setting.
const marker = "TimeZone=UTC"

// Options is the value of libpq's `options` parameter: the settings as -c flags.
func Options() string {
	parts := make([]string, 0, len(settings))
	for _, s := range settings {
		parts = append(parts, "-c "+s)
	}
	return strings.Join(parts, " ")
}

// PinDSN returns dsn with the rendering settings attached.
//
// Both DSN forms are handled because both reach this: the sidecar builds URIs,
// and an operator running qs-verify by hand may well paste a keyword/value
// string. Getting that wrong would silently leave the verifier unpinned, which
// is the one connection whose disagreement looks like a data bug.
func PinDSN(dsn string) string {
	if !Pinned || dsn == "" {
		return dsn
	}
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return dsn // not ours to repair; the driver will report it
		}
		q := u.Query()
		existing := q.Get("options")
		if strings.Contains(existing, marker) {
			return dsn
		}
		// Appending to any options the caller set, rather than replacing them:
		// a caller who asked for something specific keeps it, and ours win
		// because libpq applies -c flags left to right.
		q.Set("options", strings.TrimSpace(existing+" "+Options()))
		// url.Values.Encode spells a space as "+", and libpq's URI parser only
		// percent-decodes — so "-c+TimeZone=UTC" reaches the server as a
		// parameter literally named "+TimeZone" and every connection is refused
		// with "unrecognized configuration parameter". A literal plus in a value
		// would have been encoded as %2B, so every bare + here is a space.
		u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
		return u.String()
	}
	if strings.Contains(dsn, marker) {
		return dsn
	}
	// keyword/value form. The single quotes are libpq's, and are required
	// because the value contains spaces.
	return strings.TrimSpace(dsn) + " options='" + Options() + "'"
}

// OptionsOf returns the rendering settings a DSN carries, decoded. Used by the
// tests, and by anyone debugging a mirror that disagrees with its source about
// how a timestamp is spelled.
func OptionsOf(dsn string) string {
	if strings.Contains(dsn, "://") {
		if u, err := url.Parse(dsn); err == nil {
			return u.Query().Get("options")
		}
		return ""
	}
	i := strings.Index(dsn, "options='")
	if i < 0 {
		return ""
	}
	rest := dsn[i+len("options='"):]
	if j := strings.Index(rest, "'"); j >= 0 {
		return rest[:j]
	}
	return rest
}

package pgtext

import (
	"net/url"
	"strings"
	"testing"
)

// Both DSN forms reach this. The sidecar builds URIs; an operator running
// qs-verify by hand may paste a keyword/value string. Getting the second one
// wrong would silently leave the verifier unpinned, which is the one connection
// whose disagreement looks like a data bug rather than a configuration one.
func TestPinDSNHandlesBothForms(t *testing.T) {
	cases := map[string]string{
		"uri":                  "postgres://u@h:5432/db",
		"uri with a query":     "postgres://u@h:5432/db?sslmode=prefer",
		"uri with a socket":    "postgres://postgres@/db?host=/tmp&port=5444",
		"keyword/value":        "host=/tmp port=5444 dbname=db",
		"keyword/value spaced": "  host=/tmp dbname=db  ",
	}
	for name, dsn := range cases {
		t.Run(name, func(t *testing.T) {
			got := PinDSN(dsn)
			// Checked against the DECODED options: a URI spells these
			// percent-encoded, and a raw substring check passes or fails for
			// reasons that have nothing to do with the settings being there.
			opts := OptionsOf(got)
			if !strings.Contains(opts, "TimeZone=UTC") {
				t.Fatalf("%q was not pinned: %q", dsn, got)
			}
			for _, s := range settings {
				if !strings.Contains(opts, s) {
					t.Errorf("%q is missing from %q", s, opts)
				}
			}
			if strings.Contains(dsn, "://") {
				u, err := url.Parse(got)
				if err != nil {
					t.Fatalf("pinning produced an unparseable URI: %v", err)
				}
				// Whatever the caller already set has to survive, or pinning
				// would quietly drop an sslmode or a socket path.
				orig, _ := url.Parse(dsn)
				for k, v := range orig.Query() {
					if k == "options" {
						continue
					}
					if u.Query().Get(k) != v[0] {
						t.Errorf("pinning dropped %s=%s", k, v[0])
					}
				}
			}
		})
	}
}

// Pinning twice happens by design: the replication dialler pins defensively on
// a DSN the caller has usually pinned already. Two `options` parameters would
// mean one of them silently wins.
func TestPinDSNIsIdempotent(t *testing.T) {
	for _, dsn := range []string{
		"postgres://u@h:5432/db?sslmode=prefer",
		"host=/tmp dbname=db",
	} {
		once := PinDSN(dsn)
		twice := PinDSN(once)
		if once != twice {
			t.Errorf("pinning twice changed the DSN:\n  once:  %s\n  twice: %s", once, twice)
		}
		if n := strings.Count(OptionsOf(twice), "TimeZone=UTC"); n != 1 {
			t.Errorf("TimeZone appears %d times in %s", n, OptionsOf(twice))
		}
	}
}

// A caller's own options must be kept, and ours must come after them, because
// libpq applies -c flags left to right and the mirror's rendering is not
// negotiable.
func TestPinDSNKeepsCallerOptionsAndWins(t *testing.T) {
	got := PinDSN("postgres://u@h/db?options=-c%20statement_timeout%3D5000")
	u, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	_ = u
	opts := OptionsOf(got)
	if !strings.Contains(opts, "statement_timeout=5000") {
		t.Errorf("the caller's own option was dropped: %q", opts)
	}
	if strings.Index(opts, "statement_timeout") > strings.Index(opts, "TimeZone") {
		t.Errorf("ours must come last so they win: %q", opts)
	}
}

// libpq's URI parser percent-decodes and does NOT treat "+" as a space, so a
// DSN encoded the way Go's url.Values.Encode does it reaches the server as a
// parameter named "+TimeZone" and every connection is refused. This is the
// check that the encoding is libpq's, not Go's.
func TestPinDSNEncodesSpacesForLibpq(t *testing.T) {
	got := PinDSN("postgres://u@h/db")
	if strings.Contains(got, "+") {
		t.Errorf("a space was encoded as +, which libpq reads literally: %s", got)
	}
	if !strings.Contains(got, "%20") {
		t.Errorf("spaces are not percent-encoded: %s", got)
	}
	// ...and it still decodes back to the settings.
	if !strings.Contains(OptionsOf(got), "-c TimeZone=UTC") {
		t.Errorf("options do not decode back: %q", OptionsOf(got))
	}
}

func TestPinDSNLeavesEmptyAlone(t *testing.T) {
	if PinDSN("") != "" {
		t.Error("an empty DSN should stay empty rather than become an options string")
	}
}

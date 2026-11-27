package plugin

// The sidecar's own switches, passed through as plugin parameters.
//
// Each of these was documented in docs/16-deploying.md as an environment
// variable on the sidecar, and an operator who needed one had to patch the
// container by hand, which a reconcile undoes. Each now has a parameter, the
// same name in camelCase, validated at admission in the way the sidecar would
// otherwise have read it silently: a value it does not recognise is off, so a
// typo is refused here rather than run.
//
// The sidecar reads these at start, so changing one takes effect on the next
// Pod, like every other container setting.

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const (
	// minSlotJanitorAfter: a shorter window drops the slot of a Pod that is
	// only restarting, and the next start of that Pod is a full re-snapshot.
	minSlotJanitorAfter = time.Minute
	// maxNumericScale is PostgreSQL's limit for numeric(38, s).
	maxNumericScale = 38
)

var (
	// numericColumnPattern is schema.table.column, for QS_NUMERIC_SCALE.
	numericColumnPattern = regexp.MustCompile(`^[a-z_][a-z0-9_$]*\.[a-z_][a-z0-9_$]*\.[a-z_][a-z0-9_]*$`)
)

// slotJanitorValues are the spellings the sidecar reads; janitorOn in
// cmd/qs-mirror/janitor.go treats only the first four as off.
var slotJanitorValues = map[string]bool{
	"on": true, "off": true, "true": true, "false": true,
	"yes": true, "no": true, "1": true, "0": true,
}

// slotJanitorEnv is the value QS_SLOT_JANITOR takes for an operator's setting,
// spelled the way the sidecar's onOff is.
func slotJanitorEnv(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "off", "0", "false", "no":
		return "off"
	}
	return "on"
}

// validateSwitches checks the sidecar switch parameters, reporting through add.
func validateSwitches(params map[string]string, add func(param, value, msg string)) {
	get := func(k string) string { return strings.TrimSpace(params[k]) }

	if raw := get("slotJanitor"); raw != "" && !slotJanitorValues[strings.ToLower(raw)] {
		add("slotJanitor", raw, "must be on or off")
	}
	if raw := get("slotJanitorAfter"); raw != "" {
		d, err := time.ParseDuration(raw)
		switch {
		case err != nil:
			add("slotJanitorAfter", raw, "must be a Go duration, for example 6h")
		case d < minSlotJanitorAfter:
			add("slotJanitorAfter", raw, fmt.Sprintf("must be at least %s: a shorter window "+
				"drops the slot of a Pod that is only restarting", minSlotJanitorAfter))
		}
	}
	if raw := get("autoRebuildLegacy"); raw != "" {
		if _, err := strconv.ParseBool(raw); err != nil {
			add("autoRebuildLegacy", raw, "must be true or false")
		}
	}
	if raw := get("gateOnReplay"); raw != "" {
		if _, err := strconv.ParseBool(raw); err != nil {
			add("gateOnReplay", raw, "must be true or false")
		}
	}
	if raw := get("bloomColumns"); raw != "" && !bloomOff(raw) {
		for _, part := range strings.Split(raw, ";") {
			name, _, ok := strings.Cut(strings.TrimSpace(part), "=")
			if !ok || !tablePattern.MatchString(strings.TrimSpace(name)) {
				add("bloomColumns", raw, "must be off, or schema.table=col,col entries separated by ';' "+
					"(use - for a table with no bloom filters)")
				break
			}
		}
	}
	if raw := get("clusterBy"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			name, col, ok := strings.Cut(strings.TrimSpace(part), "=")
			col = strings.TrimSpace(col)
			if !ok || !tablePattern.MatchString(strings.TrimSpace(name)) ||
				(col != "none" && !slotPattern.MatchString(col)) {
				add("clusterBy", raw, "must be schema.table=column (or =none) entries separated by ','")
				break
			}
		}
	}
	if raw := get("numericScale"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			val := part
			if i := strings.LastIndex(part, "="); i >= 0 {
				if !numericColumnPattern.MatchString(strings.TrimSpace(part[:i])) {
					add("numericScale", raw, "a column entry must be schema.table.column=scale")
					break
				}
				val = part[i+1:]
			}
			if n, err := strconv.Atoi(strings.TrimSpace(val)); err != nil || n < 0 || n > maxNumericScale {
				add("numericScale", raw, fmt.Sprintf("a scale must be a whole number from 0 to %d, "+
					"either alone (every unconstrained numeric) or per column", maxNumericScale))
				break
			}
		}
	}
}

// bloomOff is the spelling the sidecar reads as no bloom filters at all.
func bloomOff(raw string) bool {
	switch strings.TrimSpace(raw) {
	case "off", "0", "-":
		return true
	}
	return false
}

// switchEnv is the container environment for the switches. The booleans are
// always set, in the sidecar's on/off spelling, as QS_DUCKLAKE is; the others
// only when the operator set them, so the sidecar's own default stands
// otherwise.
func switchEnv(cfg Config) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "QS_AUTO_REBUILD_LEGACY", Value: onOff(cfg.AutoRebuildLegacy)},
		{Name: "QS_GATE_ON_REPLAY", Value: onOff(cfg.GateOnReplay)},
	}
	if cfg.SlotJanitor != "" {
		env = append(env, corev1.EnvVar{Name: "QS_SLOT_JANITOR", Value: slotJanitorEnv(cfg.SlotJanitor)})
	}
	if cfg.SlotJanitorAfter > 0 {
		env = append(env, corev1.EnvVar{Name: "QS_SLOT_JANITOR_AFTER", Value: cfg.SlotJanitorAfter.String()})
	}
	for _, kv := range []struct{ name, value string }{
		{"QS_BLOOM_COLUMNS", cfg.BloomColumns},
		{"QS_CLUSTER_BY", cfg.ClusterBy},
		{"QS_NUMERIC_SCALE", cfg.NumericScale},
	} {
		if kv.value != "" {
			env = append(env, corev1.EnvVar{Name: kv.name, Value: kv.value})
		}
	}
	return env
}

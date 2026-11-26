package main

// The slot janitor: who removes a Quicksilver slot whose sidecar is gone.
//
// Every mirroring Pod owns a logical slot on the primary named after itself
// (slotname.go). A slot pins every WAL segment since its last confirmation, and
// the only thing that ever dropped one was the sidecar that owned it. So a Pod
// that went away for good (a scale-down, a replica that came back under a new
// name) left a slot nothing would ever confirm or drop, and so did the upgrade
// to per-Pod slots, which orphaned the old shared slot the moment the new
// sidecars started. Either fills the primary's disk. It was a documented manual
// step, which is a way of saying it was an outage with a runbook.
//
// The janitor runs in the one sidecar that can do something about it: the one
// standing down on the PRIMARY. It has a local superuser socket to the server
// the slots live on, and nothing else to do.
//
// WHAT IT TOUCHES. Logical, non-temporary slots named exactly the configured
// base (the legacy shared slot) or `<base>_...` (per-Pod slots of this
// cluster's Quicksilver; the plugin's default base carries the cluster name).
// Nothing else is looked at, whatever state it is in.
//
// WHEN IT DROPS. A slot is dropped only if it is INACTIVE, its Pod has no
// physical replication connection to this primary, and one of:
//
//   - it retains at least QS_MAX_SLOT_WAL_BYTES. That is the limit at which the
//     sidecar that owns it would drop it itself (slotguard.go); the janitor is
//     that guard for a sidecar that is not running to apply it.
//   - it has been inactive for QS_SLOT_JANITOR_AFTER (default 6h) AND no
//     instance of the cluster by that name is known to this server.
//
// WHO IS A CURRENT MEMBER, without the Kubernetes API. PostgreSQL knows two
// things about its standbys and both are used, either one being enough:
//
//   - a physical walsender whose application_name is the Pod's name. CNPG sets
//     application_name to the instance name in every replica's
//     primary_conninfo. This is "the Pod is here right now", and it is absolute:
//     such a slot is never dropped, not even over the WAL limit, because its
//     sidecar may be a moment from reconnecting and has its own guard.
//   - a physical replication slot named for the Pod. CNPG keeps one per replica
//     on the primary (`_cnpg_<instance>`, replicationSlots.highAvailability)
//     and removes it when the instance leaves the cluster. This is "the Pod is
//     a member even though it is down", and it is what keeps a replica that is
//     being repaired for a day from losing its mirror to the inactivity rule.
//
// A cluster with HA slots turned off has only the first signal, so there a
// replica that is DOWN for longer than the inactivity period loses its slot
// and re-snapshots when it returns. That is the safe direction.
//
// WHY DROPPING IS SAFE. A sidecar never assumes its slot is the one it left:
// at every start it creates the slot if absent, and a slot it had to create
// means a snapshot of every table, whatever its mirror holds (runMirror: "new
// replication slot; bootstrapping by snapshot"). A slot that exists is still
// compared with each table's applied LSN and anything behind is re-snapshotted
// (resume.go). So the worst a wrong drop can do is make one Pod rebuild its
// mirror; it cannot make a mirror wrong. And an ACTIVE slot is never dropped:
// the rule does not ask for it and PostgreSQL would refuse it.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	janitorInterval = envDuration("QS_SLOT_JANITOR_INTERVAL", time.Minute)
	janitorAfter = envDuration("QS_SLOT_JANITOR_AFTER", 6*time.Hour)
	// The prefix CNPG gives its per-instance physical slots
	// (replicationSlots.highAvailability.slotPrefix).
	janitorPhysicalPrefix = env("QS_SLOT_JANITOR_PHYSICAL_PREFIX", "_cnpg_")
)

func janitorOn() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QS_SLOT_JANITOR"))) {
	case "off", "0", "false", "no":
		return false
	}
	return true
}

// slotInfo is one row of pg_replication_slots, as far as the janitor cares.
type slotInfo struct {
	Name      string
	Logical   bool
	Active    bool
	Temporary bool
	// Retained is pg_current_wal_lsn() - restart_lsn; negative when the slot
	// has no restart_lsn (nothing retained, or invalidated).
	Retained int64
	// InactiveFor is how long the slot has had no consumer; negative when that
	// is not known.
	InactiveFor time.Duration
}

type janitorInput struct {
	Base  string     // the configured slot name, before the Pod suffix
	Slots []slotInfo // every slot on the server
	// Connected are the application_names of the physical walsenders.
	Connected      []string
	MaxWAL         int64         // <= 0: no WAL rule
	MaxInactive    time.Duration // <= 0: no inactivity rule
	PhysicalPrefix string
}

type slotDecision struct {
	Slot string
	Drop bool
	// Rule is a short stable name for the outcome: it is the metric label and
	// what "log each decision once" compares.
	Rule   string
	Detail string
}

const (
	ruleActive    = "active"
	ruleConnected = "pod_connected"
	// Its own rule, so that a slot which crosses the limit while its Pod is
	// connected is logged again, and louder, and is its own line in /metrics.
	ruleConnectedOver = "pod_connected_over_wal_limit"
	ruleOverWAL       = "over_wal_limit"
	ruleMember        = "member"
	ruleUnknownAge    = "inactivity_unknown"
	ruleRecent        = "recently_inactive"
	ruleOrphan        = "orphaned"
	ruleNoInactRule   = "no_inactivity_rule"
)

// decideSlots is the whole policy, with nothing in it but its arguments.
// Decisions come back sorted by slot name; slots that are not this cluster's
// Quicksilver slots are not mentioned at all.
func decideSlots(in janitorInput) []slotDecision {
	base := sanitiseSlotPart(in.Base)
	if base == "" {
		return nil
	}
	// Slot names a currently connected Pod would own. Computed forwards, with
	// the function the sidecar itself uses, because a long or unusual Pod name
	// is hashed into its slot name and cannot be read back out of it.
	connected := map[string]string{}
	for _, app := range in.Connected {
		if app = strings.TrimSpace(app); app != "" {
			connected[podSlotName(in.Base, app)] = app
		}
	}
	// And the ones an instance with a physical slot would own. A physical slot
	// name is already sanitised (CNPG turns '-' into '_'), so this side can
	// only be compared as text: base + "_" + the part after the prefix.
	const hashTagLen = 9 // "_" + 8 hex digits, see fitSlotName
	member, memberCut := map[string]string{}, map[string]string{}
	for _, s := range in.Slots {
		if s.Logical || s.Temporary {
			continue
		}
		part := s.Name
		if in.PhysicalPrefix != "" && strings.HasPrefix(part, in.PhysicalPrefix) {
			part = strings.TrimPrefix(part, in.PhysicalPrefix)
		}
		if part == "" {
			continue
		}
		if full := base + "_" + part; len(full) <= maxSlotName {
			member[full] = s.Name
		} else {
			// Too long to be a slot name as it stands, so the sidecar cut it
			// and appended a hash of the Pod's real name, which this side does
			// not have. The part before the hash is still the same text.
			memberCut[full[:maxSlotName-hashTagLen]] = s.Name
		}
	}
	isMember := func(slot string) string {
		if m := member[slot]; m != "" {
			return m
		}
		if len(slot) == maxSlotName {
			return memberCut[slot[:maxSlotName-hashTagLen]]
		}
		return ""
	}

	var out []slotDecision
	for _, s := range in.Slots {
		if !s.Logical || s.Temporary {
			continue
		}
		if s.Name != base && !strings.HasPrefix(s.Name, base+"_") {
			continue
		}
		d := slotDecision{Slot: s.Name}
		overWAL := in.MaxWAL > 0 && s.Retained >= in.MaxWAL
		switch {
		case s.Active:
			d.Rule, d.Detail = ruleActive, "a sidecar is consuming it"
		case connected[s.Name] != "":
			d.Rule = ruleConnected
			d.Detail = fmt.Sprintf("instance %q is streaming from this primary", connected[s.Name])
			if overWAL {
				d.Rule = ruleConnectedOver
				d.Detail += fmt.Sprintf("; it retains %d bytes, over the %d limit, and is "+
					"kept anyway because its Pod is here: its sidecar is not consuming it "+
					"and needs attention", s.Retained, in.MaxWAL)
			}
		case overWAL:
			d.Drop, d.Rule = true, ruleOverWAL
			d.Detail = fmt.Sprintf("inactive and retaining %d bytes of WAL, limit %d", s.Retained, in.MaxWAL)
		case isMember(s.Name) != "":
			d.Rule = ruleMember
			d.Detail = fmt.Sprintf("physical slot %q says its instance is still a member", isMember(s.Name))
		case in.MaxInactive <= 0:
			d.Rule, d.Detail = ruleNoInactRule, "the inactivity rule is off"
		case s.InactiveFor < 0:
			d.Rule, d.Detail = ruleUnknownAge, "inactive, for how long is not known yet"
		case s.InactiveFor < in.MaxInactive:
			d.Rule = ruleRecent
			d.Detail = fmt.Sprintf("inactive for %s, under %s", s.InactiveFor.Round(time.Second), in.MaxInactive)
		default:
			d.Drop, d.Rule = true, ruleOrphan
			d.Detail = fmt.Sprintf("inactive for %s (limit %s) and no instance of this cluster "+
				"has a replication connection or a physical slot by that name",
				s.InactiveFor.Round(time.Second), in.MaxInactive)
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

type janitor struct {
	log *slog.Logger
	o   options
	on  bool

	lastRun time.Time
	said    map[string]string // slot -> the rule last logged for it
	// firstInactive is when this process first saw a slot inactive: the only
	// clock there is before PostgreSQL 17, which has no inactive_since.
	firstInactive map[string]time.Time
	noInactiveCol bool

	mu      sync.Mutex
	runs    int64
	errs    int64
	dropped map[string]int64 // by rule
	kept    map[string]int   // by rule, at the last run
	lastOK  time.Time
}

func newJanitor(log *slog.Logger, o options) *janitor {
	j := &janitor{log: log, o: o, on: janitorOn(), said: map[string]string{},
		firstInactive: map[string]time.Time{}, dropped: map[string]int64{}, kept: map[string]int{}}
	if j.on {
		log.Info("slot janitor on: when this instance is the primary it drops this "+
			"cluster's Quicksilver slots that nothing will consume again",
			"slots", sanitiseSlotPart(o.slotBase)+" and "+sanitiseSlotPart(o.slotBase)+"_*",
			"interval", janitorInterval, "wal_limit_bytes", maxSlotWAL,
			"inactive_limit", janitorAfter, "off", "QS_SLOT_JANITOR=off")
	} else {
		log.Warn("slot janitor off (QS_SLOT_JANITOR): a slot whose Pod is gone pins WAL " +
			"on the primary until someone drops it; set max_slot_wal_keep_size")
	}
	return j
}

// maybeRun is called by supervise on every pass in which this node is the
// primary. Errors are counted and logged here; none of them is the caller's.
func (j *janitor) maybeRun(ctx context.Context) {
	if j == nil || !j.on || time.Since(j.lastRun) < janitorInterval {
		return
	}
	j.lastRun = time.Now()
	err := j.run(ctx)
	j.mu.Lock()
	j.runs++
	if err != nil {
		j.errs++
	} else {
		j.lastOK = time.Now()
	}
	j.mu.Unlock()
	if err != nil && ctx.Err() == nil {
		if j.said["\x00err"] != err.Error() {
			j.said["\x00err"] = err.Error()
			j.log.Warn("slot janitor could not run; orphaned slots are not being "+
				"removed until it can", "err", err)
		}
	} else if err == nil {
		delete(j.said, "\x00err")
	}
}

const janitorSlotsSQL = `
SELECT slot_name::text, slot_type = 'logical', active, temporary,
       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), -1)::bigint,
       %s
  FROM pg_replication_slots`

// Walsenders that are not serving a logical slot: the physical standbys, and a
// base backup in progress, which carries the name of whoever is taking it.
const janitorConnectedSQL = `
SELECT r.application_name
  FROM pg_stat_replication r
 WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots s
                    WHERE s.active_pid = r.pid AND s.slot_type = 'logical')`

func (j *janitor) run(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, j.o.localDSN())
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))

	// Asked again here, on the connection that will do the dropping: a slot
	// synchronised to a standby must not be touched, and this is the node's
	// answer at the moment that matters.
	var inRecovery bool
	if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
		return err
	}
	if inRecovery {
		return nil
	}

	in := janitorInput{Base: j.o.slotBase, MaxWAL: maxSlotWAL,
		MaxInactive: janitorAfter, PhysicalPrefix: janitorPhysicalPrefix}
	in.Slots, err = j.readSlots(ctx, conn)
	if err != nil {
		return err
	}
	rows, err := conn.Query(ctx, janitorConnectedSQL)
	if err != nil {
		return err
	}
	in.Connected, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return err
	}

	decisions := decideSlots(in)
	kept := map[string]int{}
	seen := map[string]bool{}
	for _, d := range decisions {
		seen[d.Slot] = true
		if !d.Drop {
			kept[d.Rule]++
			// Once per slot per outcome: this runs every minute for the life of
			// the primary, and a line per slot per minute buries the one drop.
			if j.said[d.Slot] != d.Rule {
				j.said[d.Slot] = d.Rule
				lvl := slog.LevelInfo
				if d.Rule == ruleConnectedOver {
					lvl = slog.LevelError
				}
				j.log.Log(ctx, lvl, "slot janitor: keeping slot", "slot", d.Slot, "rule", d.Rule, "why", d.Detail)
			}
			continue
		}
		if _, err := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", d.Slot); err != nil {
			// Most often: a sidecar took it between the listing and now. That
			// is the outcome the rule wanted anyway.
			var pgErr *pgconn.PgError
			if key := "drop-failed:" + d.Rule; j.said[d.Slot] != key {
				j.said[d.Slot] = key
				j.log.Warn("slot janitor: could not drop slot; it will be looked at again",
					"slot", d.Slot, "rule", d.Rule, "why", d.Detail, "err", err)
			}
			if !errors.As(err, &pgErr) {
				return err // the connection, not the slot
			}
			kept["drop_failed"]++
			continue
		}
		delete(j.said, d.Slot)
		delete(j.firstInactive, d.Slot)
		j.log.Warn("slot janitor: dropped slot; if its sidecar returns it will find no "+
			"slot, create one and rebuild its mirror from a snapshot",
			"slot", d.Slot, "rule", d.Rule, "why", d.Detail)
		j.mu.Lock()
		j.dropped[d.Rule]++
		j.mu.Unlock()
	}
	for slot := range j.said {
		if !seen[slot] && slot != "\x00err" {
			delete(j.said, slot) // gone by other hands; say it again if it returns
		}
	}
	j.mu.Lock()
	j.kept = kept
	j.mu.Unlock()
	return nil
}

func (j *janitor) readSlots(ctx context.Context, conn *pgx.Conn) ([]slotInfo, error) {
	scan := func(age string) ([]slotInfo, error) {
		rows, err := conn.Query(ctx, fmt.Sprintf(janitorSlotsSQL, age))
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []slotInfo
		for rows.Next() {
			var s slotInfo
			var secs float64
			if err := rows.Scan(&s.Name, &s.Logical, &s.Active, &s.Temporary, &s.Retained, &secs); err != nil {
				return nil, err
			}
			s.InactiveFor = -1
			if secs >= 0 {
				s.InactiveFor = time.Duration(secs * float64(time.Second))
			}
			out = append(out, s)
		}
		return out, rows.Err()
	}
	var slots []slotInfo
	var err error
	if !j.noInactiveCol {
		slots, err = scan("COALESCE(EXTRACT(EPOCH FROM now() - inactive_since), -1)::float8")
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42703" {
			j.noInactiveCol = true // before PostgreSQL 17
		} else if err != nil {
			return nil, err
		}
	}
	if j.noInactiveCol {
		if slots, err = scan("-1::float8"); err != nil {
			return nil, err
		}
	}
	// Our own clock, where the server has none: counted from when this process
	// first saw the slot inactive, which can only be later than the truth.
	now := time.Now()
	present := map[string]bool{}
	for i := range slots {
		s := &slots[i]
		present[s.Name] = true
		if s.Active {
			delete(j.firstInactive, s.Name)
			continue
		}
		if _, ok := j.firstInactive[s.Name]; !ok {
			j.firstInactive[s.Name] = now
		}
		if s.InactiveFor < 0 && j.noInactiveCol {
			s.InactiveFor = now.Sub(j.firstInactive[s.Name])
		}
	}
	for name := range j.firstInactive {
		if !present[name] {
			delete(j.firstInactive, name)
		}
	}
	return slots, nil
}

func (j *janitor) Metrics() string {
	if j == nil {
		return ""
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	var b strings.Builder
	on := 0
	if j.on {
		on = 1
	}
	b.WriteString("# HELP quicksilver_slot_janitor_enabled 1 when this sidecar removes orphaned Quicksilver slots while its node is the primary.\n")
	b.WriteString("# TYPE quicksilver_slot_janitor_enabled gauge\n")
	fmt.Fprintf(&b, "quicksilver_slot_janitor_enabled %d\n", on)
	b.WriteString("# HELP quicksilver_slot_janitor_runs_total Janitor passes on this node (it only runs on the primary).\n")
	b.WriteString("# TYPE quicksilver_slot_janitor_runs_total counter\n")
	fmt.Fprintf(&b, "quicksilver_slot_janitor_runs_total %d\n", j.runs)
	b.WriteString("# HELP quicksilver_slot_janitor_errors_total Janitor passes that failed.\n")
	b.WriteString("# TYPE quicksilver_slot_janitor_errors_total counter\n")
	fmt.Fprintf(&b, "quicksilver_slot_janitor_errors_total %d\n", j.errs)
	b.WriteString("# HELP quicksilver_slot_janitor_last_success_timestamp_seconds When the janitor last completed a pass.\n")
	b.WriteString("# TYPE quicksilver_slot_janitor_last_success_timestamp_seconds gauge\n")
	var ts int64
	if !j.lastOK.IsZero() {
		ts = j.lastOK.Unix()
	}
	fmt.Fprintf(&b, "quicksilver_slot_janitor_last_success_timestamp_seconds %d\n", ts)
	b.WriteString("# HELP quicksilver_slot_janitor_dropped_total Replication slots the janitor dropped, by rule.\n")
	b.WriteString("# TYPE quicksilver_slot_janitor_dropped_total counter\n")
	for _, rule := range []string{ruleOverWAL, ruleOrphan} {
		fmt.Fprintf(&b, "quicksilver_slot_janitor_dropped_total{rule=%q} %d\n", rule, j.dropped[rule])
	}
	b.WriteString("# HELP quicksilver_slot_janitor_slots Quicksilver slots seen and kept at the last pass, by rule.\n")
	b.WriteString("# TYPE quicksilver_slot_janitor_slots gauge\n")
	rules := make([]string, 0, len(j.kept))
	for r := range j.kept {
		rules = append(rules, r)
	}
	sort.Strings(rules)
	for _, r := range rules {
		fmt.Fprintf(&b, "quicksilver_slot_janitor_slots{rule=%q} %d\n", r, j.kept[r])
	}
	return b.String()
}

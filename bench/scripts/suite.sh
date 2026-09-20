#!/usr/bin/env bash
# Everything, in one verdict.
#
# There are a dozen scripts under bench/scripts, each with its own PASS/FAIL and
# its own preconditions, and until this existed the question "does it all still
# work" had no answer short of remembering which ones to run and in what order.
# That is how a suite rots: not by failing, but by nobody running the half that
# takes longest.
#
# THREE OUTCOMES, and the middle one is the point:
#
#   PASS        the section ran and agreed
#   FAIL        the section ran and disagreed
#   INCOMPLETE  the section could not run — a missing dependency, no cluster,
#               no Kubernetes. This is NOT a pass, it is counted separately,
#               and the suite's exit code distinguishes it (2) from a failure
#               (1). A harness that reports a skipped section as success is how
#               a benchmark comes to measure nothing.
#
# The ordering is deliberate: unit tests before anything that needs a database,
# correctness before performance, and the destructive sections (promotion,
# repeated restarts) last, because they leave the cluster needing a rebuild.
#
#   bash bench/scripts/suite.sh              # correctness, ~15 minutes
#   FULL=1 bash bench/scripts/suite.sh       # + the workload matrix and serving
#   ONLY="e2e rendering" bash bench/scripts/suite.sh
set -uo pipefail
cd "$(dirname "$0")/../.."

FULL=${FULL:-0}
ONLY=${ONLY:-}
OUT=${OUT:-/tmp/qs-suite}
mkdir -p "$OUT"

PASSED=0; FAILED=0; INCOMPLETE=0
declare -a RESULTS

# run <name> <timeout-seconds> <command...>
#
# Exit 2 is INCOMPLETE by convention across every script here, and a timeout
# (124) is a failure rather than an incomplete: a section that hangs has not
# told us it could not run, it has told us nothing.
run() {
  local name=$1 limit=$2; shift 2
  if [ -n "$ONLY" ] && ! printf '%s\n' $ONLY | grep -qx "$name"; then
    return 0
  fi
  local log="$OUT/$name.txt"
  printf '\n########  %-22s  ########\n' "$name"
  local t0 rc
  t0=$(date +%s)
  timeout "$limit" "$@" > "$log" 2>&1
  rc=$?
  local secs=$(( $(date +%s) - t0 ))
  case $rc in
    0) PASSED=$((PASSED+1));     RESULTS+=("PASS        $name (${secs}s)")
       printf 'PASS  %s  %ss\n' "$name" "$secs" ;;
    2) INCOMPLETE=$((INCOMPLETE+1)); RESULTS+=("INCOMPLETE  $name — $(tail -1 "$log" | cut -c1-70)")
       printf 'INCOMPLETE  %s — %s\n' "$name" "$(grep -m1 'INCOMPLETE' "$log" | cut -c1-90)" ;;
    124) FAILED=$((FAILED+1));   RESULTS+=("FAIL        $name (timed out after ${limit}s)")
       printf 'FAIL  %s — timed out after %ss\n' "$name" "$limit" ;;
    *) FAILED=$((FAILED+1));     RESULTS+=("FAIL        $name (exit $rc)")
       printf 'FAIL  %s — exit %s\n' "$name" "$rc"
       grep -E "FAIL:|DIFFERS|DIVERGED" "$log" | head -5 | sed 's/^/      /' ;;
  esac
  printf '      log: %s\n' "$log"
}

printf '=== quicksilver suite ===\n'
printf '  full=%s only=%s logs=%s\n' "$FULL" "${ONLY:-<all>}" "$OUT"

# ---- 0. the cluster everything below assumes ---------------------------------
#
# This used not to exist, and its absence was invisible: every database section
# opened by checking for a primary and calling skip() when there wasn't one, so
# on a machine that had never had one built by hand the suite reported a column
# of INCOMPLETEs and exited 2. Honest, and useless. A suite that cannot build
# its own fixture cannot run anywhere it has not already run.
run cluster       600 bash bench/scripts/setup_cluster.sh

# ---- 1. no database required -------------------------------------------------
run unit          900 go -C go test ./... -count=1
run race          900 go -C go test ./internal/mirror ./cmd/qs-mirror -race -count=1
run vet           300 go -C go vet ./...

# The escape hatches have to keep working, or they are not escape hatches. Each
# of these is the documented recovery from a halt or a regression.
run flags_off     900 env QS_TEMPORAL_TYPES=0 QS_PARTIAL_DELTAS=0 QS_ELIDE_UNCHANGED=0 \
                      QS_SNAPPY_DELTAS=1 QS_MAX_TICK_CHANGES=0 QS_MAX_MERGE_FILES=0 \
                      go -C go test ./internal/mirror -count=1

# ---- 2. the plugin, without Kubernetes ---------------------------------------
# fidelity is a SEPARATE module on purpose — testing the CNPG-I handshake means
# importing the CloudNativePG operator, which must not reach a shipping binary's
# dependency tree. So it needs its own -C.
run cnpgi         900 go -C go/fidelity test ./... -count=1

# ---- 3. correctness against a real cluster -----------------------------------
run manifests     600 bash bench/scripts/validate_manifests.sh
run rendering    1800 bash bench/scripts/rendering_fidelity.sh
# Answers a SELECT through PostgreSQL rather than through DuckDB. Reports
# INCOMPLETE without the patched pg_duckdb, which is not packaged anywhere —
# see bench/patches/README.md.
run serving      1800 bash bench/scripts/serving_pg17.sh

# ---- 4. performance, opt-in --------------------------------------------------
if [ "$FULL" = "1" ]; then
  run matrix     5400 env SHAPES="narrow wide jsonb" ROWS=200000 SECONDS_PER=12 \
                     QS_SERVE_COMPARE=1 bash bench/scripts/workload_matrix.sh
  run bootstrap  2400 env SHAPES="narrow" ROWS=400000 bash bench/scripts/bootstrap_profile.sh
  run pruning     900 python3 bench/scripts/pruning_check.py \
                     --mirror /var/lib/postgresql/qs17/mx-narrow
fi

# ---- 5. destructive, last ----------------------------------------------------
# e2e promotes the standby and leaves it as a primary; anything needing a
# standby has to run before this.
run e2e          1800 bash bench/scripts/e2e_mirror.sh

printf '\n========================================\n'
for r in "${RESULTS[@]}"; do printf '  %s\n' "$r"; done
printf '\n  %d passed, %d failed, %d incomplete\n' "$PASSED" "$FAILED" "$INCOMPLETE"

if [ "$FAILED" -gt 0 ]; then
  printf '\nFAIL\n'; exit 1
fi
if [ "$INCOMPLETE" -gt 0 ]; then
  # Not a pass. A section that could not run has told us nothing, and reporting
  # that as success is the failure mode this whole suite is arranged against.
  printf '\nINCOMPLETE — %d section(s) could not run, which is NOT a pass\n' "$INCOMPLETE"
  exit 2
fi
printf '\nPASS\n'

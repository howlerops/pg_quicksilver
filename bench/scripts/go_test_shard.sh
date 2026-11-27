#!/usr/bin/env bash
# Run one shard of a Go package's tests, such that the shards together run
# every test exactly once.
#
#   go_test_shard.sh <package> <shard> <of> [go test flags...]
#   go_test_shard.sh ./internal/mirror 0 3 -race -count=1 -timeout 20m
#   go_test_shard.sh --plan ./internal/mirror 3        # print who runs what
#
# Why it exists. go/internal/mirror under -race takes 610 s on two cores, which
# is past go test's default 10-minute timeout, and four tests are over half of
# it. Tests in one package run one after another, so the only way to get the
# wall time down without deleting a test is to give different tests to
# different jobs.
#
# Why not `-run` lists written by hand. A list in a workflow file is a list of
# what somebody remembered: the test added next month is in no shard and CI
# stays green without ever running it. Here the list comes from `go test -list`
# at run time, every name on it is assigned to exactly one shard, and that is
# checked before anything runs (the three sets are compared with the whole).
# The weights file only decides balance.
#
# The assignment is deterministic: heaviest first, each to the lightest shard
# so far; unweighted tests count one second and go the same way, in name order.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
WEIGHTS=${WEIGHTS:-$REPO/bench/scripts/race_test_weights.txt}
GO_DIR=${GO_DIR:-$REPO/go}

plan_only=0
if [ "${1:-}" = "--plan" ]; then plan_only=1; shift; fi
pkg=${1:?package}; shift
if [ "$plan_only" = 1 ]; then of=${1:?shard count}; shard=-1; shift
else shard=${1:?shard index}; of=${2:?shard count}; shift 2; fi

# Every runnable name: tests, fuzz targets (run as their seed corpus) and
# examples. Benchmarks are listed too and are not run by `go test` without
# -bench, so they are left out here as they are there.
names=$(cd "$GO_DIR" && go test -list '.*' "$pkg" | grep -E '^(Test|Fuzz|Example)' | sort -u) \
  || { echo "FAIL: could not list the tests of $pkg" >&2; exit 1; }
[ -n "$names" ] || { echo "FAIL: $pkg lists no tests; a shard that runs nothing would pass" >&2; exit 1; }

assignment=$(printf '%s\n' "$names" | awk -v of="$of" -v wf="$WEIGHTS" '
  BEGIN {
    while ((getline line < wf) > 0) {
      if (line ~ /^[ \t]*#/ || line ~ /^[ \t]*$/) continue
      split(line, f, " "); w[f[2]] = f[1] + 0
    }
  }
  { name[NR] = $0; wt[NR] = ($0 in w) ? w[$0] : 1 }
  END {
    # selection sort by weight descending, name ascending: a few hundred names
    for (i = 1; i <= NR; i++) order[i] = i
    for (i = 1; i <= NR; i++) {
      best = i
      for (j = i + 1; j <= NR; j++) {
        a = order[j]; b = order[best]
        if (wt[a] > wt[b] || (wt[a] == wt[b] && name[a] < name[b])) best = j
      }
      t = order[i]; order[i] = order[best]; order[best] = t
    }
    for (s = 0; s < of; s++) load[s] = 0
    for (i = 1; i <= NR; i++) {
      n = order[i]; min = 0
      for (s = 1; s < of; s++) if (load[s] < load[min]) min = s
      load[min] += wt[n]
      print min, wt[n], name[n]
    }
  }')

# The check that makes this safe: the shards partition the list.
total=$(printf '%s\n' "$names" | wc -l)
assigned=$(printf '%s\n' "$assignment" | awk '{print $3}' | sort -u | wc -l)
rows=$(printf '%s\n' "$assignment" | wc -l)
if [ "$total" != "$assigned" ] || [ "$total" != "$rows" ]; then
  echo "FAIL: $total tests listed, $rows assignments, $assigned distinct: the shards do not partition the package" >&2
  exit 1
fi

if [ "$plan_only" = 1 ]; then
  printf '%s\n' "$assignment" | awk -v of="$of" '
    { n[$1]++; w[$1] += $2; if ($2 >= 20) big[$1] = big[$1] " " $3 "(" $2 "s)" }
    END { for (s = 0; s < of; s++) printf "shard %d/%d: %3d tests, ~%4ds weighed%s\n", s, of, n[s], w[s], big[s] }'
  echo "total: $total tests, each in exactly one shard"
  exit 0
fi

case "$shard" in ''|*[!0-9]*) echo "FAIL: shard must be 0..$((of - 1))" >&2; exit 2 ;; esac
[ "$shard" -lt "$of" ] || { echo "FAIL: shard $shard of $of" >&2; exit 2; }

mine=$(printf '%s\n' "$assignment" | awk -v s="$shard" '$1 == s {print $3}')
[ -n "$mine" ] || { echo "FAIL: shard $shard/$of of $pkg is empty; use fewer shards" >&2; exit 1; }
count=$(printf '%s\n' "$mine" | wc -l)
pattern="^($(printf '%s\n' "$mine" | paste -sd '|'))\$"
echo "shard $shard/$of of $pkg: $count of $total tests"

cd "$GO_DIR"
# -json would be nicer to parse and worse to read in a CI log; -v is enough to
# count with. What ran is counted afterwards: a pattern that matched fewer
# top-level tests than it names means the list and the binary disagree.
out=$(mktemp)
trap 'rm -f "$out"' EXIT
rc=0
go test "$pkg" -run "$pattern" -v "$@" > "$out" 2>&1 || rc=$?
# Quiet on success, everything on failure: -v on a few hundred tests is noise
# until one of them fails.
if [ "$rc" -ne 0 ]; then cat "$out"; else grep -E '^(ok|FAIL|---|PASS|\?)' "$out" | grep -vE '^--- (PASS|SKIP)' || true; fi
ran=$(grep -cE '^--- (PASS|FAIL|SKIP): [^/ ]+ ' "$out" || true)
echo "ran $ran top-level tests (expected $count), slowest:"
grep -E '^--- (PASS|FAIL): [^/ ]+ \(' "$out" | sed -E 's/^--- [A-Z]+: ([^ ]+) \(([0-9.]+)s\)/\2 \1/' | sort -rn | head -5 | sed 's/^/  /'
if [ "$rc" -eq 0 ] && [ "$ran" != "$count" ]; then
  echo "FAIL: the shard names $count tests and $ran reported a result" >&2
  exit 1
fi
exit "$rc"

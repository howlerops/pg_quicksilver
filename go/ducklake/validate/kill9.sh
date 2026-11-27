#!/usr/bin/env bash
# kill9.sh ROUNDS: SIGKILL the sidecar while it is applying and publishing as
# fast as it can, then check, before anything is restarted, that the lake is
# still a readable table at the snapshot it last committed; restart, let it
# reconcile, and compare the lake with the source.
#
# The kill is aimed: it is sent the moment a new file appears in the lake
# directory, which is a publish that has written files and not yet committed.
# Whether it landed INSIDE a publish is visible afterwards: files in the
# lake directory that no catalog row names are what a publish makes before it
# commits. They are counted here and must be gone after the restart.
set -uo pipefail
. "$(dirname "$0")/env.sh"
orphans() {
python3 - "$CATALOG" "$MIRROR/_ducklake/data" <<'PY'
import os, sqlite3, sys
s = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
named = set()
for t in ("ducklake_data_file", "ducklake_delete_file"):
    named |= {r[0] for r in s.execute(f"select s.path || t.path || f.path from {t} f join ducklake_table t using (table_id) "
                                      "join ducklake_schema s using (schema_id)")}
named |= {r[0] for r in s.execute("select path from ducklake_files_scheduled_for_deletion")}
disk = {os.path.relpath(os.path.join(d, f), sys.argv[2]) for d, _, fs in os.walk(sys.argv[2]) for f in fs}
missing = named - disk
print(len(disk - named), len(missing))
PY
}
lake() {
python3 - "$CATALOG" <<'PY'
import sys, duckdb
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD sqlite")
c.execute(f"ATTACH 'ducklake:sqlite:{sys.argv[1]}' AS lake (READ_ONLY)")
n, s = c.execute("select count(*), sum(amount) from lake.public.orders").fetchone()
snap, lsn = c.execute("select snapshot_id, applied_lsn from __ducklake_metadata_lake.qs_mirror_table").fetchone()
print(f"snapshot {snap} at {lsn}: {n} rows, sum {s}")
PY
}
for round in $(seq 1 "${1:-3}"); do
  "$HERE/churn.sh" 40 0 >/dev/null 2>&1 &
  churn=$!
  sleep "$(python3 -c 'import random;print(round(random.uniform(1.0, 4.0), 2))')"
  # Aim: a publish writes its delete files, syncs them, and only then commits.
  # So the kill is sent the moment a file appears in the lake directory.
  python3 - "$MIRROR/_ducklake/data/public/orders" "$(cat "$BASE/mirror.pid")" <<'PY'
import os, signal, sys, time
d, pid = sys.argv[1], int(sys.argv[2])
seen = set(os.listdir(d))
end = time.time() + 20
while time.time() < end:
    if set(os.listdir(d)) - seen:
        break
os.kill(pid, signal.SIGKILL)
PY
  for _ in $(seq 1 100); do kill -0 "$(cat "$BASE/mirror.pid")" 2>/dev/null || break; sleep 0.05; done
  read -r extra missing < <(orphans)
  echo "round $round: killed. Lake, before any restart: $(lake)"
  echo "round $round: files in the lake no snapshot names: $extra (a publish was in flight if > 0); files named and missing: $missing"
  [ "$missing" = 0 ] || die "the catalog names files that are not there"
  mark=$(wc -l < "$BASE/mirror.log")
  "$HERE/sidecar.sh" start on || die "restart"
  wait $churn
  "$HERE/settle.sh" || exit 1
  read -r extra missing < <(orphans)
  swept=$(tail -n +"$mark" "$BASE/mirror.log" | grep -c "removing a file no snapshot names")
  echo "round $round: restarted; swept $swept unnamed files at open; unnamed now: $extra, missing: $missing"
  [ "$extra" = 0 ] && [ "$missing" = 0 ] || die "the lake directory and the catalog disagree after reconciliation"
  "$HERE/verify.sh" | tail -4
done

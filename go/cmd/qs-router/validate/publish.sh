#!/usr/bin/env bash
# (Re)publishes the mirror as views in qs_mirror, with qs-query -engine postgres.
#
# The view names the mirror's files as they are NOW. The sidecar compacts a
# table that has deltas after 30 idle seconds (QS_COMPACT_IDLE_SECONDS), the
# files are replaced, and the view then fails ("No files found that match the
# pattern") until this is run again. That broke two validation runs before it
# was understood, so this waits for pending deltas to be compacted away first,
# when a sidecar is running to do it.
set -uo pipefail
PG=/usr/lib/postgresql/17/bin
BASE=${BASE:-/var/lib/postgresql/qsrouter}
PPORT=${PPORT:-5491}
pending() {
  python3 - "$BASE/mirror" <<'EOF'
import glob, json, sys
n = 0
for p in glob.glob(sys.argv[1] + "/*/state.json"):
    try:
        n += len(json.load(open(p)).get("delta_files") or [])
    except (OSError, ValueError):
        n += 1
print(n)
EOF
}
if pgrep -f "^$BASE/qs-mirror\$" >/dev/null; then
  for _ in $(seq 1 120); do
    [ "$(pending)" = 0 ] && break
    sleep 1
  done
  [ "$(pending)" = 0 ] || echo "publish.sh: deltas still pending; the views may go stale at the next compaction" >&2
fi
for t in orders customers; do
  su postgres -c "$BASE/qs-query -mirror $BASE/mirror -table public.$t -view qs_mirror.$t -engine postgres \
     -dsn 'host=/tmp port=$PPORT user=postgres dbname=app'" 2>"$BASE/qs-query.$t.err" > "$BASE/view.$t.sql" || { cat "$BASE/qs-query.$t.err"; exit 1; }
  su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d app -q -v ON_ERROR_STOP=1 -f $BASE/view.$t.sql" || exit 1
done

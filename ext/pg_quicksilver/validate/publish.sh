#!/usr/bin/env bash
# (Re)publishes the mirror as pg_duckdb views (qs-query -engine postgres),
# registers each one with pg_quicksilver and reports the LSN the published
# files had applied. This is the part of the sidecar's job that does not exist
# in the sidecar yet (docs/40, "what an image needs").
#
# A view names the mirror's files as they are NOW; the sidecar replaces them
# when it compacts (docs/38), so this waits for pending deltas first.
set -uo pipefail
. "$(dirname "$0")/env.sh"
pending() {
  python3 - "$BASE/mirror" <<'PY'
import glob, json, sys
n = 0
for p in glob.glob(sys.argv[1] + "/*/state.json"):
    try:
        n += len(json.load(open(p)).get("delta_files") or [])
    except (OSError, ValueError):
        n += 1
print(n)
PY
}
if pgrep -f "^$BASE/qs-mirror\$" >/dev/null; then
  for _ in $(seq 1 ${PUBLISH_WAIT:-45}); do
    [ "$(pending)" = 0 ] && break
    sleep 1
  done
  [ "$(pending)" = 0 ] || echo "publish.sh: deltas still pending; the views may go stale at the next compaction" >&2
fi
psq $PPORT "CREATE SCHEMA IF NOT EXISTS qs_mirror; CREATE SCHEMA IF NOT EXISTS qs_mirror_ex" >/dev/null
publish() { # heap-schema table mirror-schema
  local q="$BASE/qs-query -mirror $BASE/mirror -table $1.$2"
  su postgres -c "$q -view $3.$2 -engine postgres -dsn 'host=/tmp port=$PPORT user=postgres dbname=$DB'" \
     2>"$BASE/qs-query.$1.$2.err" > "$BASE/view.$1.$2.sql" || { cat "$BASE/qs-query.$1.$2.err"; exit 1; }
  local lsn; lsn=$(su postgres -c "$q -lsn") || exit 1
  # pg_duckdb cannot cast to an enum; the column is published as text and the
  # extension then refuses statements that read it (stored as a different type).
  sed -i 's/::\(ex\.mood\|inet\) /::text /' "$BASE/view.$1.$2.sql"
  psqf -f "$BASE/view.$1.$2.sql" || { echo "publish.sh: $1.$2 not published" >&2; return 0; }
  psq $PPORT "SELECT quicksilver.register_mirror('$1.$2', '$3.$2'); UPDATE quicksilver.mirrors SET applied_lsn = '$lsn' WHERE heap = '$1.$2'::regclass; SELECT quicksilver.report_applied_lsn('$1.$2', '$lsn')" >/dev/null || exit 1
}
for t in orders customers qs_types; do publish public $t qs_mirror; done
for t in $EX_TABLES; do publish ex $t qs_mirror_ex; done
cp "$HERE/finite.sql" "$BASE/sql/finite.sql"; chown postgres:postgres "$BASE/sql/finite.sql"
psqf -f "$BASE/sql/finite.sql" || exit 1
exit 0

#!/usr/bin/env bash
# Turning QS_DUCKLAKE on over a mirror that already holds column-partial patch
# files: the lake table must be refused (a reader gets the reason, not a table
# missing its patches), a base rewrite must start by itself, and the table must
# be published when it lands.
set -uo pipefail
. "$(dirname "$0")/env.sh"
"$HERE/sidecar.sh" stop; "$HERE/sidecar.sh" wipe
PARTIAL=1 "$HERE/sidecar.sh" start off || die "start off"
# A patch file is what an UPDATE leaves when PostgreSQL does not send every
# column: an unchanged TOASTed value. So: a column wide enough to be stored
# out of line, then updates that do not touch it.
sql() { su postgres -c "$PG/psql -h /tmp -p $PPORT -U postgres -d $DB -q -v ON_ERROR_STOP=1" >/dev/null || die "sql"; }
sql <<'SQL'
ALTER TABLE orders ADD COLUMN IF NOT EXISTS doc text;
UPDATE orders o SET doc = (SELECT string_agg(md5(random()::text || o.seq::text || g::text), '') FROM generate_series(1, 300) g)
 WHERE seq BETWEEN 1000 AND 1500;
SQL
echo "rows with an out-of-line doc: $(psq 'SELECT count(*) FROM orders WHERE pg_column_size(doc) > 2000')"
"$HERE/settle.sh"
for i in 1 2 3; do echo "UPDATE orders SET amount = amount + 1 WHERE seq BETWEEN 1000 AND 1500;" | sql; sleep 0.4; done
"$HERE/settle.sh"
"$HERE/sidecar.sh" stop
n=$(python3 -c "import json;print(len(json.load(open('$MIRROR/public.orders/state.json')).get('partial_cols') or {}))")
echo "mirror built with QS_DUCKLAKE off: $n patch files in the manifest"
[ "$n" -gt 0 ] || die "the mirror holds no patch files; nothing to test"
mark=$(wc -l < "$BASE/mirror.log")
"$HERE/sidecar.sh" start on || die "start on"
query() {
python3 - "$CATALOG" <<'PY'
import sys, duckdb
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD sqlite")
c.execute(f"ATTACH 'ducklake:sqlite:{sys.argv[1]}' AS lake (READ_ONLY)")
try:
    print("answers:", c.execute("select count(*) from lake.public.orders").fetchone()[0], "rows")
except Exception as e:
    print("fails:", str(e).split("\n")[0][:230])
PY
}
echo "first read of the lake after QS_DUCKLAKE=on: $(query)"
for _ in $(seq 1 600); do [ "$(metric quicksilver_ducklake_refused)" = 0 ] && break; sleep 0.2; done
tail -n +"$mark" "$BASE/mirror.log" | grep -E "compaction started|msg=compacted" | cut -c1-230
echo "refused now: $(metric quicksilver_ducklake_refused); patch files in the manifest: $(python3 -c "import json;print(len(json.load(open('$MIRROR/public.orders/state.json')).get('partial_cols') or {}))")"
echo "read of the lake after the rewrite: $(query)"
"$HERE/verify.sh" | tail -4

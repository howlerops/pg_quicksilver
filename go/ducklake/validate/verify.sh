#!/usr/bin/env bash
# The mirror against the source (qs-verify), then the LAKE against the source:
# count, sum(amount), and an order-independent hash of every row, computed by
# PostgreSQL over the heap and by DuckDB over the lake table.
set -uo pipefail
. "$(dirname "$0")/env.sh"
"$HERE/settle.sh" || exit 1
su postgres -c "$BASE/qs-verify -dsn 'host=/tmp port=$PPORT user=postgres dbname=$DB' -mirror $MIRROR -table public.orders" 2>&1 | tail -3
heap=$(psq "SELECT count(*) || '|' || sum(amount) || '|' || sum(('x' || substr(md5(id::text || '|' || seq || '|' || amount || '|' || status || '|' || coalesce(note, '~') || '|' || (extract(epoch from created_at)*1000000)::bigint), 1, 8))::bit(32)::bigint) FROM orders")
lake=$(python3 - "$CATALOG" <<'PY'
import sys, duckdb
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD sqlite; SET TimeZone='UTC'")
c.execute(f"ATTACH 'ducklake:sqlite:{sys.argv[1]}' AS lake (READ_ONLY)")
r = c.execute("""SELECT count(*), sum(amount), sum(('0x' || substr(md5(id::varchar || '|' || seq || '|' || amount || '|' || status
       || '|' || coalesce(note, '~') || '|' || epoch_us(created_at)), 1, 8))::bigint) FROM lake.public.orders""").fetchone()
print(f"{r[0]}|{r[1]}|{r[2]}")
PY
)
echo "heap: $heap"
echo "lake: $lake"
[ "$heap" = "$lake" ] && echo "LAKE MATCH" || die "the lake and the source differ"

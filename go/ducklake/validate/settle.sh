#!/usr/bin/env bash
# Waits until the mirror has applied everything committed so far and, when
# QS_DUCKLAKE is on, the lake has published it.
#
# applied_lsn is the commit LSN of the last transaction applied, so "caught
# up" needs a transaction to be caught up WITH: one row is inserted after the
# position is read, and applying it takes applied_lsn past that position.
set -uo pipefail
. "$(dirname "$0")/env.sh"
want=$(psq "SELECT pg_current_wal_lsn() - '0/0'")
psq "INSERT INTO orders SELECT gen_random_uuid(), nextval('orders_seq_next'), 1.00, now(), 'settle', NULL" >/dev/null
lsn() { python3 -c "import sys;l=sys.argv[1].split('/');print(int(l[0],16)<<32|int(l[1],16))" "$1" 2>/dev/null || echo 0; }
for _ in $(seq 1 600); do
  applied=$(lsn "$(python3 -c "import json;print(json.load(open('$MIRROR/public.orders/state.json'))['applied_lsn'])" 2>/dev/null)")
  if [ "$applied" -ge "$want" ]; then
    pub=$(metric quicksilver_ducklake_published_lsn)
    [ -z "$pub" ] && exit 0                      # QS_DUCKLAKE off
    [ "$pub" -ge "$want" ] && exit 0
  fi
  sleep 0.1
done
die "the mirror did not catch up"

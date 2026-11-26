#!/bin/bash
# Item 1: four tables, DuckLake on, 10 minutes of mixed writes, checks every minute.
# Output: out/item1_run.txt (driver), out/item1.log (sidecar), out/item1_start.txt
# (catalog size at start). The sidecar is stopped by its recorded PID at the end.
. "$(dirname "$0")/qs2_env.sh"
export QS_VERIFY_BIN=$BIN/qs-verify
"$HERE/item1_reset.sh" || die "reset"
rm -f "$OUT/item1.log"
TABLES=public.ev_uuid,public.ledger,public.wide_text,public.lookup sc_start item1 item1 QS_DUCKLAKE=on QS_SLOW_TICK=1ms || die "start"
sc_ready || die "sidecar not ready"
ls -l "$BASE/mirror/item1/_ducklake/catalog.sqlite" | tee "$OUT/item1_start.txt"
python3 "$HERE/item1_mixed.py" "$BASE/mirror/item1" "$OUT/item1_run.txt" "$OUT/item1.pid"
rc=$?
sc_stop item1
echo "driver exit $rc"

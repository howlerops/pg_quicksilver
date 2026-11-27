#!/usr/bin/env bash
# Scenario e: ALTER TABLE fv.accounts ADD COLUMN tier text NOT NULL DEFAULT 'std', then writes
# (old-default rows and rows that name the column), then qs-verify MATCH and routed answers that use the column.
. "$(dirname "$0")/env.sh"; . "$(dirname "$0")/run.sh"
q $PPORT "ALTER TABLE fv.accounts ADD COLUMN tier text NOT NULL DEFAULT 'std'" && echo "added column"
python3 -I $FV/scripts/fv_mixed.py --port $PPORT --rounds 3 --seed 5 > $OUT/e_writes.txt 2>&1; echo "writes exit $?"
q $PPORT "INSERT INTO fv.accounts (code, balance, opened, label, tier) VALUES ('TIER-1', 10.5, now(), 'x', 'gold'), ('TIER-2', 20, now(), 'y', 'silver')" && echo "explicit inserts ok"
q $PPORT "UPDATE fv.accounts SET tier = 'gold' WHERE code LIKE 'MX%' AND balance > 800" && echo "updated"
python3 -I $FV/scripts/fv_probe.py --port $SPORT --expect-mirror --sql "SELECT tier, count(*), sum(balance) FROM fv.accounts GROUP BY tier" > $OUT/e_probe.txt 2>&1; echo "probe exit $?"
cat $OUT/e_probe.txt | cut -c1-250
bash $FV/scripts/check_all.sh e

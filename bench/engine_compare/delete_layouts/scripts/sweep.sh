#!/bin/bash
# sweep.sh : every variant, threads=1 on core 0 then threads=2 on cores 0-1
cd "$(dirname "$0")/.." || exit 1
V="0_baseline 0_baseline_cast 1_not_exists 1_anti_join 1_left_join_null 1_temp_table_anti 1_temp_table_notin 1_temp_table_pk 2_compacted 2_compacted_sorted 3_ducklake_deletes 3_ducklake_merged 3_ducklake_rewritten 4_iceberg_v2_posdeletes 4_iceberg_v3_dv 5_part16 5_part64 6_live_mask_positional"
for v in $V; do taskset -c 0 python3 scripts/run.py $v 1 2>&1 | tail -1; done
for v in $V; do taskset -c 0-1 python3 scripts/run.py $v 2 2>&1 | tail -1; done
taskset -c 0 python3 scripts/run.py 5_part16_skew_hypothetical 1 --noverify 2>&1 | tail -1
taskset -c 0-1 python3 scripts/run.py 5_part16_skew_hypothetical 2 --noverify 2>&1 | tail -1

#!/bin/bash
# Item 4: the status read at HEAD and at 39702d4. Each tree writes 2,000,000 rows through the
# storage package (TestItem4StatusFile: New, Apply, Compact), then the base file is inspected
# and timed with python duckdb. The baseline is a temporary worktree of 39702d4.
#   usage: item4_status.sh BASELINE_WORKTREE_GO_DIR
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
GO_HEAD=$(cd "$HERE/../.." && pwd)
BASE_GO=${1:?the go/ directory of a worktree at 39702d4}
ROOT=/var/lib/postgresql/qslake2/item4
rm -rf "$ROOT/head" "$ROOT/base"; mkdir -p "$ROOT/head" "$ROOT/base"
( cd "$GO_HEAD" && QS_ITEM4_OUT=$ROOT/head go test ./internal/mirror -run TestItem4StatusFile -count=1 -v ) | tail -2
( cd "$BASE_GO" && QS_ITEM4_OUT=$ROOT/base go test ./internal/mirror -run TestItem4StatusFile -count=1 -v ) | tail -2
H=$(ls "$ROOT"/head/public.t/base/*.parquet)
B=$(ls "$ROOT"/base/public.t/base/*.parquet)
python3 -I "$HERE/item4_time.py" 5 39702d4="$B" HEAD="$H"

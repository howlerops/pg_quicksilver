#!/usr/bin/env bash
# Run the suite inside the testbed image. Needs only Docker.
#
#   bash bench/docker/testbed.sh              # correctness, no pg_duckdb build
#   bash bench/docker/testbed.sh --serving    # + build pg_duckdb and run serving
#   bash bench/docker/testbed.sh --full       # + the performance sections
#   bash bench/docker/testbed.sh --shell      # a prompt inside the image
#
# WHAT THIS DOES NOT COVER, and it is the important half: nothing here touches a
# kubelet, so the two claims that need a real cluster — a Pod being ROLLED, and
# a Pod being REMOVED from a Service because the mirror's readiness probe failed
# — cannot be made from this script. Those are bench/docker/cluster_kind.sh.
# HANDOFF.md says which is which and why it matters.
set -uo pipefail

REPO=$(cd "$(dirname "$0")/../.." && pwd)
cd "$REPO"

IMAGE=${IMAGE:-qs-testbed}
TARGET=base
SUITE_ENV=()
WANT_SERVING=0
SHELL_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --serving) WANT_SERVING=1 ;;
    --full)    WANT_SERVING=1; SUITE_ENV+=(-e FULL=1) ;;
    --shell)   SHELL_ONLY=1 ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

engine=$(command -v docker || command -v podman) || {
  echo "no docker or podman on PATH — this script needs one of them"; exit 2; }
engine=$(basename "$engine")

if ! $engine info >/dev/null 2>&1; then
  echo "FAIL: '$engine info' does not work. The daemon is not reachable from here."
  echo "On a rootless or sandboxed host this is usually the whole problem; see"
  echo "HANDOFF.md, 'What a session needs to be able to do'."
  exit 2
fi

# The serving section needs the patched pg_duckdb, and building it compiles
# DuckDB: about an hour the first time and nothing afterwards, because it is its
# own layer. It is opt-in for exactly that reason, and suite.sh is told to SKIP
# the section rather than report INCOMPLETE when it was never asked for —
# an INCOMPLETE would be honest but would take the whole run down with it.
if [ "$WANT_SERVING" = "1" ]; then
  TARGET=testbed
else
  SUITE_ENV+=(-e SKIP=serving)
fi

echo "=== building $IMAGE:$TARGET (target=$TARGET) ==="
$engine build -f bench/docker/Dockerfile.testbed --target "$TARGET" -t "$IMAGE:$TARGET" . \
  || { echo "FAIL: image build"; exit 1; }

if [ "$SHELL_ONLY" = "1" ]; then
  exec $engine run --rm -it -v "$REPO:/repo" "$IMAGE:$TARGET" /bin/bash
fi

# --privileged, and it is worth saying why rather than leaving it to be
# discovered. The suite starts real PostgreSQL clusters, and image_e2e.sh runs
# the SHIPPED container images, which needs a usable container engine inside.
# The socket mount is what gives it one; without it the images section reports
# INCOMPLETE, which is honest and is not a pass.
sock=/var/run/docker.sock
mounts=(-v "$REPO:/repo")
[ -S "$sock" ] && mounts+=(-v "$sock:$sock")

echo
echo "=== running the suite ==="
$engine run --rm --privileged "${mounts[@]}" -w /repo \
  "${SUITE_ENV[@]}" \
  "$IMAGE:$TARGET" \
  bash -lc '
    rc=0
    bash bench/scripts/suite.sh || rc=$?
    if [ "$rc" = "2" ]; then
      echo "::INCOMPLETE:: a section could not run, which is NOT a pass"
    fi
    exit "$rc"
  '
rc=$?

echo
case $rc in
  0) echo "PASS — the suite agreed." ;;
  2) echo "INCOMPLETE — a section could not run. That is not a failure and it is"
     echo "not a pass: something it needed was missing. The section's own last"
     echo "line says what." ;;
  *) echo "FAIL — a section ran and disagreed (exit $rc)." ;;
esac
exit $rc

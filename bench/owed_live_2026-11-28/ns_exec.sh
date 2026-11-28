#!/usr/bin/env bash
# ns_exec.sh REAL_DIR SEEN_PATH CMD [ARGS...]
# Runs CMD in a private mount namespace where REAL_DIR is bind-mounted over SEEN_PATH, as two
# Pods each see their own mirror at the same path. Processes started here (a standby's server,
# its backends, its sidecar) keep the view. Nothing is mounted in the main namespace.
set -uo pipefail
src=$1; seen=$2; shift 2
exec unshare -m --propagation private bash -c 'mount --bind "$0" "$1" && shift && exec "$@"' "$src" "$seen" "$@"

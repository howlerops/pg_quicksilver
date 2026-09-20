# synchronized_standby_slots, set and — the part that was missing — put back.
#
# A logical slot with failover=true does not advance while
# synchronized_standby_slots names a slot that has not confirmed. If the named
# slot does not EXIST, it never confirms, so decoding stops forever. PostgreSQL
# logs a warning on the primary and returns no error to any client: the stream
# stays connected, the snapshot succeeds, /readyz passes, and the mirror simply
# never gains a row. It is indistinguishable from an idle source. See docs/15.
#
# Four scripts here set the GUC and none of them cleared it. ALTER SYSTEM writes
# postgresql.auto.conf, so the setting outlived the run, the standby slot it
# named did not, and the next script to use a failover slot on the same primary
# stalled silently. That is how rendering_fidelity.sh came to report DIVERGED
# with a mirror holding exactly its bootstrap rows and nothing after them.
#
# The tell, if it happens again: an ordinary logical slot works fine on the same
# server, because only failover slots wait. A quick check with pg_recvlogical
# comes back healthy and sends the investigation somewhere else.

# qs_set_sync_slots <port> <slot-name>
qs_set_sync_slots() {
  su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d postgres -Atc \
    \"ALTER SYSTEM SET synchronized_standby_slots = '$2'\"" >/dev/null 2>&1
  su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d postgres -Atc \
    'SELECT pg_reload_conf()'" >/dev/null 2>&1
}

# qs_clear_sync_slots <port> — call from the EXIT trap of anything that set it.
qs_clear_sync_slots() {
  su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d postgres -Atc \
    \"ALTER SYSTEM SET synchronized_standby_slots = ''\"" >/dev/null 2>&1
  su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d postgres -Atc \
    'SELECT pg_reload_conf()'" >/dev/null 2>&1
  return 0
}

# qs_assert_sync_slots_sane <port>
#
# For scripts that do NOT set the GUC but do use a failover slot. Prints the
# diagnosis and returns 1 if the primary names a slot that does not exist, so
# the caller can fail with a reason instead of timing out on an empty mirror.
qs_assert_sync_slots_sane() {
  local port=$1 raw name n missing=""
  raw=$(su postgres -c "$PG/psql -h /tmp -p $port -U postgres -d postgres -Atc \
    'SHOW synchronized_standby_slots'" 2>/dev/null)
  [ -z "$raw" ] && return 0
  for name in ${raw//,/ }; do
    n=$(su postgres -c "$PG/psql -h /tmp -p $port -U postgres -d postgres -Atc \
      \"SELECT count(*) FROM pg_replication_slots WHERE slot_name='$name'\"" 2>/dev/null)
    [ "$n" = "0" ] && missing="$missing $name"
  done
  [ -z "$missing" ] && return 0
  printf '  synchronized_standby_slots on :%s names a slot that does not exist:%s\n' "$port" "$missing"
  printf '  A failover logical slot will connect, pass readiness and never advance.\n'
  printf '  Left behind by an earlier benchmark run. Clear it with:\n'
  printf "    ALTER SYSTEM SET synchronized_standby_slots = ''; SELECT pg_reload_conf();\n"
  return 1
}

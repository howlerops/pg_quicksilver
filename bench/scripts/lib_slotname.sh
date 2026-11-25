# The replication slot a sidecar REALLY uses, for scripts that have to name it.
#
# QS_SLOT stopped being the slot's name when slots became one per Pod: the
# sidecar derives the slot from QS_SLOT and QS_POD_NAME (podSlotName in
# go/cmd/qs-mirror/slotname.go). Every script here sets both, and the ones that
# then dropped, terminated or measured "their" slot went on using the configured
# name — a slot that no longer exists. `pg_drop_replication_slot(...) WHERE
# slot_name='qs_e2e'` matches no row and returns no error, so the reset each of
# them relied on quietly stopped resetting anything.
#
# This mirrors podSlotName. Keep the two in step.

# qs_pod_slot <configured-slot> <pod-name>
qs_pod_slot() {
  local base=$1 pod=${2:-} name tag
  name=$(printf '%s' "$base" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_]/_/g')
  if [ -n "$pod" ]; then
    name+="_$(printf '%s' "$pod" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9_]/_/g')"
  fi
  # A Pod name outside [a-z0-9-] could fold onto another's slot, and a name
  # over 63 bytes has to be cut: both get a hash of the unsanitised pair.
  if [ ${#name} -gt 63 ] || ! [[ $pod =~ ^[a-z0-9-]*$ ]]; then
    tag=_$(printf '%s\0%s' "$base" "$pod" | sha256sum | cut -c1-8)
    name=${name:0:$((63 - ${#tag}))}$tag
  fi
  printf '%s\n' "$name"
}

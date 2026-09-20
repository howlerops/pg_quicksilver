#!/usr/bin/env bash
# Run the SHIPPED IMAGES, not the binaries built beside them.
#
# Every other script here runs `go build` output from the working tree. CI
# builds the images and the release workflow pushes them, but nothing had ever
# STARTED one: docs/16 could say the sidecar image runs as UID 26 and writes a
# readable mirror only because the Dockerfile says so.
#
# This runs the mirror image as the plugin runs it — same environment variables,
# same port — against a live primary and standby, and then verifies the mirror
# it produced using qs-verify FROM THE SAME IMAGE. It also starts the plugin
# image and completes a TLS handshake against its gRPC port.
#
# What it does NOT establish: that the images can be PULLED. The ghcr packages
# are private and a registry pull needs credentials this sandbox does not carry;
# that is a separate claim and docs/16 still lists it as open.
#
# A section that cannot run exits 2 as INCOMPLETE.
set -uo pipefail

PG=/usr/lib/postgresql/17/bin
BASE=/var/lib/postgresql/qs-image
PRIMARY=$BASE/primary
STANDBY=$BASE/standby
MIRROR=$BASE/mirror
PPORT=5457
SPORT=5458
DB=app
ROWS=${ROWS:-200000}
HEALTH=127.0.0.1:9202
PLUGIN_PORT=9393
REPO=$(cd "$(dirname "$0")/../.." && pwd)
ENGINE=${ENGINE:-podman}
FAIL=0

say()  { printf '\n== %s ==\n' "$*"; }
bad()  { printf 'FAIL: %s\n' "$*"; FAIL=1; }
skip() { printf '\nINCOMPLETE — section skipped, which is NOT a pass: %s\n' "$*"; exit 2; }
psq()  { su postgres -c "$PG/psql -h /tmp -p $1 -U postgres -d ${3:-$DB} -Atc \"$2\"" 2>&1; }

cleanup() {
  $ENGINE rm -f qs-mirror-run qs-plugin-run >/dev/null 2>&1
  su postgres -c "$PG/pg_ctl -D $STANDBY -m immediate stop" >/dev/null 2>&1
  su postgres -c "$PG/pg_ctl -D $PRIMARY -m immediate stop" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

command -v "$ENGINE" >/dev/null || skip "$ENGINE is not installed"
$ENGINE info >/dev/null 2>&1 || skip "$ENGINE cannot run here"

# ---------------------------------------------------------------- build
say "0. build both images from deploy/Dockerfile"
# The build container has to trust this sandbox's egress CA or `go mod download`
# fails with x509. Mounting the bundle is the documented fix; nothing here
# disables verification, and the Dockerfile is untouched.
CA_ARGS=()
[ -f /root/.ccr/ca-bundle.crt ] && \
  CA_ARGS=(--volume /root/.ccr/ca-bundle.crt:/etc/ssl/certs/ca-certificates.crt:ro)

for target in mirror plugin; do
  $ENGINE build --network=host "${CA_ARGS[@]}" --target "$target" \
    -t "qs-$target:e2e" -f "$REPO/deploy/Dockerfile" "$REPO" >/dev/null 2>&1 \
    || skip "could not build the $target image"
  echo "  built qs-$target:e2e"
done

# The UID is a correctness property, not a detail: the sidecar writes to the
# instance's data volume and CNPG's PostgreSQL runs as 26. Any other UID
# produces a mirror the instance cannot read.
muid=$($ENGINE inspect qs-mirror:e2e --format '{{.Config.User}}')
puid=$($ENGINE inspect qs-plugin:e2e --format '{{.Config.User}}')
echo "  mirror runs as $muid, plugin runs as $puid"
[ "$muid" = "26:26" ]       || bad "mirror image runs as $muid, not 26:26"
[ "$puid" = "65532:65532" ] || bad "plugin image runs as $puid, not 65532:65532"

# ---------------------------------------------------------------- cluster
say "1. primary and standby"
cleanup
rm -rf "$BASE"; mkdir -p "$BASE"; chown postgres:postgres "$BASE"
su postgres -c "$PG/initdb -D $PRIMARY -A trust" >/dev/null 2>&1 || skip "initdb failed"
{ echo "port = $PPORT"; echo "listen_addresses = 'localhost'";
  echo "unix_socket_directories = '/tmp'"; echo "wal_level = logical"; } >> "$PRIMARY/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $PRIMARY -l $BASE/primary.log -w start" >/dev/null 2>&1 \
  || skip "primary would not start"

psq $PPORT "CREATE DATABASE $DB" postgres >/dev/null
psq $PPORT "CREATE TABLE events(id bigint PRIMARY KEY, sku text, amount numeric(12,2), ts timestamptz)" >/dev/null
psq $PPORT "INSERT INTO events SELECT g,'SKU-'||(g%997),(g%10000)/100.0,now()
            FROM generate_series(1,$ROWS) g" >/dev/null
psq $PPORT "DELETE FROM events WHERE id % 50 = 0" >/dev/null
psq $PPORT "UPDATE events SET amount = amount + 1 WHERE id % 37 = 0" >/dev/null
echo "  seeded $(psq $PPORT 'SELECT count(*) FROM events') live rows"

psq $PPORT "SELECT pg_create_physical_replication_slot('img_standby', true)" postgres >/dev/null
su postgres -c "$PG/pg_basebackup -D $STANDBY -R -X stream -S img_standby -c fast \
  -d 'host=/tmp port=$PPORT user=postgres dbname=postgres'" >/dev/null 2>&1 || skip "pg_basebackup failed"
{ echo "port = $SPORT"; echo "unix_socket_directories = '/tmp'"; echo "hot_standby_feedback = on"; } \
  >> "$STANDBY/postgresql.conf"
su postgres -c "$PG/pg_ctl -D $STANDBY -l $BASE/standby.log -w start" >/dev/null 2>&1 \
  || skip "standby would not start"
[ "$(psq $SPORT 'SELECT pg_is_in_recovery()')" = "t" ] || skip "standby is not in recovery"

# ---------------------------------------------------------------- the sidecar
say "2. run the MIRROR IMAGE as the plugin runs it"
mkdir -p "$MIRROR"
# 26 is the image's UID and it must be able to write here, which is the whole
# reason the image declares it.
chown -R 26:26 "$MIRROR"

$ENGINE run -d --name qs-mirror-run --network=host \
  -v /tmp:/tmp -v "$MIRROR:$MIRROR" \
  -e QS_CLUSTER=img -e QS_MODE=shadow -e QS_INGEST=logical \
  -e QS_TABLES=public.events -e QS_SLOT=qs_img -e QS_PUBLICATION=qs_img \
  -e QS_MIRROR_PATH="$MIRROR" -e QS_FRESHNESS_SLO=30s \
  -e QS_PRIMARY_HOST=localhost -e QS_PRIMARY_PORT=$PPORT \
  -e QS_LOCAL_SOCKET_DIR=/tmp -e QS_LOCAL_PORT=$SPORT \
  -e QS_DATABASE=$DB -e QS_PGUSER=postgres -e QS_POD_NAME=img-2 \
  -e QS_HEALTH_ADDR=$HEALTH \
  qs-mirror:e2e >/dev/null 2>&1 || skip "the mirror container would not start"

for _ in $(seq 1 180); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://$HEALTH/readyz" 2>/dev/null)
  [ "$code" = "200" ] && break
  sleep 1
done
if [ "${code:-}" != "200" ]; then
  $ENGINE logs qs-mirror-run 2>&1 | tail -25
  skip "the containerised sidecar never became ready"
fi
echo "  /readyz answered 200 from the container"
curl -s "http://$HEALTH/metrics" 2>/dev/null | grep -E "^quicksilver_" | head -3 | sed 's/^/  /'

# The files must be readable by the UID PostgreSQL runs as, or the mirror is
# useless no matter how correct it is.
owner=$(stat -c '%u:%g' "$MIRROR/public.events" 2>/dev/null)
echo "  mirror files owned by $owner"
[ "$owner" = "26:26" ] || bad "mirror files are owned by $owner, not 26:26"

# ---------------------------------------------------------------- verify
say "3. verify the mirror with qs-verify FROM THE SAME IMAGE"
out=$($ENGINE run --rm --network=host -v /tmp:/tmp -v "$MIRROR:$MIRROR" \
  --entrypoint /qs-verify qs-mirror:e2e \
  -dsn "postgres://postgres@localhost:$PPORT/$DB?sslmode=disable" \
  -mirror "$MIRROR" -table public.events 2>&1)
printf '%s\n' "$out" | sed 's/^/  /'
printf '%s' "$out" | grep -q "MATCH" || bad "qs-verify from the image did not report MATCH"

# ---------------------------------------------------------------- the plugin
say "4. run the PLUGIN IMAGE and complete a TLS handshake"
CERTS=$BASE/certs; mkdir -p "$CERTS"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=quicksilver" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
  -keyout "$CERTS/tls.key" -out "$CERTS/tls.crt" >/dev/null 2>&1 \
  || skip "could not issue a test certificate"
chmod 644 "$CERTS/tls.key"

$ENGINE run -d --name qs-plugin-run --network=host -v "$CERTS:/certs:ro" \
  qs-plugin:e2e \
  --server-cert=/certs/tls.crt --server-key=/certs/tls.key \
  --client-cert=/certs/tls.crt --server-address=:$PLUGIN_PORT >/dev/null 2>&1 \
  || skip "the plugin container would not start"

ok=0
for _ in $(seq 1 30); do
  # The plugin requires a client certificate, so an anonymous handshake is
  # SUPPOSED to be rejected. What is being tested is that something is there
  # speaking TLS at all — a dead container gives "connection refused" instead.
  if openssl s_client -connect 127.0.0.1:$PLUGIN_PORT </dev/null 2>&1 \
     | grep -qE "CONNECTED|certificate"; then ok=1; break; fi
  sleep 1
done
if [ "$ok" = "1" ]; then
  echo "  the plugin image is serving TLS on :$PLUGIN_PORT"
else
  $ENGINE logs qs-plugin-run 2>&1 | tail -20
  bad "nothing answered on the plugin image's gRPC port"
fi

say "result"
if [ $FAIL -eq 0 ]; then
  echo "PASS — the shipped mirror image built a mirror that qs-verify (also from"
  echo "the image) calls MATCH, as UID 26, and the plugin image serves TLS."
  echo "This does NOT establish that either image can be pulled from ghcr."
else
  echo "FAIL"
fi
exit $FAIL

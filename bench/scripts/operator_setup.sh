#!/usr/bin/env bash
# Stand up as much of Kubernetes as this sandbox can run, for operator_e2e.sh.
#
# The limit, found the hard way and worth writing down so nobody spends the
# afternoon again:
#
#   runc sets a pod sandbox's oom_score_adj to -998. A process may only RAISE
#   oom_score_adj unless it holds CAP_SYS_RESOURCE, and this sandbox does not
#   have that capability in its BOUNDING set, so it cannot be acquired at all:
#
#       CapBnd: 000001fffeffffff     <- bit 24, CAP_SYS_RESOURCE, is clear
#       $ echo -998 > /proc/self/oom_score_adj
#       bash: echo: write error: Permission denied
#
#   Every pod therefore dies in nsexec with "failed to update
#   /proc/self/oom_score_adj: Permission denied", which containerd reports as
#   the far less helpful "can't get final child's PID from pipe: EOF". It
#   happens identically under kind and under k3s, because it is neither's fault.
#
#   containerd's restrict_oom_score_adj is exactly the setting for this, and
#   containerd 2.2.2 does not honour it for pod sandboxes: it is present in the
#   binary, accepted in the config at the path `containerd config default`
#   prints, and `crictl info` still reports restrictOOMScoreAdj false.
#
# So: the control plane runs, pods do not. That is enough for a plugin, because
# a plugin's whole job happens while the operator BUILDS a pod spec.
#
#   bash bench/scripts/operator_setup.sh
set -uo pipefail

CNPG_VERSION=${CNPG_VERSION:-1.30.0}
CNPG_BRANCH=${CNPG_BRANCH:-release-1.30}
KUBECTL_VERSION=${KUBECTL_VERSION:-v1.34.1}
say() { printf '\n=== %s ===\n' "$*"; }
die() { printf 'error: %s\n' "$*"; exit 2; }

say "kubectl"
if ! command -v kubectl >/dev/null; then
  curl -fsSL -o /usr/local/bin/kubectl \
    "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl" || die "kubectl download failed"
  chmod +x /usr/local/bin/kubectl
fi
echo "  $(kubectl version --client=true 2>/dev/null | head -1)"

say "k3s"
if ! command -v k3s >/dev/null; then
  curl -sfL https://get.k3s.io -o /tmp/k3s-install.sh || die "k3s installer download failed"
  INSTALL_K3S_SKIP_START=true INSTALL_K3S_SKIP_ENABLE=true sh /tmp/k3s-install.sh >/dev/null \
    || die "k3s install failed"
fi
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
if ! kubectl get nodes >/dev/null 2>&1; then
  mkdir -p /var/log/qs
  # --kubelet-arg=fail-cgroupv1=false: this host is on cgroup v1 and a modern
  # kubelet refuses to start on it outright. The flag is the documented escape
  # hatch and is the difference between a control plane and nothing at all.
  #
  # setsid, and no pkill: "pkill -f 'k3s server'" matches the shell running it.
  setsid k3s server --disable traefik --disable metrics-server --disable servicelb \
    --write-kubeconfig-mode 644 --kubelet-arg=fail-cgroupv1=false \
    > /var/log/qs/k3s.log 2>&1 < /dev/null &
  disown
  for _ in $(seq 60); do kubectl get nodes 2>/dev/null | grep -q " Ready" && break; sleep 3; done
fi
kubectl get nodes 2>&1 | tail -2
kubectl get nodes 2>/dev/null | grep -q " Ready" || die "k3s did not become ready; see /var/log/qs/k3s.log"

say "CloudNativePG $CNPG_VERSION CRDs and webhook configurations"
kubectl apply --server-side --force-conflicts \
  -f "https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/$CNPG_BRANCH/releases/cnpg-$CNPG_VERSION.yaml" \
  >/dev/null || die "CNPG manifest apply failed"
echo "  $(kubectl get crd -o name | grep -c cnpg.io) cnpg.io CRDs installed"

say "the operator binary, out of the released image"
# docker works here — ONE level of container nesting does. It is the second
# level, a pod inside it, that does not.
if ! command -v docker >/dev/null; then die "docker is required to extract the operator binary"; fi
docker info >/dev/null 2>&1 || {
  mkdir -p /var/log/qs
  setsid dockerd --iptables=false --ip6tables=false > /var/log/qs/dockerd.log 2>&1 < /dev/null &
  disown
  for _ in $(seq 30); do docker info >/dev/null 2>&1 && break; sleep 2; done
}
docker info >/dev/null 2>&1 || die "dockerd would not start; see /var/log/qs/dockerd.log"

IMG="ghcr.io/cloudnative-pg/cloudnative-pg:$CNPG_VERSION"
docker pull -q "$IMG" >/dev/null || die "could not pull $IMG"
cid=$(docker create "$IMG")
docker cp "$cid:/manager/manager" /tmp/cnpg-manager 2>/dev/null \
  || docker cp "$cid:/manager" /tmp/cnpg-manager 2>/dev/null \
  || die "could not find the manager binary in the image"
# The operator discovers which architectures it can run by globbing
# "operator/manager_*" RELATIVE to its working directory, and refuses every
# status update with "invalid architecture: amd64" when that finds nothing.
# operator_e2e.sh runs it with cwd=/, so the files belong in /operator.
mkdir -p /operator
docker cp "$cid:/operator/manager_amd64" /operator/manager_amd64 2>/dev/null \
  || die "could not extract /operator/manager_amd64"
docker rm -f "$cid" >/dev/null 2>&1
chmod +x /tmp/cnpg-manager
echo "  $(/tmp/cnpg-manager version 2>&1 | head -1)"

printf '\n========================================\nREADY — now run: bash bench/scripts/operator_e2e.sh\n'

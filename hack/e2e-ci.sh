#!/usr/bin/env bash

# Copyright AppsCode Inc. and Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Smoke test for the fargocd controller. Assumes a kind (or any other)
# cluster is already reachable via $KUBECONFIG.
#
# Steps:
#   1. Install the FluxCD HelmRelease + HelmRepository CRDs and the Argo
#      CD Application CRD.
#   2. Create the `argocd` namespace and a Service labelled
#      `app.kubernetes.io/name=argocd-server` so fargocd's namespace
#      auto-discovery succeeds without a real Argo CD running.
#   3. Build the controller and start it in the background pointed at the
#      cluster.
#   4. Apply a sample HelmRepository + HelmRelease.
#   5. Assert that an Application is created in `argocd`, with the
#      expected source and destination.

set -eou pipefail

# Versions match the controller's vendored API surface.
FLUX_HELM_CTL_VERSION=${FLUX_HELM_CTL_VERSION:-v1.2.0}
FLUX_SRC_CTL_VERSION=${FLUX_SRC_CTL_VERSION:-v1.5.0}
ARGO_CD_VERSION=${ARGO_CD_VERSION:-v3.0.11}

# Where this script's fixtures live.
FIXTURES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.github/e2e" && pwd)"

log() { printf '\n=== %s ===\n' "$*" >&2; }

log "Installing CRDs"
kubectl apply -f "https://github.com/fluxcd/helm-controller/raw/${FLUX_HELM_CTL_VERSION}/config/crd/bases/helm.toolkit.fluxcd.io_helmreleases.yaml"
kubectl apply -f "https://github.com/fluxcd/source-controller/raw/${FLUX_SRC_CTL_VERSION}/config/crd/bases/source.toolkit.fluxcd.io_helmrepositories.yaml"
kubectl apply -f "https://raw.githubusercontent.com/argoproj/argo-cd/${ARGO_CD_VERSION}/manifests/crds/application-crd.yaml"

log "Preparing argocd namespace + sentinel Service"
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -
cat <<'YAML' | kubectl apply -f -
apiVersion: v1
kind: Service
metadata:
  name: argocd-server
  namespace: argocd
  labels:
    app.kubernetes.io/name: argocd-server
spec:
  selector:
    app.kubernetes.io/name: argocd-server
  ports:
    - name: http
      port: 80
      targetPort: 8080
YAML

log "Building fargocd"
mkdir -p ./bin
CGO_ENABLED=0 GOFLAGS="-mod=vendor" go build -o ./bin/fargocd ./cmd/fargocd

log "Starting fargocd in background (in-cluster mode)"
# We point fargocd at the cluster via $KUBECONFIG. argo-namespace is
# omitted so namespace auto-discovery is exercised end-to-end.
./bin/fargocd run \
  --mode=in-cluster \
  --metrics-bind-address=0 \
  --health-probe-bind-address=0 \
  --argo-dest-server=https://kubernetes.default.svc \
  --argo-project=default \
  >/tmp/fargocd.log 2>&1 &
FARGOCD_PID=$!
trap 'kill ${FARGOCD_PID} 2>/dev/null || true; echo "--- fargocd log tail ---"; tail -n 200 /tmp/fargocd.log || true' EXIT

log "Applying fixtures"
kubectl create namespace flux-system --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "${FIXTURES_DIR}/helmrepository.yaml"
kubectl apply -f "${FIXTURES_DIR}/helmrelease.yaml"

log "Waiting for Application to appear in argocd"
for i in $(seq 1 60); do
  if kubectl get application e2e-demo -n argocd >/dev/null 2>&1; then
    echo "Application e2e-demo found after ${i} attempts"
    break
  fi
  if ! kill -0 "${FARGOCD_PID}" 2>/dev/null; then
    echo "::error::fargocd process exited prematurely"
    exit 1
  fi
  sleep 2
done

if ! kubectl get application e2e-demo -n argocd >/dev/null 2>&1; then
  echo "::error::Application e2e-demo was not created within timeout"
  exit 1
fi

log "Validating Application contents"
kubectl get application e2e-demo -n argocd -o yaml

# Required fields. We don't assert on ignoreDifferences (it depends on
# chart-render success which needs network); a separate integration test
# in pkg/ignoregen covers that path.
got_chart=$(kubectl get application e2e-demo -n argocd -o jsonpath='{.spec.source.chart}')
got_dest_ns=$(kubectl get application e2e-demo -n argocd -o jsonpath='{.spec.destination.namespace}')
got_dest_server=$(kubectl get application e2e-demo -n argocd -o jsonpath='{.spec.destination.server}')
got_backlink=$(kubectl get application e2e-demo -n argocd -o jsonpath='{.metadata.annotations.fargocd\.appscode\.com/helmrelease}')

[ "${got_chart}" = "cert-manager" ] || { echo "::error::expected chart=cert-manager got '${got_chart}'"; exit 1; }
[ "${got_dest_ns}" = "cert-manager" ] || { echo "::error::expected destination.namespace=cert-manager got '${got_dest_ns}'"; exit 1; }
[ "${got_dest_server}" = "https://kubernetes.default.svc" ] || { echo "::error::expected destination.server=https://kubernetes.default.svc got '${got_dest_server}'"; exit 1; }
[ "${got_backlink}" = "flux-system/e2e-demo" ] || { echo "::error::expected backlink=flux-system/e2e-demo got '${got_backlink}'"; exit 1; }

log "Validating finalizer was added to the HelmRelease"
got_finalizer=$(kubectl get helmrelease e2e-demo -n flux-system -o jsonpath='{.metadata.finalizers[?(@=="fargocd.appscode.com/finalizer")]}')
[ -n "${got_finalizer}" ] || { echo "::error::fargocd.appscode.com/finalizer not present"; exit 1; }

log "Deleting HelmRelease — Application must be garbage-collected"
kubectl delete helmrelease e2e-demo -n flux-system --wait=true
for i in $(seq 1 30); do
  if ! kubectl get application e2e-demo -n argocd >/dev/null 2>&1; then
    echo "Application e2e-demo cleaned up after ${i} attempts"
    break
  fi
  sleep 2
done

if kubectl get application e2e-demo -n argocd >/dev/null 2>&1; then
  echo "::error::Application e2e-demo was not cleaned up"
  exit 1
fi

log "e2e smoke test passed"

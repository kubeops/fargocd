# AGENTS.md

This file provides guidance to coding agents (e.g. Claude Code, claude.ai/code) when working with code in this repository.

## Repository purpose

Go module `kubeops.dev/fargocd` — a lightweight ArgoCD-style installer for the AppsCode Container Engine (ACE) "feature set" charts, built on **FluxCD** primitives. The controller watches FluxCD `HelmRelease` resources and reconciles a curated catalog of ACE Helm releases (OpsCenter Core / Backup / Datastore / Observability, plus the `ace` umbrella) into the target cluster. Effectively "ArgoCD for ACE" — but using `helm-controller` and `source-controller` from FluxCD instead of running a separate ArgoCD instance.

Pre-req CRDs (per `README.md`):
- `helm.toolkit.fluxcd.io_helmreleases.yaml` (fluxcd helm-controller)
- `source.toolkit.fluxcd.io_helmrepositories.yaml` (fluxcd source-controller)

The produced binary is `fargocd`.

## Architecture

- `cmd/fargocd/` — entry point.
- `pkg/cmds/`:
  - `root.go` — Cobra root.
  - `run.go` — long-running operator.
  - `completion.go` — shell completion.
- `pkg/controller/helmrelease_controller.go` — controller-runtime reconciler watching `helm.toolkit.fluxcd.io/HelmRelease`. Drives the FluxCD `HelmRelease` lifecycle for the configured feature-set releases.
- `pkg/featuresets/` — **embedded ACE feature-set definitions** that the controller materializes as `HelmRelease`s:
  - `lib.go` — feature-set library (loader, accessor).
  - `ace.yaml` — the top-level ACE feature-set descriptor.
  - `opscenter-core/`, `opscenter-backup/`, `opscenter-datastore/`, `opscenter-observability/` — per-stack descriptors.
- `Dockerfile.in` (PROD, distroless), `Dockerfile.dbg` (debian), `Dockerfile.ubi` (Red Hat certified) — three image variants.
- `hack/`, `Makefile` — AppsCode build harness.
- `vendor/` — checked-in deps.

API types come from FluxCD (`helm.toolkit.fluxcd.io`, `source.toolkit.fluxcd.io`) and are pinned in `vendor/`.

## Common commands

All Make targets run inside `ghcr.io/appscode/golang-dev` — Docker must be running.

- `make ci` — CI pipeline.
- `make build` / `make all-build` — build host or all-platform binaries.
- `make fmt`, `make lint`, `make unit-tests` / `make test` — standard.
- `make verify` — `verify-gen verify-modules`; `go mod tidy && go mod vendor` must leave the tree clean.
- `make container` — build PROD, DBG, and UBI images.
- `make push` — push all three; `make docker-manifest` writes multi-arch manifests; `make release` is the full publish flow.
- `make push-to-kind` / `make deploy-to-kind` — load into Kind and Helm-install.
- `make install` / `make uninstall` / `make purge` — Helm install lifecycle.
- `make add-license` / `make check-license` — manage license headers.

Run a single Go test (requires a local Go toolchain):

```
go test ./pkg/controller/... -run TestName -v
```

## Conventions

- Module path is `kubeops.dev/fargocd` (vanity URL). Imports must use that.
- License: `LICENSE` (Apache-2.0); new files need the standard AppsCode header (`make add-license`).
- Sign off commits (`git commit -s`); contributions follow the DCO.
- Vendor directory is checked in — `go mod tidy && go mod vendor` must leave the tree clean (enforced by `verify-modules`).
- This operator is **opinionated about FluxCD** — it talks to `helm.toolkit.fluxcd.io/HelmRelease`, not ArgoCD `Application`s. Don't introduce ArgoCD APIs as a parallel reconciliation path.
- Feature-set definitions live under `pkg/featuresets/<name>/` and are loaded via `lib.go`. To add a new featureset: drop a new directory + YAML and register it; don't hand-write `HelmRelease`s in the controller.
- The pre-req CRDs (FluxCD helm-controller + source-controller) are runtime dependencies — document version constraints in the `README.md` when bumping their vendored API versions.
- Three Dockerfiles, one binary — keep `Dockerfile.in`, `Dockerfile.dbg`, and `Dockerfile.ubi` in sync.

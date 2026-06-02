# AGENTS.md

This file provides guidance to coding agents (e.g. Claude Code, claude.ai/code) when working with code in this repository.

## Repository purpose

Go module `kubeops.dev/fargocd` — a controller that bridges **FluxCD**
sources (`HelmRelease` + `HelmRepository`) into **Argo CD** `Application`
resources. The user authors workloads as FluxCD objects; fargocd projects
each `HelmRelease` into an Argo CD `Application` and Argo CD performs the
actual rollout. The bridge supports three deployment shapes: standard Argo
CD on the same cluster, and the `argocd-agent` "autonomous" and "managed"
modes (see https://argocd-agent.readthedocs.io/latest/concepts/agent-modes/).

A defining feature is automatic generation of `ignoreDifferences`: the
controller renders the chart twice with the Helm Go SDK (never the `helm`
CLI) and diffs the rendered manifests to populate
`Application.spec.ignoreDifferences` for fields that change every render
(CA bundles, generated certs, reload annotations, etc).

Pre-req CRDs (per `README.md`):
- `helm.toolkit.fluxcd.io_helmreleases.yaml` (fluxcd helm-controller)
- `source.toolkit.fluxcd.io_helmrepositories.yaml` (fluxcd source-controller)

The produced binary is `fargocd`.

## Architecture

- `cmd/fargocd/` — entry point.
- `pkg/cmds/`:
  - `root.go` — Cobra root.
  - `run.go` — long-running operator; owns flag parsing and manager wiring.
  - `completion.go` — shell completion.
- `pkg/controller/`:
  - `helmrelease_controller.go` — controller-runtime reconciler watching `helm.toolkit.fluxcd.io/HelmRelease`. Creates/updates the mirrored `argocd.argoproj.io/Application`.
  - `naming.go` — multi-cluster Application naming, including the ACE-chart exception.
  - `*_test.go` — fake-client unit tests (no envtest required).
- `pkg/mode/mode.go` — agent-mode enum (`in-cluster`, `autonomous`, `managed`) used by `cmds` and `controller`.
- `pkg/ignoregen/` — pulls a chart with the Helm SDK, renders it twice, diffs the manifests, and emits `argov1a1.ResourceIgnoreDifferences` rules. Exposes `DetectFn` as a test seam.
- `Dockerfile.in` (PROD, distroless), `Dockerfile.dbg` (debian), `Dockerfile.ubi` (Red Hat certified) — three image variants.
- `hack/`, `Makefile` — AppsCode build harness.
- `vendor/` — checked-in deps.
- `Design.md` — architecture overview, reconcile loop, agent modes.

API types come from FluxCD (`helm.toolkit.fluxcd.io`,
`source.toolkit.fluxcd.io`) and Argo CD (`argoproj.io/v1alpha1`) and are
pinned in `vendor/`.

## Common commands

All Make targets run inside `ghcr.io/appscode/golang-dev` — Docker must be
running. Day-to-day, the host Go toolchain works fine for `go build`,
`go test`, and `golangci-lint`.

- `make ci` — CI pipeline (verify, license check, lint, build).
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
GOFLAGS=-mod=vendor go test ./pkg/controller/... -run TestName -v
```

The `pkg/ignoregen` integration suite pulls real OCI charts from
`ghcr.io/appscode-charts` — skip with `-short` if you have no network.

## Conventions

- Module path is `kubeops.dev/fargocd` (vanity URL). Imports must use that.
- License: `LICENSE` (Apache-2.0); new files need the standard AppsCode header (`make add-license`).
- Sign off commits (`git commit -s`); contributions follow the DCO.
- Vendor directory is checked in — `go mod tidy && go mod vendor` must leave the tree clean (enforced by `verify-modules`).
- This operator is **opinionated about FluxCD** as input and **Argo CD** as output. Do not call the `helm` CLI; use the Helm Go SDK (`helm.sh/helm/v3`).
- Multi-cluster naming: when `--cluster-name` is set, the generated Application name is `<HR.name>-<cluster-name>`. The ACE umbrella chart is the only exception (`pkg/controller/naming.go`). When changing this rule, update both `naming_test.go` and `Design.md`.
- The pre-req CRDs (FluxCD helm-controller + source-controller) are runtime dependencies — document version constraints in the `README.md` when bumping their vendored API versions.
- Three Dockerfiles, one binary — keep `Dockerfile.in`, `Dockerfile.dbg`, and `Dockerfile.ubi` in sync.

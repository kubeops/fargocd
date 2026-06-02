# fargocd — Design

## Goal

Let users author their workload as FluxCD `HelmRelease` and `HelmRepository`
resources while having Argo CD do the actual rollouts, drift detection, and
multi-cluster routing.

The bridge is intentionally one-way: the source of truth is the FluxCD
objects on the workload cluster. fargocd derives Argo CD `Application`
resources from them. Changes made directly to those Applications are
overwritten on the next reconcile.

## Architecture

```
                 ┌──────────────────────────────────────────┐
                 │             workload cluster              │
                 │                                           │
   user/CD ──▶   │  HelmRelease  ──▶  fargocd               │
                 │  HelmRepository    (this controller)     │
                 │       │                  │                │
                 │       │                  │ controller-runtime
                 │       │                  ▼                │
                 │       │            (helm SDK render ×2)   │
                 │       │                  │                │
                 │       │                  ▼                │
                 │       │      Application (in argocd ns)   │
                 │       │                  │                │
                 │       │                  ▼                │
                 │       │      ┌──────────────────────┐    │
                 │       │      │  Argo CD / agent     │    │
                 │       │      └──────────────────────┘    │
                 │       │                  │                │
                 │       ▼                  ▼                │
                 │  HelmRelease.status   workload pods       │
                 │  (mirrored)                               │
                 └──────────────────────────────────────────┘
```

## Components

| Package | Responsibility |
| --- | --- |
| `cmd/fargocd` | Process entry point. |
| `pkg/cmds` | Cobra command tree (`run`, `version`, `completion`) and flag handling. |
| `pkg/controller` | The HelmRelease reconciler, finalizer logic, naming, and the watches that re-enqueue on Application/HelmRepository changes. |
| `pkg/mode` | The agent-mode enum and validation (`in-cluster` / `autonomous` / `managed`). |
| `pkg/ignoregen` | Pulls and renders a chart twice with the Helm Go SDK, diffs the rendered manifests, and emits `ignoreDifferences` rules. |

## Reconcile loop

For a given `HelmRelease`:

1. **Fetch** the `HelmRelease`. If it does not exist, return.
2. **Resolve Argo CD namespace** — either the `--argo-namespace` override
   or the first namespace that hosts a Service labelled
   `app.kubernetes.io/name=argocd-server`. If neither is present, requeue
   with a 30 s backoff. (Looking it up every reconcile is cheap on the
   informer cache and lets the operator survive Argo CD being installed
   later.)
3. **Deletion** — if `DeletionTimestamp` is set and the finalizer is
   present, delete the Application and remove the finalizer. The
   Application is looked up under the cluster-aware Application name (see
   below) so multi-cluster names do not orphan resources.
4. **Finalizer** — add the finalizer if missing and requeue. Subsequent
   reconciles see the finalizer and proceed.
5. **Suspend** — honour `spec.suspend`; do nothing further if set.
6. **Dependencies** — for every `spec.dependsOn` entry, check the
   corresponding Argo CD Application is Healthy. If any dependent is
   missing or not yet Healthy, requeue with a 30 s backoff.
7. **Render values** — combine `spec.values` and `spec.valuesFrom` via
   `github.com/fluxcd/pkg/chartutil`. Empty values are normalised to the
   empty string rather than `"{}\n"` so Argo CD honours chart defaults.
8. **Detect ignoreDifferences** — render the chart twice and diff (see
   below). Failures are logged but do not block reconciliation, so a
   transient registry hiccup will not stall sync.
9. **Create-or-patch Application** — using `controllerutil.CreateOrPatch`,
   with the `fargocd.appscode.com/helmrelease` annotation backlinking the
   originating HelmRelease and, in managed mode, the
   `argocd.argoproj.io/agent-name` label so the principal can route the
   Application to the right agent.
10. **Status mirror** — patch `HelmRelease.status.conditions` with:
    - `Ready` (from `Application.status.sync.status`),
    - `Reconciling` (from `Application.status.health.status`), and
    - one condition per `Application.status.conditions[]` entry,
      mirrored verbatim. Argo CD uses that array to surface things like
      `ComparisonError`, `InvalidSpecError`, `SyncError`, and the
      `SharedResource`/`Orphaned`/`Excluded`/`Repeated` resource
      warnings; we copy the Type as the condition Type and Reason and
      the Message as-is, so `kubectl describe helmrelease` shows the
      actual underlying problem without the user having to fetch the
      Application.

## Agent modes

`fargocd run --mode=<mode>` selects between three deployment shapes:

| Mode | ArgoClient points at | Application location | Destination on Application |
| --- | --- | --- | --- |
| `in-cluster` | Local cluster | Local Argo CD namespace | `https://kubernetes.default.svc` |
| `autonomous` | Local cluster | Local Argo CD namespace | `https://kubernetes.default.svc` |
| `managed` | Remote principal (`--argo-kubeconfig`) | Per-cluster namespace on the principal | Symbolic name (`--argo-dest-name`) |

For `autonomous` mode the topology looks identical to `in-cluster` because
the Application lives next to the workload — the difference is that
`argocd-agent` (rather than a standalone Argo CD) reconciles it and pushes
status back to the principal. The mode is still surfaced as a flag so the
operator can label/annotate appropriately and so the README/docs can be
generated correctly.

### Multi-cluster naming

The Argo CD Application name is `<HelmRelease.name>-<cluster-name>` when
`--cluster-name` is set, with one exception: the ACE umbrella chart keeps
its un-suffixed `ace` name because only one ACE release exists per
principal. This rule is implemented in `pkg/controller/naming.go` and is
covered by `naming_test.go`. The exception triggers on either the
HelmRelease name being `ace` _or_ the chart name being `ace`, so renaming
the resource for organisational reasons does not break the convention.

## Auto-generated `ignoreDifferences`

Many AppsCode charts mint a fresh TLS certificate on every Helm render
(webhook CA bundles, `APIService.spec.caBundle`, secrets containing
`tls.crt`/`tls.key`, pod-template `reload` annotations, etc). Without
`ignoreDifferences`, Argo CD would mark those Applications OutOfSync on
every reconcile.

`pkg/ignoregen` solves this by rendering the chart twice using the Helm Go
SDK (we explicitly avoid shelling out to the `helm` CLI):

1. `helm pull oci://<repo>/<chart>:<version>` into a temp dir, with
   credentials sourced from the HelmRepository's `SecretRef` /
   `CertSecretRef`.
2. `helm template --dry-run=client --include-crds` twice using
   `action.Install`.
3. Parse each rendered manifest into a `map[key]Resource` keyed by
   `group/kind/namespace/name`.
4. For every resource that appears in both renders, look for differences in
   the well-known mutable fields:
   - `Secret.data` keys ending in `.crt` / `.key` → `/data`
   - `MutatingWebhookConfiguration` / `ValidatingWebhookConfiguration`
     `clientConfig.caBundle` → `.webhooks[].clientConfig.caBundle`
   - `APIService.spec.caBundle` → `/spec/caBundle`
   - `Deployment`/`StatefulSet` `spec.template.metadata.annotations` whose
     values change → `/spec/template/metadata/annotations/<key>`
   - `CustomResourceDefinition.spec` differences → `/spec`, plus
     annotation diffs → `/metadata/annotations/<key>`
5. Memoise the result by `(chart, version, repoURL, namespace)` so the
   second reconcile is free.

The package exposes `DetectFn` so unit tests can stub the helm pipeline
without needing network access.

### Why diff renders instead of hard-coding rules?

- Chart authors add new generated fields all the time; a static list would
  rot quickly.
- `helm template` produces deterministic output for everything _except_
  generated values, so two renders are enough to identify exactly the
  fields we have to ignore.
- This same approach works for arbitrary user charts, not just AppsCode's.

## Operational considerations

- **RBAC** — the operator needs `get`/`list`/`watch` on `HelmRelease`,
  `HelmRepository`, `Secret`; `create`/`update`/`patch`/`delete` on
  `Application` in the Argo CD namespace; `list` on `Service` for namespace
  auto-discovery.
- **Leader election** — `--leader-elect` uses lease
  `03b9a431.fargocd.appscode.com`. In managed mode a second lease
  (`03b9a432`) is used on the remote cluster's manager.
- **Metrics** — served on `:8443` over HTTPS with the controller-runtime
  `WithAuthenticationAndAuthorization` filter chain by default. Set
  `--metrics-secure=false` for plain HTTP, or `--metrics-bind-address=0`
  to disable entirely.
- **HTTP/2** — disabled by default (`--enable-http2=false`) per
  GHSA-qppj-fm5r-hxr3 and GHSA-4374-p667-p6c8.
- **Cert rotation** — `--cert-dir` is watched via
  `controller-runtime/certwatcher`.

## Testing strategy

| Layer | Where | Stack |
| --- | --- | --- |
| Mode parsing | `pkg/mode/mode_test.go` | Pure Go. |
| Multi-cluster naming | `pkg/controller/naming_test.go` | Pure Go. |
| Reconciler (create/update/delete/finalizer/dependency/suspend) | `pkg/controller/helmrelease_controller_test.go` | controller-runtime fake client; `ignoregen.DetectFn` stubbed so no network. |
| Diff detection | `pkg/ignoregen/ignoregen_test.go` (unit + integration) | Pure unit cases run under `-short`; the chart-pull integration suite runs against real OCI registries. |

`make ci` runs vet, lint, and build. Tests are invoked explicitly with
`make unit-tests`.

## Non-goals

- Reconciling Argo CD `Application` resources directly. fargocd never
  reads user-authored Applications back into HelmReleases — the data flow
  is HelmRelease → Application only.
- Authoring FluxCD's `helm-controller` work itself: fargocd does not call
  `helm install`. It just renders to produce ignore rules and lets Argo CD
  perform the actual rollout.
- Replacing `argocd-agent`. fargocd cooperates with both modes
  (`autonomous` and `managed`) but does not take over the agent's role.

## Future work

- Support `HelmRelease.spec.chartRef` (currently only `spec.chart` with an
  `HelmRepository` source is honoured).
- Project-per-HelmRelease and AppProject-template support.

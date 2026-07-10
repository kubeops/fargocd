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
   transient registry hiccup will not stall sync. As part of this step any
   chart-shipped CRD too large for client-side apply is created directly on
   the HelmRelease cluster if missing (see "Oversized CRDs" below).
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
the Application lives next to the workload — the difference is that a
headless Argo CD reconciles it locally and (optionally) `argocd-agent`
mirrors it up to a principal on the hub for observability. The mode is
still surfaced as a flag so the operator can label/annotate appropriately
and so the README/docs can be generated correctly.

### Headless Argo CD (Argo CD Core) is installed by b3, not this chart

Autonomous spokes still need a local, headless Argo CD (**Argo CD Core**:
application-controller, repo-server, redis; no argocd-server/UI, no dex, no
notifications) to reconcile the Applications fargocd creates. Earlier
revisions of the fargocd-manager OCM addon chart vendored and shipped Argo
CD Core itself via ManifestWork (`argocd.deploy` chart value /
`--deploy-argocd` hub-manager flag). That mechanism has been **removed**:
the backend (`b3`, see
`routers/api/v1/cluster/importer/workload_argocd.go`,
`installArgoCDAgentWorkloadStack`) now installs the same headless Argo CD
directly on the spoke via its own Helm SDK release, as part of the same
flow that installs the `argocd-agent` agent. Having both fargocd-manager
(via OCM ManifestWork) and b3 (via a direct Helm release) independently
manage the same `argocd`/`argocd-application-controller` resources on one
spoke would race, so only one owner remains.

fargocd's own `--mode`/`argocd.mode` flag and the `argocd.namespace` /
`argocd.destServer` / `argocd.destName` / `argocd.project` /
`argocd.clusterName` / `argocd.kubeconfig*` chart values are unaffected —
they configure how the fargocd controller talks to Argo CD regardless of
who installed it. When `mode=autonomous`, the chart simply assumes a headless Argo CD already
exists on the spoke (installed by b3). Note that namespace
auto-discovery (§ Reconcile loop, step 2) looks for a Service labelled
`app.kubernetes.io/name=argocd-server` — a headless Argo CD Core has no
such Service — so autonomous-mode deployments must set `argocd.namespace`
explicitly to match wherever b3 installs Argo CD Core.

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

### Oversized CRDs

Some charts ship CRDs whose documents are so large (kube-prometheus-stack's
`prometheuses` CRD, for example) that kubectl-style client-side apply cannot
write them at all: the apply records the whole object in the
`kubectl.kubernetes.io/last-applied-configuration` annotation, and the API
server caps `metadata.annotations` at 256 KiB. Argo CD's default apply is
client-side, so it can neither create nor update these CRDs.

Turning on `ServerSideApply=true` for the Application is deliberately NOT
the fix. SSA submits the rendered manifests verbatim, and the API server
then rejects chart output that Helm's client-side path quietly launders —
most commonly an explicit `null` where the schema expects an array (
kube-prometheus-stack renders `PrometheusRule` groups with `rules: null`
when all rules in a group are disabled via values). A HelmRelease that
works under the FluxCD helm-controller must keep working under fargocd, so
the Application stays on client-side apply.

Instead, fargocd handles the one thing client-side apply cannot do:
`ensureOversizedCRDs` creates any missing oversized CRD directly on the
HelmRelease cluster with a plain, annotation-free create. That cluster is
the Application's destination in every agent mode (in managed mode only the
Application *object* lives on the remote principal), so the CRD always
lands on the right cluster. From then on the generated `ignoreDifferences`
rules plus `ApplyOutOfSyncOnly=true` keep every sync away from the CRD —
Helm's install-once contract for `crds/`. Existing CRDs are never patched,
by fargocd or by Argo CD. This requires `create` on
`customresourcedefinitions` in fargocd's ClusterRole.

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

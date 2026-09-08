/*
Copyright AppsCode Inc. and Contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"kubeops.dev/fargocd/pkg/ignoregen"
	"kubeops.dev/fargocd/pkg/mode"

	argov1a1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/gitops-engine/pkg/health"
	fluxhelmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/chartutil"
	fluxsrcv1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"sigs.k8s.io/yaml"
)

const (
	// FinalizerName ensures the bridge can clean up the Argo CD Application
	// before the originating HelmRelease is removed.
	FinalizerName = "fargocd.appscode.com/finalizer"

	// HelmReleaseAnnotation links an Argo CD Application back to the
	// HelmRelease that produced it. The value is "<namespace>/<name>".
	HelmReleaseAnnotation = "fargocd.appscode.com/helmrelease"

	// AgentNameLabel is set on Applications in managed mode so the
	// argocd-agent principal can route them to the correct workload cluster.
	AgentNameLabel = "argocd.argoproj.io/agent-name"

	// DefaultRequeueAfter is used when a transient condition (such as a
	// dependency not being healthy yet) makes us re-enqueue.
	DefaultRequeueAfter = 30 * time.Second

	// argoServerLabelKey/Value is what we look for to auto-discover the
	// Argo CD namespace on the principal/local cluster.
	argoServerLabelKey   = "app.kubernetes.io/name"
	argoServerLabelValue = "argocd-server"
)

// HelmReleaseReconciler watches FluxCD HelmRelease objects and projects each
// of them into an Argo CD Application on the cluster reached by ArgoClient.
type HelmReleaseReconciler struct {
	// Client is connected to the cluster that hosts the HelmRelease objects.
	client.Client

	// Scheme used by the local manager.
	Scheme *runtime.Scheme

	// ArgoClient is connected to the cluster that hosts Argo CD. In
	// InCluster and Autonomous mode this is the same cluster as Client. In
	// Managed mode it is the remote principal.
	ArgoClient client.Client

	// Mode controls how Applications are constructed and where they are
	// written.
	Mode mode.Mode

	// ArgoNamespace, if set, overrides automatic discovery of the Argo CD
	// namespace.
	ArgoNamespace string

	// DestinationServer is the value written into Application.Spec.Destination.Server.
	// Defaults to https://kubernetes.default.svc.
	DestinationServer string

	// DestinationName is the value written into Application.Spec.Destination.Name.
	// Most useful in Managed mode where the principal references workload
	// clusters by symbolic name.
	DestinationName string

	// Project is the Argo CD project assigned to generated Applications.
	Project string

	// ClusterName identifies the workload cluster. Required in Managed mode;
	// optional otherwise. When non-empty it is used both as the suffix for
	// the Application name (so multiple clusters can share one principal
	// without colliding) and as the agent label value.
	ClusterName string

	// helmCRDs elects a single owner per CRD when several releases vendor
	// the same one. The zero value is ready to use.
	helmCRDs helmCRDRegistry

	// crdSchemas caches, per custom-resource GVK, which top-level spec
	// fields the live CRD schema declares. The zero value is ready to use.
	crdSchemas crdSchemaCache
}

// crdSchemaCache memoises CRD spec schemas fetched from the workload
// cluster. Entries expire so a CRD upgrade is picked up without a restart.
type crdSchemaCache struct {
	mu      sync.Mutex
	entries map[string]crdSchemaEntry
}

type crdSchemaEntry struct {
	// fields holds the declared top-level names under spec.properties.
	fields map[string]struct{}
	// preserveUnknown is true when the schema keeps unknown fields, in
	// which case nothing gets pruned and no rules are needed.
	preserveUnknown bool
	// found is false when no CRD exists for the GVK (built-in kind or CRD
	// not installed yet).
	found     bool
	fetchedAt time.Time
}

const crdSchemaCacheTTL = 10 * time.Minute

func (c *crdSchemaCache) get(key string) (crdSchemaEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Since(e.fetchedAt) > crdSchemaCacheTTL {
		return crdSchemaEntry{}, false
	}
	return e, true
}

func (c *crdSchemaCache) put(key string, e crdSchemaEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]crdSchemaEntry)
	}
	e.fetchedAt = time.Now()
	c.entries[key] = e
}

// helmCRDRegistry elects a deterministic CRD owner per HelmRelease. Safe
// for concurrent use; the zero value is ready.
type helmCRDRegistry struct {
	mu sync.Mutex
	// byRelease maps "<namespace>/<name>" of a HelmRelease to the set of
	// CRD names its chart ships in crds/.
	byRelease map[string]map[string]struct{}
}

// remove forgets a release entirely, releasing any CRD ownership it held so
// the election falls to the remaining releases that still ship the CRD.
func (g *helmCRDRegistry) remove(release string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.byRelease, release)
}

// set replaces the recorded crds/ set for a release.
func (g *helmCRDRegistry) set(release string, crds []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.byRelease == nil {
		g.byRelease = make(map[string]map[string]struct{})
	}
	s := make(map[string]struct{}, len(crds))
	for _, c := range crds {
		s[c] = struct{}{}
	}
	g.byRelease[release] = s
}

// undeclaredSpecFieldRules ignores spec fields the live CRD schema doesn't
// declare -- lenient apply prunes them silently, so they'd diff forever.
func (r *HelmReleaseReconciler) undeclaredSpecFieldRules(ctx context.Context, objects []ignoregen.RenderedObject) []argov1a1.ResourceIgnoreDifferences {
	var rules []argov1a1.ResourceIgnoreDifferences
	for _, obj := range objects {
		if obj.Group == "" || len(obj.Spec) == 0 {
			// Core kinds are not CRDs; nothing to look up.
			continue
		}
		entry, err := r.lookupCRDSchema(ctx, obj)
		if err != nil || !entry.found || entry.preserveUnknown {
			continue
		}

		var ptrs []string
		for field := range obj.Spec {
			if _, declared := entry.fields[field]; !declared {
				ptrs = append(ptrs, "/spec/"+strings.ReplaceAll(strings.ReplaceAll(field, "~", "~0"), "/", "~1"))
			}
		}
		if len(ptrs) == 0 {
			continue
		}
		sort.Strings(ptrs)
		rules = append(rules, argov1a1.ResourceIgnoreDifferences{
			Group:        obj.Group,
			Kind:         obj.Kind,
			Name:         obj.Name,
			Namespace:    obj.Namespace,
			JSONPointers: ptrs,
		})
	}
	return rules
}

// zeroValueSpecFieldRules ignores zero-valued CR spec fields: the owning
// controller's own omitempty round-trip drops them from the live object.
func (r *HelmReleaseReconciler) zeroValueSpecFieldRules(ctx context.Context, objects []ignoregen.RenderedObject) []argov1a1.ResourceIgnoreDifferences {
	var rules []argov1a1.ResourceIgnoreDifferences
	for _, obj := range objects {
		if obj.Group == "" || len(obj.Spec) == 0 {
			// Core kinds are not CRDs; nothing to look up.
			continue
		}
		entry, err := r.lookupCRDSchema(ctx, obj)
		if err != nil || !entry.found {
			continue
		}
		ptrs := ignoregen.ZeroValuePointers("/spec", obj.Spec)
		if len(ptrs) == 0 {
			continue
		}
		sort.Strings(ptrs)
		rules = append(rules, argov1a1.ResourceIgnoreDifferences{
			Group:        obj.Group,
			Kind:         obj.Kind,
			Name:         obj.Name,
			Namespace:    obj.Namespace,
			JSONPointers: ptrs,
		})
	}
	return rules
}

// lookupCRDSchema resolves the CRD backing a rendered object's GVK and
// returns its declared top-level spec fields, memoised in crdSchemas.
func (r *HelmReleaseReconciler) lookupCRDSchema(ctx context.Context, obj ignoregen.RenderedObject) (crdSchemaEntry, error) {
	key := obj.Group + "/" + obj.Version + "/" + obj.Kind
	if e, ok := r.crdSchemas.get(key); ok {
		return e, nil
	}

	mapping, err := r.RESTMapper().RESTMapping(schema.GroupKind{Group: obj.Group, Kind: obj.Kind}, obj.Version)
	if err != nil {
		// Unknown kind (CRD not installed yet): try again next reconcile.
		return crdSchemaEntry{}, err
	}

	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	crdName := mapping.Resource.Resource + "." + obj.Group
	if err := r.Get(ctx, types.NamespacedName{Name: crdName}, crd); err != nil {
		if apierrors.IsNotFound(err) {
			// Built-in aggregated API or similar: no CRD, nothing pruned by
			// structural schemas. Cache the miss.
			e := crdSchemaEntry{found: false}
			r.crdSchemas.put(key, e)
			return e, nil
		}
		return crdSchemaEntry{}, err
	}

	entry := crdSchemaEntry{found: true, fields: map[string]struct{}{}}
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	for _, v := range versions {
		vm, ok := v.(map[string]any)
		if !ok {
			continue
		}
		if name, _ := vm["name"].(string); name != obj.Version {
			continue
		}
		specSchema, _, _ := unstructured.NestedMap(vm, "schema", "openAPIV3Schema", "properties", "spec")
		if preserve, _ := specSchema["x-kubernetes-preserve-unknown-fields"].(bool); preserve {
			entry.preserveUnknown = true
			break
		}
		props, _ := specSchema["properties"].(map[string]any)
		for field := range props {
			entry.fields[field] = struct{}{}
		}
		break
	}

	r.crdSchemas.put(key, entry)
	return entry, nil
}

// owner deterministically elects the lexicographically smallest release
// key among those shipping crd, so every reconcile agrees on one writer.
func (g *helmCRDRegistry) owner(crd string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	owner := ""
	for release, crds := range g.byRelease {
		if _, ok := crds[crd]; !ok {
			continue
		}
		if owner == "" || release < owner {
			owner = release
		}
	}
	return owner
}

// Reconcile implements the controller-runtime contract.
func (r *HelmReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var hr fluxhelmv2.HelmRelease
	if err := r.Get(ctx, req.NamespacedName, &hr); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	argoNamespace, err := r.resolveArgoNamespace(ctx)
	if err != nil {
		logger.Error(err, "failed to determine Argo CD namespace")
		return ctrl.Result{RequeueAfter: DefaultRequeueAfter}, nil
	}

	if !hr.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &hr, argoNamespace)
	}

	if !controllerutil.ContainsFinalizer(&hr, FinalizerName) {
		controllerutil.AddFinalizer(&hr, FinalizerName)
		if err := r.Update(ctx, &hr); err != nil {
			return ctrl.Result{}, err
		}
		// The Update above bumps the HelmRelease's resourceVersion which
		// the informer's watch will deliver as a new reconcile request;
		// no explicit requeue needed.
		return ctrl.Result{}, nil
	}

	if hr.Spec.Suspend {
		logger.V(1).Info("HelmRelease is suspended, skipping")
		return ctrl.Result{}, nil
	}

	if ok, unhealthy := r.checkDependenciesHealth(ctx, &hr, argoNamespace); !ok {
		logger.Info("waiting for dependency Application to be Healthy", "dependency", unhealthy)
		return ctrl.Result{RequeueAfter: DefaultRequeueAfter}, nil
	}

	app := &argov1a1.Application{}
	app.Name = r.appName(&hr)
	app.Namespace = argoNamespace

	op, err := controllerutil.CreateOrPatch(ctx, r.ArgoClient, app, func() error {
		return r.syncApplication(ctx, app, &hr)
	})
	if err != nil {
		logger.Error(err, "failed to create/update Application")
		return ctrl.Result{}, err
	}
	logger.V(1).Info("application reconciled", "operation", op, "application", client.ObjectKeyFromObject(app))

	if err := r.updateHelmReleaseStatus(ctx, &hr, app); err != nil {
		// Status-only errors should not block reconciliation; surface them
		// in the log and let the next reconcile reattempt.
		logger.Error(err, "failed to update HelmRelease status")
	}

	return ctrl.Result{}, nil
}

// reconcileDelete drops the Argo CD Application and releases the finalizer
// so the HelmRelease can finalise.
func (r *HelmReleaseReconciler) reconcileDelete(ctx context.Context, hr *fluxhelmv2.HelmRelease, argoNamespace string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(hr, FinalizerName) {
		return ctrl.Result{}, nil
	}

	gone, err := r.deleteApplication(ctx, hr, argoNamespace)
	if err != nil {
		logger.Error(err, "failed to delete Application")
		return ctrl.Result{}, err
	}
	if !gone {
		// Argo CD's cascade delete is async -- requeue instead of removing
		// our own finalizer while it's still pruning.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	r.helmCRDs.remove(hr.Namespace + "/" + hr.Name)
	controllerutil.RemoveFinalizer(hr, FinalizerName)
	if err := r.Update(ctx, hr); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// deleteApplication ensures the Application mirroring hr is gone, waiting
// out Argo CD's cascading delete. Returns true once it no longer exists.
func (r *HelmReleaseReconciler) deleteApplication(ctx context.Context, hr *fluxhelmv2.HelmRelease, argoNamespace string) (bool, error) {
	app := &argov1a1.Application{}
	key := client.ObjectKey{Name: r.appName(hr), Namespace: argoNamespace}
	if err := r.ArgoClient.Get(ctx, key, app); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	if app.DeletionTimestamp.IsZero() {
		if err := r.ArgoClient.Delete(ctx, app); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
	}

	// Applications without the cascade finalizer (e.g. pre-upgrade) delete
	// immediately; ones carrying it stick around until Argo CD finishes pruning.
	if err := r.ArgoClient.Get(ctx, key, app); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// appName projects an HelmRelease into an Application name. It honours the
// multi-cluster naming rule documented in the Design doc.
func (r *HelmReleaseReconciler) appName(hr *fluxhelmv2.HelmRelease) string {
	return applicationName(hr, r.ClusterName)
}

// resolveArgoNamespace returns the explicitly configured namespace or, when
// none is set, looks for an argocd-server Service on the Argo CD cluster.
func (r *HelmReleaseReconciler) resolveArgoNamespace(ctx context.Context) (string, error) {
	if r.ArgoNamespace != "" {
		return r.ArgoNamespace, nil
	}

	var services corev1.ServiceList
	if err := r.ArgoClient.List(ctx, &services, client.MatchingLabels{argoServerLabelKey: argoServerLabelValue}); err != nil {
		return "", err
	}
	if len(services.Items) == 0 {
		return "", apierrors.NewNotFound(corev1.Resource("Service"), argoServerLabelValue)
	}
	return services.Items[0].Namespace, nil
}

// checkDependenciesHealth gates spec.dependsOn on Synced + (Healthy or
// Progressing), not full health, to match Flux's own semantics and avoid deadlocking bootstrap-cycle stacks.
func (r *HelmReleaseReconciler) checkDependenciesHealth(ctx context.Context, hr *fluxhelmv2.HelmRelease, argoNamespace string) (bool, string) {
	if len(hr.Spec.DependsOn) == 0 {
		return true, ""
	}

	for _, dep := range hr.Spec.DependsOn {
		depAppName := applicationName(&fluxhelmv2.HelmRelease{
			ObjectMeta: metav1.ObjectMeta{Name: dep.Name, Namespace: hr.Namespace},
		}, r.ClusterName)

		var depApp argov1a1.Application
		err := r.ArgoClient.Get(ctx, types.NamespacedName{
			Name:      depAppName,
			Namespace: argoNamespace,
		}, &depApp)
		if err != nil {
			return false, depAppName
		}
		// "Installed": synced, or last sync succeeded -- drift alone mustn't
		// block dependents, or shared-resource drift could never resolve.
		if depApp.Status.Sync.Status != argov1a1.SyncStatusCodeSynced &&
			(depApp.Status.OperationState == nil || !depApp.Status.OperationState.Phase.Successful()) {
			return false, depAppName
		}
		switch depApp.Status.Health.Status {
		case health.HealthStatusHealthy, health.HealthStatusProgressing:
			// Applied and converging: good enough to unblock dependents.
		default:
			return false, depAppName
		}
	}
	return true, ""
}

// syncApplication populates the Application from the HelmRelease. It is
// invoked under controllerutil.CreateOrPatch so it must be idempotent.
func (r *HelmReleaseReconciler) syncApplication(ctx context.Context, app *argov1a1.Application, hr *fluxhelmv2.HelmRelease) error {
	logger := log.FromContext(ctx)

	if hr.Spec.Chart == nil {
		return errors.New("HelmRelease.spec.chart is required (chartRef is not supported)")
	}

	repoURL, helmRepo, err := r.getHelmRepository(ctx, hr)
	if err != nil {
		return fmt.Errorf("resolve HelmRepository: %w", err)
	}

	// Annotations: link back to the originating HelmRelease so the watcher
	// on Application can reverse-lookup the right reconcile request.
	if app.Annotations == nil {
		app.Annotations = make(map[string]string)
	}
	hrRef, err := cache.MetaNamespaceKeyFunc(hr)
	if err != nil {
		return err
	}
	app.Annotations[HelmReleaseAnnotation] = hrRef

	// Argo CD's own cascade-delete finalizer, so deleting this Application
	// prunes its deployed resources instead of abandoning them.
	controllerutil.AddFinalizer(app, argov1a1.ResourcesFinalizerName)

	// Agent label is meaningful only in managed mode.
	if r.Mode == mode.Managed && r.ClusterName != "" {
		if app.Labels == nil {
			app.Labels = make(map[string]string)
		}
		app.Labels[AgentNameLabel] = r.ClusterName
	}

	values, err := chartutil.ChartValuesFromReferences(ctx,
		logger,
		r.Client,
		hr.Namespace,
		hr.GetValues(),
		hr.Spec.ValuesFrom...)
	if err != nil {
		return fmt.Errorf("compose values: %w", err)
	}
	rawValues, err := values.YAML()
	if err != nil {
		return fmt.Errorf("marshal values: %w", err)
	}
	// Empty values render as "{}\n" which Argo CD treats as a literal "{}"
	// override rather than no overrides. Normalise to the empty string so
	// Argo CD honours the chart defaults.
	if strings.TrimSpace(rawValues) == "{}" {
		rawValues = ""
	}

	project := r.Project
	if project == "" {
		project = "default"
	}

	destination := argov1a1.ApplicationDestination{
		Namespace: hr.GetReleaseNamespace(),
		Server:    r.DestinationServer,
		Name:      r.DestinationName,
	}
	if destination.Server == "" && destination.Name == "" {
		destination.Server = "https://kubernetes.default.svc"
	}

	app.Spec = argov1a1.ApplicationSpec{
		Project: project,
		Source: &argov1a1.ApplicationSource{
			RepoURL:        strings.TrimPrefix(repoURL, "oci://"),
			Chart:          hr.Spec.Chart.Spec.Chart,
			TargetRevision: hr.Spec.Chart.Spec.Version,
			Helm: &argov1a1.ApplicationSourceHelm{
				ReleaseName: hr.GetReleaseName(),
				Values:      rawValues,
			},
		},
		Destination: destination,
		SyncPolicy: &argov1a1.SyncPolicy{
			Automated: &argov1a1.SyncPolicyAutomated{
				Prune: true,
				// No selfHeal, matching FluxCD's own opt-in drift detection --
				// continuous re-assertion would fight operators rewriting their own CRs.
				SelfHeal: false,
			},
			SyncOptions: argov1a1.SyncOptions{
				"CreateNamespace=true",
				// Matches Helm's lenient validation; Argo CD defaults to
				// strict and would hard-fail syncs Helm accepts.
				"Validate=false",
				// Needed for the ignoreDifferences rules above to actually
				// stop a shared/oversized CRD from being re-applied every sync.
				"ApplyOutOfSyncOnly=true",
				// No ServerSideApply: it hard-fails on chart output Helm's
				// client-side apply quietly tolerates (e.g. explicit nulls).
			},
		},
	}

	// Auto-detect ignoreDifferences for fields that mutate on every render
	// (CA bundles, generated certs, etc).
	creds, err := r.resolveRegistryCredentials(ctx, &helmRepo, hr.Namespace)
	if err != nil {
		logger.Error(err, "failed to resolve registry credentials; proceeding without auth")
	}
	detected, err := ignoregen.DetectIgnoreDifferences(
		ctx,
		hr.Spec.Chart.Spec.Chart,
		hr.Spec.Chart.Spec.Version,
		strings.TrimPrefix(repoURL, "oci://"),
		hr.GetReleaseNamespace(),
		hr.GetReleaseName(),
		values.AsMap(),
		creds,
	)
	if err != nil {
		logger.Error(err, "failed to auto-detect ignoreDifferences; proceeding without")
		detected = ignoregen.Result{}
	}
	rules := detected.Rules

	// Several charts commonly vendor the same untemplated CRD; electing one
	// owner keeps schema upgrades flowing while every other release backs off.
	hrKey := hr.Namespace + "/" + hr.Name
	r.helmCRDs.set(hrKey, detected.HelmCRDs)
	oversized := make(map[string]bool, len(detected.OversizedHelmCRDs))
	for _, crd := range detected.OversizedHelmCRDs {
		oversized[crd] = true
	}

	// Argo CD can't even create oversized CRDs (client-side apply's own
	// annotation exceeds the size cap); create any missing ones directly.
	r.ensureOversizedCRDs(ctx, detected.OversizedHelmCRDDocs)
	for _, crd := range detected.HelmCRDs {
		// The elected owner keeps managing it, unless oversized -- then not
		// even the owner can patch it, so it's install-once for everyone.
		if r.helmCRDs.owner(crd) == hrKey && !oversized[crd] {
			continue
		}
		rules = append(rules, argov1a1.ResourceIgnoreDifferences{
			Group:        "apiextensions.k8s.io",
			Kind:         "CustomResourceDefinition",
			Name:         crd,
			JSONPointers: []string{"/spec", "/metadata/labels", "/metadata/annotations"},
		})
	}

	// Fields the CRD schema doesn't declare get pruned by the API server
	// under lenient apply and would diff forever; ignore them.
	rules = append(rules, r.undeclaredSpecFieldRules(ctx, detected.Objects)...)

	// Zero-valued CR spec fields (false, {}, []) get dropped by the owning
	// controller's omitempty round-trip and would diff forever; ignore them.
	rules = append(rules, r.zeroValueSpecFieldRules(ctx, detected.Objects)...)

	// Break the fight Argo CD reports via Shared/RepeatedResourceWarning by
	// ignoring the mutable parts of the contested resource; self-clears once the warning stops.
	app.Spec.IgnoreDifferences = append(rules, sharedResourceIgnoreRules(app.Status)...)

	return nil
}

// ensureOversizedCRDs directly creates any missing chart CRD too large for
// Argo CD's own client-side apply to create. Failures are only logged.
func (r *HelmReleaseReconciler) ensureOversizedCRDs(ctx context.Context, docs map[string]string) {
	logger := log.FromContext(ctx)
	crdGVK := schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}
	for name, doc := range docs {
		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(crdGVK)
		err := r.Get(ctx, types.NamespacedName{Name: name}, existing)
		if err == nil {
			continue
		}
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to check for oversized chart CRD", "crd", name)
			continue
		}
		obj := &unstructured.Unstructured{}
		if err := yaml.Unmarshal([]byte(doc), &obj.Object); err != nil {
			logger.Error(err, "failed to parse oversized chart CRD", "crd", name)
			continue
		}
		if err := r.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error(err, "failed to create oversized chart CRD", "crd", name)
			continue
		}
		logger.Info("created chart CRD too large for client-side apply", "crd", name)
	}
}

// sharedResourceIgnoreRules derives an ignoreDifferences rule for each
// resource named in a Repeated/SharedResourceWarning condition.
func sharedResourceIgnoreRules(status argov1a1.ApplicationStatus) []argov1a1.ResourceIgnoreDifferences {
	var rules []argov1a1.ResourceIgnoreDifferences
	for _, cond := range status.Conditions {
		var key resourceKey
		var ok bool

		switch cond.Type {
		case argov1a1.ApplicationConditionRepeatedResourceWarning:
			// Message embeds the full Group/Kind/Namespace/Name key directly.
			key, ok = parseRepeatedResourceWarning(cond.Message)
		case argov1a1.ApplicationConditionSharedResourceWarning:
			// Message only names Kind/Name; resolve Group/Namespace from
			// this Application's own resource inventory.
			var kind, name string
			kind, name, ok = parseSharedResourceWarning(cond.Message)
			if ok {
				key, ok = lookupResourceKey(status.Resources, kind, name)
			}
		default:
			continue
		}
		if !ok {
			continue
		}

		rules = append(rules, argov1a1.ResourceIgnoreDifferences{
			Group:     key.group,
			Kind:      key.kind,
			Namespace: key.namespace,
			Name:      key.name,
			// Two charts sharing a resource also disagree on chart/version
			// labels, not just spec -- ignore both.
			JSONPointers: []string{"/spec", "/metadata/labels", "/metadata/annotations"},
		})
	}
	return rules
}

// lookupResourceKey finds the Group/Namespace for a Kind+Name pair by
// searching this Application's own reported resources.
func lookupResourceKey(resources []argov1a1.ResourceStatus, kind, name string) (resourceKey, bool) {
	for _, r := range resources {
		if r.Kind == kind && r.Name == name {
			return resourceKey{group: r.Group, kind: r.Kind, namespace: r.Namespace, name: r.Name}, true
		}
	}
	return resourceKey{}, false
}

type resourceKey struct {
	group     string
	kind      string
	namespace string
	name      string
}

// repeatedResourceWarningPrefix/Suffix bracket the gitops-engine
// ResourceKey.String() embedded in Argo CD's condition message.
const (
	repeatedResourceWarningPrefix = "Resource "
	repeatedResourceWarningSuffix = " appeared "
)

// parseRepeatedResourceWarning extracts the resource key from Argo CD's
// condition message; ok=false on an unrecognized shape (fail safe).
func parseRepeatedResourceWarning(message string) (resourceKey, bool) {
	if !strings.HasPrefix(message, repeatedResourceWarningPrefix) {
		return resourceKey{}, false
	}
	rest := strings.TrimPrefix(message, repeatedResourceWarningPrefix)
	idx := strings.Index(rest, repeatedResourceWarningSuffix)
	if idx < 0 {
		return resourceKey{}, false
	}
	key := rest[:idx]

	parts := strings.SplitN(key, "/", 4)
	if len(parts) != 4 {
		return resourceKey{}, false
	}
	if parts[1] == "" || parts[3] == "" {
		// Kind and Name are always non-empty; Group and Namespace may be.
		return resourceKey{}, false
	}
	return resourceKey{group: parts[0], kind: parts[1], namespace: parts[2], name: parts[3]}, true
}

// sharedResourceWarningSeparator splits "<Kind>/<Name>" from the rest of
// Argo CD's message; unlike Repeated, it carries no Group/Namespace.
const sharedResourceWarningSeparator = " is part of applications "

// parseSharedResourceWarning extracts Kind/Name from Argo CD's condition
// message; ok=false on an unrecognized shape (fail safe).
func parseSharedResourceWarning(message string) (kind, name string, ok bool) {
	idx := strings.Index(message, sharedResourceWarningSeparator)
	if idx < 0 {
		return "", "", false
	}
	key := message[:idx]

	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// getHelmRepository looks up the HelmRepository referenced by hr and returns
// its URL plus the resource itself (so credentials can be resolved).
func (r *HelmReleaseReconciler) getHelmRepository(ctx context.Context, hr *fluxhelmv2.HelmRelease) (string, fluxsrcv1.HelmRepository, error) {
	var helmRepo fluxsrcv1.HelmRepository

	sourceRef := hr.Spec.Chart.Spec.SourceRef
	ns := hr.Namespace
	if sourceRef.Namespace != "" {
		ns = sourceRef.Namespace
	}

	if err := r.Get(ctx, types.NamespacedName{Name: sourceRef.Name, Namespace: ns}, &helmRepo); err != nil {
		return "", helmRepo, err
	}
	return helmRepo.Spec.URL, helmRepo, nil
}

// resolveRegistryCredentials inspects HelmRepository.SecretRef and
// CertSecretRef and returns the corresponding registry credentials. Either
// reference can be omitted; a nil return means no credentials were
// configured.
func (r *HelmReleaseReconciler) resolveRegistryCredentials(ctx context.Context, helmRepo *fluxsrcv1.HelmRepository, fallbackNS string) (*ignoregen.RegistryCredentials, error) {
	var creds ignoregen.RegistryCredentials

	repoNS := helmRepo.Namespace
	if repoNS == "" {
		repoNS = fallbackNS
	}

	if helmRepo.Spec.SecretRef != nil {
		var secret corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{
			Name:      helmRepo.Spec.SecretRef.Name,
			Namespace: repoNS,
		}, &secret); err != nil {
			return nil, fmt.Errorf("get secretRef %s/%s: %w", repoNS, helmRepo.Spec.SecretRef.Name, err)
		}
		creds.Username = string(secret.Data["username"])
		creds.Password = string(secret.Data["password"])
		creds.CACert = secret.Data["caFile"]
		creds.ClientCert = secret.Data["certFile"]
		creds.ClientKey = secret.Data["keyFile"]
	}

	if helmRepo.Spec.CertSecretRef != nil {
		var certSecret corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{
			Name:      helmRepo.Spec.CertSecretRef.Name,
			Namespace: repoNS,
		}, &certSecret); err != nil {
			return nil, fmt.Errorf("get certSecretRef %s/%s: %w", repoNS, helmRepo.Spec.CertSecretRef.Name, err)
		}
		// CertSecretRef takes precedence over SecretRef for TLS material.
		if v := certSecret.Data["ca.crt"]; len(v) > 0 {
			creds.CACert = v
		}
		if v := certSecret.Data["tls.crt"]; len(v) > 0 {
			creds.ClientCert = v
		}
		if v := certSecret.Data["tls.key"]; len(v) > 0 {
			creds.ClientKey = v
		}
	}

	if creds.Username == "" && len(creds.CACert) == 0 && len(creds.ClientCert) == 0 {
		return nil, nil
	}
	return &creds, nil
}

// updateHelmReleaseStatus mirrors the Application's sync, health, and
// per-application conditions onto the HelmRelease's standard Conditions.
//
// Three groups of conditions are written:
//
//  1. Ready — derived from Application.status.sync.status.
//  2. Reconciling — derived from Application.status.health.status.
//  3. One condition per Application.status.conditions[] entry, mirrored
//     verbatim (the Argo CD Type becomes the metav1.Condition Type and
//     Reason; the Argo CD Message is copied as-is). Argo CD uses these
//     to surface ComparisonError, InvalidSpecError, SyncError, the
//     SharedResource/Orphaned/Excluded/Repeated resource warnings, etc.
//     Their presence is what indicates the condition is active, so they
//     are mirrored with Status=True.
func (r *HelmReleaseReconciler) updateHelmReleaseStatus(ctx context.Context, hr *fluxhelmv2.HelmRelease, app *argov1a1.Application) error {
	conditions := make([]metav1.Condition, 0, 2+len(app.Status.Conditions))
	now := metav1.Now()

	if app.Status.Sync.Status != "" {
		// Flux's Ready means "install succeeded", not "zero drift" -- without
		// selfHeal, legitimate drift can leave this OutOfSync yet still ready.
		readyStatus := metav1.ConditionFalse
		reason := string(app.Status.Sync.Status)
		switch {
		case app.Status.Sync.Status == argov1a1.SyncStatusCodeSynced:
			readyStatus = metav1.ConditionTrue
		case app.Status.OperationState != nil && app.Status.OperationState.Phase.Successful():
			readyStatus = metav1.ConditionTrue
			reason = "SyncSucceeded"
		}
		conditions = append(conditions, metav1.Condition{
			Type:               "Ready",
			Status:             readyStatus,
			Reason:             reason,
			Message:            "synced state mirrored from Argo CD Application",
			LastTransitionTime: now,
		})
	}

	if app.Status.Health.Status != "" {
		reconciling := metav1.ConditionTrue
		reason := string(app.Status.Health.Status)
		switch app.Status.Health.Status {
		case health.HealthStatusHealthy:
			reconciling = metav1.ConditionFalse
			reason = "Healthy"
		case health.HealthStatusDegraded:
			reason = "Degraded"
		case health.HealthStatusProgressing:
			reason = "Progressing"
		}
		conditions = append(conditions, metav1.Condition{
			Type:               "Reconciling",
			Status:             reconciling,
			Reason:             reason,
			Message:            "health state mirrored from Argo CD Application",
			LastTransitionTime: now,
		})
	}

	for _, c := range app.Status.Conditions {
		if c.Type == "" {
			continue
		}
		t := now
		if c.LastTransitionTime != nil {
			t = *c.LastTransitionTime
		}
		conditions = append(conditions, metav1.Condition{
			Type:               c.Type,
			Status:             metav1.ConditionTrue,
			Reason:             c.Type,
			Message:            c.Message,
			LastTransitionTime: t,
		})
	}

	patch := client.MergeFrom(hr.DeepCopy())
	hr.Status.Conditions = conditions
	if app.Status.Sync.Status == argov1a1.SyncStatusCodeSynced {
		hr.Status.LastAttemptedRevision = app.Status.Sync.Revision
	}
	return r.Status().Patch(ctx, hr, patch)
}

// SetupWithManager wires watches for HelmRelease, HelmRepository, the Argo
// CD Application that mirrors each HelmRelease, and the argocd-server
// Service used for namespace auto-discovery.
func (r *HelmReleaseReconciler) SetupWithManager(mgr, argoMgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fluxhelmv2.HelmRelease{}).
		// Re-reconcile when the mirrored Application changes (status, etc).
		WatchesRawSource(source.Kind[client.Object](
			argoMgr.GetCache(),
			&argov1a1.Application{},
			handler.EnqueueRequestsFromMapFunc(func(_ context.Context, o client.Object) []ctrl.Request {
				ref, ok := o.GetAnnotations()[HelmReleaseAnnotation]
				if !ok {
					return nil
				}
				ns, name, err := cache.SplitMetaNamespaceKey(ref)
				if err != nil {
					return nil
				}
				return []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}}
			}),
		)).
		// HelmRepository changes (URL, credentials) require a re-render.
		Watches(
			&fluxsrcv1.HelmRepository{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []ctrl.Request {
				var hrList fluxhelmv2.HelmReleaseList
				if err := r.List(ctx, &hrList); err != nil {
					return nil
				}
				reqs := make([]ctrl.Request, 0, len(hrList.Items))
				for _, hr := range hrList.Items {
					if hr.Spec.Chart == nil {
						continue
					}
					ref := hr.Spec.Chart.Spec.SourceRef
					if ref.Kind != fluxsrcv1.HelmRepositoryKind || ref.Name != o.GetName() {
						continue
					}
					refNS := ref.Namespace
					if refNS == "" {
						refNS = hr.Namespace
					}
					if refNS != o.GetNamespace() {
						continue
					}
					reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
						Name:      hr.Name,
						Namespace: hr.Namespace,
					}})
				}
				return reqs
			}),
		).
		// The argocd-server Service is observed so we can rediscover the
		// Argo CD namespace if it moves.
		WatchesRawSource(source.Kind[client.Object](
			argoMgr.GetCache(),
			&corev1.Service{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []ctrl.Request {
				if o.GetLabels()[argoServerLabelKey] != argoServerLabelValue {
					return nil
				}
				var hrList fluxhelmv2.HelmReleaseList
				if err := r.List(ctx, &hrList); err != nil {
					return nil
				}
				reqs := make([]ctrl.Request, 0, len(hrList.Items))
				for _, hr := range hrList.Items {
					reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{
						Name:      hr.Name,
						Namespace: hr.Namespace,
					}})
				}
				return reqs
			}),
			predicate.LabelChangedPredicate{},
		)).
		Complete(r)
}

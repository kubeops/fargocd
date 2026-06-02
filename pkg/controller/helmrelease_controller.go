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
	"strings"
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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
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
		return ctrl.Result{Requeue: true}, nil
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

	if controllerutil.ContainsFinalizer(hr, FinalizerName) {
		if err := r.deleteApplication(ctx, hr, argoNamespace); err != nil {
			logger.Error(err, "failed to delete Application")
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(hr, FinalizerName)
		if err := r.Update(ctx, hr); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// deleteApplication removes the Application that mirrors hr.
func (r *HelmReleaseReconciler) deleteApplication(ctx context.Context, hr *fluxhelmv2.HelmRelease, argoNamespace string) error {
	app := &argov1a1.Application{}
	app.Name = r.appName(hr)
	app.Namespace = argoNamespace
	if err := r.ArgoClient.Delete(ctx, app); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
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

// checkDependenciesHealth verifies that every HelmRelease listed in
// spec.dependsOn has produced a Healthy Application. The second return
// value is the name of the first dependency we found unhealthy, useful for
// log messages.
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
		if depApp.Status.Health.Status != health.HealthStatusHealthy {
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
				Prune:    true,
				SelfHeal: true,
			},
			SyncOptions: argov1a1.SyncOptions{
				"CreateNamespace=true",
				"ServerSideApply=true",
			},
		},
	}

	// Auto-detect ignoreDifferences for fields that mutate on every render
	// (CA bundles, generated certs, etc).
	creds, err := r.resolveRegistryCredentials(ctx, &helmRepo, hr.Namespace)
	if err != nil {
		logger.Error(err, "failed to resolve registry credentials; proceeding without auth")
	}
	rules, err := ignoregen.DetectIgnoreDifferences(
		ctx,
		hr.Spec.Chart.Spec.Chart,
		hr.Spec.Chart.Spec.Version,
		strings.TrimPrefix(repoURL, "oci://"),
		hr.GetReleaseNamespace(),
		values.AsMap(),
		creds,
	)
	if err != nil {
		logger.Error(err, "failed to auto-detect ignoreDifferences; proceeding without")
	} else {
		app.Spec.IgnoreDifferences = rules
	}

	return nil
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

// updateHelmReleaseStatus mirrors the Application's sync and health state
// onto the HelmRelease's standard Conditions.
func (r *HelmReleaseReconciler) updateHelmReleaseStatus(ctx context.Context, hr *fluxhelmv2.HelmRelease, app *argov1a1.Application) error {
	conditions := make([]metav1.Condition, 0, 2)
	now := metav1.Now()

	if app.Status.Sync.Status != "" {
		readyStatus := metav1.ConditionFalse
		if app.Status.Sync.Status == argov1a1.SyncStatusCodeSynced {
			readyStatus = metav1.ConditionTrue
		}
		conditions = append(conditions, metav1.Condition{
			Type:               "Ready",
			Status:             readyStatus,
			Reason:             string(app.Status.Sync.Status),
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

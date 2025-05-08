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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	argov1a1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/gitops-engine/pkg/health"
	fluxhelmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/chartutil"
	fluxsrcv1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	"sigs.k8s.io/yaml"
)

const (
	finalizerName               = "helmrelease.finalizers.example.com"
	ignoreDifferencesAnnotation = "fargocd.appscode.com/ignore-differences"
	helmReleaseAnnotation       = "fargocd.appscode.com/helmrelease"
)

var buffers = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type HelmReleaseReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	ArgoClient        client.Client
	DestinationServer string
	ClusterName       string
}

func (r *HelmReleaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the HelmRelease instance
	var hr fluxhelmv2.HelmRelease
	if err := r.Get(ctx, req.NamespacedName, &hr); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error(err, "unable to fetch HelmRelease")
		return ctrl.Result{}, err
	}

	// Check if HelmRelease is being deleted
	if hr.ObjectMeta.DeletionTimestamp != nil {
		if controllerutil.ContainsFinalizer(&hr, finalizerName) {
			// Cleanup the Application
			if err := r.cleanupApplication(ctx, &hr); err != nil {
				log.Error(err, "failed to cleanup Application")
				return ctrl.Result{}, err
			}

			// Remove finalizer
			controllerutil.RemoveFinalizer(&hr, finalizerName)
			if err := r.Update(ctx, &hr); err != nil {
				log.Error(err, "failed to remove finalizer from HelmRelease")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(&hr, finalizerName) {
		controllerutil.AddFinalizer(&hr, finalizerName)
		if err := r.Update(ctx, &hr); err != nil {
			log.Error(err, "failed to add finalizer to HelmRelease")
			return ctrl.Result{}, err
		}
	}

	// Get ArgoCD namespace
	argoNamespace, err := r.getArgoCDNamespace(ctx)
	if err != nil {
		log.Error(err, "failed to determine ArgoCD namespace")
		return ctrl.Result{}, err
	}

	// Check dependencies' health
	if ok, unhealthyDep := r.checkDependenciesHealth(ctx, &hr, argoNamespace); !ok {
		log.Info("Waiting for dependency Application to be Healthy", "unhealthy", unhealthyDep)
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Create or update corresponding ArgoCD Application
	app := &argov1a1.Application{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.appName(hr.Name),
			Namespace: argoNamespace,
		},
	}

	op, err := controllerutil.CreateOrPatch(ctx, r.ArgoClient, app, func() error {
		return r.syncApplication(ctx, app, &hr)
	})
	if err != nil {
		log.Error(err, "failed to create/update Application")
		return ctrl.Result{}, err
	}

	// Update HelmRelease status based on Application status
	if err := r.updateHelmReleaseStatus(ctx, &hr, app); err != nil {
		log.Error(err, "failed to update HelmRelease status")
		return ctrl.Result{}, err
	}

	log.Info("Application reconciled", "operation", op)
	return ctrl.Result{}, nil
}

func (r *HelmReleaseReconciler) getArgoCDNamespace(ctx context.Context) (string, error) {
	var serviceList corev1.ServiceList
	if err := r.ArgoClient.List(ctx, &serviceList, client.MatchingLabels{"app.kubernetes.io/name": "argocd-server"}); err != nil {
		return "", err
	}

	if len(serviceList.Items) == 0 {
		return "", errors.NewNotFound(corev1.Resource("Service"), "argocd-server")
	}

	// Return the namespace of the first matching service
	return serviceList.Items[0].Namespace, nil
}

func (r *HelmReleaseReconciler) appName(hrName string) string {
	if hrName == "ace" {
		return hrName
	}
	if r.ClusterName != "" {
		return fmt.Sprintf("%s-%s", hrName, r.ClusterName)
	}
	return hrName
}

func (r *HelmReleaseReconciler) checkDependenciesHealth(ctx context.Context, hr *fluxhelmv2.HelmRelease, argoNamespace string) (bool, string) {
	if len(hr.Spec.DependsOn) == 0 {
		return true, ""
	}

	for _, dep := range hr.Spec.DependsOn {
		//depNamespace := hr.Namespace
		//if dep.Namespace != "" {
		//	depNamespace = dep.Namespace
		//}
		//depAppName := fmt.Sprintf("%s-%s", dep.Name, depNamespace)

		depAppName := r.appName(dep.Name)
		var depApp argov1a1.Application

		if err := r.ArgoClient.Get(ctx, types.NamespacedName{
			Name:      depAppName,
			Namespace: argoNamespace,
		}, &depApp); err != nil {
			if errors.IsNotFound(err) {
				return false, depAppName
			}
			log.FromContext(ctx).Error(err, "failed to check dependent Application", "app", depAppName)
			return false, depAppName
		}

		// Check if the dependent Application is Healthy
		if depApp.Status.Health.Status != health.HealthStatusHealthy {
			return false, depAppName
		}
	}
	return true, ""
}

func (r *HelmReleaseReconciler) syncApplication(ctx context.Context, app *argov1a1.Application, hr *fluxhelmv2.HelmRelease) error {
	log := log.FromContext(ctx)

	// Get the HelmRepository
	repoURL, err := r.getHelmRepositoryURL(ctx, hr)
	if err != nil {
		return err
	}

	// Ensure annotations map exists
	if app.ObjectMeta.Annotations == nil {
		app.ObjectMeta.Annotations = make(map[string]string)
	}

	// Store HelmRelease namespace and name in annotation
	helmReleaseRef, err := cache.MetaNamespaceKeyFunc(hr)
	if err != nil {
		return err
	}
	app.ObjectMeta.Annotations[helmReleaseAnnotation] = helmReleaseRef

	// Compose values based from the spec and references.
	values, err := chartutil.ChartValuesFromReferences(ctx,
		log,
		r.Client,
		hr.Namespace,
		hr.GetValues(),
		hr.Spec.ValuesFrom...)
	if err != nil {
		return err
	}
	raw, err := values.YAML()
	if err != nil {
		return err
	}

	// Set Application spec based on HelmRelease
	app.Spec = argov1a1.ApplicationSpec{
		Project: "default",
		Source: &argov1a1.ApplicationSource{
			RepoURL:        repoURL,
			Chart:          hr.Spec.Chart.Spec.Chart,
			TargetRevision: hr.Spec.Chart.Spec.Version,
			Helm: &argov1a1.ApplicationSourceHelm{
				Values: raw,
			},
		},
		Destination: argov1a1.ApplicationDestination{
			Server:    r.DestinationServer,
			Namespace: hr.GetReleaseNamespace(),
		},
		SyncPolicy: &argov1a1.SyncPolicy{
			Automated: &argov1a1.SyncPolicyAutomated{
				// Prune:    true,
				// SelfHeal: true,
			},
			SyncOptions: argov1a1.SyncOptions{
				"CreateNamespace=true",
			},
		},
	}
	// Copy ignoreDifferences from annotation
	if ignoreDiffs, ok := hr.Annotations[ignoreDifferencesAnnotation]; ok && ignoreDiffs != "" {
		vals := map[string]interface{}{
			"Chart": map[string]interface{}{
				"Name":    hr.Spec.Chart.Spec.Chart,
				"Version": hr.Spec.Chart.Spec.Version,
			},
			// "Capabilities": caps,
			"Release": map[string]interface{}{
				"Name":      app.Name,
				"Namespace": hr.GetReleaseNamespace(),
				"Service":   "Helm",
			},
		}

		tpl := template.Must(template.New("argo").Funcs(sprig.TxtFuncMap()).Parse(ignoreDiffs))
		buf := buffers.Get().(*bytes.Buffer)
		buf.Reset()
		defer buffers.Put(buf)
		err = tpl.Execute(buf, vals)
		if err != nil {
			log.Error(err, "failed to render ignore-differences annotation")
			return err
		}

		var ignoreDifferences []argov1a1.ResourceIgnoreDifferences
		if err := yaml.Unmarshal(buf.Bytes(), &ignoreDifferences); err != nil {
			log.Error(err, "failed to parse ignore-differences annotation")
			return err
		}
		app.Spec.IgnoreDifferences = ignoreDifferences
	}

	// Update HelmRelease status based on Application status
	if err := r.updateHelmReleaseStatus(ctx, hr, app); err != nil {
		log.Error(err, "failed to update HelmRelease status")
		return err
	}

	return nil
}

func (r *HelmReleaseReconciler) getHelmRepositoryURL(ctx context.Context, hr *fluxhelmv2.HelmRelease) (string, error) {
	var helmRepo fluxsrcv1.HelmRepository
	sourceRef := hr.Spec.Chart.Spec.SourceRef

	// Use the HelmRelease's namespace if not specified in SourceRef
	namespace := hr.Namespace
	if sourceRef.Namespace != "" {
		namespace = sourceRef.Namespace
	}

	if err := r.Get(ctx, types.NamespacedName{
		Name:      sourceRef.Name,
		Namespace: namespace,
	}, &helmRepo); err != nil {
		return "", err
	}

	return strings.TrimPrefix(helmRepo.Spec.URL, "oci://"), nil
}

func (r *HelmReleaseReconciler) updateHelmReleaseStatus(ctx context.Context, hr *fluxhelmv2.HelmRelease, app *argov1a1.Application) error {
	// Map ArgoCD Application conditions to standard conditions
	var conditions []metav1.Condition

	// Sync status condition
	if app.Status.Sync.Status != "" {
		status := metav1.ConditionFalse
		switch app.Status.Sync.Status {
		case argov1a1.SyncStatusCodeSynced:
			status = metav1.ConditionTrue
		case argov1a1.SyncStatusCodeOutOfSync:
			status = metav1.ConditionFalse
		}

		conditions = append(conditions, metav1.Condition{
			Type:               "Ready",
			Status:             status,
			Reason:             string(app.Status.Sync.Status),
			Message:            "Synced with ArgoCD Application",
			LastTransitionTime: metav1.Now(),
		})
	}

	// Health status condition
	if app.Status.Health.Status != "" {
		status := metav1.ConditionTrue
		reason := string(app.Status.Health.Status)

		switch app.Status.Health.Status {
		case health.HealthStatusHealthy:
			status = metav1.ConditionFalse
			reason = "Healthy"
		case health.HealthStatusDegraded:
			reason = "Degraded"
		case health.HealthStatusProgressing:
			reason = "Progressing"
		}

		conditions = append(conditions, metav1.Condition{
			Type:               "Reconciling",
			Status:             status,
			Reason:             reason,
			Message:            "Health status from ArgoCD Application",
			LastTransitionTime: metav1.Now(),
		})
	}

	// Update HelmRelease status
	hrCopy := hr.DeepCopy()
	hrCopy.Status.Conditions = conditions

	// Set LastAppliedRevision if synced
	if app.Status.Sync.Status == argov1a1.SyncStatusCodeSynced {
		hrCopy.Status.LastAttemptedRevision = app.Status.Sync.Revision
	}

	return r.Status().Update(ctx, hrCopy)
}

func (r *HelmReleaseReconciler) cleanupApplication(ctx context.Context, hr *fluxhelmv2.HelmRelease) error {
	app := &argov1a1.Application{
		ObjectMeta: metav1.ObjectMeta{
			Name:      hr.Name,
			Namespace: hr.Namespace,
		},
	}

	if err := r.ArgoClient.Delete(ctx, app); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *HelmReleaseReconciler) SetupWithManager(mgr, argoMgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fluxhelmv2.HelmRelease{}).
		WatchesRawSource(source.Kind[client.Object](
			argoMgr.GetCache(),
			&argov1a1.Application{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []ctrl.Request {
				ns, name, err := cache.SplitMetaNamespaceKey(o.GetAnnotations()[helmReleaseAnnotation])
				if err != nil {
					return nil
				}
				return []ctrl.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      name,
							Namespace: ns,
						},
					},
				}
			}),
		)).
		Watches(
			&fluxsrcv1.HelmRepository{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []ctrl.Request {
				var hrList fluxhelmv2.HelmReleaseList
				if err := r.List(ctx, &hrList); err != nil {
					req := make([]ctrl.Request, 0, len(hrList.Items))
					for _, hr := range hrList.Items {
						if hr.Spec.Chart != nil &&
							hr.Spec.Chart.Spec.SourceRef.Kind == fluxsrcv1.HelmRepositoryKind &&
							hr.Spec.Chart.Spec.SourceRef.Name == o.GetName() &&
							hr.Spec.Chart.Spec.SourceRef.Namespace == o.GetNamespace() {
							req = append(req, ctrl.Request{
								NamespacedName: types.NamespacedName{
									Name:      hr.Name,
									Namespace: hr.Namespace,
								},
							})
						}
					}
					return req
				}

				return nil
			}),
		).
		WatchesRawSource(source.Kind[client.Object](
			argoMgr.GetCache(),
			&corev1.Service{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []ctrl.Request {
				obj := o.(*corev1.Service)
				if obj.Labels["app.kubernetes.io/name"] != "argocd-server" {
					return nil
				}

				var hrList fluxhelmv2.HelmReleaseList
				if err := r.List(ctx, &hrList); err != nil {
					req := make([]ctrl.Request, 0, len(hrList.Items))
					for _, hr := range hrList.Items {
						req = append(req, ctrl.Request{
							NamespacedName: types.NamespacedName{
								Name:      hr.Name,
								Namespace: hr.Namespace,
							},
						})
					}
					return req
				}
				return nil
			}),
			predicate.LabelChangedPredicate{},
		)).
		Complete(r)
}

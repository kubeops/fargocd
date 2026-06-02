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
	"testing"

	"kubeops.dev/fargocd/pkg/ignoregen"
	"kubeops.dev/fargocd/pkg/mode"

	argov1a1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/gitops-engine/pkg/health"
	fluxhelmv2 "github.com/fluxcd/helm-controller/api/v2"
	"github.com/fluxcd/pkg/apis/meta"
	fluxsrcv1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// stubIgnoreDetect short-circuits the helm-pull/render pipeline so unit
// tests do not need network access.
func stubIgnoreDetect(t *testing.T) {
	t.Helper()
	orig := ignoregen.DetectFn
	ignoregen.DetectFn = func(_ context.Context, _ string, _ string, _ string, _ string, _ map[string]any, _ *ignoregen.RegistryCredentials) ([]argov1a1.ResourceIgnoreDifferences, error) {
		return nil, nil
	}
	t.Cleanup(func() { ignoregen.DetectFn = orig })
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	sch := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(sch))
	utilruntime.Must(fluxhelmv2.AddToScheme(sch))
	utilruntime.Must(fluxsrcv1.AddToScheme(sch))
	utilruntime.Must(argov1a1.AddToScheme(sch))
	return sch
}

// testNS and testRepoName are shared across the fake-client tests so
// helpers do not need to thread them through parameters.
const (
	testNS       = "default"
	testRepoName = "ghcr"
)

func sampleHelmRepository() *fluxsrcv1.HelmRepository {
	return &fluxsrcv1.HelmRepository{
		ObjectMeta: metav1.ObjectMeta{Name: testRepoName, Namespace: testNS},
		Spec: fluxsrcv1.HelmRepositorySpec{
			URL:  "oci://ghcr.io/appscode-charts",
			Type: "oci",
		},
	}
}

func sampleHelmRelease(name, chart string) *fluxhelmv2.HelmRelease {
	return &fluxhelmv2.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
			// emulate that the user (or fargocd) has stamped the
			// finalizer already so the reconcile loop reaches the
			// Application phase in one call.
			Finalizers: []string{FinalizerName},
		},
		Spec: fluxhelmv2.HelmReleaseSpec{
			Interval: metav1.Duration{Duration: 0},
			Chart: &fluxhelmv2.HelmChartTemplate{
				Spec: fluxhelmv2.HelmChartTemplateSpec{
					Chart:   chart,
					Version: "v1.0.0",
					SourceRef: fluxhelmv2.CrossNamespaceObjectReference{
						Kind:      fluxsrcv1.HelmRepositoryKind,
						Name:      "ghcr",
						Namespace: testNS,
					},
				},
			},
			Values: &apiextensionsv1.JSON{Raw: []byte(`{"replicaCount":2}`)},
		},
	}
}

func sampleArgoNS() *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "argocd"}}
}

func sampleArgoServerService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "argocd-server",
			Namespace: "argocd",
			Labels:    map[string]string{argoServerLabelKey: argoServerLabelValue},
		},
	}
}

// TestReconcile_CreatesApplication exercises the happy path: a HelmRelease
// becomes an Argo CD Application in the auto-discovered namespace.
func TestReconcile_CreatesApplication(t *testing.T) {
	stubIgnoreDetect(t)
	sch := newScheme(t)

	hr := sampleHelmRelease("kubedb", "kubedb")
	repo := sampleHelmRepository()
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(hr, repo, sampleArgoNS(), sampleArgoServerService()).
		WithStatusSubresource(&fluxhelmv2.HelmRelease{}, &argov1a1.Application{}).
		Build()

	r := &HelmReleaseReconciler{
		Client:            c,
		Scheme:            sch,
		ArgoClient:        c,
		Mode:              mode.InCluster,
		DestinationServer: "https://kubernetes.default.svc",
		Project:           "default",
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "kubedb", Namespace: "default"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var app argov1a1.Application
	if err := c.Get(context.Background(), types.NamespacedName{Name: "kubedb", Namespace: "argocd"}, &app); err != nil {
		t.Fatalf("application not created: %v", err)
	}
	if app.Spec.Source.Chart != "kubedb" {
		t.Errorf("chart = %q, want kubedb", app.Spec.Source.Chart)
	}
	if app.Spec.Source.RepoURL != "ghcr.io/appscode-charts" {
		t.Errorf("repoURL = %q, want ghcr.io/appscode-charts (oci:// trimmed)", app.Spec.Source.RepoURL)
	}
	if app.Spec.Destination.Server != "https://kubernetes.default.svc" {
		t.Errorf("destination.server = %q", app.Spec.Destination.Server)
	}
	if app.Spec.Destination.Namespace != "default" {
		t.Errorf("destination.namespace = %q, want default (HR namespace)", app.Spec.Destination.Namespace)
	}
	if app.Annotations[HelmReleaseAnnotation] != "default/kubedb" {
		t.Errorf("expected HR backlink annotation default/kubedb, got %q", app.Annotations[HelmReleaseAnnotation])
	}
}

// TestReconcile_ManagedModeAppendsClusterAndLabel verifies the multi-cluster
// naming rule and the agent-name label.
func TestReconcile_ManagedModeAppendsClusterAndLabel(t *testing.T) {
	stubIgnoreDetect(t)
	sch := newScheme(t)

	hr := sampleHelmRelease("kubedb", "kubedb")
	repo := sampleHelmRepository()
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(hr, repo).
		WithStatusSubresource(&fluxhelmv2.HelmRelease{}, &argov1a1.Application{}).
		Build()

	r := &HelmReleaseReconciler{
		Client:            c,
		Scheme:            sch,
		ArgoClient:        c,
		Mode:              mode.Managed,
		ArgoNamespace:     "agent-east1",
		DestinationName:   "east1",
		DestinationServer: "",
		ClusterName:       "east1",
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "kubedb", Namespace: "default"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var app argov1a1.Application
	if err := c.Get(context.Background(), types.NamespacedName{Name: "kubedb-east1", Namespace: "agent-east1"}, &app); err != nil {
		t.Fatalf("expected application kubedb-east1 in agent-east1: %v", err)
	}
	if app.Labels[AgentNameLabel] != "east1" {
		t.Errorf("agent-name label = %q, want east1", app.Labels[AgentNameLabel])
	}
	if app.Spec.Destination.Name != "east1" || app.Spec.Destination.Server != "" {
		t.Errorf("destination = %+v; want symbolic name east1", app.Spec.Destination)
	}
}

// TestReconcile_AceExceptionInManagedMode verifies the ace umbrella chart
// keeps its un-suffixed Application name even in managed mode.
func TestReconcile_AceExceptionInManagedMode(t *testing.T) {
	stubIgnoreDetect(t)
	sch := newScheme(t)

	hr := sampleHelmRelease("ace", "ace")
	repo := sampleHelmRepository()
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(hr, repo).
		WithStatusSubresource(&fluxhelmv2.HelmRelease{}, &argov1a1.Application{}).
		Build()

	r := &HelmReleaseReconciler{
		Client:        c,
		Scheme:        sch,
		ArgoClient:    c,
		Mode:          mode.Managed,
		ArgoNamespace: "argocd",
		ClusterName:   "east1",
	}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "ace", Namespace: "default"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var app argov1a1.Application
	if err := c.Get(context.Background(), types.NamespacedName{Name: "ace", Namespace: "argocd"}, &app); err != nil {
		t.Fatalf("ace application not created: %v", err)
	}
}

// TestReconcile_DependencyGate ensures dependents wait until the parent
// Application reports Healthy.
func TestReconcile_DependencyGate(t *testing.T) {
	stubIgnoreDetect(t)
	sch := newScheme(t)

	parent := sampleHelmRelease("kubedb", "kubedb")
	child := sampleHelmRelease("kubedb-opscenter", "kubedb-opscenter")
	child.Spec.DependsOn = []meta.NamespacedObjectReference{{Name: "kubedb"}}

	repo := sampleHelmRepository()

	parentApp := &argov1a1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: "kubedb", Namespace: "argocd"},
		Status: argov1a1.ApplicationStatus{
			Health: argov1a1.AppHealthStatus{Status: health.HealthStatusProgressing},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(parent, child, repo, parentApp).
		WithStatusSubresource(&fluxhelmv2.HelmRelease{}, &argov1a1.Application{}).
		Build()

	r := &HelmReleaseReconciler{
		Client:        c,
		Scheme:        sch,
		ArgoClient:    c,
		Mode:          mode.InCluster,
		ArgoNamespace: "argocd",
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "kubedb-opscenter", Namespace: "default"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected RequeueAfter to be set while dependency is unhealthy, got %+v", res)
	}

	var app argov1a1.Application
	err = c.Get(context.Background(), types.NamespacedName{Name: "kubedb-opscenter", Namespace: "argocd"}, &app)
	if !apierrors.IsNotFound(err) {
		t.Errorf("child application should not exist yet, got err=%v", err)
	}
}

// TestReconcile_DeleteCleansApplication ensures the finalizer removes the
// Application and lets the HelmRelease finalise.
func TestReconcile_DeleteCleansApplication(t *testing.T) {
	stubIgnoreDetect(t)
	sch := newScheme(t)

	now := metav1.Now()
	hr := sampleHelmRelease("kubedb", "kubedb")
	hr.DeletionTimestamp = &now

	app := &argov1a1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: "kubedb", Namespace: "argocd"},
	}
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(hr, app).
		WithStatusSubresource(&fluxhelmv2.HelmRelease{}, &argov1a1.Application{}).
		Build()

	r := &HelmReleaseReconciler{
		Client:        c,
		Scheme:        sch,
		ArgoClient:    c,
		Mode:          mode.InCluster,
		ArgoNamespace: "argocd",
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "kubedb", Namespace: "default"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	err := c.Get(context.Background(), types.NamespacedName{Name: "kubedb", Namespace: "argocd"}, app)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected application deleted, got err=%v", err)
	}

	var post fluxhelmv2.HelmRelease
	if err := c.Get(context.Background(), types.NamespacedName{Name: "kubedb", Namespace: "default"}, &post); err == nil {
		for _, f := range post.Finalizers {
			if f == FinalizerName {
				t.Errorf("finalizer %q was not removed", FinalizerName)
			}
		}
	}
}

// TestReconcile_AddsFinalizerIfMissing verifies the requeue-after-finalizer
// pattern leaves the Application uncreated on first pass.
func TestReconcile_AddsFinalizerIfMissing(t *testing.T) {
	stubIgnoreDetect(t)
	sch := newScheme(t)

	hr := sampleHelmRelease("kubedb", "kubedb")
	hr.Finalizers = nil
	repo := sampleHelmRepository()
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(hr, repo, sampleArgoServerService()).
		WithStatusSubresource(&fluxhelmv2.HelmRelease{}, &argov1a1.Application{}).
		Build()

	r := &HelmReleaseReconciler{
		Client:     c,
		Scheme:     sch,
		ArgoClient: c,
		Mode:       mode.InCluster,
	}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "kubedb", Namespace: "default"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("expected Requeue after finalizer added, got %+v", res)
	}

	var post fluxhelmv2.HelmRelease
	if err := c.Get(context.Background(), types.NamespacedName{Name: "kubedb", Namespace: "default"}, &post); err != nil {
		t.Fatalf("get HR: %v", err)
	}
	found := false
	for _, f := range post.Finalizers {
		if f == FinalizerName {
			found = true
		}
	}
	if !found {
		t.Errorf("finalizer %q not added", FinalizerName)
	}

	var app argov1a1.Application
	err = c.Get(context.Background(), types.NamespacedName{Name: "kubedb", Namespace: "argocd"}, &app)
	if !apierrors.IsNotFound(err) {
		t.Errorf("application created before second reconcile, err=%v", err)
	}
}

// TestReconcile_SuspendedSkipsApplication ensures suspend stops Application sync.
func TestReconcile_SuspendedSkipsApplication(t *testing.T) {
	stubIgnoreDetect(t)
	sch := newScheme(t)

	hr := sampleHelmRelease("kubedb", "kubedb")
	hr.Spec.Suspend = true
	repo := sampleHelmRepository()
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithObjects(hr, repo, sampleArgoServerService()).
		WithStatusSubresource(&fluxhelmv2.HelmRelease{}, &argov1a1.Application{}).
		Build()

	r := &HelmReleaseReconciler{
		Client:     c,
		Scheme:     sch,
		ArgoClient: c,
		Mode:       mode.InCluster,
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "kubedb", Namespace: "default"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	var app argov1a1.Application
	err := c.Get(context.Background(), types.NamespacedName{Name: "kubedb", Namespace: "argocd"}, &app)
	if !apierrors.IsNotFound(err) {
		t.Errorf("suspended HelmRelease must not create an application, got err=%v", err)
	}
}

var _ = client.IgnoreNotFound

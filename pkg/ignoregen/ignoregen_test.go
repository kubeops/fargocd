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

package ignoregen

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	argov1a1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/storage"
	"helm.sh/helm/v3/pkg/storage/driver"
)

type chartEntry struct {
	Name         string
	Version      string
	Namespace    string
	RepoURL      string
	AceUserRoles bool
}

// All charts referenced by opscenter-features
var testCharts = []chartEntry{
	{"aws-credential-manager", "v2026.1.20", "capa-system", "ghcr.io/appscode-charts", true},
	{"gcp-credential-manager", "v2026.3.11", "capg-system", "ghcr.io/appscode-charts", true},
	{"capi-ops-manager", "v2024.8.14", "capi-system", "ghcr.io/appscode-charts", true},
	{"crossplane", "1.14.0", "crossplane-system", "ghcr.io/appscode-charts", true},
	{"cluster-manager-hub", "v2026.2.16", "open-cluster-management", "ghcr.io/appscode-charts", true},
	{"kubestash", "v2026.2.26", "stash", "ghcr.io/appscode-charts", true},
	{"stash", "v2025.7.31", "stash", "ghcr.io/appscode-charts", true},
	{"stash-opscenter", "v2025.7.31", "stash", "ghcr.io/appscode-charts", true},
	{"aceshifter", "v2026.3.30", "kubeops", "ghcr.io/appscode-charts", true},
	{"flux2", "2.17.0", "flux-system", "ghcr.io/appscode-charts", true},
	{"license-proxyserver", "v2026.2.16", "kubeops", "ghcr.io/appscode-charts", true},
	{"kube-ui-server", "v2026.3.30", "kubeops", "ghcr.io/appscode-charts", true},
	{"keda", "2.19.0", "keda", "ghcr.io/appscode-charts", true},
	{"kubedb", "v2026.2.26", "kubedb", "ghcr.io/appscode-charts", true},
	{"kubedb-opscenter", "v2026.2.26", "kubedb", "ghcr.io/appscode-charts", true},
	{"voyager", "v2026.3.23", "voyager", "ghcr.io/appscode-charts", true},
	{"monitoring-operator", "v2026.3.30", "monitoring", "ghcr.io/appscode-charts", true},
	{"grafana-operator", "v2026.3.30", "monitoring", "ghcr.io/appscode-charts", true},
	{"panopticon", "v2026.1.15", "monitoring", "ghcr.io/appscode-charts", true},
	{"inbox-agent", "v2024.12.30", "monitoring", "ghcr.io/appscode-charts", true},
	{"gatekeeper", "3.13.3", "gatekeeper-system", "ghcr.io/appscode-charts", true},
	{"kyverno", "3.2.6", "kyverno", "ghcr.io/appscode-charts", true},
	{"kubevault", "v2026.2.27", "kubevault", "ghcr.io/appscode-charts", true},
	{"external-secrets", "0.9.12", "external-secrets", "ghcr.io/appscode-charts", true},
	{"sealed-secrets", "2.14.2", "kube-system", "ghcr.io/appscode-charts", true},
	{"config-syncer", "v0.15.4", "kubeops", "ghcr.io/appscode-charts", true},
	{"vault-secrets-operator", "0.4.3", "vault-secrets-operator-system", "ghcr.io/appscode-charts", true},
	{"cert-manager", "v1.19.3", "cert-manager", "ghcr.io/appscode-charts", false},
	{"falco", "4.0.0", "falco", "ghcr.io/appscode-charts", true},
	{"falco-ui-server", "v2026.1.15", "falco", "ghcr.io/appscode-charts", true},
	{"scanner", "v2026.1.15", "kubeops", "ghcr.io/appscode-charts", true},
	{"topolvm", "15.0.0", "topolvm-system", "ghcr.io/appscode-charts", true},
	{"longhorn", "1.7.2", "longhorn-system", "ghcr.io/appscode-charts", true},
	{"snapshot-controller", "3.0.6", "kubeops", "ghcr.io/appscode-charts", true},
	{"csi-driver-nfs", "v4.7.0", "kube-system", "ghcr.io/appscode-charts", true},
	{"operator-shard-manager", "v2026.1.15", "kubeops", "ghcr.io/appscode-charts", true},
	{"sidekick", "v2026.2.16", "kubeops", "ghcr.io/appscode-charts", true},
	{"supervisor", "v2026.1.15", "kubeops", "ghcr.io/appscode-charts", true},
	{"catalog-manager", "v2026.3.30", "ace", "ghcr.io/appscode-charts", true},
	{"service-backend", "v2026.3.30", "ace", "ghcr.io/appscode-charts", true},
	{"service-provider", "v2026.3.30", "ace", "ghcr.io/appscode-charts", true},
	{"service-gateway-presets", "v2026.3.30", "ace-gw", "ghcr.io/appscode-charts", true},
}

// Charts that are expected to have ignoreDifferences
var chartsWithDiff = map[string]bool{
	"aws-credential-manager": true,
	"capi-ops-manager":       true,
	"falco-ui-server":        true,
	"gcp-credential-manager": true,
	"grafana-operator":       true,
	"inbox-agent":            true,
	"kube-ui-server":         true,
	"kubedb-opscenter":       true,
	"kubedb":                 true,
	"kubestash":              true,
	"kubevault":              true,
	"license-proxyserver":    true,
	"monitoring-operator":    true,
	"operator-shard-manager": true,
	"panopticon":             true,
	"scanner":                true,
	"service-provider":       true,
	"sidekick":               true,
	"snapshot-controller":    true,
	"stash-opscenter":        true,
	"supervisor":             true,
	"virtual-secrets-server": true,
	"voyager":                true,
}

// renderChartTest renders a chart using Helm SDK for testing
func renderChartTest(t *testing.T, c chartEntry, renderCount int) []string {
	t.Helper()

	settings := cli.New()
	regClient, err := registry.NewClient(
		registry.ClientOptEnableCache(true),
		registry.ClientOptWriter(os.Stderr),
	)
	if err != nil {
		t.Fatalf("registry client: %v", err)
	}

	actionConfig := new(action.Configuration)
	actionConfig.Releases = storage.Init(driver.NewMemory())
	actionConfig.KubeClient = &noopKubeClient{}
	actionConfig.Log = func(format string, v ...any) {}
	actionConfig.RegistryClient = regClient

	values := make(map[string]any)
	if c.AceUserRoles {
		values["ace-user-roles"] = map[string]any{"enabled": false}
	}

	var manifests []string
	for i := 0; i < renderCount; i++ {
		pullClient := action.NewPullWithOpts(action.WithConfig(actionConfig))
		pullClient.Version = c.Version
		pullClient.Settings = settings
		pullClient.SetRegistryClient(regClient)

		tmpDir := t.TempDir()
		pullClient.DestDir = tmpDir

		chartRef := fmt.Sprintf("oci://%s/%s", c.RepoURL, c.Name)
		if _, err := pullClient.Run(chartRef); err != nil {
			t.Fatalf("pull %s:%s: %v", chartRef, c.Version, err)
		}

		var chartPath string
		entries, _ := os.ReadDir(tmpDir)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".tgz") {
				chartPath = filepath.Join(tmpDir, e.Name())
				break
			}
		}
		if chartPath == "" {
			t.Fatalf("no .tgz found in %s", tmpDir)
		}

		chrt, err := loader.Load(chartPath)
		if err != nil {
			t.Fatalf("load chart: %v", err)
		}

		installClient := action.NewInstall(actionConfig)
		installClient.DryRunOption = "client"
		installClient.ReleaseName = c.Name
		installClient.Namespace = c.Namespace
		installClient.ClientOnly = true
		installClient.IncludeCRDs = true
		installClient.Replace = true
		installClient.SetRegistryClient(regClient)

		kv, _ := chartutil.ParseKubeVersion("v1.31.0")
		installClient.KubeVersion = kv

		rel, err := installClient.RunWithContext(context.Background(), chrt, values)
		if err != nil {
			t.Fatalf("render %s (attempt %d): %v", c.Name, i+1, err)
		}

		manifests = append(manifests, rel.Manifest)
	}

	return manifests
}

// TestAllOpscenterCharts tests ignoreDifferences detection for all charts
func TestAllOpscenterCharts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running test in short mode")
	}

	for _, c := range testCharts {
		t.Run(c.Name, func(t *testing.T) {
			manifests := renderChartTest(t, c, 2)
			rules := findIgnoreDifferences(manifests)

			expectedDiff := chartsWithDiff[c.Name]

			if expectedDiff && len(rules) == 0 {
				t.Errorf("expected ignoreDifferences for %s but got none", c.Name)
			}

			// Validate rule structure
			for _, r := range rules {
				if r.Kind == "" {
					t.Error("rule has empty Kind")
				}
				if len(r.JSONPointers) == 0 && len(r.JQPathExpressions) == 0 {
					t.Errorf("rule %s/%s has no pointers", r.Kind, r.Name)
				}
				for _, p := range r.JSONPointers {
					if p[0] != '/' {
						t.Errorf("jsonPointer %q does not start with /", p)
					}
				}
				for _, e := range r.JQPathExpressions {
					if e[0] != '.' {
						t.Errorf("jqPathExpression %q does not start with .", e)
					}
				}
			}

			t.Logf("%d ignoreDifferences rules", len(rules))
		})
	}
}

// TestSpecificCharts tests key charts in detail
func TestSpecificCharts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running test in short mode")
	}

	tests := []struct {
		name        string
		chart       chartEntry
		minRules    int
		wantKinds   []string
		wantSecrets []string
	}{
		{
			name: "kubedb",
			chart: chartEntry{
				Name:         "kubedb",
				Version:      "v2026.2.26",
				Namespace:    "kubedb",
				RepoURL:      "ghcr.io/appscode-charts",
				AceUserRoles: true,
			},
			minRules:    10,
			wantKinds:   []string{"Secret", "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration", "Deployment"},
			wantSecrets: []string{"kubedb-kubedb-webhook-server-cert", "kubedb-petset-cert", "kubedb-sidekick-cert"},
		},
		{
			name: "kubevault",
			chart: chartEntry{
				Name:         "kubevault",
				Version:      "v2026.2.27",
				Namespace:    "kubevault",
				RepoURL:      "ghcr.io/appscode-charts",
				AceUserRoles: true,
			},
			minRules:    5,
			wantKinds:   []string{"Secret", "APIService", "Deployment"},
			wantSecrets: []string{"kubevault-kubevault-webhook-server-apiserver-cert"},
		},
		{
			name: "voyager",
			chart: chartEntry{
				Name:         "voyager",
				Version:      "v2026.3.23",
				Namespace:    "voyager",
				RepoURL:      "ghcr.io/appscode-charts",
				AceUserRoles: true,
			},
			minRules:    4,
			wantKinds:   []string{"Secret", "APIService", "Deployment"},
			wantSecrets: []string{"voyager-apiserver-cert"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifests := renderChartTest(t, tt.chart, 2)
			rules := findIgnoreDifferences(manifests)

			if len(rules) < tt.minRules {
				t.Errorf("got %d rules, want at least %d", len(rules), tt.minRules)
			}

			actualKinds := make(map[string]bool)
			for _, r := range rules {
				actualKinds[r.Kind] = true
			}
			for _, ek := range tt.wantKinds {
				if !actualKinds[ek] {
					t.Errorf("expected kind %q not found", ek)
				}
			}

			actualSecrets := make(map[string]bool)
			for _, r := range rules {
				if r.Kind == "Secret" {
					actualSecrets[r.Name] = true
				}
			}
			for _, es := range tt.wantSecrets {
				if !actualSecrets[es] {
					t.Errorf("expected Secret %q not found", es)
				}
			}

			// Verify webhook rules use jqPathExpressions
			for _, r := range rules {
				if r.Kind == "MutatingWebhookConfiguration" || r.Kind == "ValidatingWebhookConfiguration" {
					if len(r.JQPathExpressions) == 0 {
						t.Errorf("webhook %s has no jqPathExpressions", r.Name)
					}
				}
			}

			// Verify secret rules use /data
			for _, r := range rules {
				if r.Kind == "Secret" {
					hasData := false
					for _, p := range r.JSONPointers {
						if p == "/data" {
							hasData = true
							break
						}
					}
					if !hasData {
						t.Errorf("secret %s has no /data pointer", r.Name)
					}
				}
			}

			t.Logf("rules: %d, kinds: %v", len(rules), actualKinds)
		})
	}
}

// TestParseResources tests YAML parsing
func TestParseResources(t *testing.T) {
	manifest := `apiVersion: v1
kind: Secret
metadata:
  name: test-secret
  namespace: default
data:
  tls.crt: abc123
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-deploy
  namespace: default
spec:
  template:
    metadata:
      annotations:
        reload: xyz
---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: test-webhook
webhooks:
- clientConfig:
    caBundle: abc
`

	resources := parseResources(manifest)

	if len(resources) != 3 {
		t.Fatalf("expected 3 resources, got %d", len(resources))
	}

	// Check Secret
	secret, ok := resources["/Secret/default/test-secret"]
	if !ok {
		keys := make([]string, 0, len(resources))
		for k := range resources {
			keys = append(keys, k)
		}
		t.Fatalf("secret not found, available keys: %v", keys)
	}
	if secret.Kind != "Secret" || secret.Name != "test-secret" {
		t.Errorf("unexpected secret: %+v", secret)
	}

	// Check Deployment
	deploy, ok := resources["apps/Deployment/default/test-deploy"]
	if !ok {
		t.Fatal("deployment not found")
	}
	if deploy.Kind != "Deployment" || deploy.Group != "apps" {
		t.Errorf("unexpected deployment: %+v", deploy)
	}

	// Check Webhook
	wh, ok := resources["admissionregistration.k8s.io/MutatingWebhookConfiguration//test-webhook"]
	if !ok {
		t.Fatal("webhook not found")
	}
	if wh.Kind != "MutatingWebhookConfiguration" {
		t.Errorf("unexpected webhook: %+v", wh)
	}
}

// TestDiffResourcesUnit tests diff detection on mock data
func TestDiffResourcesUnit(t *testing.T) {
	tests := []struct {
		name     string
		renders  []string
		wantDiff bool
	}{
		{
			name: "secret cert data changes",
			renders: []string{
				`apiVersion: v1
kind: Secret
metadata:
  name: tls-secret
  namespace: default
data:
  tls.crt: abc123
  tls.key: def456`,
				`apiVersion: v1
kind: Secret
metadata:
  name: tls-secret
  namespace: default
data:
  tls.crt: xyz789
  tls.key: ghi012`,
			},
			wantDiff: true,
		},
		{
			name: "identical resources",
			renders: []string{
				`apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: default
spec:
  replicas: 1`,
				`apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: default
spec:
  replicas: 1`,
			},
			wantDiff: false,
		},
		{
			name: "deployment reload annotation changes",
			renders: []string{
				`apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: default
spec:
  template:
    metadata:
      annotations:
        reload: abc123`,
				`apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: default
spec:
  template:
    metadata:
      annotations:
        reload: xyz789`,
			},
			wantDiff: true,
		},
		{
			name: "webhook cabundle changes",
			renders: []string{
				`apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: webhook
webhooks:
- name: test.example.com
  clientConfig:
    caBundle: abc123`,
				`apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: webhook
webhooks:
- name: test.example.com
  clientConfig:
    caBundle: xyz789`,
			},
			wantDiff: true,
		},
		{
			name: "api service cabundle changes",
			renders: []string{
				`apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: v1.foo.example.com
spec:
  caBundle: abc123`,
				`apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: v1.foo.example.com
spec:
  caBundle: xyz789`,
			},
			wantDiff: true,
		},
		{
			name: "single render returns nil",
			renders: []string{
				`apiVersion: v1
kind: ConfigMap
metadata:
  name: cm
data:
  key: val`,
			},
			wantDiff: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rules := findIgnoreDifferences(tt.renders)

			if tt.wantDiff && len(rules) == 0 {
				t.Error("expected differences but got none")
			}
			if !tt.wantDiff && len(rules) > 0 {
				t.Errorf("expected no differences but got %d rules", len(rules))
			}

			if tt.wantDiff {
				// Verify rules have proper structure
				for _, r := range rules {
					if r.Kind == "" {
						t.Error("rule has empty Kind")
					}
				}
			}
		})
	}
}

// TestCaching verifies results are cached
func TestCaching(t *testing.T) {
	mu.Lock()
	cache = make(map[cacheKey]Result)
	mu.Unlock()

	// Manually populate cache
	key := cacheKey{"test-chart", "v1.0.0", "ghcr.io/test", "default", "test-release"}
	mu.Lock()
	cache[key] = Result{
		Rules: []argov1a1.ResourceIgnoreDifferences{
			{Kind: "Secret", Name: "cached-secret"},
		},
		HelmCRDs: []string{"widgets.example.com"},
	}
	mu.Unlock()

	// DetectIgnoreDifferences should return cached result without rendering
	res, err := DetectIgnoreDifferences(context.Background(), "test-chart", "v1.0.0", "ghcr.io/test", "default", "test-release", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(res.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(res.Rules))
	}

	if res.Rules[0].Name != "cached-secret" {
		t.Errorf("expected cached-secret, got %s", res.Rules[0].Name)
	}

	if len(res.HelmCRDs) != 1 || res.HelmCRDs[0] != "widgets.example.com" {
		t.Errorf("expected cached HelmCRDs, got %v", res.HelmCRDs)
	}
}

// BenchmarkFindIgnoreDifferences benchmarks diff detection
func BenchmarkFindIgnoreDifferences(b *testing.B) {
	manifest := `apiVersion: v1
kind: Secret
metadata:
  name: tls-secret
  namespace: default
data:
  tls.crt: abc123
  tls.key: def456
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: default
spec:
  template:
    metadata:
      annotations:
        reload: abc123
`
	manifest2 := strings.ReplaceAll(manifest, "abc123", "xyz789")
	renders := []string{manifest, manifest2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		findIgnoreDifferences(renders)
	}
}

func TestZeroValuePointers(t *testing.T) {
	spec := map[string]any{
		"recommended":  false,            // false bool: dropped by omitempty round-trips
		"enabled":      true,             // true bool survives round-trips
		"requirements": map[string]any{}, // empty object: dropped by omitempty round-trips
		"tags":         []any{},          // empty array: dropped by omitempty round-trips
		"args":         []any{"x"},       // non-empty array survives
		"title":        "",               // zero string has standalone k8s meaning; kept
		"count":        float64(0),       // zero number has standalone k8s meaning; kept
		"absent":       nil,              // explicit nulls belong to nullPointers
		"nested": map[string]any{
			"flag": false,
			"name": "x",
		},
		"we/ird~key": false, // exercises RFC 6901 escaping
	}

	got := ZeroValuePointers("/spec", spec)
	sort.Strings(got)
	want := []string{
		"/spec/nested/flag",
		"/spec/recommended",
		"/spec/requirements",
		"/spec/tags",
		"/spec/we~1ird~0key",
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

// Regression test: templated CRDs (e.g. keda's) used to be invisible to
// helmCRDNames and skip oversized-CRD detection entirely.
func TestDetectIgnoreDifferences_TemplatedCRDDetectedAsOversized(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running test in short mode")
	}

	result, err := DetectIgnoreDifferences(context.Background(),
		"keda", "2.19.0", "ghcr.io/appscode-charts", "keda", "keda", nil, nil)
	if err != nil {
		t.Fatalf("DetectIgnoreDifferences: %v", err)
	}

	found := false
	for _, name := range result.OversizedHelmCRDs {
		if name == "scaledjobs.keda.sh" {
			found = true
		}
	}
	if !found {
		t.Errorf("scaledjobs.keda.sh not in OversizedHelmCRDs (got %v) - templated CRDs (chart's templates/crds/, not the conventional crds/ directory) must still be detected", result.OversizedHelmCRDs)
	}
	if _, ok := result.OversizedHelmCRDDocs["scaledjobs.keda.sh"]; !ok {
		t.Error("OversizedHelmCRDDocs missing scaledjobs.keda.sh - ensureOversizedCRDs needs the raw doc to create it")
	}
}

func TestHelmCRDNames_OversizedDocs(t *testing.T) {
	small := `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: small.example.com
spec:
  group: example.com`
	// Pad a second CRD past the client-side-apply size cutoff with a long
	// description, the way real prometheus-operator CRDs blow the limit.
	big := `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: big.example.com
spec:
  group: example.com
  description: ` + strings.Repeat("x", oversizedCRDBytes)

	names, oversized, docs := helmCRDNames(small + "\n---\n" + big)

	wantNames := []string{"big.example.com", "small.example.com"}
	if fmt.Sprint(names) != fmt.Sprint(wantNames) {
		t.Fatalf("names = %v, want %v", names, wantNames)
	}
	if fmt.Sprint(oversized) != fmt.Sprint([]string{"big.example.com"}) {
		t.Fatalf("oversized = %v, want [big.example.com]", oversized)
	}
	if len(docs) != 1 {
		t.Fatalf("oversizedDocs has %d entries, want 1: %v", len(docs), docs)
	}
	if doc := docs["big.example.com"]; !strings.HasPrefix(doc, "apiVersion:") || !strings.Contains(doc, "name: big.example.com") {
		t.Fatalf("oversizedDocs[big.example.com] does not carry the raw manifest")
	}
}

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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	argov1a1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/kube"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/storage"
	"helm.sh/helm/v3/pkg/storage/driver"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// RegistryCredentials holds authentication credentials for OCI registries.
type RegistryCredentials struct {
	// Username for basic auth
	Username string
	// Password for basic auth
	Password string
	// CACert is a PEM-encoded CA certificate for TLS
	CACert []byte
	// ClientCert is a PEM-encoded client certificate for mTLS
	ClientCert []byte
	// ClientKey is a PEM-encoded private key for mTLS
	ClientKey []byte
}

// Cache key for memoizing results
type cacheKey struct {
	Chart   string
	Version string
	RepoURL string
	NS      string
}

var (
	mu    sync.RWMutex
	cache = make(map[cacheKey][]argov1a1.ResourceIgnoreDifferences)
)

// DetectIgnoreDifferences renders a chart twice and returns ignoreDifferences
// for fields that change between renders.
func DetectIgnoreDifferences(ctx context.Context, chartName, chartVersion, repoURL, namespace string, values map[string]interface{}, creds *RegistryCredentials) ([]argov1a1.ResourceIgnoreDifferences, error) {
	key := cacheKey{chartName, chartVersion, repoURL, namespace}

	mu.RLock()
	if rules, ok := cache[key]; ok {
		mu.RUnlock()
		return rules, nil
	}
	mu.RUnlock()

	// Render chart twice
	var manifests []string
	for i := 0; i < 2; i++ {
		m, err := renderChart(ctx, chartName, chartVersion, repoURL, namespace, values, creds)
		if err != nil {
			return nil, fmt.Errorf("render %d: %w", i+1, err)
		}
		manifests = append(manifests, m)
	}

	// Find differences
	rules := findIgnoreDifferences(manifests)

	// Cache result
	mu.Lock()
	cache[key] = rules
	mu.Unlock()

	return rules, nil
}

func renderChart(ctx context.Context, chartName, chartVersion, repoURL, namespace string, values map[string]interface{}, creds *RegistryCredentials) (string, error) {
	settings := cli.New()

	// Build registry client options
	regOpts := []registry.ClientOption{
		registry.ClientOptEnableCache(true),
		registry.ClientOptWriter(io.Discard),
	}

	if creds != nil {
		// Username/password for basic auth
		if creds.Username != "" {
			regOpts = append(regOpts, registry.ClientOptBasicAuth(creds.Username, creds.Password))
		}

		// TLS configuration: CA cert and/or client cert+key
		if len(creds.CACert) > 0 || (len(creds.ClientCert) > 0 && len(creds.ClientKey) > 0) {
			httpClient, err := buildTLSClient(creds)
			if err != nil {
				return "", fmt.Errorf("build TLS client: %w", err)
			}
			regOpts = append(regOpts, registry.ClientOptHTTPClient(httpClient))
		}
	}

	regClient, err := registry.NewClient(regOpts...)
	if err != nil {
		return "", fmt.Errorf("registry client: %w", err)
	}

	actionConfig := new(action.Configuration)
	actionConfig.Releases = storage.Init(driver.NewMemory())
	actionConfig.KubeClient = &noopKubeClient{}
	actionConfig.Log = func(format string, v ...interface{}) {}
	actionConfig.RegistryClient = regClient

	// Pull chart
	pullClient := action.NewPullWithOpts(action.WithConfig(actionConfig))
	pullClient.Version = chartVersion
	pullClient.Settings = settings
	pullClient.SetRegistryClient(regClient)

	tmpDir, err := os.MkdirTemp("", "helm-pull-*")
	if err != nil {
		return "", fmt.Errorf("temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)
	pullClient.DestDir = tmpDir

	chartRef := fmt.Sprintf("oci://%s/%s", repoURL, chartName)
	if _, err := pullClient.Run(chartRef); err != nil {
		return "", fmt.Errorf("pull %s:%s: %w", chartRef, chartVersion, err)
	}

	// Load chart
	var chartPath string
	entries, _ := os.ReadDir(tmpDir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tgz") {
			chartPath = filepath.Join(tmpDir, e.Name())
			break
		}
	}
	if chartPath == "" {
		return "", fmt.Errorf("no chart found in %s", tmpDir)
	}

	chrt, err := loader.Load(chartPath)
	if err != nil {
		return "", fmt.Errorf("load chart: %w", err)
	}

	// Render
	installClient := action.NewInstall(actionConfig)
	installClient.DryRunOption = "client"
	installClient.ReleaseName = chartName
	installClient.Namespace = namespace
	installClient.ClientOnly = true
	installClient.IncludeCRDs = true
	installClient.Replace = true
	installClient.SetRegistryClient(regClient)

	kv, _ := chartutil.ParseKubeVersion("v1.31.0")
	installClient.KubeVersion = kv

	rel, err := installClient.RunWithContext(ctx, chrt, values)
	if err != nil {
		return "", fmt.Errorf("render: %w", err)
	}

	return rel.Manifest, nil
}

// Resource represents a parsed Kubernetes resource
type Resource struct {
	Group     string
	Kind      string
	Name      string
	Namespace string
	Data      interface{}
	Spec      interface{}
	Metadata  map[string]interface{}
	Raw       map[string]interface{}
}

func (r *Resource) Key() string {
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Kind, r.Namespace, r.Name)
}

func parseResources(rendered string) map[string]*Resource {
	resources := make(map[string]*Resource)
	docs := strings.Split(rendered, "---")

	for _, doc := range docs {
		doc = strings.TrimSpace(doc)
		if doc == "" || strings.HasPrefix(doc, "---") {
			continue
		}

		var raw map[string]interface{}
		if err := yaml.Unmarshal([]byte(doc), &raw); err != nil {
			continue
		}

		apiVersion, _ := raw["apiVersion"].(string)
		kind, _ := raw["kind"].(string)
		metadata, _ := raw["metadata"].(map[string]interface{})
		name, _ := metadata["name"].(string)
		ns, _ := metadata["namespace"].(string)

		if kind == "" || name == "" {
			continue
		}

		group := ""
		parts := strings.SplitN(apiVersion, "/", 2)
		if len(parts) == 2 {
			group = parts[0]
		}

		resources[fmt.Sprintf("%s/%s/%s/%s", group, kind, ns, name)] = &Resource{
			Group:     group,
			Kind:      kind,
			Name:      name,
			Namespace: ns,
			Data:      raw["data"],
			Spec:      raw["spec"],
			Metadata:  metadata,
			Raw:       raw,
		}
	}

	return resources
}

func findIgnoreDifferences(renders []string) []argov1a1.ResourceIgnoreDifferences {
	if len(renders) < 2 {
		return nil
	}

	var allParsed []map[string]*Resource
	for _, render := range renders {
		allParsed = append(allParsed, parseResources(render))
	}

	type rule struct {
		group        string
		kind         string
		name         string
		namespace    string
		jsonPointers []string
		jqExprs      []string
	}

	ruleMap := make(map[string]*rule)

	addRule := func(res *Resource) *rule {
		key := res.Key()
		if r, exists := ruleMap[key]; exists {
			return r
		}
		r := &rule{
			group:     res.Group,
			kind:      res.Kind,
			name:      res.Name,
			namespace: res.Namespace,
		}
		ruleMap[key] = r
		return r
	}

	addPointer := func(r *rule, ptr string) {
		for _, p := range r.jsonPointers {
			if p == ptr {
				return
			}
		}
		r.jsonPointers = append(r.jsonPointers, ptr)
	}

	addJQ := func(r *rule, expr string) {
		for _, e := range r.jqExprs {
			if e == expr {
				return
			}
		}
		r.jqExprs = append(r.jqExprs, expr)
	}

	for key, res1 := range allParsed[0] {
		for i := 1; i < len(allParsed); i++ {
			res2, exists := allParsed[i][key]
			if !exists {
				continue
			}

			// Check Secret data changes
			if res1.Kind == "Secret" && !deepEqualJSON(res1.Data, res2.Data) {
				if isCertificateData(res1.Data) || isCertificateData(res2.Data) {
					r := addRule(res1)
					addPointer(r, "/data")
				}
			}

			// Check webhook caBundle changes
			if res1.Kind == "MutatingWebhookConfiguration" || res1.Kind == "ValidatingWebhookConfiguration" {
				if hasWebhookCaBundleDiff(res1.Raw, res2.Raw) {
					r := addRule(res1)
					addJQ(r, ".webhooks[].clientConfig.caBundle")
				}
			}

			// Check APIService caBundle changes
			if res1.Kind == "APIService" {
				if hasAPICaBundleDiff(res1.Spec, res2.Spec) {
					r := addRule(res1)
					addPointer(r, "/spec/caBundle")
				}
			}

			// Check Deployment/StatefulSet template annotation changes
			if res1.Kind == "Deployment" || res1.Kind == "StatefulSet" {
				for _, ptr := range diffTemplateAnnotations(res1.Spec, res2.Spec) {
					r := addRule(res1)
					addPointer(r, ptr)
				}
			}

			// Check CRD changes
			if res1.Kind == "CustomResourceDefinition" {
				if !deepEqualJSON(res1.Spec, res2.Spec) {
					r := addRule(res1)
					addPointer(r, "/spec")
				}
				for _, ptr := range diffAnnotations(res1.Metadata, res2.Metadata, "/metadata/annotations") {
					r := addRule(res1)
					addPointer(r, ptr)
				}
			}
		}
	}

	// Convert to ArgoCD format
	var rules []argov1a1.ResourceIgnoreDifferences
	for _, r := range ruleMap {
		if len(r.jsonPointers) == 0 && len(r.jqExprs) == 0 {
			continue
		}

		// Deduplicate jsonPointers
		seen := make(map[string]bool)
		var pointers []string
		for _, p := range r.jsonPointers {
			if !seen[p] {
				seen[p] = true
				pointers = append(pointers, p)
			}
		}
		sort.Strings(pointers)

		rules = append(rules, argov1a1.ResourceIgnoreDifferences{
			Group:             r.group,
			Kind:              r.kind,
			Name:              r.name,
			Namespace:         r.namespace,
			JSONPointers:      pointers,
			JQPathExpressions: r.jqExprs,
		})
	}

	sort.Slice(rules, func(i, j int) bool {
		return rules[i].Kind+rules[i].Name < rules[j].Kind+rules[j].Name
	})

	return rules
}

func isCertificateData(data interface{}) bool {
	m, ok := data.(map[string]interface{})
	if !ok {
		return false
	}
	for key := range m {
		if key == "ca.crt" || key == "tls.crt" || key == "tls.key" ||
			strings.HasSuffix(key, ".crt") || strings.HasSuffix(key, ".key") {
			return true
		}
	}
	return false
}

func diffAnnotations(metaA, metaB map[string]interface{}, prefix string) []string {
	annA, _ := metaA["annotations"].(map[string]interface{})
	annB, _ := metaB["annotations"].(map[string]interface{})
	if annA == nil || annB == nil {
		return nil
	}

	var diffs []string
	for k, v1 := range annA {
		v2, exists := annB[k]
		if !exists {
			continue
		}
		if fmt.Sprintf("%v", v1) != fmt.Sprintf("%v", v2) {
			encoded := strings.ReplaceAll(k, "/", "~1")
			diffs = append(diffs, prefix+"/"+encoded)
		}
	}
	return diffs
}

func diffTemplateAnnotations(specA, specB interface{}) []string {
	sA, _ := specA.(map[string]interface{})
	sB, _ := specB.(map[string]interface{})
	if sA == nil || sB == nil {
		return nil
	}

	tplA, _ := sA["template"].(map[string]interface{})
	tplB, _ := sB["template"].(map[string]interface{})
	if tplA == nil || tplB == nil {
		return nil
	}

	metaA, _ := tplA["metadata"].(map[string]interface{})
	metaB, _ := tplB["metadata"].(map[string]interface{})
	if metaA == nil || metaB == nil {
		return nil
	}

	return diffAnnotations(metaA, metaB, "/spec/template/metadata/annotations")
}

func hasWebhookCaBundleDiff(a, b map[string]interface{}) bool {
	webhooksA, _ := a["webhooks"].([]interface{})
	webhooksB, _ := b["webhooks"].([]interface{})
	if len(webhooksA) != len(webhooksB) {
		return false
	}
	for i := 0; i < len(webhooksA); i++ {
		whA, _ := webhooksA[i].(map[string]interface{})
		whB, _ := webhooksB[i].(map[string]interface{})
		if whA == nil || whB == nil {
			continue
		}
		ccA, _ := whA["clientConfig"].(map[string]interface{})
		ccB, _ := whB["clientConfig"].(map[string]interface{})
		if ccA == nil || ccB == nil {
			continue
		}
		cbA, _ := ccA["caBundle"].(string)
		cbB, _ := ccB["caBundle"].(string)
		if cbA != "" && cbB != "" && cbA != cbB {
			return true
		}
	}
	return false
}

func hasAPICaBundleDiff(a, b interface{}) bool {
	specA, _ := a.(map[string]interface{})
	specB, _ := b.(map[string]interface{})
	if specA == nil || specB == nil {
		return false
	}
	cbA, _ := specA["caBundle"].(string)
	cbB, _ := specB["caBundle"].(string)
	return cbA != "" && cbB != "" && cbA != cbB
}

func deepEqualJSON(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	jsonA, _ := json.Marshal(a)
	jsonB, _ := json.Marshal(b)
	return bytes.Equal(jsonA, jsonB)
}

// buildTLSClient creates an http.Client configured with CA cert and/or client cert+key.
func buildTLSClient(creds *RegistryCredentials) (*http.Client, error) {
	tlsConfig := &tls.Config{}

	// CA cert for server verification
	if len(creds.CACert) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(creds.CACert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = pool
	}

	// Client cert + key for mTLS
	if len(creds.ClientCert) > 0 && len(creds.ClientKey) > 0 {
		cert, err := tls.X509KeyPair(creds.ClientCert, creds.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}

// noopKubeClient implements kube.Interface for dry-run mode
type noopKubeClient struct{}

func (c *noopKubeClient) Create(resources kube.ResourceList) (*kube.Result, error) {
	return &kube.Result{Created: resources}, nil
}

func (c *noopKubeClient) Wait(resources kube.ResourceList, timeout time.Duration) error {
	return nil
}

func (c *noopKubeClient) WaitWithJobs(resources kube.ResourceList, timeout time.Duration) error {
	return nil
}

func (c *noopKubeClient) Delete(resources kube.ResourceList) (*kube.Result, []error) {
	return &kube.Result{Deleted: resources}, nil
}

func (c *noopKubeClient) WatchUntilReady(resources kube.ResourceList, timeout time.Duration) error {
	return nil
}

func (c *noopKubeClient) Update(original, target kube.ResourceList, force bool) (*kube.Result, error) {
	return &kube.Result{Updated: target}, nil
}

func (c *noopKubeClient) Build(reader io.Reader, validate bool) (kube.ResourceList, error) {
	return kube.ResourceList{}, nil
}

func (c *noopKubeClient) WaitAndGetCompletedPodPhase(name string, timeout time.Duration) (v1.PodPhase, error) {
	return v1.PodSucceeded, nil
}

func (c *noopKubeClient) IsReachable() error {
	return nil
}

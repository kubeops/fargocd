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
	"regexp"
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
	Chart       string
	Version     string
	RepoURL     string
	NS          string
	ReleaseName string
}

// Result is chart analysis's output: unconditionally-safe ignoreDifferences
// rules, plus crds/ CRD names (reported, not ruled -- ownership is the caller's call).
type Result struct {
	Rules []argov1a1.ResourceIgnoreDifferences
	// HelmCRDs lists the names of CustomResourceDefinitions shipped in the
	// chart's crds/ directory (including subcharts').
	HelmCRDs []string
	// OversizedHelmCRDs is the subset of HelmCRDs too large for client-side
	// apply to ever patch (exceeds the 256KiB last-applied-config annotation cap).
	OversizedHelmCRDs []string
	// OversizedHelmCRDDocs maps each OversizedHelmCRDs name to its raw
	// manifest, for a caller to create directly (client-side apply can't even create these).
	OversizedHelmCRDDocs map[string]string
	// Objects lets callers cross-check rendered specs against the live CRD
	// schema, since undeclared fields get pruned under lenient apply.
	Objects []RenderedObject
}

// RenderedObject is a rendered resource's identity plus its spec.
type RenderedObject struct {
	Group     string
	Version   string
	Kind      string
	Name      string
	Namespace string
	Spec      map[string]any
}

var (
	mu    sync.RWMutex
	cache = make(map[cacheKey]Result)
)

// DetectFn is a package variable so tests can stub the helm-pull/render
// pipeline (network access) via t.Cleanup, instead of hitting it for real.
var DetectFn = detectIgnoreDifferences

// DetectIgnoreDifferences renders a chart twice, diffs what changed, and
// memoises the Result per (chart, version, repoURL, namespace, releaseName).
func DetectIgnoreDifferences(ctx context.Context, chartName, chartVersion, repoURL, namespace, releaseName string, values map[string]any, creds *RegistryCredentials) (Result, error) {
	return DetectFn(ctx, chartName, chartVersion, repoURL, namespace, releaseName, values, creds)
}

func detectIgnoreDifferences(ctx context.Context, chartName, chartVersion, repoURL, namespace, releaseName string, values map[string]any, creds *RegistryCredentials) (Result, error) {
	key := cacheKey{chartName, chartVersion, repoURL, namespace, releaseName}

	mu.RLock()
	if res, ok := cache[key]; ok {
		mu.RUnlock()
		return res, nil
	}
	mu.RUnlock()

	// Render chart twice
	var manifests []string
	var crdManifest string
	for i := 0; i < 2; i++ {
		m, crds, err := renderChart(ctx, chartName, chartVersion, repoURL, namespace, releaseName, values, creds)
		if err != nil {
			return Result{}, fmt.Errorf("render %d: %w", i+1, err)
		}
		manifests = append(manifests, m)
		crdManifest = crds
	}

	// Find differences
	rules := findIgnoreDifferences(manifests)
	rules = append(rules, detectForeignChartResources(manifests[0], chartName, chartVersion, releaseName)...)
	rules = append(rules, nullFieldRules(manifests[0])...)

	helmCRDs, oversizedCRDs, oversizedDocs := helmCRDNames(crdManifest)
	res := Result{
		Rules:                dedupeRules(rules),
		HelmCRDs:             helmCRDs,
		OversizedHelmCRDs:    oversizedCRDs,
		OversizedHelmCRDDocs: oversizedDocs,
		Objects:              renderedObjects(manifests[0]),
	}

	// Cache result
	mu.Lock()
	cache[key] = res
	mu.Unlock()

	return res, nil
}

// detectForeignChartResources finds resources whose helm.sh/chart or
// app.kubernetes.io/instance labels don't match this release -- a vendored resource the double-render diff alone can't catch.
func detectForeignChartResources(manifest, chartName, chartVersion, releaseName string) []argov1a1.ResourceIgnoreDifferences {
	expectedChart := chartName + "-" + chartVersion

	var rules []argov1a1.ResourceIgnoreDifferences
	for _, res := range parseResources(manifest) {
		labels, _ := res.Metadata["labels"].(map[string]any)
		if labels == nil {
			continue
		}

		chartLabel, _ := labels["helm.sh/chart"].(string)
		instanceLabel, _ := labels["app.kubernetes.io/instance"].(string)

		chartMismatch := chartLabel != "" && chartLabel != expectedChart
		instanceMismatch := instanceLabel != "" && releaseName != "" && instanceLabel != releaseName
		if !chartMismatch && !instanceMismatch {
			continue
		}

		rules = append(rules, argov1a1.ResourceIgnoreDifferences{
			Group:        res.Group,
			Kind:         res.Kind,
			Name:         res.Name,
			Namespace:    res.Namespace,
			JSONPointers: []string{"/spec", "/metadata/labels", "/metadata/annotations"},
		})
	}
	return rules
}

// oversizedCRDBytes stays comfortably under the API server's 256KiB
// metadata.annotations cap that client-side apply's own annotation hits.
const oversizedCRDBytes = 200 * 1024

// helmCRDNames returns CRD names from crdManifest (the chart's crds/
// content), plus the oversized subset (see oversizedCRDBytes) and their raw docs.
func helmCRDNames(crdManifest string) (names, oversized []string, oversizedDocs map[string]string) {
	if strings.TrimSpace(crdManifest) == "" {
		return nil, nil, nil
	}

	for _, doc := range yamlDocSeparator.Split(crdManifest, -1) {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}
		res := parseResources(doc)
		for _, r := range res {
			if r.Kind != "CustomResourceDefinition" {
				continue
			}
			names = append(names, r.Name)
			if len(doc) > oversizedCRDBytes {
				oversized = append(oversized, r.Name)
				if oversizedDocs == nil {
					oversizedDocs = make(map[string]string)
				}
				oversizedDocs[r.Name] = doc
			}
		}
	}
	sort.Strings(names)
	sort.Strings(oversized)
	return names, oversized, oversizedDocs
}

// nullFieldRules ignores explicit-null fields: they never survive a write
// (pruned server-side, dropped by typed controllers' omitempty), so they'd diff forever otherwise.
func nullFieldRules(manifest string) []argov1a1.ResourceIgnoreDifferences {
	var rules []argov1a1.ResourceIgnoreDifferences
	for _, res := range parseResources(manifest) {
		spec, ok := res.Spec.(map[string]any)
		if !ok {
			continue
		}
		ptrs := nullPointers("/spec", spec)
		if len(ptrs) == 0 {
			continue
		}
		sort.Strings(ptrs)
		rules = append(rules, argov1a1.ResourceIgnoreDifferences{
			Group:        res.Group,
			Kind:         res.Kind,
			Name:         res.Name,
			Namespace:    res.Namespace,
			JSONPointers: ptrs,
		})
	}
	return rules
}

// nullPointers returns a JSON pointer per null-valued field. Arrays aren't
// descended into -- a null element's index-based pointer is too fragile.
func nullPointers(prefix string, m map[string]any) []string {
	var ptrs []string
	for k, v := range m {
		p := prefix + "/" + escapeJSONPointer(k)
		switch vv := v.(type) {
		case nil:
			ptrs = append(ptrs, p)
		case map[string]any:
			ptrs = append(ptrs, nullPointers(p, vv)...)
		}
	}
	return ptrs
}

// escapeJSONPointer escapes a map key for use in an RFC 6901 JSON pointer.
func escapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}

// ZeroValuePointers returns a pointer per false/empty-object/array field
// (dropped by omitempty); zero strings/numbers are kept -- they carry real meaning in Kubernetes APIs.
func ZeroValuePointers(prefix string, m map[string]any) []string {
	var ptrs []string
	for k, v := range m {
		p := prefix + "/" + escapeJSONPointer(k)
		switch vv := v.(type) {
		case bool:
			if !vv {
				ptrs = append(ptrs, p)
			}
		case map[string]any:
			if len(vv) == 0 {
				ptrs = append(ptrs, p)
			} else {
				ptrs = append(ptrs, ZeroValuePointers(p, vv)...)
			}
		case []any:
			if len(vv) == 0 {
				ptrs = append(ptrs, p)
			}
		}
	}
	return ptrs
}

// renderedObjects lists every rendered resource that carries a spec map,
// with enough identity for callers to look up the matching CRD schema.
func renderedObjects(manifest string) []RenderedObject {
	var objs []RenderedObject
	for _, res := range parseResources(manifest) {
		spec, ok := res.Spec.(map[string]any)
		if !ok {
			continue
		}
		objs = append(objs, RenderedObject{
			Group:     res.Group,
			Version:   res.Version,
			Kind:      res.Kind,
			Name:      res.Name,
			Namespace: res.Namespace,
			Spec:      spec,
		})
	}
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].Kind+objs[i].Name < objs[j].Kind+objs[j].Name
	})
	return objs
}

// dedupeRules merges same-resource rules into one, unioning their
// pointers/expressions -- detection passes can overlap on a resource.
func dedupeRules(rules []argov1a1.ResourceIgnoreDifferences) []argov1a1.ResourceIgnoreDifferences {
	index := make(map[string]int, len(rules))
	var out []argov1a1.ResourceIgnoreDifferences
	for _, r := range rules {
		key := r.Group + "/" + r.Kind + "/" + r.Namespace + "/" + r.Name
		i, ok := index[key]
		if !ok {
			index[key] = len(out)
			out = append(out, r)
			continue
		}
		out[i].JSONPointers = unionStrings(out[i].JSONPointers, r.JSONPointers)
		out[i].JQPathExpressions = unionStrings(out[i].JQPathExpressions, r.JQPathExpressions)
	}
	return out
}

// unionStrings returns the sorted, deduplicated union of a and b.
func unionStrings(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	var out []string
	for _, s := range append(append([]string{}, a...), b...) {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func renderChart(ctx context.Context, chartName, chartVersion, repoURL, namespace, releaseName string, values map[string]any, creds *RegistryCredentials) (manifest, crdManifest string, err error) {
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
				return "", "", fmt.Errorf("build TLS client: %w", err)
			}
			regOpts = append(regOpts, registry.ClientOptHTTPClient(httpClient))
		}
	}

	regClient, err := registry.NewClient(regOpts...)
	if err != nil {
		return "", "", fmt.Errorf("registry client: %w", err)
	}

	actionConfig := new(action.Configuration)
	actionConfig.Releases = storage.Init(driver.NewMemory())
	actionConfig.KubeClient = &noopKubeClient{}
	actionConfig.Log = func(format string, v ...any) {}
	actionConfig.RegistryClient = regClient

	// Pull chart
	pullClient := action.NewPullWithOpts(action.WithConfig(actionConfig))
	pullClient.Version = chartVersion
	pullClient.Settings = settings
	pullClient.SetRegistryClient(regClient)

	tmpDir, err := os.MkdirTemp("", "helm-pull-*")
	if err != nil {
		return "", "", fmt.Errorf("temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	pullClient.DestDir = tmpDir

	chartRef := fmt.Sprintf("oci://%s/%s", repoURL, chartName)
	if _, err := pullClient.Run(chartRef); err != nil {
		return "", "", fmt.Errorf("pull %s:%s: %w", chartRef, chartVersion, err)
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
		return "", "", fmt.Errorf("no chart found in %s", tmpDir)
	}

	chrt, err := loader.Load(chartPath)
	if err != nil {
		return "", "", fmt.Errorf("load chart: %w", err)
	}

	// Render
	installClient := action.NewInstall(actionConfig)
	installClient.DryRunOption = "client"
	// Render under the real release name -- otherwise every resource of a
	// release named differently from its chart would look foreign.
	installClient.ReleaseName = releaseName
	if installClient.ReleaseName == "" {
		installClient.ReleaseName = chartName
	}
	installClient.Namespace = namespace
	installClient.ClientOnly = true
	installClient.IncludeCRDs = true
	installClient.Replace = true
	installClient.SetRegistryClient(regClient)

	kv, _ := chartutil.ParseKubeVersion("v1.31.0")
	installClient.KubeVersion = kv

	rel, err := installClient.RunWithContext(ctx, chrt, values)
	if err != nil {
		return "", "", fmt.Errorf("render: %w", err)
	}

	// Also collect CRDs some charts (e.g. keda) author as regular templates,
	// invisible to helmCRDNames.
	var crdDocs []string
	crdNames := make(map[string]bool)
	for _, crd := range chrt.CRDObjects() {
		doc := string(crd.File.Data)
		crdDocs = append(crdDocs, doc)
		for _, r := range parseResources(doc) {
			if r.Kind == "CustomResourceDefinition" {
				crdNames[r.Name] = true
			}
		}
	}
	for _, doc := range yamlDocSeparator.Split(rel.Manifest, -1) {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}
		for _, r := range parseResources(doc) {
			if r.Kind == "CustomResourceDefinition" && !crdNames[r.Name] {
				crdNames[r.Name] = true
				crdDocs = append(crdDocs, doc)
			}
		}
	}

	return rel.Manifest, strings.Join(crdDocs, "\n---\n"), nil
}

// Resource represents a parsed Kubernetes resource
type Resource struct {
	Group     string
	Version   string
	Kind      string
	Name      string
	Namespace string
	Data      any
	Spec      any
	Metadata  map[string]any
	Raw       map[string]any
}

func (r *Resource) Key() string {
	return fmt.Sprintf("%s/%s/%s/%s", r.Group, r.Kind, r.Namespace, r.Name)
}

// yamlDocSeparator matches a "---" line exactly; splitting on the bare
// substring would shred embedded PEM blocks in string values.
var yamlDocSeparator = regexp.MustCompile(`(?m)^---\s*$`)

func parseResources(rendered string) map[string]*Resource {
	resources := make(map[string]*Resource)
	docs := yamlDocSeparator.Split(rendered, -1)

	for _, doc := range docs {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}

		var raw map[string]any
		if err := yaml.Unmarshal([]byte(doc), &raw); err != nil {
			continue
		}

		apiVersion, _ := raw["apiVersion"].(string)
		kind, _ := raw["kind"].(string)
		metadata, _ := raw["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		ns, _ := metadata["namespace"].(string)

		if kind == "" || name == "" {
			continue
		}

		group, version := "", apiVersion
		parts := strings.SplitN(apiVersion, "/", 2)
		if len(parts) == 2 {
			group, version = parts[0], parts[1]
		}

		resources[fmt.Sprintf("%s/%s/%s/%s", group, kind, ns, name)] = &Resource{
			Group:     group,
			Version:   version,
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

			// A Secret/ConfigMap payload that differs between renders is
			// generated or lookup-guarded -- leave the live value alone.
			if res1.Kind == "Secret" || res1.Kind == "ConfigMap" {
				if !deepEqualJSON(res1.Data, res2.Data) {
					r := addRule(res1)
					addPointer(r, "/data")
				}
				if !deepEqualJSON(res1.Raw["stringData"], res2.Raw["stringData"]) {
					r := addRule(res1)
					addPointer(r, "/data")
					addPointer(r, "/stringData")
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

func diffAnnotations(metaA, metaB map[string]any, prefix string) []string {
	annA, _ := metaA["annotations"].(map[string]any)
	annB, _ := metaB["annotations"].(map[string]any)
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

func diffTemplateAnnotations(specA, specB any) []string {
	sA, _ := specA.(map[string]any)
	sB, _ := specB.(map[string]any)
	if sA == nil || sB == nil {
		return nil
	}

	tplA, _ := sA["template"].(map[string]any)
	tplB, _ := sB["template"].(map[string]any)
	if tplA == nil || tplB == nil {
		return nil
	}

	metaA, _ := tplA["metadata"].(map[string]any)
	metaB, _ := tplB["metadata"].(map[string]any)
	if metaA == nil || metaB == nil {
		return nil
	}

	return diffAnnotations(metaA, metaB, "/spec/template/metadata/annotations")
}

func hasWebhookCaBundleDiff(a, b map[string]any) bool {
	webhooksA, _ := a["webhooks"].([]any)
	webhooksB, _ := b["webhooks"].([]any)
	if len(webhooksA) != len(webhooksB) {
		return false
	}
	for i := 0; i < len(webhooksA); i++ {
		whA, _ := webhooksA[i].(map[string]any)
		whB, _ := webhooksB[i].(map[string]any)
		if whA == nil || whB == nil {
			continue
		}
		ccA, _ := whA["clientConfig"].(map[string]any)
		ccB, _ := whB["clientConfig"].(map[string]any)
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

func hasAPICaBundleDiff(a, b any) bool {
	specA, _ := a.(map[string]any)
	specB, _ := b.(map[string]any)
	if specA == nil || specB == nil {
		return false
	}
	cbA, _ := specA["caBundle"].(string)
	cbB, _ := specB["caBundle"].(string)
	return cbA != "" && cbB != "" && cbA != cbB
}

func deepEqualJSON(a, b any) bool {
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

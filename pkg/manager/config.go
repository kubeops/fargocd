/*
Copyright AppsCode Inc. and Contributors

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

package manager

import (
	"fmt"
	"os"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kmapi "kmodules.xyz/client-go/api/v1"
	"open-cluster-management.io/addon-framework/pkg/addonfactory"
	addonv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"
	clusterv1 "open-cluster-management.io/api/cluster/v1"
	"sigs.k8s.io/yaml"
)

// GetConfigValues returns the addon-framework GetValues func that
// renders per-ManagedCluster values for the embedded fargocd chart.
//
// Hub-wide flags become argocd.* keys; the per-spoke clusterName is
// taken from cluster.Name; on OpenShift spokes the chart's runAsUser /
// fsGroup are cleared so it falls back to the project's SCC range.
func GetConfigValues(opts *ManagerOptions) (addonfactory.GetValuesFunc, error) {
	var argoKubeconfig string
	if opts.ArgoKubeconfigFile != "" {
		raw, err := os.ReadFile(opts.ArgoKubeconfigFile)
		if err != nil {
			return nil, fmt.Errorf("read --argo-kubeconfig-file: %w", err)
		}
		argoKubeconfig = string(raw)
	}

	return func(cluster *clusterv1.ManagedCluster, _ *addonv1alpha1.ManagedClusterAddOn) (addonfactory.Values, error) {
		data, err := FS.ReadFile(AgentManifestsDir + "/values.yaml")
		if err != nil {
			return nil, err
		}
		var vals map[string]any
		if err := yaml.Unmarshal(data, &vals); err != nil {
			return nil, err
		}

		argocd := map[string]any{
			"mode": opts.Mode,
		}
		if opts.ArgoNamespace != "" {
			argocd["namespace"] = opts.ArgoNamespace
		}
		if opts.DestinationServer != "" {
			argocd["destServer"] = opts.DestinationServer
		}
		if opts.DestinationName != "" {
			argocd["destName"] = opts.DestinationName
		}
		if opts.Project != "" {
			argocd["project"] = opts.Project
		}
		argocd["clusterName"] = cluster.Name
		if argoKubeconfig != "" {
			argocd["kubeconfig"] = argoKubeconfig
		}
		if opts.ArgoKubeconfigSecret != "" {
			argocd["kubeconfigSecret"] = opts.ArgoKubeconfigSecret
		}

		overrides := map[string]any{
			"argocd":          argocd,
			"imagePullPolicy": "Always",
		}
		if opts.RegistryFQDN != "" {
			overrides["registryFQDN"] = opts.RegistryFQDN
		}

		vals = addonfactory.MergeValues(vals, overrides)

		for _, cc := range cluster.Status.ClusterClaims {
			if cc.Name != kmapi.ClusterClaimKeyInfo {
				continue
			}
			var info kmapi.ClusterClaimInfo
			if err := yaml.Unmarshal([]byte(cc.Value), &info); err != nil {
				return nil, err
			}
			if slices.Contains(info.ClusterMetadata.ClusterManagers, kmapi.ClusterManagerOpenShift.Name()) {
				if err := unstructured.SetNestedField(vals, nil, "image", "securityContext", "runAsUser"); err != nil {
					return nil, err
				}
				if err := unstructured.SetNestedField(vals, nil, "podSecurityContext", "fsGroup"); err != nil {
					return nil, err
				}
			}
			break
		}

		return vals, nil
	}, nil
}

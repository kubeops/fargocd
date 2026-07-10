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
	"kubeops.dev/fargocd/pkg/mode"

	"github.com/spf13/pflag"
)

// ManagerOptions are the hub-side flags controlling how each spoke is
// rendered; map onto `argocd.*` chart values, falling through when empty.
type ManagerOptions struct {
	RegistryFQDN string

	Mode              string
	ArgoNamespace     string
	DestinationServer string
	DestinationName   string
	Project           string

	// Propagated to every spoke as argocd.kubeconfig. Required when
	// Mode=managed and ArgoKubeconfigSecret is empty.
	ArgoKubeconfigFile string
	// Pre-created per-spoke Secret holding the principal kubeconfig;
	// mutually exclusive with ArgoKubeconfigFile.
	ArgoKubeconfigSecret string
}

func NewManagerOptions() *ManagerOptions {
	return &ManagerOptions{
		// Preserves pre-existing behavior across binary upgrades.
		Mode: string(mode.InCluster),
		// No default: see run.go's runOptions for why.
		Project: "default",
	}
}

func (s *ManagerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&s.RegistryFQDN, "registryFQDN", s.RegistryFQDN, "Docker registry FQDN used for the agent image (overrides chart default).")
	fs.StringVar(&s.Mode, "mode", s.Mode, "How fargocd on spoke clusters talks to Argo CD: 'in-cluster', 'autonomous', or 'managed'.")
	fs.StringVar(&s.ArgoNamespace, "argo-namespace", s.ArgoNamespace, "Namespace on the Argo CD cluster where Applications are written. Auto-discovered when empty.")
	fs.StringVar(&s.DestinationServer, "argo-dest-server", s.DestinationServer, "Application.spec.destination.server.")
	fs.StringVar(&s.DestinationName, "argo-dest-name", s.DestinationName, "Application.spec.destination.name.")
	fs.StringVar(&s.Project, "argo-project", s.Project, "Argo CD Project assigned to generated Applications.")
	fs.StringVar(&s.ArgoKubeconfigFile, "argo-kubeconfig-file", s.ArgoKubeconfigFile, "Path to a kubeconfig for the Argo CD principal cluster, propagated to every spoke (managed mode).")
	fs.StringVar(&s.ArgoKubeconfigSecret, "argo-kubeconfig-secret", s.ArgoKubeconfigSecret, "Name of an existing Secret on each spoke holding the principal kubeconfig (managed mode; alternative to --argo-kubeconfig-file).")
}

func (s *ManagerOptions) Validate() []error {
	var errs []error
	if _, err := mode.Parse(s.Mode); err != nil {
		errs = append(errs, err)
	}
	return errs
}

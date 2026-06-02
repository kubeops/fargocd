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

// Package mode defines the deployment shapes that fargocd supports.
//
// The bridge can be configured to talk to an Argo CD that lives on the same
// cluster as the workload, or to an argocd-agent principal that lives on a
// remote cluster, using either the "autonomous" or "managed" agent mode
// described in https://argocd-agent.readthedocs.io/latest/concepts/agent-modes/.
package mode

import "fmt"

// Mode identifies how fargocd should communicate with Argo CD.
type Mode string

const (
	// InCluster is the classic deployment: a regular Argo CD installation
	// runs on the same cluster as fargocd. The bridge writes Application
	// resources to that local Argo CD and the destination defaults to the
	// in-cluster API server.
	InCluster Mode = "in-cluster"

	// Autonomous corresponds to the argocd-agent "autonomous" mode. The
	// agent runs alongside the workload. fargocd writes Application
	// resources locally, the agent reconciles them against the local API
	// server, and any status changes are pushed back to the principal.
	Autonomous Mode = "autonomous"

	// Managed corresponds to the argocd-agent "managed" mode. The Argo CD
	// principal runs on a remote cluster. fargocd writes Application
	// resources to a namespace on the principal (typically named after the
	// workload cluster), and the agent on the workload cluster pulls them.
	Managed Mode = "managed"
)

// Parse validates a user-supplied mode string and returns the canonical
// Mode value, or an error if the input is not recognised.
func Parse(s string) (Mode, error) {
	switch Mode(s) {
	case InCluster, Autonomous, Managed:
		return Mode(s), nil
	case "":
		return InCluster, nil
	default:
		return "", fmt.Errorf("unknown mode %q (want one of %s, %s, %s)",
			s, InCluster, Autonomous, Managed)
	}
}

// RemotePrincipal reports whether the mode points at an Argo CD principal
// running on a different cluster than fargocd itself.
func (m Mode) RemotePrincipal() bool {
	return m == Managed
}

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
	"fmt"

	fluxhelmv2 "github.com/fluxcd/helm-controller/api/v2"
)

// aceChartName is the AppsCode Container Engine umbrella chart. By
// convention only one ACE release exists per principal Argo CD even when
// many spoke clusters share that principal, so it does not get a cluster
// suffix appended to its Application name.
const aceChartName = "ace"

// applicationName returns the Argo CD Application name that should mirror a
// given HelmRelease.
//
// When clusterName is set (typically in argocd-agent managed mode where a
// single principal serves several workload clusters), the suffix
// "-<clusterName>" is appended so that releases from different clusters do
// not collide. The ACE umbrella chart is an explicit exception per the
// project's deployment model.
func applicationName(hr *fluxhelmv2.HelmRelease, clusterName string) string {
	if hr == nil {
		return ""
	}
	if isACE(hr) || clusterName == "" {
		return hr.Name
	}
	return fmt.Sprintf("%s-%s", hr.Name, clusterName)
}

// isACE reports whether the HelmRelease points at the ACE umbrella chart.
// Both the HelmRelease's name and the chart name are checked because users
// sometimes rename the resource but keep the chart identifier intact.
func isACE(hr *fluxhelmv2.HelmRelease) bool {
	if hr.Name == aceChartName {
		return true
	}
	if hr.Spec.Chart != nil && hr.Spec.Chart.Spec.Chart == aceChartName {
		return true
	}
	return false
}

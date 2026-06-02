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
	"testing"

	fluxhelmv2 "github.com/fluxcd/helm-controller/api/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestApplicationName(t *testing.T) {
	tests := []struct {
		name        string
		hrName      string
		chart       string
		clusterName string
		want        string
	}{
		{"no cluster suffix when clusterName empty", "kubedb", "kubedb", "", "kubedb"},
		{"appends clusterName when provided", "kubedb", "kubedb", "prod", "kubedb-prod"},
		{"ace HR name is exempt", "ace", "ace", "prod", "ace"},
		{"ace chart name is exempt even with different HR name", "ace-prod", "ace", "prod", "ace-prod"},
		{"no suffix without cluster, ace too", "ace", "ace", "", "ace"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hr := &fluxhelmv2.HelmRelease{
				ObjectMeta: metav1.ObjectMeta{Name: tc.hrName},
				Spec: fluxhelmv2.HelmReleaseSpec{
					Chart: &fluxhelmv2.HelmChartTemplate{
						Spec: fluxhelmv2.HelmChartTemplateSpec{Chart: tc.chart},
					},
				},
			}
			got := applicationName(hr, tc.clusterName)
			if got != tc.want {
				t.Errorf("applicationName(%q, chart=%q, cluster=%q) = %q, want %q",
					tc.hrName, tc.chart, tc.clusterName, got, tc.want)
			}
		})
	}
}

func TestApplicationName_NilSafe(t *testing.T) {
	if got := applicationName(nil, "prod"); got != "" {
		t.Errorf("nil HelmRelease should yield empty name, got %q", got)
	}
}

// Specifically: the ace exception must apply even when isACE is checked
// through the chart name alone. This guards against future code that
// trims the HelmRelease.Name suffix from the ace check.
func TestApplicationName_AceByChart(t *testing.T) {
	hr := &fluxhelmv2.HelmRelease{
		ObjectMeta: metav1.ObjectMeta{Name: "ace-east1"},
		Spec: fluxhelmv2.HelmReleaseSpec{
			Chart: &fluxhelmv2.HelmChartTemplate{
				Spec: fluxhelmv2.HelmChartTemplateSpec{Chart: "ace"},
			},
		},
	}
	if got := applicationName(hr, "east1"); got != "ace-east1" {
		t.Errorf("expected name unchanged for ace chart, got %q", got)
	}
}

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

// Wires client-go's reflector (informer) metrics into the legacy
// Prometheus registry that backs the addon-framework's /metrics
// endpoint. Pairs with the workqueue blank import in metrics.go:
// workqueue covers reconcile backlog/throughput, reflector covers
// list/watch behaviour against the API server.

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/metrics/legacyregistry"
)

const reflectorSubsystem = "reflector"

var summaryObjectives = map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001}

var (
	reflectorListsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: reflectorSubsystem,
			Name:      "lists_total",
			Help:      "Total number of API lists done by the reflectors.",
		},
		[]string{"name"},
	)
	reflectorListDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Subsystem:  reflectorSubsystem,
			Name:       "list_duration_seconds",
			Help:       "How long an API list takes to return and decode for the reflectors.",
			Objectives: summaryObjectives,
		},
		[]string{"name"},
	)
	reflectorItemsPerList = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Subsystem:  reflectorSubsystem,
			Name:       "items_per_list",
			Help:       "How many items an API list returns to the reflectors.",
			Objectives: summaryObjectives,
		},
		[]string{"name"},
	)
	reflectorWatchesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: reflectorSubsystem,
			Name:      "watches_total",
			Help:      "Total number of API watches done by the reflectors.",
		},
		[]string{"name"},
	)
	reflectorShortWatchesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Subsystem: reflectorSubsystem,
			Name:      "short_watches_total",
			Help:      "Total number of short API watches done by the reflectors.",
		},
		[]string{"name"},
	)
	reflectorWatchDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Subsystem:  reflectorSubsystem,
			Name:       "watch_duration_seconds",
			Help:       "How long an API watch takes to return and decode for the reflectors.",
			Objectives: summaryObjectives,
		},
		[]string{"name"},
	)
	reflectorItemsPerWatch = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Subsystem:  reflectorSubsystem,
			Name:       "items_per_watch",
			Help:       "How many items an API watch returns to the reflectors.",
			Objectives: summaryObjectives,
		},
		[]string{"name"},
	)
	reflectorLastResourceVersion = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: reflectorSubsystem,
			Name:      "last_resource_version",
			Help:      "Last resource version seen for the reflectors.",
		},
		[]string{"name"},
	)
)

type prometheusReflectorMetricsProvider struct{}

func (prometheusReflectorMetricsProvider) NewListsMetric(name string) cache.CounterMetric {
	return reflectorListsTotal.WithLabelValues(name)
}

func (prometheusReflectorMetricsProvider) NewListDurationMetric(name string) cache.SummaryMetric {
	return reflectorListDuration.WithLabelValues(name)
}

func (prometheusReflectorMetricsProvider) NewItemsInListMetric(name string) cache.SummaryMetric {
	return reflectorItemsPerList.WithLabelValues(name)
}

func (prometheusReflectorMetricsProvider) NewWatchesMetric(name string) cache.CounterMetric {
	return reflectorWatchesTotal.WithLabelValues(name)
}

func (prometheusReflectorMetricsProvider) NewShortWatchesMetric(name string) cache.CounterMetric {
	return reflectorShortWatchesTotal.WithLabelValues(name)
}

func (prometheusReflectorMetricsProvider) NewWatchDurationMetric(name string) cache.SummaryMetric {
	return reflectorWatchDuration.WithLabelValues(name)
}

func (prometheusReflectorMetricsProvider) NewItemsInWatchMetric(name string) cache.SummaryMetric {
	return reflectorItemsPerWatch.WithLabelValues(name)
}

func (prometheusReflectorMetricsProvider) NewLastResourceVersionMetric(name string) cache.GaugeMetric {
	return reflectorLastResourceVersion.WithLabelValues(name)
}

func init() {
	legacyregistry.RawMustRegister(
		reflectorListsTotal,
		reflectorListDuration,
		reflectorItemsPerList,
		reflectorWatchesTotal,
		reflectorShortWatchesTotal,
		reflectorWatchDuration,
		reflectorItemsPerWatch,
		reflectorLastResourceVersion,
	)
	// SetReflectorMetricsProvider uses sync.Once internally; if some
	// other package already won the race, our collectors stay
	// registered but unused — harmless, they'll just report zeros.
	cache.SetReflectorMetricsProvider(prometheusReflectorMetricsProvider{})
}

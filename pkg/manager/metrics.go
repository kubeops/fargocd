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

// Wires client-go's workqueue metrics into the legacy Prometheus
// registry that backs the addon-framework's /metrics endpoint. Without
// this, /metrics only returns Go runtime and process collectors — the
// addon controllers' workqueue depth, add/retry rates, and processing
// latencies stay invisible.
import _ "k8s.io/component-base/metrics/prometheus/workqueue"

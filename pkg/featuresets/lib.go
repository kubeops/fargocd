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

package featuresets

import (
	"bytes"
	"embed"
	"os"

	argov1a1 "github.com/argoproj/argo-cd/v2/pkg/apis/application/v1alpha1"
	"sigs.k8s.io/yaml"
)

//go:embed *.yaml **/*.yaml
var fs embed.FS

func IgnoreDifferences(filename string) ([]argov1a1.ResourceIgnoreDifferences, error) {
	data, err := fs.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, nil
	}

	var app argov1a1.Application
	err = yaml.Unmarshal(data, &app)
	if err != nil {
		return nil, err
	}
	return app.Spec.IgnoreDifferences, nil
}

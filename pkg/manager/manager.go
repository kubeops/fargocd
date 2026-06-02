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

// Package manager exposes the OCM AddOn manager subcommand. It runs on
// an Open Cluster Management hub and uses the addon-framework Helm
// agent factory to ship the embedded fargocd installer chart to every
// selected ManagedCluster (spoke).
package manager

import (
	"context"
	"embed"

	"github.com/spf13/cobra"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/version"
	"k8s.io/klog/v2"
	"open-cluster-management.io/addon-framework/pkg/addonfactory"
	"open-cluster-management.io/addon-framework/pkg/addonmanager"
	cmdfactory "open-cluster-management.io/addon-framework/pkg/cmd/factory"
	addonv1alpha1 "open-cluster-management.io/api/addon/v1alpha1"
)

//go:embed all:agent-manifests
var FS embed.FS

// NewManagerCommand returns the `fargocd manager` Cobra subcommand.
func NewManagerCommand() *cobra.Command {
	opts := NewManagerOptions()

	cmd := cmdfactory.
		NewControllerCommandConfig(AddonName, version.Get(), func(ctx context.Context, config *rest.Config) error {
			return runManagerController(ctx, config, opts)
		}).
		NewCommand()
	cmd.Use = "manager"
	cmd.Short = "Run the fargocd OCM AddOn manager on an Open Cluster Management hub"
	opts.AddFlags(cmd.Flags())

	return cmd
}

func runManagerController(ctx context.Context, cfg *rest.Config, opts *ManagerOptions) error {
	if errs := opts.Validate(); len(errs) > 0 {
		return errs[0]
	}

	getValues, err := GetConfigValues(opts)
	if err != nil {
		return err
	}

	addonManager, err := addonmanager.New(cfg)
	if err != nil {
		return err
	}

	agentAddon, err := addonfactory.NewAgentAddonFactory(AddonName, FS, AgentManifestsDir).
		WithScheme(scheme).
		WithGetValuesFuncs(getValues).
		WithAgentInstallNamespace(func(addon *addonv1alpha1.ManagedClusterAddOn) (string, error) {
			return AddonInstallationNamespace, nil
		}).
		BuildHelmAgentAddon()
	if err != nil {
		klog.Errorf("build fargocd agent addon: %v", err)
		return err
	}

	if err := addonManager.AddAgent(agentAddon); err != nil {
		return err
	}
	return addonManager.Start(ctx)
}

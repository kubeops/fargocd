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

package cmds

import (
	"context"
	"crypto/tls"
	"fmt"
	"path/filepath"

	"kubeops.dev/fargocd/pkg/controller"
	"kubeops.dev/fargocd/pkg/mode"

	argov1a1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	fluxhelmv2 "github.com/fluxcd/helm-controller/api/v2"
	fluxsrcv1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/spf13/cobra"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	uiapi "kmodules.xyz/resource-metadata/apis/ui/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(fluxhelmv2.AddToScheme(scheme))
	utilruntime.Must(fluxsrcv1.AddToScheme(scheme))
	utilruntime.Must(argov1a1.AddToScheme(scheme))
	utilruntime.Must(uiapi.AddToScheme(scheme))
}

// runOptions collects the user-facing flags accepted by `fargocd run`.
type runOptions struct {
	metricsAddr          string
	probeAddr            string
	enableLeaderElection bool
	secureMetrics        bool
	enableHTTP2          bool
	certDir              string

	mode              string
	argoKubeconfig    string
	argoNamespace     string
	destinationServer string
	destinationName   string
	project           string
	clusterName       string
}

func NewCmdRun() *cobra.Command {
	opts := runOptions{
		metricsAddr:       "0",
		probeAddr:         ":8081",
		secureMetrics:     true,
		mode:              string(mode.InCluster),
		destinationServer: "https://kubernetes.default.svc",
		project:           "default",
	}

	cmd := &cobra.Command{
		Use:               "run",
		Short:             "Launch the FluxCD to Argo CD bridge controller",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runOperator(opts)
		},
	}

	cmd.Flags().StringVar(&opts.metricsAddr, "metrics-bind-address", opts.metricsAddr,
		"The address the metrics endpoint binds to. Use :8443 for HTTPS, :8080 for HTTP, or 0 to disable.")
	cmd.Flags().StringVar(&opts.probeAddr, "health-probe-bind-address", opts.probeAddr,
		"The address the probe endpoint binds to.")
	cmd.Flags().BoolVar(&opts.enableLeaderElection, "leader-elect", opts.enableLeaderElection,
		"Enable leader election so only one fargocd instance is active at a time.")
	cmd.Flags().BoolVar(&opts.secureMetrics, "metrics-secure", opts.secureMetrics,
		"Serve the metrics endpoint over HTTPS.")
	cmd.Flags().StringVar(&opts.certDir, "cert-dir", opts.certDir,
		"Directory containing the webhook/metrics server certificate.")
	cmd.Flags().BoolVar(&opts.enableHTTP2, "enable-http2", opts.enableHTTP2,
		"Enable HTTP/2 for the metrics and webhook servers (disabled by default; see GHSA-qppj-fm5r-hxr3).")

	cmd.Flags().StringVar(&opts.mode, "mode", opts.mode,
		"How fargocd talks to Argo CD: 'in-cluster', 'autonomous', or 'managed'.")
	cmd.Flags().StringVar(&opts.argoKubeconfig, "argo-kubeconfig", opts.argoKubeconfig,
		"Path to a kubeconfig for the Argo CD principal cluster (required in managed mode).")
	cmd.Flags().StringVar(&opts.argoNamespace, "argo-namespace", opts.argoNamespace,
		"Namespace on the Argo CD cluster where Applications are written. Auto-discovered when empty.")
	cmd.Flags().StringVar(&opts.destinationServer, "argo-dest-server", opts.destinationServer,
		"Application.spec.destination.server. Defaults to the in-cluster API server.")
	cmd.Flags().StringVar(&opts.destinationName, "argo-dest-name", opts.destinationName,
		"Application.spec.destination.name. Useful in managed mode to reference a cluster by symbolic name.")
	cmd.Flags().StringVar(&opts.project, "argo-project", opts.project,
		"Argo CD Project assigned to generated Applications.")
	cmd.Flags().StringVar(&opts.clusterName, "cluster-name", opts.clusterName,
		"Symbolic name of the workload cluster. Required in managed mode; suffixes Application names to avoid collisions when one principal serves many clusters.")
	return cmd
}

// runOperator wires up the manager(s) and starts the reconciler.
func runOperator(opts runOptions) error {
	ctrl.SetLogger(klog.NewKlogr())

	mode, err := mode.Parse(opts.mode)
	if err != nil {
		setupLog.Error(err, "invalid mode")
		return err
	}
	if mode.RemotePrincipal() {
		if opts.argoKubeconfig == "" {
			return fmt.Errorf("--argo-kubeconfig is required when --mode=managed")
		}
		if opts.clusterName == "" {
			return fmt.Errorf("--cluster-name is required when --mode=managed")
		}
	}

	// http/2 is disabled by default; see GHSA-qppj-fm5r-hxr3 / GHSA-4374-p667-p6c8.
	var tlsOpts []func(*tls.Config)
	if !opts.enableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			setupLog.Info("disabling http/2")
			c.NextProtos = []string{"http/1.1"}
		})
	}

	var certWatcher *certwatcher.CertWatcher
	if opts.certDir != "" {
		setupLog.Info("initializing certificate watcher",
			"cert-dir", opts.certDir, "cert-name", core.TLSCertKey, "cert-key", core.TLSPrivateKeyKey)
		var err error
		certWatcher, err = certwatcher.New(
			filepath.Join(opts.certDir, core.TLSCertKey),
			filepath.Join(opts.certDir, core.TLSPrivateKeyKey),
		)
		if err != nil {
			setupLog.Error(err, "failed to initialize certificate watcher")
			return err
		}
		tlsOpts = append(tlsOpts, func(config *tls.Config) {
			config.GetCertificate = certWatcher.GetCertificate
		})
	}

	webhookServer := webhook.NewServer(webhook.Options{TLSOpts: tlsOpts})

	metricsServerOptions := metricsserver.Options{
		BindAddress:   opts.metricsAddr,
		SecureServing: opts.secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if opts.secureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: opts.probeAddr,
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       "03b9a431.fargocd.appscode.com",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		return err
	}

	argoManager, err := buildArgoManager(opts, mgr)
	if err != nil {
		setupLog.Error(err, "unable to start Argo CD cluster manager")
		return err
	}

	reconciler := &controller.HelmReleaseReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		ArgoClient:        argoManager.GetClient(),
		Mode:              mode,
		ArgoNamespace:     opts.argoNamespace,
		DestinationServer: opts.destinationServer,
		DestinationName:   opts.destinationName,
		Project:           opts.project,
		ClusterName:       opts.clusterName,
	}
	if err := reconciler.SetupWithManager(mgr, argoManager); err != nil {
		setupLog.Error(err, "unable to set up controller", "controller", "HelmRelease")
		return err
	}

	if certWatcher != nil {
		if err := mgr.Add(certWatcher); err != nil {
			setupLog.Error(err, "unable to add certificate watcher to manager")
			return err
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	setupLog.Info("starting manager", "mode", mode)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		return err
	}
	return nil
}

// buildArgoManager returns the manager that owns the connection to the Argo
// CD cluster. When --argo-kubeconfig is set, it manages a remote cluster;
// otherwise it just reuses the local manager.
func buildArgoManager(opts runOptions, mgr ctrl.Manager) (ctrl.Manager, error) {
	if opts.argoKubeconfig == "" {
		return mgr, nil
	}

	argoConfig, err := clientcmd.BuildConfigFromFlags("", opts.argoKubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build Argo CD rest config: %w", err)
	}
	argoMgr, err := ctrl.NewManager(argoConfig, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       "03b9a432.fargocd.appscode.com",
	})
	if err != nil {
		return nil, fmt.Errorf("create Argo CD manager: %w", err)
	}
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		return argoMgr.Start(ctx)
	})); err != nil {
		return nil, fmt.Errorf("add Argo CD manager: %w", err)
	}
	return argoMgr, nil
}

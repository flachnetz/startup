package startup_kube

import (
	"github.com/flachnetz/startup/v2/startup_base"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubernetesOptions struct {
	KubeConfig  string `long:"kube-config" description:"Filename of the kubeconfig to use. Uses in cluster config if not specified"`
	KubeContext string `long:"kube-context" description:"Select the kubernetes context to use from the config"`
}

func (opts *KubernetesOptions) Client() *kubernetes.Clientset {
	if opts.KubeConfig != "" {
		config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: opts.KubeConfig},
			&clientcmd.ConfigOverrides{CurrentContext: opts.KubeContext}).ClientConfig()

		startup_base.FatalOnError(err, "Initializing kube config failed")

		clientSet, err := kubernetes.NewForConfig(config)
		startup_base.FatalOnError(err, "Initialize kube client with config at %q", opts.KubeConfig)

		return clientSet
	}

	config, err := rest.InClusterConfig()
	startup_base.FatalOnError(err, "Initialize in cluster config")

	clientSet, err := kubernetes.NewForConfig(config)
	startup_base.FatalOnError(err, "Initialize kubernetes client from within cluster")

	return clientSet
}

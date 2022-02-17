/*
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

package main

import (
	"github.com/aws/karpenter/pkg/apis"
	"github.com/aws/karpenter/pkg/cloudprovider"
	cloudprovidermetrics "github.com/aws/karpenter/pkg/cloudprovider/metrics"
	"github.com/aws/karpenter/pkg/cloudprovider/registry"
	"github.com/aws/karpenter/pkg/controllers/counter"
	metricsnode "github.com/aws/karpenter/pkg/controllers/metrics/node"
	metricspod "github.com/aws/karpenter/pkg/controllers/metrics/pod"
	"github.com/aws/karpenter/pkg/controllers/node"
	"github.com/aws/karpenter/pkg/controllers/persistentvolumeclaim"
	"github.com/aws/karpenter/pkg/controllers/provisioning"
	"github.com/aws/karpenter/pkg/controllers/selection"
	"github.com/aws/karpenter/pkg/controllers/termination"
	"github.com/aws/karpenter/pkg/utils/injection"
	"github.com/aws/karpenter/pkg/utils/options"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/util/flowcontrol"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/signals"
	controllerruntime "sigs.k8s.io/controller-runtime"
)

var (
	scheme    = runtime.NewScheme()
	opts      = options.MustParse()
	component = "controller"
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(apis.AddToScheme(scheme))
}

func main() {
	config := controllerruntime.GetConfigOrDie()
	config.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(float32(opts.KubeClientQPS), opts.KubeClientBurst)
	config.UserAgent = "karpenter"
	clientSet := kubernetes.NewForConfigOrDie(config)

	ctx := signals.NewContext()
	ctx = injection.WithConfig(ctx, config)
	ctx = injection.WithOptions(ctx, opts)

	// Set up controller runtime controller
	cloudProvider := registry.NewCloudProvider(ctx, cloudprovider.Options{ClientSet: clientSet})
	cloudProvider = cloudprovidermetrics.Decorate(cloudProvider)

	ctx = cloudprovider.With(ctx, cloudProvider)
	ctx = provisioning.With(ctx, provisioning.NewProvisioners())

	// TODO (todd): figure out metrics / health probes

	/*		MetricsBindAddress:     fmt.Sprintf(":%d", opts.MetricsPort),
			HealthProbeBindAddress: fmt.Sprintf(":%d", opts.HealthProbePort),*/

	sharedmain.MainWithContext(ctx, "controller",
		provisioning.NewController,
		selection.NewController,
		persistentvolumeclaim.NewController,
		node.NewController,
		counter.NewController,
		metricspod.NewController,
		metricsnode.NewController,
		termination.NewController,
	)
}

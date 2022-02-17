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

package provisioning

import (
	"context"

	provisioningclient "github.com/aws/karpenter/pkg/client/injection/client"
	provisioninginformer "github.com/aws/karpenter/pkg/client/injection/informers/provisioning/v1alpha5/provisioner"
	provisioningreconciler "github.com/aws/karpenter/pkg/client/injection/reconciler/provisioning/v1alpha5/provisioner"
	"github.com/aws/karpenter/pkg/cloudprovider"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
)

var _ provisioningreconciler.Interface = (*Reconciler)(nil)
var _ provisioningreconciler.Finalizer = (*Reconciler)(nil)

func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	r := NewReconciler(ctx)

	impl := provisioningreconciler.NewImpl(ctx, r)
	impl.Name = "provisioning"

	provisioninginformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	return impl
}

func NewReconciler(ctx context.Context) *Reconciler {
	kubeClient := kubeclient.Get(ctx)

	return &Reconciler{
		KarpClient:    provisioningclient.Get(ctx),
		KubeClient:    kubeClient,
		Provisioners:  GetProvisioners(ctx),
		CloudProvider: cloudprovider.GetCloudProvider(ctx),
	}
}

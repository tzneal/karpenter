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

package termination

import (
	"context"
	"fmt"

	provisioning "github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/cloudprovider"
	nodereconciler "github.com/aws/karpenter/pkg/k8sgen/reconciler/core/v1/node"
	"github.com/aws/karpenter/pkg/utils/functional"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
)

const controllerName = "termination"

var _ nodereconciler.Interface = (*Reconciler)(nil)
var _ nodereconciler.Finalizer = (*Reconciler)(nil)

// Reconciler for the resource
type Reconciler struct {
	Terminator *Terminator
	KubeClient kubernetes.Interface
}

// NewController constructs a controller instance
func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	r := NewReconciler(ctx)
	impl := nodereconciler.NewImpl(ctx, r)
	impl.Name = controllerName
	nodeinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	return impl
}

// NewReconciler constructs a new Reconciler and is exposed for testing only
func NewReconciler(ctx context.Context) *Reconciler {
	kubeClient := kubeclient.Get(ctx)

	return &Reconciler{
		KubeClient: kubeClient,
		Terminator: &Terminator{
			KubeClient:    kubeClient,
			CoreV1Client:  kubeClient.CoreV1(),
			CloudProvider: cloudprovider.GetCloudProvider(ctx),
			EvictionQueue: NewEvictionQueue(ctx, kubeClient.CoreV1()),
		},
	}
}

func (c *Reconciler) FinalizeKind(ctx context.Context, node *v1.Node) reconciler.Event {
	// 1. Check if node is terminable (deletion timestamp should always be set)
	if node.DeletionTimestamp.IsZero() || !functional.ContainsString(node.Finalizers, provisioning.TerminationFinalizer) {
		return nil
	}
	// 3. Cordon node
	if err := c.Terminator.cordon(ctx, node); err != nil {
		return fmt.Errorf("cordoning node %s, %w", node.Name, err)
	}
	// 4. Drain node
	drained, err := c.Terminator.drain(ctx, node)
	if err != nil {
		return fmt.Errorf("draining node %s, %w", node.Name, err)
	}
	if !drained {
		// TODO (todd): do we really want it requeued immediately? What is 'immediately' in reconcile-land?
		return controller.NewRequeueImmediately()
	}
	// 5. If fully drained, terminate the node
	if err := c.Terminator.terminate(ctx, node); err != nil {
		return fmt.Errorf("terminating node %s, %w", node.Name, err)
	}
	return nil
}

// ReconcileKind executes a termination control loop for the resource
func (c *Reconciler) ReconcileKind(ctx context.Context, node *v1.Node) reconciler.Event {
	// ctx = logging.WithLogger(ctx, logging.FromContext(ctx).Named(controllerName).With("node", req.Name))
	// ctx = injection.WithControllerName(ctx, controllerName)

	return nil
}

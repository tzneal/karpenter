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

package node

import (
	"context"
	"fmt"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/client/clientset/versioned"
	provisioningclient "github.com/aws/karpenter/pkg/client/injection/client"
	provisionerinformer "github.com/aws/karpenter/pkg/client/injection/informers/provisioning/v1alpha5/provisioner"
	kc "github.com/aws/karpenter/pkg/controllers"
	nodereconciler "github.com/aws/karpenter/pkg/k8sgen/reconciler/core/v1/node"
	"github.com/aws/karpenter/pkg/utils/result"
	"go.uber.org/multierr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const controllerName = "node"

// NewController constructs a controller instance
func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {

	r := NewReconciler(ctx)
	impl := nodereconciler.NewImpl(ctx, r)
	impl.Name = controllerName

	nodeinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	// enqueue modes if their provisioner changes
	provisionerinformer.Get(ctx).Informer().
		AddEventHandler(controller.HandleAll(
			kc.EnqueueNodeForProvisioner(r.kubeClient, impl.EnqueueKey, logging.FromContext(ctx))))
	// or their pods change
	podinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(
		kc.EnqueueNodeForPod(impl.EnqueueKey)))
	return impl
}

// NewReconciler creates a new Node Reconciler and is exposed for testing purposes only
func NewReconciler(ctx context.Context) *Reconciler {
	kubeClient := kubeclient.Get(ctx)

	return &Reconciler{
		KarpClient:     provisioningclient.Get(ctx),
		kubeClient:     kubeClient,
		initialization: &Initialization{kubeClient: kubeClient},
		emptiness:      &Emptiness{kubeClient: kubeClient},
		expiration:     &Expiration{kubeClient: kubeClient},
	}
}

// Reconciler manages a set of properties on karpenter provisioned nodes, such as
// taints, labels, finalizers.
type Reconciler struct {
	kubeClient     kubernetes.Interface
	initialization *Initialization
	emptiness      *Emptiness
	expiration     *Expiration
	finalizer      *Finalizer
	KarpClient     versioned.Interface
}

// ReconcileKind executes a reallocation control loop for the resource
func (c *Reconciler) ReconcileKind(ctx context.Context, stored *v1.Node) reconciler.Event {

	// ctx = logging.WithLogger(ctx, logging.FromContext(ctx).Named(controllerName).With("node", req.Name))

	// 1. Retrieve Node, ignore if not provisioned (nodes with a deletion timestamp aren't passed into reconcileKind)
	if _, ok := stored.Labels[v1alpha5.ProvisionerNameLabelKey]; !ok {
		return nil
	}

	// 2. Retrieve Provisioner
	provisioner, err := c.KarpClient.KarpenterV1alpha5().Provisioners().Get(ctx, stored.Labels[v1alpha5.ProvisionerNameLabelKey], metav1.GetOptions{})
	all, _ := c.KarpClient.KarpenterV1alpha5().Provisioners().List(ctx, metav1.ListOptions{})
	_ = all
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// 3. Execute reconcilers
	node := stored.DeepCopy()
	var results []reconcile.Result
	var errs error
	for _, subReconciler := range []interface {
		Reconcile(context.Context, *v1alpha5.Provisioner, *v1.Node) (reconcile.Result, error)
	}{
		c.initialization,
		c.expiration,
		c.emptiness,
		c.finalizer,
	} {
		res, err := subReconciler.Reconcile(ctx, provisioner, node)
		errs = multierr.Append(errs, err)
		results = append(results, res)
	}

	// 4. Patch any changes, regardless of errors
	if !equality.Semantic.DeepEqual(node, stored) {
		patch := client.MergeFrom(stored)
		data, err := patch.Data(node)
		if err != nil {
			return fmt.Errorf("generating patch: %w", err)
		}
		if _, err := c.kubeClient.CoreV1().Nodes().Patch(ctx, node.Name, patch.Type(), data, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("patching node, %w", err)
		}
	}
	// 5. Requeue if error or if retryAfter is set
	if errs != nil {
		return errs
	}
	// (todd) TODO: remove this usage of result
	minRes := result.Min(results...)
	if minRes.IsZero() || !minRes.Requeue {
		return nil
	}
	return controller.NewRequeueAfter(minRes.RequeueAfter)
}

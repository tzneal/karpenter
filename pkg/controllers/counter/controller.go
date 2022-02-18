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

package counter

import (
	"context"
	"fmt"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/client/clientset/versioned"
	provisioningclient "github.com/aws/karpenter/pkg/client/injection/client"
	provisioninginformer "github.com/aws/karpenter/pkg/client/injection/informers/provisioning/v1alpha5/provisioner"
	provisioningreconciler "github.com/aws/karpenter/pkg/client/injection/reconciler/provisioning/v1alpha5/provisioner"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/reconciler"
)

// Reconciler for the resource
type Reconciler struct {
	kubeClient kubernetes.Interface
	karpClient versioned.Interface
}

// NewController constructs a controller instance
func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	r := NewReconciler(ctx)
	impl := provisioningreconciler.NewImpl(ctx, r)
	impl.Name = "counter"

	provisioninginformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))

	// reconcile the provisioner if nodes arr added/removed
	enqueueProvisionerForNode := r.enqueueProvisionerForNode(impl.EnqueueKey)
	nodeinformer.Get(ctx).Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: enqueueProvisionerForNode,
			// No need to reconcile the provisioner when nodes are updated since those don't affect the status of the provisioner.
			UpdateFunc: nil,
			DeleteFunc: enqueueProvisionerForNode,
		})
	return impl
}

func NewReconciler(ctx context.Context) *Reconciler {
	return &Reconciler{
		kubeClient: kubeclient.Get(ctx),
		karpClient: provisioningclient.Get(ctx),
	}
}

// ReconcileKind reconciles the provisioner
func (c *Reconciler) ReconcileKind(ctx context.Context, provisioner *v1alpha5.Provisioner) reconciler.Event {

	// Determine resource usage and update provisioner.status.resources
	resourceCounts, err := c.resourceCountsFor(ctx, provisioner.Name)
	if err != nil {
		return fmt.Errorf("computing resource usage, %w", err)
	}

	provisioner.Status.MarkReady()
	// status can be directly updated and knative will update it on the server
	provisioner.Status.Resources = resourceCounts
	return nil
}

func (c *Reconciler) resourceCountsFor(ctx context.Context, provisionerName string) (v1.ResourceList, error) {
	nodes, err := c.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(labels.Set{v1alpha5.ProvisionerNameLabelKey: provisionerName}).String(),
	})
	if err != nil {
		return nil, err
	}
	var cpu = resource.NewScaledQuantity(0, 0)
	var memory = resource.NewScaledQuantity(0, resource.Giga)
	for _, node := range nodes.Items {
		cpu.Add(*node.Status.Capacity.Cpu())
		memory.Add(*node.Status.Capacity.Memory())
	}
	return v1.ResourceList{
		v1.ResourceCPU:    *cpu,
		v1.ResourceMemory: *memory,
	}, nil
}

func (c *Reconciler) enqueueProvisionerForNode(enqueueKey func(key types.NamespacedName)) func(interface{}) {
	return func(obj interface{}) {
		node := obj.(*v1.Node)
		if name, ok := node.GetLabels()[v1alpha5.ProvisionerNameLabelKey]; ok {
			enqueueKey(types.NamespacedName{Name: name})
		}
	}
}

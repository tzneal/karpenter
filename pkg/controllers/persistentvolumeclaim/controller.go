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

package persistentvolumeclaim

import (
	"context"
	"fmt"

	pvcreconciler "github.com/aws/karpenter/pkg/k8sgen/reconciler/core/v1/persistentvolumeclaim"
	"github.com/aws/karpenter/pkg/utils/functional"
	"github.com/aws/karpenter/pkg/utils/pod"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/listers/core/v1"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	pvcinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/persistentvolumeclaim"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	controllerName         = "volume"
	SelectedNodeAnnotation = "volume.kubernetes.io/selected-node"
)

// Reconciler for the resource
type Reconciler struct {
	KubeClient kubernetes.Interface
	PodLister  corev1.PodLister
}

func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	kubeClient := kubeclient.Get(ctx)

	r := &Reconciler{
		KubeClient: kubeClient,
		PodLister:  podinformer.Get(ctx).Lister(),
	}

	impl := pvcreconciler.NewImpl(ctx, r)
	impl.Name = controllerName

	pvcinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	// enqueue PVCs that belong to updated pods
	podinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(r.EnqueuePVCForPod(impl.EnqueueKey)))
	return impl
}

func (c *Reconciler) EnqueuePVCForPod(enqueueKey func(key types.NamespacedName)) func(interface{}) {
	return func(obj interface{}) {
		for _, pvc := range c.pvcsForPod(obj.(*v1.Pod)) {
			enqueueKey(pvc)
		}
	}
}

func (c *Reconciler) ReconcileKind(ctx context.Context, pvc *v1.PersistentVolumeClaim) reconciler.Event {
	/*	ctx = logging.WithLogger(ctx, logging.FromContext(ctx).Named(controllerName).With("resource", req.String()))
		ctx = injection.WithNamespacedName(ctx, req.NamespacedName)
		ctx = injection.WithControllerName(ctx, controllerName)
	*/

	pod, err := c.podForPvc(ctx, pvc)
	if err != nil {
		return err
	}
	if pod == nil {
		return nil
	}
	if nodeName, ok := pvc.Annotations[SelectedNodeAnnotation]; ok && nodeName == pod.Spec.NodeName {
		return nil
	}
	if !c.isBindable(pod) {
		return nil
	}
	pvc.Annotations = functional.UnionStringMaps(pvc.Annotations, map[string]string{SelectedNodeAnnotation: pod.Spec.NodeName})

	if _, err := c.KubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("binding persistent volume claim for pod %s/%s to node %q, %w", pod.Namespace, pod.Name, pod.Spec.NodeName, err)
	}
	logging.FromContext(ctx).Infof("Bound persistent volume claim to node %s", pod.Spec.NodeName)
	return nil
}

func (c *Reconciler) podForPvc(ctx context.Context, pvc *v1.PersistentVolumeClaim) (*v1.Pod, error) {
	// (todd): TODO: podlister vs using the client itself?
	pods, err := c.PodLister.Pods(pvc.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, pod := range pods {
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
				return pod, nil
			}
		}
	}
	return nil, nil
}

func (c *Reconciler) pvcsForPod(o client.Object) (requests []types.NamespacedName) {
	if !c.isBindable(o.(*v1.Pod)) {
		return requests
	}
	for _, volume := range o.(*v1.Pod).Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		requests = append(requests, types.NamespacedName{Namespace: o.GetNamespace(), Name: volume.PersistentVolumeClaim.ClaimName})
	}
	return requests
}

func (c *Reconciler) isBindable(p *v1.Pod) bool {
	return pod.IsScheduled(p) && !pod.IsTerminal(p) && !pod.IsTerminating(p)
}

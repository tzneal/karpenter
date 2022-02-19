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
	"log"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"knative.dev/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/cloudprovider"
	"github.com/aws/karpenter/pkg/utils/functional"
	"github.com/aws/karpenter/pkg/utils/injectabletime"
	"github.com/aws/karpenter/pkg/utils/pod"
	"github.com/aws/karpenter/pkg/utils/ptr"
)

type Terminator struct {
	EvictionQueue *EvictionQueue
	KubeClient    kubernetes.Interface
	CoreV1Client  corev1.CoreV1Interface
	CloudProvider cloudprovider.CloudProvider
}

// cordon cordons a node
func (t *Terminator) cordon(ctx context.Context, node *v1.Node) error {
	ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("node", node.Name))

	// 1. Check if node is already cordoned
	if node.Spec.Unschedulable {
		return nil
	}

	// 2. Cordon node
	persisted := node.DeepCopy()
	node.Spec.Unschedulable = true

	// TODO (todd): does this patch work?
	patch := client.MergeFrom(persisted)
	data, err := patch.Data(node)
	if err != nil {
		return fmt.Errorf("error creating patch: %w", err)
	}
	if _, err := t.KubeClient.CoreV1().Nodes().Patch(ctx, node.Name, patch.Type(), data, metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("patching node %s, %w", node.Name, err)
	}
	logging.FromContext(ctx).Infof("Cordoned node")
	return nil
}

// drain evicts pods from the node and returns true when all pods are evicted
// https://kubernetes.io/docs/concepts/architecture/nodes/#graceful-node-shutdown
func (t *Terminator) drain(ctx context.Context, node *v1.Node) (bool, error) {
	// Get evictable pods
	pods, err := t.getPods(ctx, node)
	if err != nil {
		return false, fmt.Errorf("listing pods for node, %w", err)
	}
	// Skip node due to do-not-evict
	for _, pod := range pods {
		if val := pod.Annotations[v1alpha5.DoNotEvictPodAnnotationKey]; val == "true" {
			logging.FromContext(ctx).Debugf("Unable to drain node, pod %s/%s has do-not-evict annotation", pod.Namespace, pod.Name)
			return false, nil
		}
	}
	// Enqueue for eviction
	t.evict(pods)
	return len(pods) == 0, nil
}

// terminate calls cloud provider delete then removes the finalizer to delete the node
func (t *Terminator) terminate(ctx context.Context, node *v1.Node) error {
	ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("node", node.Name))
	// 1. Delete the instance associated with node
	if err := t.CloudProvider.Delete(ctx, node); err != nil {
		return fmt.Errorf("terminating cloudprovider instance, %w", err)
	}
	// 2. Remove finalizer from node in APIServer
	persisted := node.DeepCopy()
	node.Finalizers = functional.StringSliceWithout(node.Finalizers, v1alpha5.TerminationFinalizer)

	// TODO (todd): does this patch work?
	patch := client.MergeFrom(persisted)
	data, err := patch.Data(node)
	if err != nil {
		return fmt.Errorf("error creating patch: %w", err)
	}
	if _, err := t.KubeClient.CoreV1().Nodes().Patch(ctx, node.Name, patch.Type(), data, metav1.PatchOptions{}); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("removing finalizer from node, %w", err)
	}
	logging.FromContext(ctx).Infof("Deleted node")
	return nil
}

// getPods returns a list of evictable pods for the node
func (t *Terminator) getPods(ctx context.Context, node *v1.Node) ([]*v1.Pod, error) {
	podList, err := t.KubeClient.CoreV1().Pods(v1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.Set{"spec.nodeName": node.Name}.String(),
	})

	if err != nil {
		return nil, fmt.Errorf("listing pods on node, %w", err)
	}
	var pods []*v1.Pod
	for _, p := range podList.Items {
		// Ignore if unschedulable is tolerated, since they will reschedule
		if (v1alpha5.Taints{{Key: v1.TaintNodeUnschedulable, Effect: v1.TaintEffectNoSchedule}}).Tolerates(ptr.Pod(p)) == nil {
			continue
		}
		// Ignore if kubelet is partitioned and pods are beyond graceful termination window
		if IsStuckTerminating(ptr.Pod(p)) {
			continue
		}
		// Ignore static mirror pods
		if pod.IsOwnedByNode(ptr.Pod(p)) {
			continue
		}
		pods = append(pods, ptr.Pod(p))
	}
	return pods, nil
}

func (t *Terminator) evict(pods []*v1.Pod) {
	// 1. Prioritize noncritical pods https://kubernetes.io/docs/concepts/architecture/nodes/#graceful-node-shutdown
	critical := []*v1.Pod{}
	nonCritical := []*v1.Pod{}
	for _, pod := range pods {
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		if pod.Spec.PriorityClassName == "system-cluster-critical" || pod.Spec.PriorityClassName == "system-node-critical" {
			critical = append(critical, pod)
		} else {
			nonCritical = append(nonCritical, pod)
		}
	}
	// 2. Evict critical pods if all noncritical are evicted
	if len(nonCritical) == 0 {
		t.EvictionQueue.Add(critical)
	} else {
		t.EvictionQueue.Add(nonCritical)
	}
}

func IsStuckTerminating(pod *v1.Pod) bool {
	if pod.DeletionTimestamp == nil {
		return false
	}
	return injectabletime.Now().After(pod.DeletionTimestamp.Time)
}

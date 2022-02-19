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
	"time"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/utils/functional"
	"github.com/aws/karpenter/pkg/utils/injectabletime"
	"github.com/aws/karpenter/pkg/utils/node"
	"github.com/aws/karpenter/pkg/utils/pod"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Emptiness is a subreconciler that deletes nodes that are empty after a ttl
type Emptiness struct {
	kubeClient kubernetes.Interface
}

// Reconcile reconciles the node
func (r *Emptiness) Reconcile(ctx context.Context, provisioner *v1alpha5.Provisioner, n *v1.Node) (reconcile.Result, error) {
	// 1. Ignore node if not applicable
	if provisioner.Spec.TTLSecondsAfterEmpty == nil {
		return reconcile.Result{}, nil
	}
	if !node.IsReady(n) {
		return reconcile.Result{}, nil
	}
	// 2. Remove ttl if not empty
	empty, err := r.isEmpty(ctx, n)
	if err != nil {
		return reconcile.Result{}, err
	}

	emptinessTimestamp, hasEmptinessTimestamp := n.Annotations[v1alpha5.EmptinessTimestampAnnotationKey]
	if !empty {
		if hasEmptinessTimestamp {
			delete(n.Annotations, v1alpha5.EmptinessTimestampAnnotationKey)
			logging.FromContext(ctx).Infof("Removed emptiness TTL from node")
		}
		return reconcile.Result{}, nil
	}
	// 3. Set TTL if not set
	n.Annotations = functional.UnionStringMaps(n.Annotations)
	ttl := time.Duration(ptr.Int64Value(provisioner.Spec.TTLSecondsAfterEmpty)) * time.Second
	if !hasEmptinessTimestamp {
		n.Annotations[v1alpha5.EmptinessTimestampAnnotationKey] = injectabletime.Now().Format(time.RFC3339)
		logging.FromContext(ctx).Infof("Added TTL to empty node")
		return reconcile.Result{RequeueAfter: ttl}, nil
	}
	// 4. Delete node if beyond TTL
	emptinessTime, err := time.Parse(time.RFC3339, emptinessTimestamp)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("parsing emptiness timestamp, %s", emptinessTimestamp)
	}
	if injectabletime.Now().After(emptinessTime.Add(ttl)) {
		logging.FromContext(ctx).Infof("Triggering termination after %s for empty node", ttl)
		if err := r.kubeClient.CoreV1().Nodes().Delete(ctx, n.Name, metav1.DeleteOptions{}); err != nil {
			return reconcile.Result{}, fmt.Errorf("deleting node, %w", err)
		}
	}
	return reconcile.Result{RequeueAfter: emptinessTime.Add(ttl).Sub(injectabletime.Now())}, nil
}

func (r *Emptiness) isEmpty(ctx context.Context, n *v1.Node) (bool, error) {
	pods := &v1.PodList{}

	// (todd): TODO: is the field selector correct?
	pods, err := r.kubeClient.CoreV1().Pods(v1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.Set{"spec.nodeName": n.Name}.String(),
	})
	if err != nil {
		return false, fmt.Errorf("listing pods for node, %w", err)
	}
	for i := range pods.Items {
		p := pods.Items[i]
		if pod.IsTerminal(&p) {
			continue
		}
		if !pod.IsOwnedByDaemonSet(&p) && !pod.IsOwnedByNode(&p) {
			return false, nil
		}
	}
	return true, nil
}

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
	"github.com/aws/karpenter/pkg/utils/injectabletime"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// Expiration is a subreconciler that terminates nodes after a period of time.
type Expiration struct {
	kubeClient kubernetes.Interface
}

// Reconcile reconciles the node
func (r *Expiration) Reconcile(ctx context.Context, provisioner *v1alpha5.Provisioner, node *v1.Node) (reconcile.Result, error) {
	// 1. Ignore node if not applicable
	if provisioner.Spec.TTLSecondsUntilExpired == nil {
		return reconcile.Result{}, nil
	}
	// 2. Trigger termination workflow if expired
	expirationTTL := time.Duration(ptr.Int64Value(provisioner.Spec.TTLSecondsUntilExpired)) * time.Second
	expirationTime := node.CreationTimestamp.Add(expirationTTL)
	if injectabletime.Now().After(expirationTime) {
		logging.FromContext(ctx).Infof("Triggering termination for expired node after %s (+%s)", expirationTTL, time.Since(expirationTime))
		if err := r.kubeClient.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{}); err != nil {
			return reconcile.Result{}, fmt.Errorf("deleting node, %w", err)
		}
	}
	// 3. Backoff until expired
	return reconcile.Result{RequeueAfter: time.Until(expirationTime)}, nil
}

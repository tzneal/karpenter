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

package selection

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/controllers/provisioning"
	"github.com/aws/karpenter/pkg/utils/pod"
	"go.uber.org/multierr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	podreconciler "knative.dev/pkg/client/injection/kube/reconciler/core/v1/pod"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reconciler for the resource
type Reconciler struct {
	KubeClient     kubernetes.Interface
	Provisioners   *provisioning.Provisioners
	Preferences    *Preferences
	VolumeTopology *VolumeTopology
}

func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	r := NewReconciler(ctx)

	impl := podreconciler.NewImpl(ctx, r)
	impl.Name = "selection"
	podinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	return impl
}

func NewReconciler(ctx context.Context) *Reconciler {
	kubeClient := kubeclient.Get(ctx)
	return &Reconciler{
		KubeClient:     kubeClient,
		Provisioners:   provisioning.GetProvisioners(ctx),
		Preferences:    NewPreferences(),
		VolumeTopology: NewVolumeTopology(kubeClient),
	}
}

func (c *Reconciler) ReconcileKind(ctx context.Context, pod *v1.Pod) reconciler.Event {
	ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("pod", client.ObjectKeyFromObject(pod)))

	// Ensure the pod can be provisioned
	if !isProvisionable(pod) {
		return nil
	}
	if err := validate(pod); err != nil {
		logging.FromContext(ctx).Debugf("Ignoring pod, %s", err)
		return nil
	}
	// Select a provisioner, wait for it to bind the pod, and verify scheduling succeeded in the next loop
	if err := c.selectProvisioner(ctx, pod); err != nil {
		logging.FromContext(ctx).Debugf("Could not schedule pod, %s", err)
		return err
	}
	return controller.NewRequeueAfter(time.Second * 5)
}

func (c *Reconciler) selectProvisioner(ctx context.Context, pod *v1.Pod) (errs error) {
	// Relax Preferences if pod has previously failed to schedule.
	c.Preferences.Relax(ctx, pod)
	// Inject volume topological requirements
	if err := c.VolumeTopology.Inject(ctx, pod); err != nil {
		return fmt.Errorf("getting volume topology requirements, %w", err)
	}
	// Pick provisioner
	var provisioner *provisioning.Provisioner
	provisioners := c.Provisioners.List(ctx)
	if len(provisioners) == 0 {
		return nil
	}
	for _, candidate := range c.Provisioners.List(ctx) {
		if err := candidate.Spec.DeepCopy().ValidatePod(pod); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("tried provisioner/%s: %w", candidate.Name, err))
		} else {
			provisioner = candidate
			break
		}
	}
	if provisioner == nil {
		return fmt.Errorf("matched 0/%d Provisioners, %w", len(multierr.Errors(errs)), errs)
	}
	select {
	case <-provisioner.Add(pod):
	case <-ctx.Done():
	}
	return nil
}

func isProvisionable(p *v1.Pod) bool {
	return !pod.IsScheduled(p) &&
		!pod.IsPreempting(p) &&
		pod.FailedToSchedule(p) &&
		!pod.IsOwnedByDaemonSet(p) &&
		!pod.IsOwnedByNode(p)
}

func validate(p *v1.Pod) error {
	return multierr.Combine(
		validateAffinity(p),
		validateTopology(p),
	)
}

func validateTopology(pod *v1.Pod) (errs error) {
	for _, constraint := range pod.Spec.TopologySpreadConstraints {
		if supported := sets.NewString(v1.LabelHostname, v1.LabelTopologyZone); !supported.Has(constraint.TopologyKey) {
			errs = multierr.Append(errs, fmt.Errorf("unsupported topology key, %s not in %s", constraint.TopologyKey, supported))
		}
	}
	return errs
}

func validateAffinity(pod *v1.Pod) (errs error) {
	if pod.Spec.Affinity == nil {
		return nil
	}
	if pod.Spec.Affinity.PodAffinity != nil {
		errs = multierr.Append(errs, fmt.Errorf("pod affinity is not supported"))
	}
	if pod.Spec.Affinity.PodAntiAffinity != nil {
		errs = multierr.Append(errs, fmt.Errorf("pod anti-affinity is not supported"))
	}
	if pod.Spec.Affinity.NodeAffinity != nil {
		for _, term := range pod.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			errs = multierr.Append(errs, validateNodeSelectorTerm(term.Preference))
		}
		if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			for _, term := range pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				errs = multierr.Append(errs, validateNodeSelectorTerm(term))
			}
		}
	}
	return errs
}

func validateNodeSelectorTerm(term v1.NodeSelectorTerm) (errs error) {
	if term.MatchFields != nil {
		errs = multierr.Append(errs, fmt.Errorf("node selector term with matchFields is not supported"))
	}
	if term.MatchExpressions != nil {
		for _, requirement := range term.MatchExpressions {
			if !v1alpha5.SupportedNodeSelectorOps.Has(string(requirement.Operator)) {
				errs = multierr.Append(errs, fmt.Errorf("node selector term has unsupported operator, %s", requirement.Operator))
			}
		}
	}
	return errs
}

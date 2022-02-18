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

package pod

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/karpenter/pkg/controllers"
	"github.com/aws/karpenter/pkg/utils/apiobject"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	podreconciler "knative.dev/pkg/client/injection/kube/reconciler/core/v1/pod"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	podName             = "name"
	podNameSpace        = "namespace"
	ownerSelfLink       = "owner"
	podHostName         = "node"
	podProvisioner      = "provisioner"
	podHostZone         = "zone"
	podHostArchitecture = "arch"
	podHostCapacityType = "capacity_type"
	podHostInstanceType = "instance_type"
	podPhase            = "phase"
)

var (
	podGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "pods",
			Name:      "state",
			Help:      "Pod state.",
		},
		labelNames(),
	)
)

// Reconciler for the resource
type Reconciler struct {
	KubeClient kubernetes.Interface
	LabelsMap  map[types.NamespacedName]prometheus.Labels
}

func init() {
	crmetrics.Registry.MustRegister(podGaugeVec)
}

func labelNames() []string {
	return []string{
		podName,
		podNameSpace,
		ownerSelfLink,
		podHostName,
		podProvisioner,
		podHostZone,
		podHostArchitecture,
		podHostCapacityType,
		podHostInstanceType,
		podPhase,
	}
}

var _ podreconciler.Interface = (*Reconciler)(nil)
var _ podreconciler.Finalizer = (*Reconciler)(nil)

// NewController constructs a controller instance
func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	r := NewReconciler(ctx)
	impl := podreconciler.NewImpl(ctx, r, controllers.FinalizerNamed("metricspod"))
	impl.Name = "podmetrics"

	podinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	return impl
}

// NewReconciler constructs a new Reconciler and is exposed for testing only
func NewReconciler(ctx context.Context) *Reconciler {
	return &Reconciler{
		KubeClient: kubeclient.Get(ctx),
		LabelsMap:  make(map[types.NamespacedName]prometheus.Labels),
	}
}

// ReconcileKind executes a termination control loop for the resource
func (c *Reconciler) ReconcileKind(ctx context.Context, pod *v1.Pod) reconciler.Event {
	ctx = logging.WithLogger(ctx, logging.FromContext(ctx).With("pod", pod.Name))

	// Remove the previous gauge after pod labels are updated
	if labels, ok := c.LabelsMap[apiobject.NamespacedNameFromObject(pod)]; ok {
		podGaugeVec.Delete(labels)
	}

	// Retrieve pod from reconcile request
	c.record(ctx, pod)
	return nil
}

func (c *Reconciler) FinalizeKind(ctx context.Context, pod *v1.Pod) reconciler.Event {
	if labels, ok := c.LabelsMap[apiobject.NamespacedNameFromObject(pod)]; ok {
		podGaugeVec.Delete(labels)
	}
	return nil
}

func (c *Reconciler) record(ctx context.Context, pod *v1.Pod) {
	labels := c.labels(ctx, pod)
	podGaugeVec.With(labels).Set(float64(1))
	c.LabelsMap[apiobject.NamespacedNameFromObject(pod)] = labels
}

// labels creates the labels using the current state of the pod
func (c *Reconciler) labels(ctx context.Context, pod *v1.Pod) prometheus.Labels {
	metricLabels := prometheus.Labels{}
	metricLabels[podName] = pod.GetName()
	metricLabels[podNameSpace] = pod.GetNamespace()
	// Selflink has been deprecated after v.1.20
	// Manually generate the selflink for the first owner reference
	// Currently we do not support multiple owner references
	selflink := ""
	if len(pod.GetOwnerReferences()) > 0 {
		ownerreference := pod.GetOwnerReferences()[0]
		selflink = fmt.Sprintf("/apis/%s/namespaces/%s/%ss/%s", ownerreference.APIVersion, pod.Namespace, strings.ToLower(ownerreference.Kind), ownerreference.Name)
	}
	metricLabels[ownerSelfLink] = selflink
	metricLabels[podHostName] = pod.Spec.NodeName
	metricLabels[podPhase] = string(pod.Status.Phase)
	node := &v1.Node{}
	node, err := c.KubeClient.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		metricLabels[podHostZone] = "N/A"
		metricLabels[podHostArchitecture] = "N/A"
		metricLabels[podHostCapacityType] = "N/A"
		metricLabels[podHostInstanceType] = "N/A"
		if provisionerName, ok := pod.Spec.NodeSelector[v1alpha5.ProvisionerNameLabelKey]; ok {
			metricLabels[podProvisioner] = provisionerName
		} else {
			metricLabels[podProvisioner] = "N/A"
		}
	} else {
		metricLabels[podHostZone] = node.Labels[v1.LabelTopologyZone]
		metricLabels[podHostArchitecture] = node.Labels[v1.LabelArchStable]
		if capacityType, ok := node.Labels[v1alpha5.LabelCapacityType]; !ok {
			metricLabels[podHostCapacityType] = "N/A"
		} else {
			metricLabels[podHostCapacityType] = capacityType
		}
		metricLabels[podHostInstanceType] = node.Labels[v1.LabelInstanceTypeStable]
		if provisionerName, ok := node.Labels[v1alpha5.ProvisionerNameLabelKey]; !ok {
			metricLabels[podProvisioner] = "N/A"
		} else {
			metricLabels[podProvisioner] = provisionerName
		}
	}
	return metricLabels
}

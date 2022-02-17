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
	"strings"

	provisionerinformer "github.com/aws/karpenter/pkg/client/injection/informers/provisioning/v1alpha5/provisioner"
	kc "github.com/aws/karpenter/pkg/controllers"
	nodereconciler "github.com/aws/karpenter/pkg/k8sgen/reconciler/core/v1/node"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
	nodeinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/node"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	podutil "github.com/aws/karpenter/pkg/utils/pod"
	"github.com/aws/karpenter/pkg/utils/resources"
	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	resourceType     = "resource_type"
	nodeName         = "node_name"
	nodeProvisioner  = "provisioner"
	nodeZone         = "zone"
	nodeArchitecture = "arch"
	nodeCapacityType = "capacity_type"
	nodeInstanceType = "instance_type"
	nodePhase        = "phase"
)

var (
	allocatableGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "nodes",
			Name:      "allocatable",
			Help:      "Node allocatable",
		},
		labelNames(),
	)
	podRequestsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "nodes",
			Name:      "total_pod_requests",
			Help:      "Node total pod requests",
		},
		labelNames(),
	)
	podLimitsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "nodes",
			Name:      "total_pod_limits",
			Help:      "Node total pod limits",
		},
		labelNames(),
	)
	daemonRequestsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "nodes",
			Name:      "total_daemon_requests",
			Help:      "Node total daemon requests",
		},
		labelNames(),
	)
	daemonLimitsGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "nodes",
			Name:      "total_daemon_limits",
			Help:      "Node total daemon limits",
		},
		labelNames(),
	)
	overheadGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "karpenter",
			Subsystem: "nodes",
			Name:      "system_overhead",
			Help:      "Node system daemon overhead",
		},
		labelNames(),
	)
)

func init() {
	crmetrics.Registry.MustRegister(allocatableGaugeVec)
	crmetrics.Registry.MustRegister(podRequestsGaugeVec)
	crmetrics.Registry.MustRegister(podLimitsGaugeVec)
	crmetrics.Registry.MustRegister(daemonRequestsGaugeVec)
	crmetrics.Registry.MustRegister(daemonLimitsGaugeVec)
	crmetrics.Registry.MustRegister(overheadGaugeVec)
}

func labelNames() []string {
	return []string{
		resourceType,
		nodeName,
		nodeProvisioner,
		nodeZone,
		nodeArchitecture,
		nodeCapacityType,
		nodeInstanceType,
		nodePhase,
	}
}

var _ nodereconciler.Interface = (*Reconciler)(nil)
var _ nodereconciler.Finalizer = (*Reconciler)(nil)

type Reconciler struct {
	kubeClient      kubernetes.Interface
	LabelCollection map[types.NamespacedName][]prometheus.Labels
}

// NewController constructs a controller instance
func NewController(ctx context.Context, cmw configmap.Watcher) *controller.Impl {
	r := NewReconciler(ctx)
	impl := nodereconciler.NewImpl(ctx, r)
	impl.Name = "nodemetrics"

	nodeinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(impl.Enqueue))
	provisionerinformer.Get(ctx).Informer().
		AddEventHandler(controller.HandleAll(kc.EnqueueNodeForProvisioner(r.kubeClient, impl.EnqueueKey, logging.FromContext(ctx))))
	podinformer.Get(ctx).Informer().AddEventHandler(controller.HandleAll(
		kc.EnqueueNodeForPod(impl.EnqueueKey)))
	return impl
}

// NewReconciler constructs a controller instance
func NewReconciler(ctx context.Context) *Reconciler {
	return &Reconciler{
		kubeClient:      kubeclient.Get(ctx),
		LabelCollection: make(map[types.NamespacedName][]prometheus.Labels),
	}
}

func (c *Reconciler) FinalizeKind(ctx context.Context, node *v1.Node) reconciler.Event {
	c.cleanup(client.ObjectKeyFromObject(node))
	return nil
}

// ReconcileKind executes a termination control loop for the resource
func (c *Reconciler) ReconcileKind(ctx context.Context, node *v1.Node) reconciler.Event {
	// ctx = logging.WithLogger(ctx, logging.FromContext(ctx).Named("nodemetrics").With("node", req.Name))

	// Remove the previous gauge after node labels are updated
	c.cleanup(client.ObjectKeyFromObject(node))

	if err := c.record(ctx, node); err != nil {
		logging.FromContext(ctx).Errorf("Failed to update gauges: %s", err)
		return err
	}
	return nil
}
func (c *Reconciler) cleanup(nodeNamespacedName types.NamespacedName) {
	if labelSet, ok := c.LabelCollection[nodeNamespacedName]; ok {
		for _, labels := range labelSet {
			allocatableGaugeVec.Delete(labels)
			podRequestsGaugeVec.Delete(labels)
			podLimitsGaugeVec.Delete(labels)
			daemonRequestsGaugeVec.Delete(labels)
			daemonLimitsGaugeVec.Delete(labels)
			overheadGaugeVec.Delete(labels)
		}
	}
	c.LabelCollection[nodeNamespacedName] = []prometheus.Labels{}
}

// labels creates the labels using the current state of the pod
func (c *Reconciler) labels(node *v1.Node, resourceTypeName string) prometheus.Labels {
	metricLabels := prometheus.Labels{}
	metricLabels[resourceType] = resourceTypeName
	metricLabels[nodeName] = node.GetName()
	if provisionerName, ok := node.Labels[v1alpha5.ProvisionerNameLabelKey]; !ok {
		metricLabels[nodeProvisioner] = "N/A"
	} else {
		metricLabels[nodeProvisioner] = provisionerName
	}
	metricLabels[nodeZone] = node.Labels[v1.LabelTopologyZone]
	metricLabels[nodeArchitecture] = node.Labels[v1.LabelArchStable]
	if capacityType, ok := node.Labels[v1alpha5.LabelCapacityType]; !ok {
		metricLabels[nodeCapacityType] = "N/A"
	} else {
		metricLabels[nodeCapacityType] = capacityType
	}
	metricLabels[nodeInstanceType] = node.Labels[v1.LabelInstanceTypeStable]
	metricLabels[nodePhase] = string(node.Status.Phase)
	return metricLabels
}

func (c *Reconciler) record(ctx context.Context, node *v1.Node) error {
	podlist, err := c.kubeClient.CoreV1().Pods(v1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.Set{"spec.nodeName": node.Name}.String(),
	})
	if err != nil {
		return fmt.Errorf("listing pods on node %s, %w", node.Name, err)
	}
	var daemons, pods []*v1.Pod
	for index := range podlist.Items {
		if podutil.IsOwnedByDaemonSet(&podlist.Items[index]) {
			daemons = append(daemons, &podlist.Items[index])
		} else {
			pods = append(pods, &podlist.Items[index])
		}
	}
	podRequest := resources.RequestsForPods(pods...)
	podLimits := resources.LimitsForPods(pods...)
	daemonRequest := resources.RequestsForPods(daemons...)
	daemonLimits := resources.LimitsForPods(daemons...)
	systemOverhead := getSystemOverhead(node)
	allocatable := node.Status.Capacity
	if len(node.Status.Allocatable) > 0 {
		allocatable = node.Status.Allocatable
	}
	// Populate  metrics
	for gaugeVec, resourceList := range map[*prometheus.GaugeVec]v1.ResourceList{
		overheadGaugeVec:       systemOverhead,
		podRequestsGaugeVec:    podRequest,
		podLimitsGaugeVec:      podLimits,
		daemonRequestsGaugeVec: daemonRequest,
		daemonLimitsGaugeVec:   daemonLimits,
		allocatableGaugeVec:    allocatable,
	} {
		if err := c.set(resourceList, node, gaugeVec); err != nil {
			logging.FromContext(ctx).Errorf("Failed to generate gauge: %w", err)
		}
	}
	return nil
}

func getSystemOverhead(node *v1.Node) v1.ResourceList {
	systemOverheads := v1.ResourceList{}
	if len(node.Status.Allocatable) > 0 {
		// calculating system daemons overhead
		for resourceName, quantity := range node.Status.Allocatable {
			overhead := node.Status.Capacity[resourceName]
			overhead.Sub(quantity)
			systemOverheads[resourceName] = overhead
		}
	}
	return systemOverheads
}

// set sets the value for the node gauge
func (c *Reconciler) set(resourceList v1.ResourceList, node *v1.Node, gaugeVec *prometheus.GaugeVec) error {
	for resourceName, quantity := range resourceList {
		resourceTypeName := strings.ReplaceAll(strings.ToLower(string(resourceName)), "-", "_")
		labels := c.labels(node, resourceTypeName)
		// Register the set of labels that are generated for node
		nodeNamespacedName := types.NamespacedName{Name: node.Name}
		c.LabelCollection[nodeNamespacedName] = append(c.LabelCollection[nodeNamespacedName], labels)
		gauge, err := gaugeVec.GetMetricWith(labels)
		if err != nil {
			return fmt.Errorf("generate new gauge: %w", err)
		}
		if resourceName == v1.ResourceCPU {
			gauge.Set(float64(quantity.MilliValue()) / float64(1000))
		} else {
			gauge.Set(float64(quantity.Value()))
		}
	}
	return nil
}

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

package scheduling

import (
	"context"

	"go.uber.org/multierr"

	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/aws/karpenter/pkg/utils/injection"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/metrics"
)

var schedulingDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: metrics.Namespace,
		Subsystem: "allocation_controller",
		Name:      "scheduling_duration_seconds",
		Help:      "Duration of scheduling process in seconds. Broken down by provisioner and error.",
		Buckets:   metrics.DurationBuckets(),
	},
	[]string{metrics.ProvisionerLabel},
)

func init() {
	crmetrics.Registry.MustRegister(schedulingDuration)
}

type Scheduler struct {
	KubeClient client.Client
}

type Schedule struct {
	*v1alpha5.Constraints
	// Pods is a set of pods that may schedule to the node; used for binpacking.
	Pods []*v1.Pod
}

func NewScheduler(kubeClient client.Client) *Scheduler {
	return &Scheduler{
		KubeClient: kubeClient,
	}
}

func (s *Scheduler) Solve(ctx context.Context, provisioner *v1alpha5.Provisioner, pods []*v1.Pod) ([]*Schedule, error) {
	defer metrics.Measure(schedulingDuration.WithLabelValues(injection.GetNamespacedName(ctx).Name))()
	cluster := NewVirtualCluster(provisioner, s.KubeClient)
	var err error
	for _, p := range pods {
		err = multierr.Append(err, cluster.SchedulePod(ctx, p))
	}

	var schedules []*Schedule
	for _, vn := range cluster.nodes {
		// TODO(todd): rework this
		tightened := provisioner.Spec.Constraints.Tighten(vn.pods[0])
		schedules = append(schedules, &Schedule{
			Constraints: tightened,
			Pods:        vn.pods,
		})
	}
	return schedules, err
}

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
	"time"

	"go.uber.org/multierr"
	"knative.dev/pkg/logging"

	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/aws/karpenter/pkg/utils/injection"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/metrics"
)

// this is used internally only to mark why we've failed to schedule a pod to
// provide a better user experience
const scheduleFailureReasonKey = "karpenter.sh/scheduleFailureReason"

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

	start := time.Now()
	cluster := NewVirtualCluster(provisioner, s.KubeClient)

	var merr error
	// construct a topological sort of our pods by pod affinities
	pods, merr = cluster.SortPods(ctx, pods)

	// schedule our pods onto virtual nodes
	for _, p := range pods {
		_, err := cluster.SchedulePod(ctx, p)
		merr = multierr.Append(merr, err)
	}

	var schedules []*Schedule
	// since each virtual node contains a set of compatible pods, we return those to the bin-packer
	// to construct 1 to N actual nodes for each virtual node.
	for _, vn := range cluster.nodes {
		if len(vn.pods) == 0 {
			logging.FromContext(ctx).Errorf("scheduling error, vn with zero pods")
			continue
		}
		// TODO(todd): rework this
		tightened := provisioner.Spec.Constraints.Tighten(vn.pods[0])
		schedules = append(schedules, &Schedule{
			Constraints: tightened,
			Pods:        vn.pods,
		})
	}
	logging.FromContext(ctx).Infof("identified %d compatible batches of pods in %s", len(schedules), time.Since(start))
	return schedules, merr
}

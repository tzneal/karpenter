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
	"sort"

	v1 "k8s.io/api/core/v1"

	"github.com/aws/karpenter/pkg/utils/resources"
)

// Queue is a queue of pods that is scheduled.  It's used to attempt to schedule pods as long as we are making progress
// in scheduling. This is sometimes required to maintain zonal topology spreads with constrained pods, and can satisfy
// pod affinities that occur in a batch of pods if there are enough constraints provided.
type Queue struct {
	pods       []*v1.Pod
	lastPopped *v1.Pod
	attempts   int
}

// NewQueue constructs a new queue given the input pods, sorting them to optimize for bin-packing into nodes.
func NewQueue(pods ...*v1.Pod) *Queue {
	sort.Slice(pods, byCPUAndMemoryDescending(pods))
	return &Queue{
		pods:     pods,
		attempts: len(pods),
	}
}

// Next returns the next pod or false if no longer making progress
func (q *Queue) Pop() (*v1.Pod, bool) {
	if len(q.pods) == 0 || q.attempts == 0 {
		return nil, false
	}
	q.lastPopped = q.pods[0]
	q.pods = q.pods[1:]
	return q.lastPopped, true
}

// Push a pod onto the queue, counting each time a pod is immediately requeued. This is used to detect staleness.
func (q *Queue) Push(pod *v1.Pod, relaxed bool) {
	q.pods = append(q.pods, pod)
	if relaxed || q.lastPopped != pod {
		q.attempts = len(q.pods)
	} else {
		q.attempts--
	}
}

func (q *Queue) List() []*v1.Pod {
	return q.pods
}

func byCPUAndMemoryDescending(pods []*v1.Pod) func(i int, j int) bool {
	return func(i, j int) bool {
		lhs := resources.RequestsForPods(pods[i])
		rhs := resources.RequestsForPods(pods[j])

		cpuCmp := resources.Cmp(lhs[v1.ResourceCPU], rhs[v1.ResourceCPU])
		if cpuCmp < 0 {
			// LHS has less CPU, so it should be sorted after
			return false
		} else if cpuCmp > 0 {
			return true
		}
		memCmp := resources.Cmp(lhs[v1.ResourceMemory], rhs[v1.ResourceMemory])

		if memCmp < 0 {
			return false
		} else if memCmp > 0 {
			return true
		}
		return false
	}
}

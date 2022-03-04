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
	"fmt"
	"math"

	"github.com/aws/karpenter/pkg/utils/pod"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Topology struct {
	Key       string
	MaxSkew   int32
	selector  labels.Selector
	domains   map[string]int32 // TODO(ellistarn) explore replacing with a minheap
	maxCount  int32
	tc        v1.TopologySpreadConstraint
	namespace string
}

func NewTopology(tc v1.TopologySpreadConstraint, namespace string) (*Topology, error) {
	selector, err := metav1.LabelSelectorAsSelector(tc.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("creating label selector: %w", err)
	}

	return &Topology{
		Key:       tc.TopologyKey,
		MaxSkew:   tc.MaxSkew,
		namespace: namespace,
		selector:  selector,
		tc:        tc,
		domains:   map[string]int32{},
	}, nil
}

func (t *Topology) Matches(podLabels labels.Set) bool {
	return t.selector.Matches(podLabels)
}

func (t *Topology) RecordUsage(domain string) {
	t.domains[domain]++
	if t.domains[domain] > t.maxCount {
		t.maxCount = t.domains[domain]
	}
}

// NextDomain returns the best domain to choose next and what the max-skew would be if we
// chose that domain
func (t *Topology) NextDomain(creatable bool, reqts []v1.NodeSelectorRequirement) (string, int32) {

	selector := buildSelector(reqts)

	minCount := int32(math.MaxInt32)
	var minDomain string
	l := labels.Set{}

	for domain, count := range t.domains {
		l[t.Key] = domain
		// we only consider domains that match the pod's node affinity
		if selector.Matches(l) {
			if count < minCount {
				minCount = count
				minDomain = domain
			}
		}
	}

	// only a single item in the domain, so we treat this as increasing skew since skew is somewhat
	// non-sensical for single element domains
	if len(t.domains) == 1 || (creatable && minCount != 0) {
		return minDomain, t.maxCount + 1
	}

	// none of the topology domains have any pods assigned, so we'll just be at
	// a max-skew of 1 when we create somthing
	if t.maxCount == 0 {
		return minDomain, 1
	}

	// Calculate what the max skew will be if we chose the min domain.  This may not be the actual max-skew
	// if this domain is used as there may be multiple domains with the same minimum pod count at the moment.
	maxSkew := t.maxCount - (minCount + 1)

	// if max = min, skew would be negative since our new domain selection would be the new max
	if maxSkew < 0 {
		maxSkew = 1
	}

	return minDomain, maxSkew
}

func buildSelector(reqts []v1.NodeSelectorRequirement) labels.Selector {
	selector := labels.NewSelector()
	for _, req := range reqts {
		var op selection.Operator
		switch req.Operator {
		case v1.NodeSelectorOpIn:
			op = selection.In
		case v1.NodeSelectorOpNotIn:
			op = selection.NotIn
		case v1.NodeSelectorOpExists:
			op = selection.Exists
		case v1.NodeSelectorOpDoesNotExist:
			op = selection.DoesNotExist
		case v1.NodeSelectorOpGt:
			op = selection.GreaterThan
		case v1.NodeSelectorOpLt:
			op = selection.LessThan
		}
		r, err := labels.NewRequirement(req.Key, op, req.Values)
		if err != nil {
			continue
		}
		selector = selector.Add(*r)
	}
	return selector
}

func (t *Topology) RegisterDomain(zone string) {
	if _, ok := t.domains[zone]; !ok {
		t.domains[zone] = 0
	}
}

func (t *Topology) UpdateFromCluster(ctx context.Context, kubeClient client.Client) error {
	pods := &v1.PodList{}
	if err := kubeClient.List(ctx, pods, TopologyListOptions(t.namespace, &t.tc)); err != nil {
		return fmt.Errorf("listing pods, %w", err)
	}
	for i, p := range pods.Items {
		if IgnoredForTopology(&pods.Items[i]) {
			continue
		}
		node := &v1.Node{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Name: p.Spec.NodeName}, node); err != nil {
			return fmt.Errorf("getting node %s, %w", p.Spec.NodeName, err)
		}
		domain, ok := node.Labels[t.Key]
		if !ok {
			continue // Don't include pods if node doesn't contain domain https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/#conventions
		}
		t.RecordUsage(domain)
	}
	return nil
}

func IgnoredForTopology(p *v1.Pod) bool {
	return !pod.IsScheduled(p) || pod.IsTerminal(p) || pod.IsTerminating(p)
}

func TopologyListOptions(namespace string, constraint *v1.TopologySpreadConstraint) *client.ListOptions {
	selector := labels.Everything()
	if constraint.LabelSelector == nil {
		return &client.ListOptions{Namespace: namespace, LabelSelector: selector}
	}
	for key, value := range constraint.LabelSelector.MatchLabels {
		requirement, _ := labels.NewRequirement(key, selection.Equals, []string{value})
		selector = selector.Add(*requirement)
	}
	for _, expression := range constraint.LabelSelector.MatchExpressions {
		requirement, _ := labels.NewRequirement(expression.Key, selection.Operator(expression.Operator), expression.Values)
		selector = selector.Add(*requirement)
	}
	return &client.ListOptions{Namespace: namespace, LabelSelector: selector}
}

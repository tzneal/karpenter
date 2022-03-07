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

type affinityDerivation byte

const (
	// standard topology spread
	affinityDerivationNone affinityDerivation = iota
	// tracking due to a pod affinity
	affinityDerivationPodAffinity
	// tracking due to a pod anti-affinity
	affinityDerivationPodAntiAffinity
)

// Topology is used to track pod counts that match a selector by the topology domain (e.g. SELECT COUNT(*) FROM pods GROUP BY(topology_ke
type Topology struct {
	Key                string
	MaxSkew            int32
	PodAffinityDerived affinityDerivation
	AllowViolation     bool  // if true, this is from a preferred affinity
	Weight             int32 // weight of the preferred affinity

	selector       labels.Selector
	domains        map[string]int32 // TODO(ellistarn) explore replacing with a minheap
	maxCount       int32
	tc             v1.TopologySpreadConstraint
	namespaces     map[string]struct{}
	affinityOwners map[client.ObjectKey]struct{}
}

func NewTopology(tc v1.TopologySpreadConstraint, namespaces []string) (*Topology, error) {
	selector, err := metav1.LabelSelectorAsSelector(tc.LabelSelector)
	if err != nil {
		return nil, fmt.Errorf("creating label selector: %w", err)
	}

	nsMap := map[string]struct{}{}
	for _, ns := range namespaces {
		nsMap[ns] = struct{}{}
	}

	return &Topology{
		Key:            tc.TopologyKey,
		MaxSkew:        tc.MaxSkew,
		AllowViolation: tc.WhenUnsatisfiable == v1.ScheduleAnyway,
		namespaces:     nsMap,
		selector:       selector,
		tc:             tc,
		domains:        map[string]int32{},
		affinityOwners: map[client.ObjectKey]struct{}{},
	}, nil
}

func (t *Topology) Matches(namespace string, podLabels labels.Set) bool {
	_, nsMatch := t.namespaces[namespace]
	if !nsMatch {
		return false
	}
	return t.selector.Matches(podLabels)
}

func (t *Topology) RecordUsage(domain string) {
	t.domains[domain]++
	if t.domains[domain] > t.maxCount {
		t.maxCount = t.domains[domain]
	}
}

// NextDomainMinimizeSkew returns the best domain to choose next and what the max-skew would be if we
// chose that domain
//gocyclo:ignore
func (t *Topology) NextDomainMinimizeSkew(creatable bool, reqts []v1.NodeSelectorRequirement) (string, int32, bool) {
	selector := buildSelectorForOnly(reqts, t.Key)

	minCount := int32(math.MaxInt32)
	globalMin := int32(math.MaxInt32)
	var minDomain string
	l := labels.Set{}
	for domain, count := range t.domains {
		l[t.Key] = domain
		if count < globalMin {
			globalMin = count
		}

		// we only consider domains that match the pod's node affinity for the purposes
		// of what we select as our next domain
		if selector.Matches(l) {
			if count < minCount {
				minCount = count
				minDomain = domain
			}
		}
	}

	// only a single item in the domain, so we treat this as increasing skew since skew is somewhat
	// non-sensical for single element domains
	if len(t.domains) == 1 || (creatable && globalMin != 0) {
		return minDomain, t.maxCount + 1, true
	}

	// none of the topology domains have any pods assigned, so we'll just be at
	// a max-skew of 1 when we create something
	if t.maxCount == 0 {
		return minDomain, 1, true
	}

	// Calculate what the max skew will be if we chose the min domain.
	maxSkew := t.maxCount - (minCount + 1)
	if globalMin != minCount {
		// if the global min is less than the count of pods in domains that match the node selector
		// the max-skew is based on the global min as we can't change it
		maxSkew = t.maxCount - globalMin
		// the domain we're allowed to pick happens to be the maximum value, so by picking it we are increasing skew
		// even more
		if minCount == t.maxCount {
			maxSkew++
		}
	}

	// if max = min, skew would be negative since our new domain selection would be the new max
	if maxSkew < 0 {
		maxSkew = 1
	}

	// We need to know if we are increasing or decreasing skew.  If we are above the max-skew, but assigning this
	// topology domain decreases skew, we should do it.
	oldMaxSkew := t.maxCount - globalMin
	increasing := maxSkew > oldMaxSkew
	return minDomain, maxSkew, increasing
}

func (t *Topology) MaxDomain(reqts []v1.NodeSelectorRequirement) string {

	selector := buildSelectorForOnly(reqts, t.Key)

	maxCount := int32(math.MinInt32)
	var maxDomain string
	l := labels.Set{}

	for domain, count := range t.domains {
		l[t.Key] = domain
		// we only consider domains that match the pod's node selectors
		if selector.Matches(l) {
			if count > maxCount {
				maxCount = count
				maxDomain = domain
			}
		}
	}

	return maxDomain
}

func buildSelectorForOnly(reqts []v1.NodeSelectorRequirement, keys ...string) labels.Selector {
	var filtered []v1.NodeSelectorRequirement
	seen := map[string]struct{}{}
	for _, key := range keys {
		seen[key] = struct{}{}
	}
	for _, req := range reqts {
		if _, ok := seen[req.Key]; ok {
			filtered = append(filtered, req)
		}
	}
	return buildSelector(filtered)
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
	podList := &v1.PodList{}

	// collect the pods from all the specified namespaces (don't see a way to query multiple namespaces
	// simultaneously)
	var pods []v1.Pod
	for ns := range t.namespaces {
		if err := kubeClient.List(ctx, podList, TopologyListOptions(ns, &t.tc)); err != nil {
			return fmt.Errorf("listing pods, %w", err)
		}
		pods = append(pods, podList.Items...)
	}

	for i, p := range pods {
		if IgnoredForTopology(&pods[i]) {
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

func (t *Topology) HasEmptyDomains(reqts []v1.NodeSelectorRequirement) bool {
	selector := buildSelectorForOnly(reqts, t.Key)
	l := labels.Set{}
	for domain, count := range t.domains {
		l[t.Key] = domain
		if !selector.Matches(l) {
			continue
		}
		if count == 0 {
			return true
		}
	}
	return false
}

func (t *Topology) HasNonEmptyDomains(reqts []v1.NodeSelectorRequirement) bool {
	selector := buildSelectorForOnly(reqts, t.Key)
	l := labels.Set{}
	for domain, count := range t.domains {
		l[t.Key] = domain
		if !selector.Matches(l) {
			continue
		}
		if count != 0 {
			return true
		}
	}
	return false
}

func (t *Topology) AddAffinityOwner(key client.ObjectKey) {
	t.affinityOwners[key] = struct{}{}
}

func (t *Topology) IsOwnedBy(object client.ObjectKey) bool {
	_, ok := t.affinityOwners[object]
	return ok
}

func (t *Topology) UnregisterDomainIfUnused(domain string) {
	if cnt, ok := t.domains[domain]; ok && cnt == 0 {
		delete(t.domains, domain)
	}
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

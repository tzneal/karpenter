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
	"fmt"
	"math"

	"github.com/mitchellh/hashstructure/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	utilsets "k8s.io/apimachinery/pkg/util/sets"

	"github.com/aws/karpenter/pkg/utils/sets"
)

type TopologyType byte

const (
	TopologyTypeSpread TopologyType = iota
	TopologyTypePodAffinity
	TopologyTypePodAntiAffinity
)

func (t TopologyType) String() string {
	switch t {
	case TopologyTypeSpread:
		return "topology spread"
	case TopologyTypePodAffinity:
		return "pod affinity"
	case TopologyTypePodAntiAffinity:
		return "pod anti-affinity"
	}
	return ""
}

// TopologyGroup is used to track pod counts that match a selector by the topology domain (e.g. SELECT COUNT(*) FROM pods GROUP BY(topology_ke
type TopologyGroup struct {
	// Hashed Fields
	Key        string
	Type       TopologyType
	maxSkew    int32
	namespaces utilsets.String
	selector   *metav1.LabelSelector
	// Pod Index
	owners map[types.UID]struct{} // Pods that have this topology as a scheduling rule
	// Internal state
	domains map[string]int32 // TODO(ellistarn) explore replacing with a minheap
}

func NewTopologyGroup(topologyType TopologyType, key string, namespaces utilsets.String, labelSelector *metav1.LabelSelector, maxSkew int32, domains utilsets.String) *TopologyGroup {
	domainCounts := map[string]int32{}
	for domain := range domains {
		domainCounts[domain] = 0
	}
	return &TopologyGroup{
		Type:       topologyType,
		Key:        key,
		namespaces: namespaces,
		selector:   labelSelector,
		maxSkew:    maxSkew,
		domains:    domainCounts,
		owners:     map[types.UID]struct{}{},
	}
}

func (t *TopologyGroup) Next(pod *v1.Pod, allowedDomains sets.Set) (string, error) {
	domains := sets.NewSet()
	for domain := range t.domains {
		domains.Insert(domain)
	}
	domains = domains.Intersection(allowedDomains)
	if domains.Len() == 0 {
		return "", fmt.Errorf("%s violated, no viable domain for topology spread with key %s", t.Key, "TODO")
	}
	switch t.Type {
	case TopologyTypeSpread:
		return t.nextTopologySpreadDomain(domains), nil
	case TopologyTypePodAffinity:
		return t.nextAffinityDomain(pod, domains)
	case TopologyTypePodAntiAffinity:
		return t.nextAntiAffinitydomain(domains)
	default:
		return "", fmt.Errorf("unknown topology type %s", t.Type)
	}
}

func (t *TopologyGroup) Record(domains ...string) {
	for _, domain := range domains {
		t.domains[domain]++
	}
}

func (t *TopologyGroup) Matches(namespace string, podLabels labels.Set) bool {
	selector, err := metav1.LabelSelectorAsSelector(t.selector)
	runtime.Must(err)
	return t.namespaces.Has(namespace) && selector.Matches(podLabels)
}

// Register ensures that the topology is aware of the given domain names.
func (t *TopologyGroup) Register(domains ...string) {
	for _, domain := range domains {
		t.domains[domain] = 0
	}
}

func (t *TopologyGroup) AddOwner(key types.UID) {
	t.owners[key] = struct{}{}
}

func (t *TopologyGroup) RemoveOwner(key types.UID) {
	delete(t.owners, key)
}

func (t *TopologyGroup) IsOwnedBy(key types.UID) bool {
	_, ok := t.owners[key]
	return ok
}

// Hash is used so we can track single topologies that affect multiple groups of pods.  If a deployment has 100x pods
// with self anti-affinity, we track that as a single topology with 100 owners instead of 100x topologies.
func (t *TopologyGroup) Hash() uint64 {
	hash, err := hashstructure.Hash(struct {
		TopologyKey   string
		Type          TopologyType
		Namespaces    utilsets.String
		LabelSelector *metav1.LabelSelector
		MaxSkew       int32
	}{
		TopologyKey:   t.Key,
		Type:          t.Type,
		Namespaces:    t.namespaces,
		LabelSelector: t.selector,
		MaxSkew:       t.maxSkew,
	}, hashstructure.FormatV2, &hashstructure.HashOptions{SlicesAsSets: true})
	runtime.Must(err)
	return hash
}

func (t *TopologyGroup) nextTopologySpreadDomain(domains sets.Set) string {
	// Pick the domain that minimizes skew.
	min := int32(math.MaxInt32)
	minDomain := ""
	for domain := range domains.Values() {
		if t.domains[domain] < min {
			min = t.domains[domain]
			minDomain = domain
		}
	}
	return minDomain
}

func (t *TopologyGroup) nextAffinityDomain(pod *v1.Pod, domains sets.Set) (string, error) {
	// Pick any domain that exists, but choose the max to optimize density
	max := int32(0)
	maxDomain := ""
	for domain := range domains.Values() {
		if t.domains[domain] > max {
			max = t.domains[domain]
			maxDomain = domain
		}
	}
	if max > 0 {
		return maxDomain, nil
	}
	if t.Matches(pod.Namespace, pod.Labels) {
		return maxDomain, nil
	}
	return "", fmt.Errorf("no affinity to %s TODO", "")
}

func (t *TopologyGroup) nextAntiAffinitydomain(domains sets.Set) (string, error) {
	for domain := range domains.Values() {
		if t.domains[domain] == 0 {
			return domain, nil
		}
	}
	return "", fmt.Errorf("ERROR")
}

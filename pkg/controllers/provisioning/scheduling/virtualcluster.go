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
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/aws/karpenter/pkg/utils/pod"

	"go.uber.org/multierr"

	"github.com/mitchellh/hashstructure/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
)

// VirtualCluster represents a simulated cluster that holds VirtualNodes which hold sets of compatible pods.
type VirtualCluster struct {
	nodes            []*VirtualNode
	zones            sets.String
	tsc              map[uint64]*Topology
	client           client.Client
	provRequirements []v1.NodeSelectorRequirement
}

func NewVirtualCluster(prov *v1alpha5.Provisioner, client client.Client) *VirtualCluster {
	vc := &VirtualCluster{
		zones:            prov.Spec.Requirements.Zones(),
		provRequirements: prov.Spec.Requirements.Requirements,
		tsc:              map[uint64]*Topology{},
		client:           client,
	}
	return vc
}

func (vc *VirtualCluster) preparePod(ctx context.Context, p *v1.Pod) error {
	// need to first ensure that we know all of the topology constraints.  This may require looking up
	// existing pods in the running cluster to determine zonal topology skew.
	if err := vc.trackTopologySpread(ctx, p); err != nil {
		return fmt.Errorf("tracking topology spread: %w", err)
	}

	if err := vc.trackPodAffinityTopology(ctx, p); err != nil {
		return fmt.Errorf("tracking topology spread: %w", err)
	}

	return nil
}

// SchedulePod schedules a pod against our virtual cluster nodes. If a pod is not schedulable due to some constraint
// violation, wasScheduled will be false, but no error will be reported.  Errors are only reported upon some
// internal failure in scheduling.
func (vc *VirtualCluster) SchedulePod(ctx context.Context, p *v1.Pod) (wasScheduled bool, err error) {
	// just to be safe since we mutate the pod below when adding node selectors
	p = p.DeepCopy()

	// node affinities may limit how much topology skew we can perform and must be
	// taken into consideration to filter out our possible topology domains
	nodeSelectorReqts := vc.getRequiredNodeSelectorRequirements(p)

	vc.applyTopologyConstraints(p, nodeSelectorReqts)

	// did the topology constraints make it unschedulable?
	if reason, ok := p.Spec.NodeSelector[scheduleFailureReasonKey]; ok {
		logging.FromContext(ctx).With("pod", client.ObjectKeyFromObject(p)).Infof("scheduling failed due to %s", reason)
		return false, nil
	}

	var possibleNodes []*VirtualNode
	for _, n := range vc.nodes {
		if n.CanService(p) {
			possibleNodes = append(possibleNodes, n)
		}
	}

	sort.Slice(possibleNodes, byPodPreference(possibleNodes, p))
	if len(possibleNodes) != 0 {
		err = possibleNodes[0].AddPod(p)
	} else {
		// we haven't found a node that we could schedule on so far, so create a new one
		n := NewVirtualNode(vc)

		delete(p.Spec.NodeSelector, v1.LabelHostname)
		vc.nodes = append(vc.nodes, n)
		err = multierr.Append(err, n.AddPod(p))
	}

	err = multierr.Append(err, vc.recordTopologyDecisions(p))

	// we scheduled it
	return true, err
}

func (vc *VirtualCluster) recordTopologyDecisions(p *v1.Pod) error {
	// once we've now committed to a domain, we record the usage in every topology that cares about it
	var err error
	podLabels := labels.Set(p.Labels)
	for _, tc := range vc.tsc {
		if tc.Matches(p.Namespace, podLabels) {
			domain, ok := p.Spec.NodeSelector[tc.Key]
			if !ok || domain == "" {
				err = multierr.Append(err, fmt.Errorf("empty or missing domain for topology key %s", tc.Key))
			} else {
				tc.RecordUsage(domain)
			}
		}
		// ensure that we are aware of any new node names that were created
		if tc.Key == v1.LabelHostname {
			for _, n := range vc.nodes {
				tc.RegisterDomain(n.node.Name)
			}
		}
	}
	return err
}

func byPodPreference(nodes []*VirtualNode, p *v1.Pod) func(a int, b int) bool {
	return func(a, b int) bool {
		n1 := nodes[a]
		n2 := nodes[b]
		paf := getPodAffinityRequirements(p)
		n1Score, n1Hf := paf.Score(n1.pods)
		n2Score, n2Hf := paf.Score(n2.pods)

		// check if either is a hard failure, really shouldn't occur at this point, but just in case
		if !n1Hf && n2Hf {
			return true
		} else if n1Hf && !n2Hf {
			return false
		}

		// we want the higher scoring node first in the list
		if n1Score > n2Score {
			return true
		} else if n1Score < n2Score {
			return false
		}

		// and as a last resort, prefer the node with fewer pods
		return len(n1.pods) < len(n2.pods)
	}
}

type matchingTopology struct {
	topo             *Topology
	matchesPod       bool // does the topology match the pod?
	podOwnsTopology  bool // does the pod own the topology?
	affinityToItself bool // does the pod both own the topology and the topology matches the pod?
}

// applyTopologyConstraints looks at the topology constraints that apply to the pod and turns those into node selector
// labels placed directly on the pod.  If this produces an unsatisfiable set of constraints, it will set a node selector
// of scheduleFailureReasonKey on the pod with the text  being a user presentable string describing why scheduling for
// this pod is not possible.
//gocyclo:ignore
func (vc *VirtualCluster) applyTopologyConstraints(p *v1.Pod, nodeSelectorReqts []v1.NodeSelectorRequirement) {
	podLabels := labels.Set(p.Labels)

	if p.Spec.NodeSelector == nil {
		p.Spec.NodeSelector = map[string]string{}
	}

	var createdNode *VirtualNode

	// first figure out which topologies that we care about
	var matchingTscs []matchingTopology
	for _, tc := range vc.tsc {
		// Topologies are a bit weird to think about. The label selector is the pods that we are counting, and the
		// spread applies to the owner of the topology.  Normally in a standard topology spread, these are the same
		// set of pods. This isn't guaranteed to be the case so we need to track the topology domains for the pods
		// that the labels apply to and enforce it for the pods that own the topology contraints.
		matchesPod := tc.Matches(p.Namespace, podLabels)
		podOwnsTopology := tc.IsOwnedBy(client.ObjectKeyFromObject(p))
		if matchesPod || podOwnsTopology {
			matchingTscs = append(matchingTscs, matchingTopology{
				topo:             tc,
				matchesPod:       matchesPod,
				podOwnsTopology:  podOwnsTopology,
				affinityToItself: matchesPod && podOwnsTopology,
			})
		}
	}

	// we want to apply hard constraints and then preferred constraints sorted by weight
	sort.Slice(matchingTscs, byAffinityWeight(matchingTscs))

	for _, tcItem := range matchingTscs {
		tc := tcItem.topo
		// the incoming pod already has a NodeSelector value specified for the
		// topology key. We can't change it, so we just skip.
		if domain, ok := p.Spec.NodeSelector[tc.Key]; ok {
			nreq := v1.NodeSelectorRequirement{
				Key:      tc.Key,
				Operator: v1.NodeSelectorOpIn,
				Values:   []string{domain},
			}
			merged := false
			for i, nsel := range nodeSelectorReqts {
				if nsel.Key == tc.Key {
					nodeSelectorReqts[i] = v1alpha5.MergeRequirement(nodeSelectorReqts[i], nreq)
					if len(nodeSelectorReqts[i].Values) == 0 {
						p.Spec.NodeSelector[scheduleFailureReasonKey] = fmt.Sprintf("unsatisfiable constraints for %s", tc.Key)
						return
					}
					merged = true
				}
			}
			if !merged {
				nodeSelectorReqts = append(nodeSelectorReqts, nreq)
			}
		}

		// can we create new domains of this type on demand? This affects the max-skew calculation.
		creatable := tc.Key == v1.LabelHostname

		var nextDomain string
		var maxSkew int32
		// if the pod owns the topology, then the topology is affecting the scheduling of the pod.
		if tcItem.podOwnsTopology {
			switch tc.PodAffinityDerived {
			case affinityDerivationPodAffinity:
				// for a pod affinity, we need a non-empty domain to schedule to
				if !tc.HasNonEmptyDomains(nodeSelectorReqts) {
					if tc.AllowViolation {
						// this is a preferred affinity that we can't service
						continue
					}
					if tcItem.affinityToItself {
						//  the pod has affinity to itself which will be satisfied once we schedule the pod
						continue
					}
					p.Spec.NodeSelector[scheduleFailureReasonKey] = fmt.Sprintf("no valid affinity topology domain found for %s", tc.Key)
					return
				}
			case affinityDerivationPodAntiAffinity:
				// for anti-affinity, we need a domain that has no other matching pods
				if !tc.HasEmptyDomains(nodeSelectorReqts) {
					// for hostnames only, we can generate a new empty domain if it's not set already
					if tc.Key == v1.LabelHostname && p.Spec.NodeSelector[v1.LabelHostname] == "" {
						createdNode = NewVirtualNode(vc)
						nextDomain = createdNode.node.Name
						tc.RegisterDomain(nextDomain)
						defer tc.UnregisterDomainIfUnused(nextDomain)
					} else {
						if tc.AllowViolation {
							// this is a preferred affinity and we can't service it
							continue
						}
						p.Spec.NodeSelector[scheduleFailureReasonKey] = fmt.Sprintf("no free anti-affinity topology domain found for %s", tc.Key)
						return
					}
				}
			}
		}

		// if we didn't create a new hostname and domain above
		if nextDomain == "" {
			// normally we select the domain with the minimum number of matching pods in order
			// to reduce skew
			if tc.PodAffinityDerived != affinityDerivationPodAffinity || !tcItem.podOwnsTopology {
				var increasingSkew bool
				// this maxSkew is what the max-skew will be if we schedule this pod
				nextDomain, maxSkew, increasingSkew = tc.NextDomainMinimizeSkew(creatable, nodeSelectorReqts)
				// hostname spreads are special since we can create new hosts on the fly
				if tc.Key == v1.LabelHostname && p.Spec.NodeSelector[v1.LabelHostname] == "" {
					if maxSkew > tc.MaxSkew || nextDomain == "" {
						// just create the node, we'll add to it later as long as we can find a valid domain
						createdNode = NewVirtualNode(vc)
						nextDomain = createdNode.node.Name
						maxSkew = 0
						tc.RegisterDomain(nextDomain)
						defer tc.UnregisterDomainIfUnused(nextDomain)
					}
				}

				// if our constraint can't be violated, we would be violate the max skew if we scheduled this pod
				// and we are increasing skew, we don't schedule.  Checking for increasingSkew is important to handle
				// the situation of moving from a state that already violates max-skew to one that violates it less.
				if !tc.AllowViolation && maxSkew > tc.MaxSkew && increasingSkew {
					tc.NextDomainMinimizeSkew(creatable, nodeSelectorReqts)
					p.Spec.NodeSelector[scheduleFailureReasonKey] = fmt.Sprintf("scheduling would violate max-skew for %s", tc.Key)
					return
				}
			} else {
				// but for pod-affinity, we want to select the domain with the maximum number of matching pods
				nextDomain = tc.MaxDomain(nodeSelectorReqts)
			}
		}

		// Couldn't find a valid next topology domain.  This occurs when there are conflicting constraints (e.g.
		// pod affinity + topology spreads) that we can't satisfy at the same time.
		if nextDomain == "" {
			if tc.AllowViolation {
				// this is a preferred affinity, so it can't make the pod unschedulable
				return
			}
			p.Spec.NodeSelector[scheduleFailureReasonKey] = fmt.Sprintf("unsatisfiable constraints for %s", tc.Key)
			return
		}

		// TODO(todd): possible/probably issue here if you specify both a topology spread and a pod affinity/anti-affinity
		// for the same topology key
		p.Spec.NodeSelector[tc.Key] = nextDomain
	}
	if createdNode != nil {
		vc.nodes = append(vc.nodes, createdNode)
	}
}

func byAffinityWeight(matchingTscs []matchingTopology) func(a int, b int) bool {
	return func(a, b int) bool {
		lhs := matchingTscs[a].topo
		rhs := matchingTscs[b].topo

		// hard constraints false
		if !lhs.AllowViolation && rhs.AllowViolation {
			return true
		} else if lhs.AllowViolation && !rhs.AllowViolation {
			return false
		}

		// then higher weighted preferred constraints
		if lhs.Weight > rhs.Weight {
			return true
		} else if lhs.Weight < rhs.Weight {
			return false
		}

		// and finally just a deterministic ordering
		if lhs.Key != rhs.Key {
			return lhs.Key < rhs.Key
		}
		if lhs.PodAffinityDerived != rhs.PodAffinityDerived {
			return lhs.PodAffinityDerived < rhs.PodAffinityDerived
		}
		return false
	}
}

//gocyclo:ignore
func (vc *VirtualCluster) trackPodAffinityTopology(ctx context.Context, p *v1.Pod) error {
	type affinityKey struct {
		Preferred     bool
		Weight        int32
		AntiAffinity  bool
		TopologyKey   string
		Namespaces    []string
		LabelSelector *metav1.LabelSelector
	}

	var keys []affinityKey
	// first ensure that we are tracking all pod topology constraints
	if pod.HasPodAffinity(p) {
		for _, v := range p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			key := affinityKey{
				TopologyKey:   v.TopologyKey,
				Namespaces:    vc.buildNamespaceList(ctx, p, v.Namespaces, v.NamespaceSelector),
				LabelSelector: v.LabelSelector}
			keys = append(keys, key)
		}
		for _, v := range p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			key := affinityKey{
				Preferred:     true,
				Weight:        v.Weight,
				TopologyKey:   v.PodAffinityTerm.TopologyKey,
				Namespaces:    vc.buildNamespaceList(ctx, p, v.PodAffinityTerm.Namespaces, v.PodAffinityTerm.NamespaceSelector),
				LabelSelector: v.PodAffinityTerm.LabelSelector}
			keys = append(keys, key)
		}
	}

	if pod.HasPodAntiAffinity(p) {
		for _, v := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			key := affinityKey{
				AntiAffinity:  true,
				TopologyKey:   v.TopologyKey,
				Namespaces:    vc.buildNamespaceList(ctx, p, v.Namespaces, v.NamespaceSelector),
				LabelSelector: v.LabelSelector}
			keys = append(keys, key)
		}
		for _, v := range p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			key := affinityKey{
				AntiAffinity:  true,
				Preferred:     true,
				Weight:        v.Weight,
				TopologyKey:   v.PodAffinityTerm.TopologyKey,
				Namespaces:    vc.buildNamespaceList(ctx, p, v.PodAffinityTerm.Namespaces, v.PodAffinityTerm.NamespaceSelector),
				LabelSelector: v.PodAffinityTerm.LabelSelector}
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		hash, err := hashstructure.Hash(key, hashstructure.FormatV2, &hashstructure.HashOptions{SlicesAsSets: true})
		if err != nil {
			return fmt.Errorf("hashing topology constraint: %w", err)
		}
		tsc, found := vc.tsc[hash]
		if !found {
			var cs v1.TopologySpreadConstraint
			cs.TopologyKey = key.TopologyKey
			cs.LabelSelector = key.LabelSelector
			if key.Preferred {
				cs.WhenUnsatisfiable = v1.ScheduleAnyway
			} else {
				cs.WhenUnsatisfiable = v1.DoNotSchedule
			}

			// MaxSkew is unused by the affinity constraint itself, but it's useful to set it to a large value.  This
			// is because we have to commit to topology decisions for the target pods of a pod-affinity.  If nothing
			// else affects it, we don't really care how many of these pods end up per domain.  We only care about what
			// the affinity owner does in relation to those pods and the domain.
			cs.MaxSkew = math.MaxInt32

			topology, err := vc.buildAndUpdateTopology(ctx, cs, key.Namespaces...)
			if err != nil {
				return err
			}
			if key.AntiAffinity {
				topology.PodAffinityDerived = affinityDerivationPodAntiAffinity
			} else {
				topology.PodAffinityDerived = affinityDerivationPodAffinity
			}
			topology.Weight = key.Weight

			tsc = topology
			vc.tsc[hash] = topology
		}
		tsc.AddAffinityOwner(client.ObjectKeyFromObject(p))
	}

	return nil
}

func (vc *VirtualCluster) buildAndUpdateTopology(ctx context.Context, cs v1.TopologySpreadConstraint, namespaces ...string) (*Topology, error) {
	topology, err := NewTopology(cs, namespaces)
	if err != nil {
		return nil, err
	}
	if topology.Key == v1.LabelTopologyZone {
		for _, zone := range vc.zones.List() {
			topology.RegisterDomain(zone)
		}
		// get existing zone spread information
		if err := topology.UpdateFromCluster(ctx, vc.client); err != nil {
			logging.FromContext(ctx).Errorf("unable to get existing zonal topology information, %s", err)
		}
	} else if topology.Key == v1.LabelHostname {
		for _, n := range vc.nodes {
			topology.RegisterDomain(n.node.Name)
		}
		// we don't care about spread on existing hostnames.
		// TODO(todd): maybe we should? e.g. existing cluster has 10 pods per node due to topo spread, do we need to match that?
	} else {
		if err := topology.UpdateFromCluster(ctx, vc.client); err != nil {
			logging.FromContext(ctx).Errorf("unable to get existing %s topology information, %s", topology.Key, err)
		}
	}
	return topology, nil
}

func (vc *VirtualCluster) trackTopologySpread(ctx context.Context, p *v1.Pod) error {
	// first ensure that we are tracking all pod topology constraints
	for _, cs := range p.Spec.TopologySpreadConstraints {
		// constraints apply within a single namespace so we have to ensure we include the namespace in our key
		key := struct {
			Namespace string
			Cs        v1.TopologySpreadConstraint
		}{p.Namespace, cs}

		hash, err := hashstructure.Hash(key, hashstructure.FormatV2, &hashstructure.HashOptions{SlicesAsSets: true})
		if err != nil {
			return fmt.Errorf("hashing topology constraint: %w", err)
		}
		_, found := vc.tsc[hash]
		if !found {
			topology, err := vc.buildAndUpdateTopology(ctx, cs, p.Namespace)
			if err != nil {
				return err
			}
			vc.tsc[hash] = topology
			topology.AddAffinityOwner(client.ObjectKeyFromObject(p))
		}
	}
	return nil
}

func (vc *VirtualCluster) buildNamespaceList(ctx context.Context, p *v1.Pod, namespaces []string, selector *metav1.LabelSelector) []string {
	uniq := map[string]struct{}{}
	uniq[p.Namespace] = struct{}{}

	for _, n := range namespaces {
		uniq[n] = struct{}{}
	}

	if selector != nil {
		var namespaceList v1.NamespaceList
		lsel, err := metav1.LabelSelectorAsSelector(selector)
		if err != nil {
			logging.FromContext(ctx).With("pod", client.ObjectKeyFromObject(p)).Errorf("constructing namespace label selector: %s", err)
			return nil
		}

		if err := vc.client.List(context.Background(), &namespaceList, &client.ListOptions{LabelSelector: lsel}); err != nil {
			logging.FromContext(ctx).With("pod", client.ObjectKeyFromObject(p)).Errorf("fetching namespace list: %s", err)
			return nil
		}
		for _, v := range namespaceList.Items {
			uniq[v.Name] = struct{}{}
		}
	}

	uniqNamespaces := make([]string, 0, len(uniq))
	for k := range uniq {
		uniqNamespaces = append(uniqNamespaces, k)
	}
	return uniqNamespaces
}

// getRequiredNodeSelectorRequirements gets the intersection of any pod node affinity requirements and the provisioner's
// node requirements
func (vc *VirtualCluster) getRequiredNodeSelectorRequirements(p *v1.Pod) []v1.NodeSelectorRequirement {
	reqMap := map[string]v1.NodeSelectorRequirement{}

	// any node affinity requirements
	if p.Spec.Affinity != nil &&
		p.Spec.Affinity.NodeAffinity != nil &&
		p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for _, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			for _, match := range term.MatchExpressions {
				reqMap[match.Key] = match
			}
		}
	}

	// add the provisioner requirements
	for _, req := range vc.provRequirements {
		if existing, ok := reqMap[req.Key]; ok {
			reqMap[req.Key] = v1alpha5.MergeRequirement(existing, req)
		} else {
			reqMap[req.Key] = req
		}
	}
	var nodeSelectorReqts []v1.NodeSelectorRequirement
	for _, req := range reqMap {
		nodeSelectorReqts = append(nodeSelectorReqts, req)
	}
	return nodeSelectorReqts
}

// SortPods prepares pods for scheduling by identifying topology constraints, looking up counts for the domains and
// returning a list of pods topologically sorted by pod affinity relationships.  This ensures that if pods A,B,C have
// affinities such that A->B->C, the pod order in the resulting list will be C,B,A.
func (vc *VirtualCluster) SortPods(ctx context.Context, pods []*v1.Pod) ([]*v1.Pod, error) {
	podLookup := map[client.ObjectKey]*v1.Pod{}
	var g TopoGraph
	var merr error
	for _, p := range pods {
		merr = multierr.Append(merr, vc.preparePod(ctx, p))
		key := client.ObjectKeyFromObject(p)
		podLookup[key] = p
		g.AddNode(key)
	}

	for _, tsc := range vc.tsc {
		for _, p := range pods {
			key := client.ObjectKeyFromObject(p)
			// We only add edges for pod affinities. If we did it for topology spreads, we would
			// get tons of extra edges and cycles, none of which are useful in determining the
			// scheduling order.
			if tsc.PodAffinityDerived != affinityDerivationNone && tsc.Matches(p.Namespace, p.Labels) {
				for owner := range tsc.affinityOwners {
					if owner != key {
						// the item we matched should be scheduled before the
						// topology owner
						g.AddEdge(key, owner)
					}
				}
			}
		}
	}
	pods = nil
	for _, key := range g.Sorted() {
		pods = append(pods, podLookup[key])
	}
	return pods, merr
}

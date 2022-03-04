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

	"go.uber.org/multierr"

	"github.com/mitchellh/hashstructure/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"knative.dev/pkg/logging"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
)

type VirtualCluster struct {
	nodes  []*VirtualNode
	zones  sets.String
	tsc    map[uint64]*Topology
	client client.Client
}

func NewVirtualCluster(prov *v1alpha5.Provisioner, client client.Client) *VirtualCluster {
	vc := &VirtualCluster{
		zones:  prov.Spec.Requirements.Zones(),
		tsc:    map[uint64]*Topology{},
		client: client,
	}
	return vc
}

func (c *VirtualCluster) SchedulePod(ctx context.Context, p *v1.Pod) error {
	// just to be safe since we mutate the pod below when we adding node selectors
	p = p.DeepCopy()

	// need to first ensure that we know all of the topology constraints.  This may require looking up
	// existing pods in the running cluster to determine zonal topology skew.
	if err := c.trackTopologySpread(ctx, p); err != nil {
		return fmt.Errorf("tracking topology spread: %w", err)
	}

	// required node affinities may limit how much topology skew we can perform and must be
	// taken into consideration
	nodeSelectorReqts := getNodeSelectorRequirements(p)

	//
	if err := c.applyTopologyConstraints(p, nodeSelectorReqts); err != nil {
		return fmt.Errorf("applying topology constraints: %w", err)
	}

	var err error
	scheduled := false
	for _, n := range c.nodes {
		// TODO(todd): collect all serviceable nodes and sort by priority
		if n.CanService(p) {
			err = multierr.Append(err, n.AddPod(p))
			scheduled = true
		}
	}
	if !scheduled {
		// we haven't found a node that we could schedule on so far, so create a new one
		n := NewVirtualNode()
		c.nodes = append(c.nodes, n)
		err = multierr.Append(err, n.AddPod(p))
	}
	return err
}

// applyTopologyConstraints looks at the topology constraints that apply to the node and turns those into node selector
// labels placed directly on the pod
func (c *VirtualCluster) applyTopologyConstraints(p *v1.Pod, nodeSelectorReqts []v1.NodeSelectorRequirement) error {
	podLabels := labels.Set(p.Labels)
	for _, tc := range c.tsc {
		if tc.Matches(podLabels) {
			if p.Spec.NodeSelector == nil {
				p.Spec.NodeSelector = map[string]string{}
			}
			// the incoming pod already has a NodeSelector value specified for the
			// topology key. We can't change it, so we record it to track skew and
			// continue
			if domain, ok := p.Spec.NodeSelector[tc.Key]; ok {
				tc.RecordUsage(domain)
				continue
			}

			// can we create new domains of this type on demand? It's important as it affects
			// how we calculate skew
			creatable := tc.Key == v1.LabelHostname

			// HACK
			if tc.Key == v1.LabelHostname {
				for _, n := range c.nodes {
					tc.RegisterDomain(n.node.Name)
				}
			}

			nextDomain, maxSkew := tc.NextDomain(creatable, nodeSelectorReqts)

			// hostname spreads are special since we can create new hosts on the fly
			if tc.Key == v1.LabelHostname {
				if maxSkew > tc.MaxSkew || nextDomain == "" {
					// just create the node, we'll schedule to it later
					n := NewVirtualNode()
					nextDomain = n.node.Name
					c.nodes = append(c.nodes, n)
				}
			}

			if nextDomain == "" {
				return fmt.Errorf("unable to find valid topology domain for %s", tc.Key)
			}

			// TODO(todd): we could check here if the current skew is greater than the max skew allowed, and not
			// schedule the pod if the whenUnsatisfiable constraint indicates that we shouldn't.  In that case
			// we probably really care about if we are decreasing the skew though, even it will still be over the max
			// once we schedule this pod.

			p.Spec.NodeSelector[tc.Key] = nextDomain
			tc.RecordUsage(nextDomain)
		}
	}
	return nil
}

func (c *VirtualCluster) trackTopologySpread(ctx context.Context, p *v1.Pod) error {
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
		_, found := c.tsc[hash]
		if !found {
			topology, err := NewTopology(cs, p.Namespace)
			if err != nil {
				return err
			}
			if topology.Key == v1.LabelTopologyZone {
				for _, zone := range c.zones.List() {
					topology.RegisterDomain(zone)
				}
				// get existing zone spread information
				if err := topology.UpdateFromCluster(ctx, c.client); err != nil {
					logging.FromContext(ctx).Errorf("unable to get existing zonal topology information, %s", err)
				}
			} else if topology.Key == v1.LabelHostname {
				for _, n := range c.nodes {
					topology.RegisterDomain(n.node.Name)
				}
				// we don't care about spread on existing hostnames.
				// TODO(todd): maybe we should? e.g. existing cluster has 10 pods per node due to topo spread, do we need to match that?
			} else {
				if err := topology.UpdateFromCluster(ctx, c.client); err != nil {
					logging.FromContext(ctx).Errorf("unable to get existing %s topology information, %s", topology.Key, err)
				}
			}
			c.tsc[hash] = topology
		}
	}
	return nil
}

func getNodeSelectorRequirements(p *v1.Pod) []v1.NodeSelectorRequirement {
	var nodeSelectorReqts []v1.NodeSelectorRequirement
	if p.Spec.Affinity != nil &&
		p.Spec.Affinity.NodeAffinity != nil &&
		p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for _, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			nodeSelectorReqts = append(nodeSelectorReqts, term.MatchExpressions...)
		}
	}
	return nodeSelectorReqts
}

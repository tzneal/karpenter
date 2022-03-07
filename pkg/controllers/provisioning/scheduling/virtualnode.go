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
	"math/rand"
	"sync"

	"go.uber.org/atomic"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	stringsets "k8s.io/apimachinery/pkg/util/sets"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/utils/resources"
)

type VirtualNode struct {
	node             v1.Node
	pods             []*v1.Pod
	nodeRequirements map[string]*nodeRequirement
	gpuResourceTypes stringsets.String
	usedPorts        map[portInfo]struct{}
	cluster          *VirtualCluster
}

type portInfo struct {
	proto v1.Protocol
	port  int32
}

var nodeNumber atomic.Int32
var randSuffix int
var once sync.Once

func NewVirtualNode(c *VirtualCluster) *VirtualNode {
	once.Do(func() {
		// #nosec G404 - not using this for cryptographic purposes
		randSuffix = rand.Int()
	})

	hostname := fmt.Sprintf("node-%d-%d.karpenter.sh", nodeNumber.Inc(), randSuffix)
	return &VirtualNode{
		node: v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   hostname,
				Labels: map[string]string{v1.LabelHostname: hostname},
			},
		},
		cluster:          c,
		nodeRequirements: map[string]*nodeRequirement{},
		gpuResourceTypes: stringsets.NewString(),
		usedPorts:        map[portInfo]struct{}{},
	}
}

type nodeRequirement struct {
	reqs []v1.NodeSelectorRequirement
}

func (n *nodeRequirement) Intersect(req v1.NodeSelectorRequirement) {
	if len(n.reqs) == 0 {
		n.reqs = append(n.reqs, req)
		return
	}
	for i, r := range n.reqs {
		if r.Key == req.Key {
			n.reqs[i] = v1alpha5.MergeRequirement(r, req)
		}
	}
}

func (n *nodeRequirement) Conflicts(req v1.NodeSelectorRequirement) bool {
	for _, r := range n.reqs {
		if r.Key == req.Key {
			result := v1alpha5.MergeRequirement(r, req)
			if result.Operator == v1.NodeSelectorOpIn {
				if len(result.Values) == 0 {
					return true
				}
			} else if result.Operator == v1.NodeSelectorOpNotIn {
				// additional 'not in' shouldn't be an issue
			} else {
				// shouldn't occur
				return false
			}
		}
	}
	return false
}

func (n *VirtualNode) CanService(p *v1.Pod) bool {
	if !n.isGPUCompatible(p) {
		return false
	}

	if !n.isPodCompatibleWithNode(p) {
		return false
	}

	if !n.isPodCompatibleWithNetworking(p) {
		return false
	}

	var selector labels.Selector

	var nodeLabelKeys []string
	// first check pod node selectors
	if len(p.Spec.NodeSelector) > 0 {
		nodeSel := map[string]string{}
		for k, v := range p.Spec.NodeSelector {
			if _, ok := n.node.Labels[k]; ok {
				nodeSel[k] = v
				nodeLabelKeys = append(nodeLabelKeys, k)
			}
		}
		selector = labels.SelectorFromSet(nodeSel)
	}
	nodeLabels := labels.Set(n.node.Labels)
	if selector != nil && !selector.Matches(nodeLabels) {
		return false
	}

	// the pod's node affinity
	sel := buildSelectorForOnly(n.cluster.getRequiredNodeSelectorRequirements(p), nodeLabelKeys...)
	if !sel.Matches(nodeLabels) {
		return false
	}

	// check pod affinity/anti-affinity to other pods on the node
	paf := getPodAffinityRequirements(p)
	_, hardFailure := paf.Score(n.pods)

	// we can service it as long as there's no hard affinity conflict
	return !hardFailure
}

func getPodAffinityRequirements(p *v1.Pod) podaffinity {
	var pa podaffinity
	if p.Spec.Affinity != nil {
		if p.Spec.Affinity.PodAffinity != nil {
			for _, term := range p.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				pa.AddRequired(term)
			}
			for _, term := range p.Spec.Affinity.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				pa.AddPreferred(term)
			}
		}
		if p.Spec.Affinity.PodAntiAffinity != nil {
			for _, term := range p.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
				pa.AddRequiredAnti(term)
			}
			for _, term := range p.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
				pa.AddPreferredAnti(term)
			}
		}
	}
	return pa
}

func (n *VirtualNode) AddPod(p *v1.Pod) error {
	if n.node.Labels == nil {
		n.node.Labels = map[string]string{}
	}

	// copy the pod's node selectors to this node
	for k, v := range p.Spec.NodeSelector {
		n.node.Labels[k] = v
	}

	// commit back the node name to the pod
	if p.Spec.NodeSelector[v1.LabelHostname] == "" {
		p.Spec.NodeSelector[v1.LabelHostname] = n.node.Name
	} else if p.Spec.NodeSelector[v1.LabelHostname] != n.node.Name {
		return fmt.Errorf("aattmpted to cheduled pod with node selector %s=%s to %s", v1.LabelHostname,
			p.Spec.NodeSelector[v1.LabelHostname], n.node.Name)
	}

	// update our running set of node requirements
	for _, req := range getNodeAffinityRequirements(p) {
		if _, ok := n.nodeRequirements[req.Key]; !ok {
			n.nodeRequirements[req.Key] = &nodeRequirement{}
		}
		n.nodeRequirements[req.Key].Intersect(req)
	}

	// if the pod requires a GPU, we need to know for compatibility purposes
	// so we can avoid scheduling other pods that have an incompatible GPU
	// request on this node
	for k := range resources.GPULimitsFor(p) {
		n.gpuResourceTypes.Insert(string(k))
	}

	// record which host ports this pod uses so we don't conflict with another pod
	for _, c := range p.Spec.Containers {
		for _, port := range c.Ports {
			if port.HostPort != 0 {
				n.usedPorts[portInfo{
					proto: port.Protocol,
					port:  port.HostPort,
				}] = struct{}{}
			}
		}
	}
	n.pods = append(n.pods, p)
	return nil
}

// isGPUCompatible returns true if the pod's GPU requirements are compatible with the node's GPU resources
func (n *VirtualNode) isGPUCompatible(p *v1.Pod) bool {
	podGPURequests := resources.GPULimitsFor(p)
	// both the node and this potential pod have some type of GPU requests
	if len(n.gpuResourceTypes) != 0 && len(podGPURequests) != 0 {
		for k := range podGPURequests {
			if !n.gpuResourceTypes.Has(string(k)) {
				return false
			}
		}
	}
	return true
}

// isPodCompatibleWithNode returns true if the pod's node affinity requirements are compatible with the node
func (n *VirtualNode) isPodCompatibleWithNode(p *v1.Pod) bool {
	// do our current node requirements conflict with the pod's additional requirements?
	for _, req := range getNodeAffinityRequirements(p) {
		if exReq, ok := n.nodeRequirements[req.Key]; ok && exReq.Conflicts(req) {
			return false
		}
	}
	return true
}

func (n *VirtualNode) isPodCompatibleWithNetworking(p *v1.Pod) bool {
	// don't allow any conflicts between HostPort
	for _, c := range p.Spec.Containers {
		for _, port := range c.Ports {
			if _, exists := n.usedPorts[portInfo{
				proto: port.Protocol,
				port:  port.HostPort,
			}]; exists {
				return false
			}
		}
	}
	return true
}

func getNodeAffinityRequirements(p *v1.Pod) []v1.NodeSelectorRequirement {
	var rqts []v1.NodeSelectorRequirement
	if p.Spec.Affinity != nil && p.Spec.Affinity.NodeAffinity != nil && p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for _, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			rqts = append(rqts, term.MatchExpressions...)
		}
	}
	return rqts
}

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
	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/utils/resources"
	"go.uber.org/atomic"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	stringsets "k8s.io/apimachinery/pkg/util/sets"
	"math/rand"
	"sync"
)

type VirtualNode struct {
	node             v1.Node
	pods             []*v1.Pod
	nodeRequirements map[string]*nodeRequirement
	gpuResourceTypes stringsets.String
}

var nodeNumber atomic.Int32
var randSuffix int
var once sync.Once

func NewVirtualNode() *VirtualNode {
	once.Do(func() {
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
		nodeRequirements: map[string]*nodeRequirement{},
		gpuResourceTypes: stringsets.NewString(),
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
				// TODO(todd): what to do here
				panic("figure this out")
			}
		}
	}
	return false
}

func (n *VirtualNode) CanService(p *v1.Pod) bool {
	podGPURequests := resources.GPULimitsFor(p)
	// both the node and this potential pod have some type of GPU requests
	if len(n.gpuResourceTypes) != 0 && len(podGPURequests) != 0 {
		for k := range podGPURequests {
			if !n.gpuResourceTypes.Has(string(k)) {
				return false
			}
		}
	}

	// do our current node requirements conflict with the pod's additional requirements?
	for _, req := range n.getNodeAffinityRequirements(p) {
		if exReq, ok := n.nodeRequirements[req.Key]; ok && exReq.Conflicts(req) {
			return false
		}
	}

	var selector labels.Selector
	// first check pod node selectors
	if len(p.Spec.NodeSelector) > 0 {
		nodeSel := map[string]string{}
		for k, v := range p.Spec.NodeSelector {
			if _, ok := n.node.Labels[k]; ok {
				nodeSel[k] = v
			}
		}
		selector = labels.SelectorFromSet(nodeSel)
	}

	nodeLabels := labels.Set(n.node.Labels)
	if selector != nil && !selector.Matches(nodeLabels) {
		return false
	}

	// does the pod's node affinity match this node?
	sel := buildSelector(getNodeSelectorRequirements(p))
	if !sel.Matches(nodeLabels) {
		return false
	}

	return true
}

func (n *VirtualNode) AddPod(p *v1.Pod) error {
	if n.node.Labels == nil {
		n.node.Labels = map[string]string{}
	}

	// copy the pod's node selectors to this node
	for k, v := range p.Spec.NodeSelector {
		n.node.Labels[k] = v
	}

	// update our running set of node requirements
	for _, req := range n.getNodeAffinityRequirements(p) {
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
	n.pods = append(n.pods, p)
	return nil
}

func (n *VirtualNode) getNodeAffinityRequirements(p *v1.Pod) []v1.NodeSelectorRequirement {
	var rqts []v1.NodeSelectorRequirement
	if p.Spec.Affinity != nil && p.Spec.Affinity.NodeAffinity != nil && p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		for _, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
			rqts = append(rqts, term.MatchExpressions...)
		}
	}
	return rqts
}

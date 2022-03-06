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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type podaffinity struct {
	required              []v1.PodAffinityTerm
	requiredAnti          []v1.PodAffinityTerm
	requiredSelectors     []labels.Selector
	requiredAntiSelectors []labels.Selector

	preferred              []v1.WeightedPodAffinityTerm
	preferredAnti          []v1.WeightedPodAffinityTerm
	preferredSelectors     []labels.Selector
	preferredAntiSelectors []labels.Selector
}

func (p *podaffinity) AddRequired(term v1.PodAffinityTerm) {
	// ignore the empty pod affinity term
	if term.LabelSelector == nil || len(term.LabelSelector.MatchLabels) == 0 {
		return
	}
	p.required = append(p.required, term)
	p.requiredSelectors = append(p.requiredSelectors, labels.SelectorFromSet(term.LabelSelector.MatchLabels))
}

func (p *podaffinity) AddPreferred(term v1.WeightedPodAffinityTerm) {
	if term.PodAffinityTerm.LabelSelector == nil || len(term.PodAffinityTerm.LabelSelector.MatchLabels) == 0 {
		return
	}
	p.preferred = append(p.preferred, term)
	p.preferredSelectors = append(p.preferredSelectors, labels.SelectorFromSet(term.PodAffinityTerm.LabelSelector.MatchLabels))
}

func (p *podaffinity) AddRequiredAnti(term v1.PodAffinityTerm) {
	if term.LabelSelector == nil || len(term.LabelSelector.MatchLabels) == 0 {
		return
	}
	p.requiredAnti = append(p.requiredAnti, term)
	p.requiredAntiSelectors = append(p.requiredAntiSelectors, labels.SelectorFromSet(term.LabelSelector.MatchLabels))
}

func (p *podaffinity) AddPreferredAnti(term v1.WeightedPodAffinityTerm) {
	if term.PodAffinityTerm.LabelSelector == nil || len(term.PodAffinityTerm.LabelSelector.MatchLabels) == 0 {
		return
	}
	p.preferredAnti = append(p.preferredAnti, term)
	p.preferredAntiSelectors = append(p.preferredAntiSelectors, labels.SelectorFromSet(term.PodAffinityTerm.LabelSelector.MatchLabels))
}

// Score returns a numerical score and a boolean indicating if the pod affinity is a hard failure (e.g. a pod
// violates an anti-affinity rule)
func (p *podaffinity) Score(pods []*v1.Pod) (score int, hardFailure bool) {
	// can't fail to match a required selector if there are none
	matchedNoRequired := len(p.requiredSelectors) > 0
	for _, pd := range pods {
		podLabels := labels.Set(pd.Labels)

		// pod affinity
		for _, sel := range p.requiredSelectors {
			if sel.Matches(podLabels) {
				score++
				matchedNoRequired = false
			}
		}

		for i, sel := range p.preferredSelectors {
			if sel.Matches(podLabels) {
				score += int(p.preferred[i].Weight)
			}
		}

		// pod anti-affinity
		for _, sel := range p.requiredAntiSelectors {
			// matching any anti-affinity is an immediate failure
			if sel.Matches(podLabels) {
				return -1, true
			}
		}

		for i, sel := range p.preferredAntiSelectors {
			if sel.Matches(podLabels) {
				score -= int(p.preferredAnti[i].Weight)
			}
		}
	}

	// didn't match any of the required affinity selectors to any pod
	if matchedNoRequired {
		return -1, true
	}
	return score, false
}

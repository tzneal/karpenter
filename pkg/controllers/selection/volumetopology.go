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

package selection

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/ptr"
)

func NewVolumeTopology(kubeClient kubernetes.Interface) *VolumeTopology {
	return &VolumeTopology{kubeClient: kubeClient}
}

type VolumeTopology struct {
	kubeClient kubernetes.Interface
}

func (v *VolumeTopology) Inject(ctx context.Context, pod *v1.Pod) error {
	var requirements []v1.NodeSelectorRequirement
	for _, volume := range pod.Spec.Volumes {
		req, err := v.getRequirements(ctx, pod, volume)
		if err != nil {
			return err
		}
		requirements = append(requirements, req...)
	}
	if len(requirements) == 0 {
		return nil
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &v1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}
	if len(pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms) == 0 {
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []v1.NodeSelectorTerm{{}}
	}
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions = append(
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions, requirements...)
	return nil
}

func (v *VolumeTopology) getRequirements(ctx context.Context, pod *v1.Pod, volume v1.Volume) ([]v1.NodeSelectorRequirement, error) {
	// Get PVC
	if volume.PersistentVolumeClaim == nil {
		return nil, nil
	}
	pvc, err := v.kubeClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, volume.PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting persistent volume claim %s, %w", volume.PersistentVolumeClaim.ClaimName, err)
	}
	// Persistent Volume Requirements
	if pvc.Spec.VolumeName != "" {
		requirements, err := v.getPersistentVolumeRequirements(ctx, pod, pvc)
		if err != nil {
			return nil, fmt.Errorf("getting existing requirements, %w", err)
		}
		return requirements, nil
	}
	// Storage Class Requirements
	if ptr.StringValue(pvc.Spec.StorageClassName) != "" {
		requirements, err := v.getStorageClassRequirements(ctx, pvc)
		if err != nil {
			return nil, err
		}
		return requirements, nil
	}
	return nil, nil
}

func (v *VolumeTopology) getStorageClassRequirements(ctx context.Context, pvc *v1.PersistentVolumeClaim) ([]v1.NodeSelectorRequirement, error) {
	storageClass, err := v.kubeClient.StorageV1().StorageClasses().Get(ctx, ptr.StringValue(pvc.Spec.StorageClassName), metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting storage class %q, %w", ptr.StringValue(pvc.Spec.StorageClassName), err)
	}
	var requirements []v1.NodeSelectorRequirement
	if len(storageClass.AllowedTopologies) > 0 {
		// Terms are ORed, only use the first term
		for _, requirement := range storageClass.AllowedTopologies[0].MatchLabelExpressions {
			requirements = append(requirements, v1.NodeSelectorRequirement{Key: requirement.Key, Operator: v1.NodeSelectorOpIn, Values: requirement.Values})
		}
	}
	return requirements, nil
}

func (v *VolumeTopology) getPersistentVolumeRequirements(ctx context.Context, pod *v1.Pod, pvc *v1.PersistentVolumeClaim) ([]v1.NodeSelectorRequirement, error) {
	pv, err := v.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})

	if err != nil {
		return nil, fmt.Errorf("getting persistent volume %q, %w", pvc.Spec.VolumeName, err)
	}
	if pv.Spec.NodeAffinity == nil {
		return nil, nil
	}
	if pv.Spec.NodeAffinity.Required == nil {
		return nil, nil
	}
	var requirements []v1.NodeSelectorRequirement
	if len(pv.Spec.NodeAffinity.Required.NodeSelectorTerms) > 0 {
		// Terms are ORed, only use the first term
		requirements = append(requirements, pv.Spec.NodeAffinity.Required.NodeSelectorTerms[0].MatchExpressions...)
	}
	return requirements, nil
}

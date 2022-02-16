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

package provisioning

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/client/clientset/versioned"
	"github.com/aws/karpenter/pkg/cloudprovider"
	"github.com/aws/karpenter/pkg/utils/functional"
	"github.com/mitchellh/hashstructure/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/reconciler"
)

type Reconciler struct {
	provisioners  *sync.Map
	kubeClient    kubernetes.Interface
	cloudProvider cloudprovider.CloudProvider
	karpClient    *versioned.Clientset
}

func (r *Reconciler) ReconcileKind(ctx context.Context, provisioner *v1alpha5.Provisioner) reconciler.Event {
	if err := r.Apply(ctx, provisioner); err != nil {
		return err
	}
	// Requeue in order to discover any changes from GetInstanceTypes.
	return controller.NewRequeueAfter(5 * time.Minute)
}

func (r *Reconciler) Apply(ctx context.Context, provisioner *v1alpha5.Provisioner) error {
	// Refresh global requirements using instance type availability
	instanceTypes, err := r.cloudProvider.GetInstanceTypes(ctx, provisioner.Spec.Provider)
	if err != nil {
		return err
	}
	provisioner.Spec.Labels = functional.UnionStringMaps(provisioner.Spec.Labels, map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name})
	provisioner.Spec.Requirements = provisioner.Spec.Requirements.
		Add(requirements(instanceTypes)...).
		Add(v1alpha5.NewLabelRequirements(provisioner.Spec.Labels).Requirements...)
	if err := provisioner.Spec.Requirements.Validate(); err != nil {
		return fmt.Errorf("requirements are not compatible with cloud provider, %w", err)
	}
	// Update the provisioner if anything has changed
	if r.hasChanged(ctx, provisioner) {
		r.Delete(provisioner.Name)
		r.provisioners.Store(provisioner.Name, NewProvisioner(ctx, provisioner, r.kubeClient, r.karpClient, r.cloudProvider))
	}
	return nil
}

// Delete stops and removes a provisioner. Enqueued pods will be provisioned.
func (r *Reconciler) Delete(name string) {
	if p, ok := r.provisioners.LoadAndDelete(name); ok {
		p.(*Provisioner).Stop()
	}
}

// Returns true if the new candidate provisioner is different than the provisioner in memory.
func (c *Reconciler) hasChanged(ctx context.Context, provisionerNew *v1alpha5.Provisioner) bool {
	oldProvisioner, ok := c.provisioners.Load(provisionerNew.Name)
	if !ok {
		return true
	}
	hashKeyOld, err := hashstructure.Hash(oldProvisioner.(*Provisioner).Spec, hashstructure.FormatV2, &hashstructure.HashOptions{SlicesAsSets: true})
	if err != nil {
		logging.FromContext(ctx).Fatalf("Unable to hash old provisioner spec: %s", err)
	}
	hashKeyNew, err := hashstructure.Hash(provisionerNew.Spec, hashstructure.FormatV2, &hashstructure.HashOptions{SlicesAsSets: true})
	if err != nil {
		logging.FromContext(ctx).Fatalf("Unable to hash new provisioner spec: %s", err)
	}
	return hashKeyOld != hashKeyNew
}

// List active provisioners in order of priority
func (c *Reconciler) List(ctx context.Context) []*Provisioner {
	provisioners := []*Provisioner{}
	c.provisioners.Range(func(key, value interface{}) bool {
		provisioners = append(provisioners, value.(*Provisioner))
		return true
	})
	sort.Slice(provisioners, func(i, j int) bool { return provisioners[i].Name < provisioners[j].Name })
	return provisioners
}

func requirements(instanceTypes []cloudprovider.InstanceType) []v1.NodeSelectorRequirement {
	supported := map[string]sets.String{
		v1.LabelInstanceTypeStable: sets.NewString(),
		v1.LabelTopologyZone:       sets.NewString(),
		v1.LabelArchStable:         sets.NewString(),
		v1.LabelOSStable:           sets.NewString(),
		v1alpha5.LabelCapacityType: sets.NewString(),
	}
	for _, instanceType := range instanceTypes {
		for _, offering := range instanceType.Offerings() {
			supported[v1.LabelTopologyZone].Insert(offering.Zone)
			supported[v1alpha5.LabelCapacityType].Insert(offering.CapacityType)
		}
		supported[v1.LabelInstanceTypeStable].Insert(instanceType.Name())
		supported[v1.LabelArchStable].Insert(instanceType.Architecture())
		supported[v1.LabelOSStable].Insert(instanceType.OperatingSystems().List()...)
	}
	requirements := []v1.NodeSelectorRequirement{}
	for key, values := range supported {
		requirements = append(requirements, v1.NodeSelectorRequirement{Key: key, Operator: v1.NodeSelectorOpIn, Values: values.UnsortedList()})
	}
	return requirements
}

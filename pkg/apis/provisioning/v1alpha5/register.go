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

package v1alpha5

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/pkg/apis"
)

var (
	DefaultHook  = func(ctx context.Context, constraints *Constraints) {}
	ValidateHook = func(ctx context.Context, constraints *Constraints) *apis.FieldError { return nil }
)

var (
	Group              = "karpenter.sh"
	ExtensionsGroup    = "extensions." + Group
	SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: "v1alpha5"}
	SchemeBuilder      = runtime.NewSchemeBuilder(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(SchemeGroupVersion,
			&Provisioner{},
			&ProvisionerList{},
		)
		metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
		return nil
	})
	ProvisionerNameLabelKey         = Group + "/provisioner-name"
	NotReadyTaintKey                = Group + "/not-ready"
	DoNotEvictPodAnnotationKey      = Group + "/do-not-evict"
	EmptinessTimestampAnnotationKey = Group + "/emptiness-timestamp"
	TerminationFinalizer            = Group + "/termination"
)

const (
	// Active is a condition implemented by all resources. It indicates that the
	// controller is able to take actions: it's correctly configured, can make
	// necessary API calls, and isn't disabled.
	Active apis.ConditionType = "Active"
)

var (
	// AddToScheme adds the types known to this package to an existing schema.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Resource takes an unqualified resource and returns a Group qualified GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

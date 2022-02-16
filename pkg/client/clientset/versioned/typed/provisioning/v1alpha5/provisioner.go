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

// Code generated by client-gen. DO NOT EDIT.

package v1alpha5

import (
	"context"
	"time"

	v1alpha5 "github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	scheme "github.com/aws/karpenter/pkg/client/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// ProvisionersGetter has a method to return a ProvisionerInterface.
// A group's client should implement this interface.
type ProvisionersGetter interface {
	Provisioners() ProvisionerInterface
}

// ProvisionerInterface has methods to work with Provisioner resources.
type ProvisionerInterface interface {
	Create(ctx context.Context, provisioner *v1alpha5.Provisioner, opts v1.CreateOptions) (*v1alpha5.Provisioner, error)
	Update(ctx context.Context, provisioner *v1alpha5.Provisioner, opts v1.UpdateOptions) (*v1alpha5.Provisioner, error)
	UpdateStatus(ctx context.Context, provisioner *v1alpha5.Provisioner, opts v1.UpdateOptions) (*v1alpha5.Provisioner, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha5.Provisioner, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha5.ProvisionerList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha5.Provisioner, err error)
	ProvisionerExpansion
}

// provisioners implements ProvisionerInterface
type provisioners struct {
	client rest.Interface
}

// newProvisioners returns a Provisioners
func newProvisioners(c *KarpenterV1alpha5Client) *provisioners {
	return &provisioners{
		client: c.RESTClient(),
	}
}

// Get takes name of the provisioner, and returns the corresponding provisioner object, and an error if there is any.
func (c *provisioners) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha5.Provisioner, err error) {
	result = &v1alpha5.Provisioner{}
	err = c.client.Get().
		Resource("provisioners").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of Provisioners that match those selectors.
func (c *provisioners) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha5.ProvisionerList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha5.ProvisionerList{}
	err = c.client.Get().
		Resource("provisioners").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested provisioners.
func (c *provisioners) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Resource("provisioners").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a provisioner and creates it.  Returns the server's representation of the provisioner, and an error, if there is any.
func (c *provisioners) Create(ctx context.Context, provisioner *v1alpha5.Provisioner, opts v1.CreateOptions) (result *v1alpha5.Provisioner, err error) {
	result = &v1alpha5.Provisioner{}
	err = c.client.Post().
		Resource("provisioners").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(provisioner).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a provisioner and updates it. Returns the server's representation of the provisioner, and an error, if there is any.
func (c *provisioners) Update(ctx context.Context, provisioner *v1alpha5.Provisioner, opts v1.UpdateOptions) (result *v1alpha5.Provisioner, err error) {
	result = &v1alpha5.Provisioner{}
	err = c.client.Put().
		Resource("provisioners").
		Name(provisioner.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(provisioner).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *provisioners) UpdateStatus(ctx context.Context, provisioner *v1alpha5.Provisioner, opts v1.UpdateOptions) (result *v1alpha5.Provisioner, err error) {
	result = &v1alpha5.Provisioner{}
	err = c.client.Put().
		Resource("provisioners").
		Name(provisioner.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(provisioner).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the provisioner and deletes it. Returns an error if one occurs.
func (c *provisioners) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Resource("provisioners").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *provisioners) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Resource("provisioners").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched provisioner.
func (c *provisioners) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha5.Provisioner, err error) {
	result = &v1alpha5.Provisioner{}
	err = c.client.Patch(pt).
		Resource("provisioners").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}

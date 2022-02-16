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

// Code generated by informer-gen. DO NOT EDIT.

package v1alpha5

import (
	"context"
	time "time"

	provisioningv1alpha5 "github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	versioned "github.com/aws/karpenter/pkg/client/clientset/versioned"
	internalinterfaces "github.com/aws/karpenter/pkg/client/informers/externalversions/internalinterfaces"
	v1alpha5 "github.com/aws/karpenter/pkg/client/listers/provisioning/v1alpha5"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	watch "k8s.io/apimachinery/pkg/watch"
	cache "k8s.io/client-go/tools/cache"
)

// ProvisionerInformer provides access to a shared informer and lister for
// Provisioners.
type ProvisionerInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() v1alpha5.ProvisionerLister
}

type provisionerInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
}

// NewProvisionerInformer constructs a new informer for Provisioner type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewProvisionerInformer(client versioned.Interface, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredProvisionerInformer(client, resyncPeriod, indexers, nil)
}

// NewFilteredProvisionerInformer constructs a new informer for Provisioner type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredProvisionerInformer(client versioned.Interface, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options v1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KarpenterV1alpha5().Provisioners().List(context.TODO(), options)
			},
			WatchFunc: func(options v1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.KarpenterV1alpha5().Provisioners().Watch(context.TODO(), options)
			},
		},
		&provisioningv1alpha5.Provisioner{},
		resyncPeriod,
		indexers,
	)
}

func (f *provisionerInformer) defaultInformer(client versioned.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredProvisionerInformer(client, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *provisionerInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&provisioningv1alpha5.Provisioner{}, f.defaultInformer)
}

func (f *provisionerInformer) Lister() v1alpha5.ProvisionerLister {
	return v1alpha5.NewProvisionerLister(f.Informer().GetIndexer())
}

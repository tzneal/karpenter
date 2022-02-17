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

package expectations

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/controllers/provisioning"
	"github.com/aws/karpenter/pkg/controllers/selection"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
	"knative.dev/pkg/controller"

	//nolint:revive,stylecheck
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ReconcilerPropagationTime = 10 * time.Second
	RequestInterval           = 1 * time.Second
)

func ExpectProvisionerExists(ctx context.Context, c client.Client, name string) *v1alpha5.Provisioner {
	prov := &v1alpha5.Provisioner{}
	Expect(c.Get(ctx, client.ObjectKey{Name: name}, prov)).To(Succeed())
	return prov
}

func ExpectPodExistsLister(ctx context.Context, name, namespace string) {
	Eventually(func() bool {
		pod, _ := podinformer.Get(ctx).Lister().Pods(namespace).Get(name)
		return pod != nil
	}).Should(BeTrue())
}
func ExpectPodExists(ctx context.Context, c client.Client, name string, namespace string) *v1.Pod {
	pod := &v1.Pod{}
	Expect(c.Get(ctx, client.ObjectKey{Name: name, Namespace: namespace}, pod)).To(Succeed())
	return pod
}

func ExpectNodeExists(ctx context.Context, c client.Client, name string) *v1.Node {
	node := &v1.Node{}
	Expect(c.Get(ctx, client.ObjectKey{Name: name}, node)).To(Succeed())
	return node
}

func ExpectNotFound(ctx context.Context, c client.Client, objects ...client.Object) {
	for _, object := range objects {
		Eventually(func() bool {
			return errors.IsNotFound(c.Get(ctx, types.NamespacedName{Name: object.GetName(), Namespace: object.GetNamespace()}, object))
		}, ReconcilerPropagationTime, RequestInterval).Should(BeTrue(), func() string {
			return fmt.Sprintf("expected %s to be deleted, but it still exists", client.ObjectKeyFromObject(object))
		})
	}
}

func ExpectScheduled(ctx context.Context, c client.Client, pod *v1.Pod) *v1.Node {
	p := ExpectPodExists(ctx, c, pod.Name, pod.Namespace)
	Expect(p.Spec.NodeName).ToNot(BeEmpty(), fmt.Sprintf("expected %s/%s to be scheduled", pod.Namespace, pod.Name))
	return ExpectNodeExists(ctx, c, p.Spec.NodeName)
}

func ExpectNotScheduled(ctx context.Context, c client.Client, pod *v1.Pod) {
	p := ExpectPodExists(ctx, c, pod.Name, pod.Namespace)
	Eventually(p.Spec.NodeName).Should(BeEmpty(), fmt.Sprintf("expected %s/%s to not be scheduled", pod.Namespace, pod.Name))
}

func ExpectApplied(ctx context.Context, c client.Client, objects ...client.Object) {
	for _, object := range objects {
		if object.GetResourceVersion() == "" {
			Expect(c.Create(ctx, object)).To(Succeed())
		} else {
			Expect(c.Update(ctx, object)).To(Succeed())
		}
	}
}

func ExpectStatusUpdated(ctx context.Context, c client.Client, objects ...client.Object) {
	for _, object := range objects {
		Expect(c.Status().Update(ctx, object)).To(Succeed())
	}
}

func ExpectCreated(ctx context.Context, c client.Client, objects ...client.Object) {
	for _, object := range objects {
		Expect(c.Create(ctx, object)).To(Succeed())
	}
}

func ExpectCreatedWithStatus(ctx context.Context, c client.Client, objects ...client.Object) {
	for _, object := range objects {
		updatecopy := object.DeepCopyObject().(client.Object)
		deletecopy := object.DeepCopyObject().(client.Object)
		ExpectApplied(ctx, c, object)

		// some objects (e.g. PDB) require that the resource version match prior to an update
		Expect(c.Get(ctx, client.ObjectKeyFromObject(object), object)).To(Succeed())
		updatecopy.SetResourceVersion(object.GetResourceVersion())

		Expect(c.Status().Update(ctx, updatecopy)).To(Succeed())
		if deletecopy.GetDeletionTimestamp() != nil {
			Expect(c.Delete(ctx, deletecopy, &client.DeleteOptions{GracePeriodSeconds: ptr.Int64(int64(time.Until(deletecopy.GetDeletionTimestamp().Time).Seconds()))})).ToNot(HaveOccurred())
		}
	}
}

func ExpectDeleted(ctx context.Context, c client.Client, objects ...client.Object) {
	for _, object := range objects {
		if err := c.Delete(ctx, object, &client.DeleteOptions{GracePeriodSeconds: ptr.Int64(0)}); !errors.IsNotFound(err) {
			Expect(err).To(BeNil())
		}
		ExpectNotFound(ctx, c, object)
	}
}

func ExpectCleanedUp(ctx context.Context, c client.Client) {
	wg := sync.WaitGroup{}
	namespaces := &v1.NamespaceList{}
	Expect(c.List(ctx, namespaces)).To(Succeed())
	nodes := &v1.NodeList{}
	Expect(c.List(ctx, nodes))
	for i := range nodes.Items {
		nodes.Items[i].SetFinalizers([]string{})
		Expect(c.Update(ctx, &nodes.Items[i])).To(Succeed())
	}

	for _, object := range []client.Object{
		&v1.Pod{},
		&v1.Node{},
		&appsv1.DaemonSet{},
		&v1beta1.PodDisruptionBudget{},
		&v1.PersistentVolumeClaim{},
		&v1.PersistentVolume{},
		&storagev1.StorageClass{},
		&v1alpha5.Provisioner{},
	} {
		for _, namespace := range namespaces.Items {
			wg.Add(1)
			go func(object client.Object, namespace string) {
				Expect(c.DeleteAllOf(ctx, object, client.InNamespace(namespace))).ToNot(HaveOccurred())
				wg.Done()
			}(object, namespace.Name)
		}
	}
	wg.Wait()
}

// ExpectProvisioningCleanedUp includes additional cleanup logic for provisioning workflows
func ExpectProvisioningCleanedUp(ctx context.Context, c client.Client, reconciler *provisioning.Reconciler) {
	provisioners := v1alpha5.ProvisionerList{}
	Expect(c.List(ctx, &provisioners)).To(Succeed())
	ExpectCleanedUp(ctx, c)
	for i := range provisioners.Items {
		ExpectReconcileSucceeded(ctx, c, reconciler, &provisioners.Items[i])
	}
}

func ExpectProvisioned(ctx context.Context, c client.Client, selectionController *selection.Reconciler, provisioningController *provisioning.Reconciler, provisioner *v1alpha5.Provisioner, pods ...*v1.Pod) (result []*v1.Pod) {
	provisioning.MaxItemsPerBatch = len(pods)
	// Persist objects
	ExpectApplied(ctx, c, provisioner)
	ExpectStatusUpdated(ctx, c, provisioner)
	for _, pod := range pods {
		ExpectCreatedWithStatus(ctx, c, pod)
	}

	// Wait for reconcile
	ExpectReconcileSucceeded(ctx, c, provisioningController, provisioner)
	wg := sync.WaitGroup{}
	for _, pod := range pods {
		wg.Add(1)
		go func(pod *v1.Pod) {
			// need to look the pod back up so the reconciler gets the latest version
			pod = ExpectPodExists(ctx, c, pod.Name, pod.Namespace)
			selectionController.ReconcileKind(ctx, pod)
			wg.Done()
		}(pod)
	}
	wg.Wait()
	// Return updated pods
	for _, pod := range pods {
		result = append(result, ExpectPodExists(ctx, c, pod.GetName(), pod.GetNamespace()))
	}
	return result
}

// ExpectReconcileSucceeded tests that reconcile succeeds. It is meant to mimic the generated knative reconcile
// implementation which calls ReconcileKind or FinalizeKind depending on the object's deletion timestamp.
func ExpectReconcileSucceeded(ctx context.Context, c client.Client, reconciler interface{}, obj client.Object) error {
	// fetch the latest object (knative reconciler gets a key that it then looks up, so we emulate that)
	aerr := c.Get(ctx, client.ObjectKeyFromObject(obj), obj)

	if errors.IsNotFound(aerr) {
		// user specified an object, but it no longer exists. We do the best we can by making this look
		// like a deletion so the appropriate method will be called
		if obj.GetDeletionTimestamp().IsZero() {
			n := metav1.Now()
			obj.SetDeletionTimestamp(&n)
		}
	} else {
		Expect(aerr).Should(Succeed())
	}

	var method reflect.Value
	objectDescription := ""
	wantedMethod := ""
	if obj.GetDeletionTimestamp().IsZero() {
		objectDescription = "live"
		wantedMethod = "ReconcileKind"
	} else {
		objectDescription = "deleted"
		wantedMethod = "FinalizeKind"
	}

	rv := reflect.ValueOf(reconciler)
	method = rv.MethodByName(wantedMethod)
	Expect(method).ToNot(Equal(reflect.Value{}),
		fmt.Sprintf("reconciled a %s object, but %T has no %s method", objectDescription, reconciler, wantedMethod))

	result := method.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(obj)})
	Expect(len(result)).Should(BeNumerically("==", 1))
	err := result[0].Interface()
	if err == nil {
		return nil
	}

	// allow skips/requeues, but not errors
	Expect(err).To(
		Or(WithTransform(controller.IsSkipKey, Equal(true)),
			WithTransform(func(err error) bool { ok, _ := controller.IsRequeueKey(err); return ok }, Equal(true))))

	return err.(error)
}

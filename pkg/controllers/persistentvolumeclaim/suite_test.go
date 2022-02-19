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

package persistentvolumeclaim_test

import (
	"context"
	"strings"
	"testing"

	"github.com/Pallinder/go-randomdata"
	"github.com/aws/karpenter/pkg/controllers/persistentvolumeclaim"
	"github.com/aws/karpenter/pkg/test"
	. "github.com/aws/karpenter/pkg/test/expectations"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	. "knative.dev/pkg/logging/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	// podinformer "github.com/aws/karpenter/pkg/k8sgen/informers/core/v1/pod"
	podinformer "knative.dev/pkg/client/injection/kube/informers/core/v1/pod"
)

var ctx context.Context
var pvccontroller *persistentvolumeclaim.Reconciler
var env *test.Environment

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Volume")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(ctx, func(e *test.Environment) {
		// need to use the environment's modified context
		ctx = e.Ctx
		pvccontroller = &persistentvolumeclaim.Reconciler{
			KubeClient: kubernetes.NewForConfigOrDie(e.Config),
			PodLister:  podinformer.Get(ctx).Lister(),
		}
	})
	Expect(env.Start()).To(Succeed(), "Failed to start environment")
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Reconcile", func() {
	var pvc *v1.PersistentVolumeClaim

	BeforeEach(func() {
		pvc = test.PersistentVolumeClaim()
	})

	AfterEach(func() {
		ExpectCleanedUp(ctx, env.Client)
	})

	It("should ignore a pvc without pods", func() {
		ExpectCreated(ctx, env.Client, pvc)
		ExpectReconcileSucceeded(ctx, pvccontroller, pvc)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)).To(Succeed())
		Expect(pvc.Annotations[persistentvolumeclaim.SelectedNodeAnnotation]).To(BeEmpty())
	})
	It("should ignore a pvc with unscheduled or terminal pods", func() {
		ExpectCreated(ctx, env.Client, pvc,
			test.Pod(test.PodOptions{Phase: v1.PodPending}),
			test.Pod(test.PodOptions{NodeName: strings.ToLower(randomdata.SillyName()), Phase: v1.PodSucceeded}),
			test.Pod(test.PodOptions{NodeName: strings.ToLower(randomdata.SillyName()), Phase: v1.PodFailed}),
		)
		ExpectReconcileSucceeded(ctx, pvccontroller, pvc)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)).To(Succeed())
		Expect(pvc.Annotations[persistentvolumeclaim.SelectedNodeAnnotation]).To(BeEmpty())
	})
	It("should bind a pvc to a pod's node", func() {
		pod := test.Pod(test.PodOptions{NodeName: strings.ToLower(randomdata.SillyName()), PersistentVolumeClaims: []string{pvc.Name}})
		ExpectCreated(ctx, env.Client, pvc, pod)
		ExpectPodExistsLister(ctx, pod.Name, pod.Namespace)
		ExpectReconcileSucceeded(ctx, pvccontroller, pvc)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)).To(Succeed())
		Expect(pvc.Annotations[persistentvolumeclaim.SelectedNodeAnnotation]).To(Equal(pod.Spec.NodeName))
	})
})

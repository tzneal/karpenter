/*
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

package node_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"github.com/aws/karpenter/pkg/client/clientset/versioned"
	"github.com/aws/karpenter/pkg/controllers/node"
	"github.com/aws/karpenter/pkg/test"
	. "github.com/aws/karpenter/pkg/test/expectations"
	"github.com/aws/karpenter/pkg/utils/injectabletime"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/controller"
	. "knative.dev/pkg/logging/testing"
	"knative.dev/pkg/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ctx context.Context
var reconciler *node.Reconciler
var env *test.Environment

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Node")
}

var _ = BeforeSuite(func() {

	env = test.NewEnvironment(ctx, func(e *test.Environment) {
		ctx = e.Ctx
		reconciler = node.NewReconciler(ctx)
	})
	Expect(env.Start()).To(Succeed(), "Failed to start environment")
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = Describe("Reconciler", func() {
	var provisioner *v1alpha5.Provisioner
	BeforeEach(func() {
		provisioner = &v1alpha5.Provisioner{
			ObjectMeta: metav1.ObjectMeta{Name: strings.ToLower(randomdata.SillyName())},
			Spec:       v1alpha5.ProvisionerSpec{},
		}
	})

	AfterEach(func() {
		injectabletime.Now = time.Now
		ExpectCleanedUp(ctx, env.Client)
	})

	Context("Expiration", func() {
		It("should ignore nodes without TTLSecondsUntilExpired", func() {
			n := test.Node(test.NodeOptions{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{v1alpha5.TerminationFinalizer},
					Labels: map[string]string{
						v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
					},
				},
			})
			ExpectCreated(ctx, env.Client, provisioner, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.DeletionTimestamp.IsZero()).To(BeTrue())
		})
		It("should ignore nodes without a provisioner", func() {
			n := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{Finalizers: []string{v1alpha5.TerminationFinalizer}}})
			ExpectCreated(ctx, env.Client, provisioner, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.DeletionTimestamp.IsZero()).To(BeTrue())
		})
		It("should delete nodes after expiry", func() {
			provisioner.Spec.TTLSecondsUntilExpired = ptr.Int64(30)
			n := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Finalizers: []string{v1alpha5.TerminationFinalizer},
				Labels: map[string]string{
					v1alpha5.ProvisionerNameLabelKey: provisioner.Name,
				},
			}})
			ExpectCreated(ctx, env.Client, provisioner, n)

			a := env.Client.Get(ctx, client.ObjectKeyFromObject(provisioner), provisioner)
			_ = a
			// Should still exist
			ExpectReconcileSucceeded(ctx, reconciler, n)
			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.DeletionTimestamp.IsZero()).To(BeTrue())

			// Simulate time passing
			injectabletime.Now = func() time.Time {
				return time.Now().Add(time.Duration(*provisioner.Spec.TTLSecondsUntilExpired) * time.Second)
			}

			kc, _ := versioned.NewForConfig(env.Config)
			all, _ := kc.KarpenterV1alpha5().Provisioners().List(ctx, metav1.ListOptions{})
			_ = all

			ExpectReconcileSucceeded(ctx, reconciler, n)
			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.DeletionTimestamp.IsZero()).To(BeFalse())
		})
	})

	Context("Initialization", func() {
		It("should not remove the readiness taint if not ready", func() {
			n := test.Node(test.NodeOptions{
				ReadyStatus: v1.ConditionUnknown,
				ObjectMeta:  metav1.ObjectMeta{Labels: map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name}},
				Taints: []v1.Taint{
					{Key: v1alpha5.NotReadyTaintKey, Effect: v1.TaintEffectNoSchedule},
					{Key: randomdata.SillyName(), Effect: v1.TaintEffectNoSchedule},
				},
			})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Spec.Taints).To(Equal(n.Spec.Taints))
		})
		It("should remove the readiness taint if ready", func() {
			n := test.Node(test.NodeOptions{
				ReadyStatus: v1.ConditionTrue,
				ObjectMeta:  metav1.ObjectMeta{Labels: map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name}},
				Taints: []v1.Taint{
					{Key: v1alpha5.NotReadyTaintKey, Effect: v1.TaintEffectNoSchedule},
					{Key: randomdata.SillyName(), Effect: v1.TaintEffectNoSchedule},
				},
			})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Spec.Taints).ToNot(Equal([]v1.Taint{n.Spec.Taints[1]}))
		})
		It("should do nothing if ready and the readiness taint does not exist", func() {
			n := test.Node(test.NodeOptions{
				ReadyStatus: v1.ConditionTrue,
				ObjectMeta:  metav1.ObjectMeta{Labels: map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name}},
				Taints:      []v1.Taint{{Key: randomdata.SillyName(), Effect: v1.TaintEffectNoSchedule}},
			})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Spec.Taints).To(Equal(n.Spec.Taints))
		})
		It("should do nothing if not owned by a provisioner", func() {
			n := test.Node(test.NodeOptions{
				ReadyStatus: v1.ConditionTrue,
				Taints: []v1.Taint{
					{Key: v1alpha5.NotReadyTaintKey, Effect: v1.TaintEffectNoSchedule},
					{Key: randomdata.SillyName(), Effect: v1.TaintEffectNoSchedule},
				},
			})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Spec.Taints).To(Equal(n.Spec.Taints))
		})
		It("should delete nodes if node not ready even after Initialization timeout ", func() {
			n := test.Node(test.NodeOptions{
				ObjectMeta: metav1.ObjectMeta{
					Finalizers: []string{v1alpha5.TerminationFinalizer},
					Labels:     map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
				},
				ReadyStatus: v1.ConditionUnknown,
				ReadyReason: "NodeStatusNeverUpdated",
				Taints: []v1.Taint{
					{Key: v1alpha5.NotReadyTaintKey, Effect: v1.TaintEffectNoSchedule},
				},
			})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)

			ExpectReconcileSucceeded(ctx, reconciler, n)

			// Expect node not be deleted
			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.DeletionTimestamp.IsZero()).To(BeTrue())

			// Simulate time passing and a n failing to join
			injectabletime.Now = func() time.Time { return time.Now().Add(node.InitializationTimeout) }
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.DeletionTimestamp.IsZero()).To(BeFalse())
		})
	})
	Describe("Emptiness", func() {
		It("should not TTL nodes that have ready status unknown", func() {
			provisioner.Spec.TTLSecondsAfterEmpty = ptr.Int64(30)
			node := test.Node(test.NodeOptions{
				ObjectMeta:  metav1.ObjectMeta{Labels: map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name}},
				ReadyStatus: v1.ConditionUnknown,
			})

			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, reconciler, node)

			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Annotations).ToNot(HaveKey(v1alpha5.EmptinessTimestampAnnotationKey))
		})
		It("should not TTL nodes that have ready status false", func() {
			provisioner.Spec.TTLSecondsAfterEmpty = ptr.Int64(30)
			node := test.Node(test.NodeOptions{
				ObjectMeta:  metav1.ObjectMeta{Labels: map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name}},
				ReadyStatus: v1.ConditionFalse,
			})

			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, reconciler, node)

			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Annotations).ToNot(HaveKey(v1alpha5.EmptinessTimestampAnnotationKey))
		})
		It("should label nodes as underutilized and add TTL", func() {
			provisioner.Spec.TTLSecondsAfterEmpty = ptr.Int64(30)
			node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
			}})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, node)
			ExpectReconcileSucceeded(ctx, reconciler, node)

			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Annotations).To(HaveKey(v1alpha5.EmptinessTimestampAnnotationKey))
		})
		It("should remove labels from non-empty nodes", func() {
			provisioner.Spec.TTLSecondsAfterEmpty = ptr.Int64(30)
			node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
				Annotations: map[string]string{
					v1alpha5.EmptinessTimestampAnnotationKey: time.Now().Add(100 * time.Second).Format(time.RFC3339),
				}},
			})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, node)
			ExpectCreatedWithStatus(ctx, env.Client, test.Pod(test.PodOptions{
				NodeName:   node.Name,
				Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
			}))
			ExpectReconcileSucceeded(ctx, reconciler, node)

			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.Annotations).ToNot(HaveKey(v1alpha5.EmptinessTimestampAnnotationKey))
		})
		It("should delete empty nodes past their TTL", func() {
			provisioner.Spec.TTLSecondsAfterEmpty = ptr.Int64(30)
			node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Finalizers: []string{v1alpha5.TerminationFinalizer},
				Labels:     map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
				Annotations: map[string]string{
					v1alpha5.EmptinessTimestampAnnotationKey: time.Now().Add(-100 * time.Second).Format(time.RFC3339),
				}},
			})
			ExpectCreated(ctx, env.Client, provisioner, node)
			ExpectReconcileSucceeded(ctx, reconciler, node)

			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.DeletionTimestamp.IsZero()).To(BeFalse())
		})
		It("should requeue reconcile if node is empty, but not past emptiness TTL", func() {
			provisioner.Spec.TTLSecondsAfterEmpty = ptr.Int64(30)
			now := time.Now()
			injectabletime.Now = func() time.Time { return now } // injectabletime.Now() is called multiple times in function being tested.
			emptinessTime := injectabletime.Now().Add(-10 * time.Second)
			// Emptiness timestamps are first formatted to a string friendly (time.RFC3339) (to put it in the node object)
			// and then eventually parsed back into time.Time when comparing ttls. Repeating that logic in the test.
			emptinessTimestamp, _ := time.Parse(time.RFC3339, emptinessTime.Format(time.RFC3339))
			expectedRequeueTime := emptinessTimestamp.Add(time.Duration(30 * time.Second)).Sub(injectabletime.Now()) // we should requeue in ~20 seconds.
			node := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Finalizers: []string{v1alpha5.TerminationFinalizer},
				Labels:     map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
				Annotations: map[string]string{
					v1alpha5.EmptinessTimestampAnnotationKey: emptinessTime.Format(time.RFC3339),
				}},
			})
			ExpectCreated(ctx, env.Client, provisioner, node)
			result := ExpectReconcileSucceeded(ctx, reconciler, node)
			Expect(result).To(Equal(controller.NewRequeueAfter(expectedRequeueTime)))
			node = ExpectNodeExists(ctx, env.Client, node.Name)
			Expect(node.DeletionTimestamp.IsZero()).To(BeTrue())
		})
	})
	Context("Finalizer", func() {
		It("should add the termination finalizer if missing", func() {
			n := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Labels:     map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
				Finalizers: []string{"fake.com/finalizer"},
			}})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Finalizers).To(ConsistOf(n.Finalizers[0], v1alpha5.TerminationFinalizer))
		})
		It("should do nothing if terminating", func() {
			n := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Labels:     map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
				Finalizers: []string{"fake.com/finalizer"},
			}})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			Expect(env.Client.Delete(ctx, n)).To(Succeed())
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Finalizers).To(Equal(n.Finalizers))
		})
		It("should do nothing if the termination finalizer already exists", func() {
			n := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Labels:     map[string]string{v1alpha5.ProvisionerNameLabelKey: provisioner.Name},
				Finalizers: []string{v1alpha5.TerminationFinalizer, "fake.com/finalizer"},
			}})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Finalizers).To(Equal(n.Finalizers))
		})
		It("should do nothing if the not owned by a provisioner", func() {
			n := test.Node(test.NodeOptions{ObjectMeta: metav1.ObjectMeta{
				Finalizers: []string{"fake.com/finalizer"},
			}})
			ExpectCreated(ctx, env.Client, provisioner)
			ExpectCreatedWithStatus(ctx, env.Client, n)
			ExpectReconcileSucceeded(ctx, reconciler, n)

			n = ExpectNodeExists(ctx, env.Client, n.Name)
			Expect(n.Finalizers).To(Equal(n.Finalizers))
		})
	})
})

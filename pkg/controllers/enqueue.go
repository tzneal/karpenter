package controllers

import (
	"context"

	"github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// EnqueueNodeForProvisioner is used to reconcile all nodes related to a provisioner when the Provisioner changes.
func EnqueueNodeForProvisioner(kubeClient kubernetes.Interface, enqueueKey func(key types.NamespacedName), logger *zap.SugaredLogger) func(interface{}) {
	return func(obj interface{}) {
		prov := obj.(*v1alpha5.Provisioner)
		selector := labels.SelectorFromValidatedSet(labels.Set{v1alpha5.ProvisionerNameLabelKey: prov.GetName()})
		nodeList, err := kubeClient.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{
			LabelSelector: selector.String(),
		})
		if err != nil {
			logger.Errorf("Failed to list nodes when mapping expiration watch events, %s", err)
			return
		}

		for _, node := range nodeList.Items {
			enqueueKey(types.NamespacedName{Name: node.Name})
		}
	}
}

// EnqueueNodeForPod reconciles the node when a pod assigned to it changes.
func EnqueueNodeForPod(enqueueKey func(key types.NamespacedName)) func(interface{}) {
	return func(obj interface{}) {
		pod := obj.(*v1.Pod)
		if pod.Spec.NodeName != "" {
			enqueueKey(types.NamespacedName{Name: pod.Spec.NodeName})
		}
	}
}

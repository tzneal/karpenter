package controllers

import (
	"fmt"

	apisprovisioningv1alpha5 "github.com/aws/karpenter/pkg/apis/provisioning/v1alpha5"
	"knative.dev/pkg/controller"
)

func FinalizerNamed(name string) func(impl *controller.Impl) controller.Options {
	return func(impl *controller.Impl) controller.Options {
		o := controller.Options{}
		o.FinalizerName = fmt.Sprintf("%s/%s", apisprovisioningv1alpha5.Group, name)
		return o
	}
}

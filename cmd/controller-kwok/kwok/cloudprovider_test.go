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
package kwok_test

import (
	"context"
	"testing"

	"github.com/aws/karpenter-core/pkg/operator/scheme"
	"github.com/aws/karpenter-core/pkg/test"
	"github.com/aws/karpenter/cmd/controller-kwok/kwok"
	"github.com/aws/karpenter/pkg/apis/settings"
)

func TestCloudProviderDescribeInstanceTypes(t *testing.T) {
	env := test.NewEnvironment(scheme.Scheme)
	cp := kwok.NewCloudProvider(env.KubernetesInterface)

	ctx := settings.ToContext(context.Background(), &settings.Settings{
		ClusterName:                "clustername",
		ClusterEndpoint:            "clusterendpoint",
		DefaultInstanceProfile:     "profilename",
		EnablePodENI:               false,
		EnableENILimitedPodDensity: false,
		IsolatedVPC:                false,
		VMMemoryOverheadPercent:    0,
		InterruptionQueueName:      "",
		Tags:                       nil,
		ReservedENIs:               0,
	})
	prov := test.Provisioner()
	instanceTypes, err := cp.GetInstanceTypes(ctx, prov)
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}
	if len(instanceTypes) == 0 {
		t.Errorf("expected > 0 instance types")
	}
}

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

package v1alpha1

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	. "knative.dev/pkg/logging/testing"
)

var ctx context.Context

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Node")
}

var _ = Describe("Provider Validation", func() {
	var provider *AWS
	BeforeEach(func() {
		provider = &AWS{
			AMIFamily:             aws.String("AL2"),
			InstanceProfile:       aws.String("my-profile"),
			SubnetSelector:        map[string]string{"my-tag": "my-value"},
			SecurityGroupSelector: map[string]string{"my-tag": "my-value"},
			LaunchTemplate:        LaunchTemplate{},
		}
	})
	It("should fail to validate if the KMS key is not the full ARN", func() {
		size := resource.MustParse("100Gi")
		provider.BlockDeviceMappings = append(provider.BlockDeviceMappings, &BlockDeviceMapping{
			DeviceName: aws.String("/dev/xvda"),
			EBS: &BlockDevice{
				DeleteOnTermination: aws.Bool(true),
				Encrypted:           aws.Bool(true),
				KMSKeyID:            aws.String("1234abcd-12ab-34cd-56ef-1234567890ab"),
				VolumeSize:          &size,
				VolumeType:          aws.String("gp3"),
			},
		})
		err := provider.Validate()
		Expect(err).ToNot(BeNil())
		Expect(err.Error()).To(Equal("invalid value: 1234abcd-12ab-34cd-56ef-1234567890ab: provider.blockDeviceMappings[0].ebs.kmsKeyID\nmust be the full ARN of the key"))
	})
	It("should validate if the KMS key is a full ARN", func() {
		size := resource.MustParse("100Gi")
		provider.BlockDeviceMappings = append(provider.BlockDeviceMappings, &BlockDeviceMapping{
			DeviceName: aws.String("/dev/xvda"),
			EBS: &BlockDevice{
				DeleteOnTermination: aws.Bool(true),
				Encrypted:           aws.Bool(true),
				KMSKeyID:            aws.String("arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab"),
				VolumeSize:          &size,
				VolumeType:          aws.String("gp3"),
			},
		})
		Expect(provider.Validate()).To(Succeed())
	})
})

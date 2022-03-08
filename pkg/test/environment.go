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

package test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Masterminds/semver"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/aws/karpenter/pkg/apis"
	"github.com/aws/karpenter/pkg/utils/project"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = apis.AddToScheme(scheme)
}

/*
Environment is for e2e local testing. It stands up an API Server, ETCD,
and a controller-runtime manager. It's possible to run multiple environments
simultaneously, as the ports are randomized. A common use case for this is
parallel tests using ginkgo's parallelization functionality. The environment is
typically instantiated once in a test file and re-used between different test
cases. Resources for each test should be isolated into its own namespace.
env := new Local(func(local *Local) {
	// Register test controller with manager
	controllerruntime.NewControllerManagedBy(local.Manager).For(...)
	return nil
})
BeforeSuite(func() { env.Start() })
AfterSuite(func() { env.Stop() })
*/
type Environment struct {
	envtest.Environment
	Client client.Client
	Ctx    context.Context
	K8sVer *semver.Version

	options []EnvironmentOption
	stop    context.CancelFunc
	cleanup *sync.WaitGroup
}

// EnvironmentOption passes the local environment to an option function. This is
// useful for registering controllers with the controller-runtime manager or for
// customizing Client, Scheme, or other variables.
type EnvironmentOption func(env *Environment)

func NewEnvironment(ctx context.Context, options ...EnvironmentOption) *Environment {
	ctx, stop := context.WithCancel(ctx)
	return &Environment{
		Environment: envtest.Environment{
			CRDDirectoryPaths: []string{project.RelativeToRoot("charts/karpenter/crds/karpenter.sh_provisioners.yaml")},
		},
		Ctx:     ctx,
		stop:    stop,
		options: options,
		cleanup: &sync.WaitGroup{},
	}
}

func (e *Environment) Start() (err error) {
	k8sVerStr := "1.21.x"
	if envVer := os.Getenv("K8S_VERSION"); envVer != "" {
		k8sVerStr = envVer
	}
	// turn it into a valid semver
	k8sVerStr = strings.Replace(k8sVerStr, ".x", ".0", -1)

	e.K8sVer = semver.MustParse(k8sVerStr)
	if e.K8sVer.Minor() >= 21 {
		// PodAffinityNamespaceSelector is used for label selectors in pod affinities.  If the feature-gate is turned off,
		// the api-server just clears out the label selector so we never see it.  If we turn it on, the label selectors
		// are passed to us and we handle them. This feature is alpha in v1.21, beta in v1.22 and will be GA in 1.24. See
		// https://github.com/kubernetes/enhancements/issues/2249 for more info.
		e.Environment.ControlPlane.GetAPIServer().Configure().Set("feature-gates", "PodAffinityNamespaceSelector=true")
	}

	// Environment
	if _, err = e.Environment.Start(); err != nil {
		return fmt.Errorf("starting environment, %w", err)
	}

	// Client
	e.Client, err = client.New(e.Config, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}

	// options
	for _, option := range e.options {
		option(e)
	}
	return nil
}

func (e *Environment) Stop() error {
	e.stop()
	e.cleanup.Wait()
	return e.Environment.Stop()
}

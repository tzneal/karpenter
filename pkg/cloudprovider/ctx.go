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

package cloudprovider

import (
	"context"
	"log"
)

type cpKey struct{}

// With associates a CloudProvider with the context
func With(ctx context.Context, cp CloudProvider) context.Context {
	return context.WithValue(ctx, cpKey{}, cp)
}

// GetConfig gets the CloudProvider from the context.
func GetOrDie(ctx context.Context) CloudProvider {
	value := ctx.Value(cpKey{})
	if value == nil {
		log.Fatal("CloudProvider was not found in context")
	}
	return value.(CloudProvider)
}

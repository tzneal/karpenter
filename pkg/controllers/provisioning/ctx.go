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

package provisioning

import (
	"context"
	"log"
)

type provKey struct{}

// With associates a Provisioners with the context
func With(ctx context.Context, cp *Provisioners) context.Context {
	return context.WithValue(ctx, provKey{}, cp)
}

// GetOrDie gets the Provisioners from the context.
func GetOrDie(ctx context.Context) *Provisioners {
	value := ctx.Value(provKey{})
	if value == nil {
		log.Fatal("Provisioners was not found in context")
	}
	return value.(*Provisioners)
}

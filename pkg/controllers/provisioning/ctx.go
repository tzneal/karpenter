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
)

type provKey struct{}

// WithProvisioners associates a Provisioners with the context
func WithProvisioners(ctx context.Context, cp *Provisioners) context.Context {
	return context.WithValue(ctx, provKey{}, cp)
}

// GetProvisioners gets the Provisioners from the context.
func GetProvisioners(ctx context.Context) *Provisioners {
	value := ctx.Value(provKey{})
	if value == nil {
		return nil
	}
	return value.(*Provisioners)
}

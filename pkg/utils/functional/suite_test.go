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

package functional

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFunctional(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Functional Suite")
}

var _ = Describe("Functional", func() {
	Context("UnionStringMaps", func() {
		empty := map[string]string{}
		original := map[string]string{
			"a": "b",
			"c": "d",
		}
		overwriter := map[string]string{
			"a": "y",
			"c": "z",
		}
		disjoiner := map[string]string{
			"d": "y",
			"e": "z",
		}
		uberwriter := map[string]string{
			"d": "q",
			"e": "z",
		}

		Specify("no args returns empty", func() {
			Expect(UnionStringMaps()).To(BeEmpty())
		})

		Specify("multiple empty returns empty", func() {
			Expect(UnionStringMaps(empty, empty, empty, empty)).To(BeEmpty())
		})

		Specify("one arg returns the arg", func() {
			Expect(UnionStringMaps(original)).To(Equal(original))
		})

		Specify("2nd arg overrides 1st", func() {
			Expect(UnionStringMaps(original, overwriter)).To(Equal(overwriter))
		})

		Specify("returns union when disjoint", func() {
			expected := map[string]string{
				"a": "b",
				"c": "d",
				"d": "y",
				"e": "z",
			}
			Expect(UnionStringMaps(original, disjoiner)).To(Equal(expected))
		})

		Specify("final arg takes precedence", func() {
			expected := map[string]string{
				"a": "b",
				"c": "d",
				"d": "q",
				"e": "z",
			}
			Expect(UnionStringMaps(original, disjoiner, empty, uberwriter)).To(Equal(expected))
		})
	})
	Context("Intersect", func() {
		Specify("intersect", func() {
			Expect(Intersect([]string{"a", "b", "c"}, []string{"b"})).To(Equal([]string{"b"}))
		})
		Specify("disjoint", func() {
			Expect(Intersect([]string{"a", "b", "c"}, []string{"f", "g", "h"})).To(HaveLen(0))
		})
	})
})

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

package scheduling

import "sigs.k8s.io/controller-runtime/pkg/client"

// TopoGraph is used to construct a topological sorting of K8s objects.  The resulting list
// includes all items in the graph with any cycles sorted last.
type TopoGraph struct {
	edges map[client.ObjectKey][]client.ObjectKey
}

func (g *TopoGraph) AddNode(key client.ObjectKey) {
	if g.edges == nil {
		g.edges = map[client.ObjectKey][]client.ObjectKey{}
	}
	if _, ok := g.edges[key]; !ok {
		g.edges[key] = nil
	}

}

func (g *TopoGraph) AddEdge(from client.ObjectKey, to client.ObjectKey) {
	if g.edges == nil {
		g.edges = map[client.ObjectKey][]client.ObjectKey{}
	}
	g.AddNode(from)
	g.AddNode(to)
	g.edges[from] = append(g.edges[from], to)
}

// Sorted returns a
func (g *TopoGraph) Sorted() []client.ObjectKey {
	inDegress := map[client.ObjectKey]int{}
	for _, toList := range g.edges {
		for _, to := range toList {
			inDegress[to]++
		}
	}
	var noIncoming []client.ObjectKey
	for v := range g.edges {
		if inDegress[v] == 0 {
			noIncoming = append(noIncoming, v)
		}
	}
	var sorted []client.ObjectKey
	for len(noIncoming) > 0 {
		node := noIncoming[len(noIncoming)-1]
		noIncoming = noIncoming[0 : len(noIncoming)-1]
		sorted = append(sorted, node)

		for _, to := range g.edges[node] {
			inDegress[to]--
			if inDegress[to] == 0 {
				noIncoming = append(noIncoming, to)
				delete(inDegress, to)
			}
		}
	}

	// add any remaining cycles
	for k, v := range inDegress {
		if v != 0 {
			sorted = append(sorted, k)
		}
	}
	return sorted
}

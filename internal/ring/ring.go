// Package ring maps tenants onto nodes with a consistent hash.
//
// What the ring is FOR, in this system, is worth stating plainly because it is
// easy to assume it is doing more than it is: it decides which node
// coordinates a tenant's checks. It does NOT decide whether a request is
// admitted. Counters live in Redis and every admission is backed by an atomic
// script there, so two nodes briefly disagreeing about who owns a tenant
// cannot over-admit -- the worst it can cost is an extra hop. That is a
// deliberate design property, and it is what makes a rebalance a latency event
// rather than a correctness one.
//
// The mapping has to be identical in every process, so everything here is
// derived from the node ids and the virtual-node count alone: the same inputs
// give the same ring whatever order they arrive in, on any machine.
package ring

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"
)

// DefaultVNodes is how many points on the ring each node occupies.
//
// Virtual nodes are what stop three nodes from splitting the keyspace into
// three arbitrary arcs: with one point each, the largest share is routinely
// double the smallest. The standard deviation of the share falls as
// 1/sqrt(vnodes), so 256 gives roughly 6%, which is close enough to even that
// no tenant distribution needs thinking about. The cost is a 256*N entry
// sorted array and a binary search per lookup.
const DefaultVNodes = 256

// MaxVNodes bounds the ring so a config typo cannot allocate a huge array.
const MaxVNodes = 4096

// Node is a ring member.
type Node struct {
	// ID is stable across restarts and is what the ring hashes. An address can
	// change -- a pod is rescheduled, a port is remapped -- without moving a
	// single tenant, which is the entire reason ownership is keyed by id.
	ID string `json:"id"`
	// Addr is where peers reach this node's gRPC service.
	Addr string `json:"addr"`
}

// Ring maps a key to the node that owns it.
//
// It is immutable once built. A membership change produces a new Ring that is
// swapped in whole, so a lookup never sees a half-updated ring and no lock is
// needed on the request path.
type Ring struct {
	vnodes int
	nodes  []Node
	points []point
}

type point struct {
	hash uint64
	node int // index into nodes
}

// New builds a ring. Nodes may arrive in any order; the result does not depend
// on it.
func New(nodes []Node, vnodes int) (*Ring, error) {
	if len(nodes) == 0 {
		return nil, errors.New("ring: no nodes")
	}
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}
	if vnodes > MaxVNodes {
		return nil, fmt.Errorf("ring: %d virtual nodes exceeds the maximum of %d", vnodes, MaxVNodes)
	}

	sorted := make([]Node, len(nodes))
	copy(sorted, nodes)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	seen := make(map[string]bool, len(sorted))
	for _, n := range sorted {
		if n.ID == "" {
			return nil, errors.New("ring: a node has an empty id")
		}
		if seen[n.ID] {
			return nil, fmt.Errorf("ring: two nodes share the id %q", n.ID)
		}
		seen[n.ID] = true
	}

	r := &Ring{vnodes: vnodes, nodes: sorted, points: make([]point, 0, len(sorted)*vnodes)}
	for i, n := range sorted {
		for v := 0; v < vnodes; v++ {
			r.points = append(r.points, point{hash: vnodeHash(n.ID, v), node: i})
		}
	}
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		// A hash collision must not make ownership depend on sort stability,
		// which is not guaranteed. Break the tie on the node id instead, which
		// every process agrees about.
		return r.nodes[r.points[i].node].ID < r.nodes[r.points[j].node].ID
	})
	return r, nil
}

// vnodeHash places one virtual node on the ring.
//
// The separator matters: without it, node "a" vnode 11 and node "a1" vnode 1
// would hash the same string and land on the same point.
func vnodeHash(id string, v int) uint64 {
	return xxhash.Sum64String(id + "\x00vnode\x00" + strconv.Itoa(v))
}

// KeyHash is where a key sits on the ring.
//
// Exported because the hash input is a compatibility surface: every node must
// agree, and the project's own claim that "a tenant always lands on the same
// shard" depends on this taking the tenant alone. Hashing (tenant, limit)
// would spread load more evenly and would quietly give a tenant with three
// limits three owners, which is a different system.
func KeyHash(key string) uint64 { return xxhash.Sum64String(key) }

// Owner returns the node that owns a key: the first virtual node at or after
// the key's position, wrapping around the end.
func (r *Ring) Owner(key string) Node {
	idx := r.search(KeyHash(key))
	return r.nodes[r.points[idx].node]
}

func (r *Ring) search(h uint64) int {
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		return 0 // past the last point: wrap to the first
	}
	return i
}

// Successors returns up to n distinct nodes starting at the key's owner,
// walking the ring.
//
// This is the replication order, and this project uses it for exactly one
// thing: deciding who takes over a tenant when its owner leaves. It is NOT a
// replication factor for counter state -- counters live in Redis, and their
// durability is Redis's business. Saying that here because "replication
// factor" in a consistent-hash ring usually means the other thing.
func (r *Ring) Successors(key string, n int) []Node {
	if n <= 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	out := make([]Node, 0, n)
	seen := make(map[string]bool, n)

	start := r.search(KeyHash(key))
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		p := r.points[(start+i)%len(r.points)]
		node := r.nodes[p.node]
		if seen[node.ID] {
			continue
		}
		seen[node.ID] = true
		out = append(out, node)
	}
	return out
}

// Nodes returns the members, in id order.
func (r *Ring) Nodes() []Node {
	out := make([]Node, len(r.nodes))
	copy(out, r.nodes)
	return out
}

// Len is the number of members.
func (r *Ring) Len() int { return len(r.nodes) }

// VNodes is how many points each member occupies.
func (r *Ring) VNodes() int { return r.vnodes }

// Has reports whether a node id is a member.
func (r *Ring) Has(id string) bool {
	for _, n := range r.nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

// Node returns a member by id.
func (r *Ring) Node(id string) (Node, bool) {
	for _, n := range r.nodes {
		if n.ID == id {
			return n, true
		}
	}
	return Node{}, false
}

// Distribution reports how many of the ring's points each node holds, which is
// what a balance test measures and what an operator looks at when one node is
// hotter than the others.
func (r *Ring) Distribution() map[string]int {
	out := make(map[string]int, len(r.nodes))
	for _, p := range r.points {
		out[r.nodes[p.node].ID]++
	}
	return out
}

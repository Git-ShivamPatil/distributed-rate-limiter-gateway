// Package cluster holds this node's view of who is in the ring.
//
// In this milestone the membership is static, read from the configuration
// file. Milestone 6 replaces the source with a Raft-committed one; what does
// not change is the shape -- the request path asks "who owns this tenant, and
// is that me", and gets an answer from an immutable snapshot that is swapped
// whole.
//
// Reads are lock-free. Ownership is consulted on every request, and a mutex
// there would serialise the whole gateway behind a value that changes a few
// times a day.
package cluster

import (
	"fmt"
	"sync/atomic"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

// View is this node's current picture of the cluster.
type View struct {
	self    string
	current atomic.Pointer[snapshot]
}

type snapshot struct {
	ring  *ring.Ring
	epoch uint64
}

// New builds a view with an initial membership.
//
// The node's own id must be a member: a gateway that is not in its own ring
// would forward every request away, including the ones it should answer.
func New(self string, members []ring.Node, vnodes int) (*View, error) {
	if self == "" {
		return nil, fmt.Errorf("cluster: this node has no id")
	}
	r, err := ring.New(members, vnodes)
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}
	if !r.Has(self) {
		return nil, fmt.Errorf("cluster: this node (%q) is not in its own membership list", self)
	}
	v := &View{self: self}
	v.current.Store(&snapshot{ring: r, epoch: 1})
	return v, nil
}

// Self is this node's id.
func (v *View) Self() string { return v.self }

// Epoch increases every time the membership changes. It is what a fencing
// check compares, and what a reader uses to tell two views apart.
func (v *View) Epoch() uint64 { return v.current.Load().epoch }

// Ring returns the current ring. It is immutable; a membership change produces
// a new one rather than editing this.
func (v *View) Ring() *ring.Ring { return v.current.Load().ring }

// Owner reports which node coordinates a tenant, and whether that is this one.
func (v *View) Owner(tenant string) (node ring.Node, isSelf bool, ok bool) {
	r := v.current.Load().ring
	if r.Len() == 0 {
		return ring.Node{}, false, false
	}
	n := r.Owner(tenant)
	return n, n.ID == v.self, true
}

// Replace swaps in a new membership and returns the new epoch.
//
// Milestone 6 calls this when Raft commits a configuration change. The whole
// snapshot is replaced at once, so a request in flight sees either the old
// ring or the new one and never a half-built mixture.
func (v *View) Replace(members []ring.Node, vnodes int) (uint64, error) {
	r, err := ring.New(members, vnodes)
	if err != nil {
		return 0, fmt.Errorf("cluster: %w", err)
	}
	if !r.Has(v.self) {
		return 0, fmt.Errorf("cluster: the new membership does not include this node (%q)", v.self)
	}
	for {
		old := v.current.Load()
		next := &snapshot{ring: r, epoch: old.epoch + 1}
		if v.current.CompareAndSwap(old, next) {
			return next.epoch, nil
		}
	}
}

// Members lists the current membership, in id order.
func (v *View) Members() []ring.Node { return v.current.Load().ring.Nodes() }

// Describe is what the cluster endpoint and the dashboard render.
type Describe struct {
	Self    string        `json:"self"`
	Epoch   uint64        `json:"epoch"`
	VNodes  int           `json:"vnodes"`
	Members []MemberState `json:"members"`
}

// MemberState is one member, as seen from here.
type MemberState struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	Self  bool   `json:"self"`
	Share int    `json:"ring_points"`
}

// Describe reports the view, including each member's share of the ring, which
// is what somebody looks at when one node is busier than the others.
func (v *View) Describe() Describe {
	snap := v.current.Load()
	dist := snap.ring.Distribution()
	out := Describe{Self: v.self, Epoch: snap.epoch, VNodes: snap.ring.VNodes()}
	for _, n := range snap.ring.Nodes() {
		out.Members = append(out.Members, MemberState{
			ID:    n.ID,
			Addr:  n.Addr,
			Self:  n.ID == v.self,
			Share: dist[n.ID],
		})
	}
	return out
}

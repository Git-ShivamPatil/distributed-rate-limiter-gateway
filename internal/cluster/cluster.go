// Package cluster holds this node's view of who is in the ring.
//
// The view is derived from a State -- the same State the replicated log in
// state.go commits -- so there is one shape for "who are the shards" whether
// it came from a configuration file or from a Raft entry. What the request
// path asks is unchanged either way: who owns this tenant, and is that me.
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

// snapshot pairs a committed state with the ring derived from it. The two are
// stored together and swapped together, so a reader can never see a ring built
// from one membership next to the epoch of another.
type snapshot struct {
	state State
	ring  *ring.Ring
}

// New builds a view with an initial membership.
//
// The members are applied as entries, so the same rules that govern a
// committed membership govern a configured one: no duplicate ids, no two
// nodes on one address, and an epoch that counts how many times a tenant
// could have moved.
//
// The node's own id must be a member: a gateway that is not in its own ring
// would forward every request away, including the ones it should answer.
func New(self string, members []ring.Node, vnodes int) (*View, error) {
	if self == "" {
		return nil, fmt.Errorf("cluster: this node has no id")
	}
	state := State{VNodes: vnodes}
	for _, m := range members {
		next, _, err := state.Apply(Command{Kind: AddShard, Member: m})
		if err != nil {
			return nil, fmt.Errorf("cluster: %w", err)
		}
		state = next
	}
	v := &View{self: self}
	if err := v.Adopt(state); err != nil {
		return nil, err
	}
	return v, nil
}

// Self is this node's id.
func (v *View) Self() string { return v.self }

// Epoch is the ring's fencing token: it advances when a tenant could have
// moved between shards, and not when anything else changes.
func (v *View) Epoch() uint64 { return v.current.Load().state.RingEpoch }

// PolicyGen is the highest policy generation this node knows the cluster has
// minted.
func (v *View) PolicyGen() uint64 { return v.current.Load().state.PolicyGen }

// StoreGen names the lifetime of the counter store the cluster agreed on.
func (v *View) StoreGen() uint64 { return v.current.Load().state.StoreGen }

// State returns the committed state this view was built from.
func (v *View) State() State { return v.current.Load().state.clone() }

// Ring returns the current ring. It is immutable; a membership change produces
// a new one rather than editing this.
func (v *View) Ring() *ring.Ring { return v.current.Load().ring }

// Owner reports which node coordinates a tenant, and whether that is this one.
func (v *View) Owner(tenant string) (node ring.Node, isSelf bool, ok bool) {
	r := v.current.Load().ring
	if r == nil || r.Len() == 0 {
		return ring.Node{}, false, false
	}
	n := r.Owner(tenant)
	return n, n.ID == v.self, true
}

// Adopt installs a committed state, replacing the ring in one swap.
//
// This is what the state machine calls when an entry changes the membership.
// The whole snapshot is replaced at once, so a request in flight sees either
// the old ring or the new one and never a half-built mixture.
//
// A state that does not include this node is REFUSED rather than adopted. The
// alternative is a node that has been removed from the cluster continuing to
// serve while forwarding every request to somebody else; refusing leaves it
// answering on its last known ring, which a draining node should do until it
// is stopped.
func (v *View) Adopt(s State) error {
	if !s.Has(v.self) {
		return fmt.Errorf("cluster: the membership does not include this node (%q), so it would forward every request away, including its own tenants",
			v.self)
	}
	r, err := s.Ring()
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	v.current.Store(&snapshot{state: s, ring: r})
	return nil
}

// Replace swaps in a new membership and returns the new epoch.
//
// It is the configuration-file path: the members are turned into the entries
// that would have produced them, so a ring built from a file and a ring built
// from the log are the same object reached two ways.
func (v *View) Replace(members []ring.Node, vnodes int) (uint64, error) {
	next := v.current.Load().state

	// Departures first. A member that left and a new one that took its
	// address would otherwise collide on the way in.
	keep := make(map[string]bool, len(members))
	for _, m := range members {
		keep[m.ID] = true
	}
	for _, m := range next.Members {
		if !keep[m.ID] {
			gone, _, err := next.Apply(Command{Kind: RemoveShard, ID: m.ID})
			if err != nil {
				return 0, fmt.Errorf("cluster: %w", err)
			}
			next = gone
		}
	}
	for _, m := range members {
		added, _, err := next.Apply(Command{Kind: AddShard, Member: m})
		if err != nil {
			return 0, fmt.Errorf("cluster: %w", err)
		}
		next = added
	}
	if vnodes != next.VNodes {
		set, _, err := next.Apply(Command{Kind: SetVNodes, VNodes: vnodes})
		if err != nil {
			return 0, fmt.Errorf("cluster: %w", err)
		}
		next = set
	}

	if err := v.Adopt(next); err != nil {
		return 0, err
	}
	return next.RingEpoch, nil
}

// Members lists the current membership, in id order.
func (v *View) Members() []ring.Node {
	r := v.current.Load().ring
	if r == nil {
		return nil
	}
	return r.Nodes()
}

// Describe is what the cluster endpoint and the dashboard render.
type Describe struct {
	Self   string `json:"self"`
	Epoch  uint64 `json:"epoch"`
	VNodes int    `json:"vnodes"`
	// PolicyGen and StoreGen are reported because when a node is refusing a
	// tenant's traffic at a fence, the first question is which generation it
	// is carrying and which one the cluster agreed on.
	PolicyGen uint64        `json:"policy_gen"`
	StoreGen  uint64        `json:"store_gen"`
	Members   []MemberState `json:"members"`
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
	out := Describe{
		Self:      v.self,
		Epoch:     snap.state.RingEpoch,
		PolicyGen: snap.state.PolicyGen,
		StoreGen:  snap.state.StoreGen,
	}
	if snap.ring == nil {
		return out
	}
	dist := snap.ring.Distribution()
	out.VNodes = snap.ring.VNodes()
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

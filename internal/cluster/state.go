package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

// State is everything the cluster agrees about: who the shards are, how the
// ring is shaped, and three generations.
//
// What is NOT in here is the point. There are no counters, no quotas and no
// per-request anything. Counters live in Redis behind an atomic script, and
// putting them through a consensus log instead would cost one to three
// fsync-bounded round trips per check -- not a latency regression, an
// impossibility. Entries land here per deploy and per policy edit, and a
// cluster that is serving traffic normally commits none at all.
//
// A State is immutable. Apply returns the next one rather than editing this
// one, which is what lets a reader hold a snapshot with no lock while the
// membership changes underneath.
type State struct {
	// Members are the shards, always sorted by id. The sort is not tidiness:
	// two nodes that applied the same entries must serialise to the same
	// bytes, or a snapshot taken on one and restored on another produces a
	// different ring and the two disagree about who owns a tenant.
	Members []ring.Node `json:"members"`
	// VNodes is how many points each shard occupies. Zero means the ring
	// package's default.
	VNodes int `json:"vnodes"`
	// RingEpoch is the fencing token. It advances only when a tenant could
	// have moved -- see the rule on Apply.
	RingEpoch uint64 `json:"ring_epoch"`
	// PolicyGen is a totally ordered counter, minted here and bound to a
	// tenant by whoever writes the policy. Redis refuses a check carrying a
	// generation below the one bound to that tenant, which is what makes a
	// widening un-appliable while some gateway still holds the old limit.
	PolicyGen uint64 `json:"policy_gen"`
	// StoreGen names one lifetime of the counter store. It is minted from
	// Redis and committed here, so that a store that was wiped and came back
	// empty is a detected, counted event rather than a silent full-quota
	// amnesty -- and so that the fence still orders owners correctly
	// afterwards, which a generation held inside the wiped store would not.
	StoreGen uint64 `json:"store_gen"`
}

// CommandKind names the entries the log carries. The string values are the
// wire format of a committed entry, so they do not change.
type CommandKind string

const (
	// SeedRing installs the initial membership and ring configuration in one
	// entry, and only into a state that has none.
	SeedRing CommandKind = "seed_ring"
	// AddShard adds a member, or updates an existing member's address.
	AddShard CommandKind = "add_shard"
	// RemoveShard drops a member.
	RemoveShard CommandKind = "remove_shard"
	// SetVNodes changes how many ring points each member holds.
	SetVNodes CommandKind = "set_vnodes"
	// BumpPolicyGen mints the next policy generation.
	BumpPolicyGen CommandKind = "bump_policy_gen"
	// SetStoreGen records which lifetime of the counter store is in use.
	SetStoreGen CommandKind = "set_store_gen"
)

// Command is one entry of the log.
//
// One struct rather than one type per kind, because every node has to decode
// bytes written by a node that may be running a different build: a decoder
// that switches on a field tolerates an unknown kind by refusing it, where a
// union of concrete types would have to guess.
type Command struct {
	Kind CommandKind `json:"kind"`
	// Members is the whole initial membership, for SeedRing.
	Members []ring.Node `json:"members,omitempty"`
	// Member is the shard being added, for AddShard.
	Member ring.Node `json:"member,omitempty"`
	// ID is the shard being removed, for RemoveShard.
	ID string `json:"id,omitempty"`
	// VNodes is the new ring point count, for SetVNodes.
	VNodes int `json:"vnodes,omitempty"`
	// Gen is the store generation, for SetStoreGen.
	Gen uint64 `json:"gen,omitempty"`
}

// ErrNotApplicable means the entry was decoded and understood, and the state
// it asks for is one this state machine will not move to.
//
// It is returned rather than panicked because every node reaches the same
// verdict from the same bytes: a rejection is as deterministic as an
// acceptance, so the log stays consistent and only the caller is told no.
var ErrNotApplicable = errors.New("cluster: entry does not apply")

// Apply returns the state after one entry, and whether anything changed.
//
// It is a pure function of (state, entry). Nothing here reads a clock, a
// random source, a file or the network, because two nodes applying the same
// entry to the same state must reach the same state -- a state machine that
// consults anything else diverges silently and is only found much later, when
// two nodes disagree about who owns a tenant.
//
// THE EPOCH RULE. RingEpoch advances when, and only when, the SET OF MEMBER
// IDS changes or the vnode count changes. Those are the two things that move a
// tenant from one shard to another. An address change does not advance it: the
// ring hashes ids, so a member that moved host owns exactly the tenants it
// owned before, and advancing a fencing token for a change that fences nothing
// is how a rolling deploy becomes a takeover storm. Re-adding a member on the
// address it already has changes nothing at all.
func (s State) Apply(c Command) (State, bool, error) {
	next := s.clone()

	switch c.Kind {
	case SeedRing:
		// The initial configuration is ONE entry, not a sequence of them.
		// Seeding a member at a time would have every node build a ring out of
		// however much of the membership had arrived -- a one-member ring, then
		// a two-member ring -- and each of those is a real ring that really
		// routes, so a tenant would move twice before the cluster had even
		// finished starting.
		if len(next.Members) > 0 {
			return s, false, nil // already configured; the log is the truth now
		}
		if len(c.Members) == 0 {
			return s, false, fmt.Errorf("%w: seed_ring names no members", ErrNotApplicable)
		}
		if c.VNodes < 0 || c.VNodes > ring.MaxVNodes {
			return s, false, fmt.Errorf("%w: vnodes %d is outside 0..%d", ErrNotApplicable, c.VNodes, ring.MaxVNodes)
		}
		// A seed is a whole configuration, not an edit, so an id named twice is
		// a mistake in the file rather than an address change. Applying it
		// incrementally would silently keep whichever address came last.
		seen := make(map[string]bool, len(c.Members))
		for _, m := range c.Members {
			if seen[m.ID] {
				return s, false, fmt.Errorf("%w: seed_ring names %q twice", ErrNotApplicable, m.ID)
			}
			seen[m.ID] = true
		}
		seeded := State{VNodes: c.VNodes, RingEpoch: next.RingEpoch, PolicyGen: next.PolicyGen, StoreGen: next.StoreGen}
		for _, m := range c.Members {
			added, _, err := seeded.Apply(Command{Kind: AddShard, Member: m})
			if err != nil {
				return s, false, err
			}
			seeded = added
		}
		// One configuration, one epoch: the members arrived together, so they
		// count as a single change.
		seeded.RingEpoch = next.RingEpoch + 1
		return seeded, true, nil

	case AddShard:
		if c.Member.ID == "" {
			return s, false, fmt.Errorf("%w: add_shard names no id", ErrNotApplicable)
		}
		if c.Member.Addr == "" {
			return s, false, fmt.Errorf("%w: add_shard for %q names no address, so nothing could forward to it",
				ErrNotApplicable, c.Member.ID)
		}
		// Two ids on one address is one process answering as two shards, which
		// makes ownership meaningless. The check has to come BEFORE the
		// address-change branch below as well as before the append: moving an
		// existing member onto an address somebody else holds is the same
		// collision arrived at from the other direction.
		for _, m := range next.Members {
			if m.ID != c.Member.ID && m.Addr == c.Member.Addr {
				return s, false, fmt.Errorf("%w: %q already answers on %s", ErrNotApplicable, m.ID, c.Member.Addr)
			}
		}
		for i, m := range next.Members {
			if m.ID != c.Member.ID {
				continue
			}
			if m.Addr == c.Member.Addr {
				return s, false, nil // already a member, at that address
			}
			// An address change: the member keeps its tenants, so the epoch
			// stays where it is.
			next.Members[i].Addr = c.Member.Addr
			return next, true, nil
		}
		next.Members = append(next.Members, c.Member)
		next.sortMembers()
		next.RingEpoch++
		return next, true, nil

	case RemoveShard:
		if c.ID == "" {
			return s, false, fmt.Errorf("%w: remove_shard names no id", ErrNotApplicable)
		}
		for i, m := range next.Members {
			if m.ID != c.ID {
				continue
			}
			next.Members = append(next.Members[:i], next.Members[i+1:]...)
			next.RingEpoch++
			return next, true, nil
		}
		return s, false, nil // not a member; removing it again is a no-op

	case SetVNodes:
		if c.VNodes < 0 {
			return s, false, fmt.Errorf("%w: vnodes %d is negative", ErrNotApplicable, c.VNodes)
		}
		if c.VNodes > ring.MaxVNodes {
			return s, false, fmt.Errorf("%w: vnodes %d exceeds the maximum of %d",
				ErrNotApplicable, c.VNodes, ring.MaxVNodes)
		}
		if c.VNodes == next.VNodes {
			return s, false, nil
		}
		next.VNodes = c.VNodes
		next.RingEpoch++ // every tenant may move
		return next, true, nil

	case BumpPolicyGen:
		next.PolicyGen++
		return next, true, nil

	case SetStoreGen:
		if c.Gen == 0 {
			return s, false, fmt.Errorf("%w: store generation 0 is not a generation", ErrNotApplicable)
		}
		if c.Gen == next.StoreGen {
			return s, false, nil
		}
		if c.Gen < next.StoreGen {
			// Monotone, deliberately. A generation that went backwards is a
			// zombie writer or a replayed entry, and accepting it would let a
			// stale node's checks start passing a fence they had already
			// failed.
			return s, false, fmt.Errorf("%w: store generation %d is below the committed %d",
				ErrNotApplicable, c.Gen, next.StoreGen)
		}
		next.StoreGen = c.Gen
		return next, true, nil

	default:
		return s, false, fmt.Errorf("%w: unknown kind %q", ErrNotApplicable, c.Kind)
	}
}

func (s State) clone() State {
	out := s
	out.Members = make([]ring.Node, len(s.Members))
	copy(out.Members, s.Members)
	return out
}

func (s *State) sortMembers() {
	sort.Slice(s.Members, func(i, j int) bool { return s.Members[i].ID < s.Members[j].ID })
}

// Has reports whether an id is a member.
func (s State) Has(id string) bool {
	for _, m := range s.Members {
		if m.ID == id {
			return true
		}
	}
	return false
}

// Ring builds the ring this state describes.
//
// It is derived rather than stored, so there is exactly one way a member list
// becomes a ring and every node runs it. A state with no members has no ring,
// which is a single node answering for every tenant itself rather than an
// error.
func (s State) Ring() (*ring.Ring, error) {
	if len(s.Members) == 0 {
		return nil, nil
	}
	return ring.New(s.Members, s.VNodes)
}

// Encode serialises the state.
//
// The encoding is canonical: members are sorted by id and the struct's fields
// are written in declaration order, so two nodes that applied the same entries
// produce identical bytes. That is what makes "did these two nodes agree"
// answerable by comparing snapshots instead of by reasoning about them.
func (s State) Encode() ([]byte, error) {
	s.sortMembers()
	if s.Members == nil {
		s.Members = []ring.Node{}
	}
	return json.Marshal(s)
}

// DecodeState reads a snapshot.
func DecodeState(b []byte) (State, error) {
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, fmt.Errorf("cluster: decoding state: %w", err)
	}
	s.sortMembers()
	return s, nil
}

// Encode serialises one entry.
func (c Command) Encode() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("cluster: encoding %s: %w", c.Kind, err)
	}
	return b, nil
}

// DecodeCommand reads one entry.
func DecodeCommand(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("cluster: decoding entry: %w", err)
	}
	if c.Kind == "" {
		return Command{}, errors.New("cluster: entry has no kind")
	}
	return c, nil
}

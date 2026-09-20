package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

func node(id, addr string) ring.Node { return ring.Node{ID: id, Addr: addr} }

func add(id, addr string) Command { return Command{Kind: AddShard, Member: node(id, addr)} }

// applyAll runs a sequence and fails on the first entry the state machine
// refuses, so a test that meant to build a state cannot quietly build a
// different one.
func applyAll(t *testing.T, s State, cmds ...Command) State {
	t.Helper()
	for i, c := range cmds {
		next, _, err := s.Apply(c)
		if err != nil {
			t.Fatalf("entry %d (%s): %v", i, c.Kind, err)
		}
		s = next
	}
	return s
}

// The epoch is a fencing token, so what advances it is the whole question.
// Adding a shard moves tenants; re-adding one that is already there moves
// nothing.
func TestTheEpochAdvancesOnlyWhenATenantCouldHaveMoved(t *testing.T) {
	s := applyAll(t, State{}, add("gateway-1", "127.0.0.1:9091"), add("gateway-2", "127.0.0.1:9092"))
	if s.RingEpoch != 2 {
		t.Fatalf("two members added, epoch = %d, want 2", s.RingEpoch)
	}

	// Already a member, on the address it already has.
	next, changed, err := s.Apply(add("gateway-2", "127.0.0.1:9092"))
	if err != nil {
		t.Fatal(err)
	}
	if changed || next.RingEpoch != 2 {
		t.Fatalf("re-adding an unchanged member: changed=%v epoch=%d", changed, next.RingEpoch)
	}

	// A member that moved host keeps every tenant it had, because the ring
	// hashes ids. Fencing that would be fencing nothing.
	next, changed, err = s.Apply(add("gateway-2", "127.0.0.1:9192"))
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("an address change was recorded as no change")
	}
	if next.RingEpoch != 2 {
		t.Fatalf("an address change advanced the epoch to %d", next.RingEpoch)
	}
	if before, after := s.mustRing(t).Owner("acme").ID, next.mustRing(t).Owner("acme").ID; before != after {
		t.Fatalf("an address change moved acme from %s to %s", before, after)
	}
}

func (s State) mustRing(t *testing.T) *ring.Ring {
	t.Helper()
	r, err := s.Ring()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRemovingAShardAdvancesTheEpochAndRemovingItAgainDoesNot(t *testing.T) {
	s := applyAll(t, State{},
		add("gateway-1", "127.0.0.1:9091"),
		add("gateway-2", "127.0.0.1:9092"),
		add("gateway-3", "127.0.0.1:9093"),
	)

	next, changed, err := s.Apply(Command{Kind: RemoveShard, ID: "gateway-2"})
	if err != nil {
		t.Fatal(err)
	}
	if !changed || next.RingEpoch != 4 || len(next.Members) != 2 {
		t.Fatalf("after removing one: changed=%v epoch=%d members=%d", changed, next.RingEpoch, len(next.Members))
	}
	if next.Has("gateway-2") {
		t.Fatal("the removed member is still in the state")
	}

	// A leader that commits the same removal twice must not walk the epoch up
	// for a change that already happened.
	again, changed, err := next.Apply(Command{Kind: RemoveShard, ID: "gateway-2"})
	if err != nil {
		t.Fatal(err)
	}
	if changed || again.RingEpoch != next.RingEpoch {
		t.Fatalf("removing a non-member: changed=%v epoch=%d", changed, again.RingEpoch)
	}
}

func TestChangingTheVnodeCountAdvancesTheEpochBecauseEveryTenantMayMove(t *testing.T) {
	s := applyAll(t, State{}, add("gateway-1", "127.0.0.1:9091"), add("gateway-2", "127.0.0.1:9092"))

	next, changed, err := s.Apply(Command{Kind: SetVNodes, VNodes: 128})
	if err != nil {
		t.Fatal(err)
	}
	if !changed || next.RingEpoch != 3 || next.VNodes != 128 {
		t.Fatalf("set_vnodes: changed=%v epoch=%d vnodes=%d", changed, next.RingEpoch, next.VNodes)
	}
	if _, changed, _ := next.Apply(Command{Kind: SetVNodes, VNodes: 128}); changed {
		t.Fatal("setting the vnode count to what it already is was recorded as a change")
	}
	if _, _, err := next.Apply(Command{Kind: SetVNodes, VNodes: ring.MaxVNodes + 1}); err == nil {
		t.Fatal("a vnode count the ring would refuse was accepted into the state")
	}
}

// The store generation orders owners across a wipe, so a number that went
// backwards has to be refused: accepting it would let a zombie's checks start
// passing a fence they had already failed.
func TestTheStoreGenerationIsMonotone(t *testing.T) {
	s := applyAll(t, State{}, Command{Kind: SetStoreGen, Gen: 7})
	if s.StoreGen != 7 {
		t.Fatalf("store gen = %d", s.StoreGen)
	}
	if _, changed, err := s.Apply(Command{Kind: SetStoreGen, Gen: 7}); changed || err != nil {
		t.Fatalf("re-committing the same generation: changed=%v err=%v", changed, err)
	}
	if _, _, err := s.Apply(Command{Kind: SetStoreGen, Gen: 6}); !errors.Is(err, ErrNotApplicable) {
		t.Fatalf("a generation below the committed one was accepted: %v", err)
	}
	if _, _, err := s.Apply(Command{Kind: SetStoreGen, Gen: 0}); err == nil {
		t.Fatal("generation zero was accepted")
	}
	next, _, err := s.Apply(Command{Kind: SetStoreGen, Gen: 8})
	if err != nil {
		t.Fatal(err)
	}
	if next.StoreGen != 8 || next.RingEpoch != s.RingEpoch {
		t.Fatalf("a store generation moved the ring epoch: gen=%d epoch=%d", next.StoreGen, next.RingEpoch)
	}
}

func TestPolicyGenerationsAreMintedInOrder(t *testing.T) {
	s := State{}
	for i := 1; i <= 5; i++ {
		next, changed, err := s.Apply(Command{Kind: BumpPolicyGen})
		if err != nil || !changed {
			t.Fatalf("bump %d: changed=%v err=%v", i, changed, err)
		}
		if next.PolicyGen != uint64(i) {
			t.Fatalf("bump %d produced generation %d", i, next.PolicyGen)
		}
		s = next
	}
	if s.RingEpoch != 0 {
		t.Fatalf("policy generations moved the ring epoch to %d", s.RingEpoch)
	}
}

// Every node decodes bytes some other node wrote, possibly from a different
// build. An entry this build does not understand must be refused loudly: a
// state machine that silently skips what it cannot read has diverged from the
// one that applied it, and nothing finds out until two nodes disagree about
// who owns a tenant.
func TestAnUnknownEntryIsRefusedRatherThanIgnored(t *testing.T) {
	if _, _, err := (State{}).Apply(Command{Kind: "reticulate_splines"}); !errors.Is(err, ErrNotApplicable) {
		t.Fatalf("an unknown entry was applied: %v", err)
	}
	if _, err := DecodeCommand([]byte(`{}`)); err == nil {
		t.Fatal("an entry with no kind decoded")
	}
	if _, err := DecodeCommand([]byte(`not json`)); err == nil {
		t.Fatal("malformed bytes decoded")
	}
}

func TestAddShardRefusesAnIncompleteOrConflictingMember(t *testing.T) {
	s := applyAll(t, State{}, add("gateway-1", "127.0.0.1:9091"))

	if _, _, err := s.Apply(Command{Kind: AddShard, Member: node("", "127.0.0.1:9099")}); err == nil {
		t.Fatal("a member with no id was accepted")
	}
	if _, _, err := s.Apply(Command{Kind: AddShard, Member: node("gateway-9", "")}); err == nil {
		t.Fatal("a member with no address was accepted, so nothing could forward to it")
	}
	// Two ids on one address is one process answering as two shards.
	if _, _, err := s.Apply(add("gateway-2", "127.0.0.1:9091")); err == nil {
		t.Fatal("two members on one address were accepted")
	}

	// The same collision arrived at from the other direction: an existing
	// member MOVED onto an address somebody else already holds. This is the
	// one a check placed only on the append path misses.
	two := applyAll(t, s, add("gateway-2", "127.0.0.1:9092"))
	if _, _, err := two.Apply(add("gateway-2", "127.0.0.1:9091")); err == nil {
		t.Fatal("a member was moved onto an address another member already holds")
	}
}

// Apply is a pure function of (state, entry): it must not edit the state it
// was handed, or a node that keeps a snapshot for readers would see it change
// underneath them.
func TestApplyDoesNotEditTheStateItWasGiven(t *testing.T) {
	s := applyAll(t, State{}, add("gateway-1", "127.0.0.1:9091"), add("gateway-2", "127.0.0.1:9092"))
	before, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := s.Apply(add("gateway-3", "127.0.0.1:9093")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Apply(Command{Kind: RemoveShard, ID: "gateway-1"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Apply(add("gateway-2", "127.0.0.1:9192")); err != nil {
		t.Fatal(err)
	}

	after, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("applying entries edited the state they were applied to:\n before %s\n after  %s", before, after)
	}
}

// Nodes receive the same committed entries in the same order, but a node that
// restores from a snapshot and one that replayed the log must still end up
// byte-identical, and so must two nodes whose entries arrived in a different
// order for any reason. This is the property that makes "did these two agree"
// answerable by comparing bytes.
func TestTheSameMembersInAnyOrderSerialiseIdentically(t *testing.T) {
	orders := [][]Command{
		{add("gateway-1", "127.0.0.1:9091"), add("gateway-2", "127.0.0.1:9092"), add("gateway-3", "127.0.0.1:9093")},
		{add("gateway-3", "127.0.0.1:9093"), add("gateway-1", "127.0.0.1:9091"), add("gateway-2", "127.0.0.1:9092")},
		{add("gateway-2", "127.0.0.1:9092"), add("gateway-3", "127.0.0.1:9093"), add("gateway-1", "127.0.0.1:9091")},
	}

	var first []byte
	for i, order := range orders {
		s := applyAll(t, State{}, order...)
		b, err := s.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = b
			continue
		}
		if string(b) != string(first) {
			t.Fatalf("order %d serialised differently:\n %s\n %s", i, first, b)
		}
	}

	// The control. Sorting is what makes the bytes identical, so a state that
	// kept its members in arrival order must serialise differently -- if this
	// half passes too, the assertion above is confirming nothing.
	unsorted := State{Members: []ring.Node{
		node("gateway-3", "127.0.0.1:9093"),
		node("gateway-1", "127.0.0.1:9091"),
		node("gateway-2", "127.0.0.1:9092"),
	}, RingEpoch: 3}
	raw, err := json.Marshal(unsorted)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) == string(first) {
		t.Fatal("members in arrival order serialised the same as members in id order, so the sort proves nothing")
	}
}

func TestSnapshotRoundTripsAndSortsWhatItReads(t *testing.T) {
	s := applyAll(t, State{},
		add("gateway-2", "127.0.0.1:9092"),
		add("gateway-1", "127.0.0.1:9091"),
		Command{Kind: SetVNodes, VNodes: 128},
		Command{Kind: BumpPolicyGen},
		Command{Kind: SetStoreGen, Gen: 42},
	)
	b, err := s.Encode()
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeState(b)
	if err != nil {
		t.Fatal(err)
	}
	again, err := back.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(b) {
		t.Fatalf("a snapshot did not round trip:\n %s\n %s", b, again)
	}
	if back.VNodes != 128 || back.PolicyGen != 1 || back.StoreGen != 42 || back.RingEpoch != s.RingEpoch {
		t.Fatalf("restored state = %+v", back)
	}

	// A snapshot written by something that did not sort is repaired on the way
	// in, so a node restoring it still computes the same ring.
	out, err := DecodeState([]byte(`{"members":[{"id":"b","addr":"127.0.0.1:2"},{"id":"a","addr":"127.0.0.1:1"}],"vnodes":0,"ring_epoch":2,"policy_gen":0,"store_gen":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if out.Members[0].ID != "a" {
		t.Fatalf("a snapshot in arrival order was restored in arrival order: %+v", out.Members)
	}
}

// The ring is derived from the state rather than stored in it, so there is one
// way a member list becomes a ring and every node runs it.
func TestTheRingIsDerivedAndAgreesAcrossNodes(t *testing.T) {
	a := applyAll(t, State{}, add("gateway-1", "127.0.0.1:9091"), add("gateway-2", "127.0.0.1:9092"), add("gateway-3", "127.0.0.1:9093"))
	b := applyAll(t, State{}, add("gateway-3", "127.0.0.1:9093"), add("gateway-2", "127.0.0.1:9092"), add("gateway-1", "127.0.0.1:9091"))

	ra, rb := a.mustRing(t), b.mustRing(t)
	for i := 0; i < 10000; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		if ra.Owner(tenant).ID != rb.Owner(tenant).ID {
			t.Fatalf("%q: one node says %s, the other says %s", tenant, ra.Owner(tenant).ID, rb.Owner(tenant).ID)
		}
	}

	// No members is a single node answering for every tenant itself, not an
	// error.
	r, err := (State{}).Ring()
	if err != nil || r != nil {
		t.Fatalf("an empty state produced ring=%v err=%v", r, err)
	}
}

func TestEntriesRoundTripThroughTheLog(t *testing.T) {
	for _, c := range []Command{
		{Kind: SeedRing, Members: []ring.Node{node("gateway-1", "127.0.0.1:9091")}, VNodes: 128},
		add("gateway-1", "127.0.0.1:9091"),
		{Kind: RemoveShard, ID: "gateway-1"},
		{Kind: SetVNodes, VNodes: 256},
		{Kind: BumpPolicyGen},
		{Kind: SetStoreGen, Gen: 9},
	} {
		b, err := c.Encode()
		if err != nil {
			t.Fatalf("%s: %v", c.Kind, err)
		}
		back, err := DecodeCommand(b)
		if err != nil {
			t.Fatalf("%s: %v", c.Kind, err)
		}
		if !reflect.DeepEqual(back, c) {
			t.Fatalf("%s did not round trip: %+v then %+v", c.Kind, c, back)
		}
	}
}

// The initial configuration arrives as ONE entry. Seeding a member at a time
// would have every node build a ring out of however much had arrived so far --
// a one-member ring, then a two-member ring, each of them a real ring that
// really routes -- so a tenant would move twice before the cluster had
// finished starting. Worse, the vnode count would arrive after the members,
// and a ring built at the default width and rebuilt at the configured one
// moves nearly every tenant.
func TestTheInitialRingArrivesInOneEntry(t *testing.T) {
	members := []ring.Node{
		node("gateway-3", "127.0.0.1:9093"),
		node("gateway-1", "127.0.0.1:9091"),
		node("gateway-2", "127.0.0.1:9092"),
	}
	seed := Command{Kind: SeedRing, Members: members, VNodes: 128}

	s, changed, err := (State{}).Apply(seed)
	if err != nil || !changed {
		t.Fatalf("seeding an empty state: changed=%v err=%v", changed, err)
	}
	if len(s.Members) != 3 || s.VNodes != 128 {
		t.Fatalf("seeded state = %+v", s)
	}
	// One configuration, one epoch. Three members arriving together are one
	// change, not three.
	if s.RingEpoch != 1 {
		t.Fatalf("seeding three members advanced the epoch to %d, want 1", s.RingEpoch)
	}
	// The ring is at its configured width the first time it exists, so there
	// is no moment at which it is built at the default and then rebuilt.
	if s.mustRing(t).VNodes() != 128 {
		t.Fatalf("the seeded ring has %d points per member", s.mustRing(t).VNodes())
	}

	// After the first membership commits, the log is the truth. A leader that
	// still held a stale configuration file must not be able to put back a
	// shard the cluster removed by re-seeding.
	trimmed := applyAll(t, s, Command{Kind: RemoveShard, ID: "gateway-3"})
	again, changed, err := trimmed.Apply(seed)
	if err != nil {
		t.Fatal(err)
	}
	if changed || again.Has("gateway-3") {
		t.Fatal("re-seeding put back a shard the cluster had removed")
	}

	if _, _, err := (State{}).Apply(Command{Kind: SeedRing, VNodes: 128}); err == nil {
		t.Fatal("a seed with no members was accepted")
	}
	if _, _, err := (State{}).Apply(Command{Kind: SeedRing, Members: members, VNodes: ring.MaxVNodes + 1}); err == nil {
		t.Fatal("a seed with a vnode count the ring would refuse was accepted")
	}
	dup := []ring.Node{node("gateway-1", "127.0.0.1:9091"), node("gateway-1", "127.0.0.1:9099")}
	if _, _, err := (State{}).Apply(Command{Kind: SeedRing, Members: dup}); err == nil {
		t.Fatal("a seed naming one id twice was accepted")
	}

	// Generations already minted survive the seed: a cluster whose ring was
	// seeded after a store generation was committed must not forget it.
	withGens := applyAll(t, State{}, Command{Kind: SetStoreGen, Gen: 4}, Command{Kind: BumpPolicyGen})
	seeded := applyAll(t, withGens, seed)
	if seeded.StoreGen != 4 || seeded.PolicyGen != 1 {
		t.Fatalf("seeding the ring dropped the generations: %+v", seeded)
	}
}

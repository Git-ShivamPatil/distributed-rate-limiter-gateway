package cluster

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// A three-node cluster with in-memory transports and stores.
//
// In memory, and in one process, on purpose. What these tests are about is
// what the state machine does with committed entries and what happens when
// the leader goes away, and both of those are decided long before a byte
// reaches a socket. Running them over real TCP would add a scheduler, a
// kernel and two cores' worth of contention to every assertion, which is how a
// consensus test becomes a test of the machine it ran on.
type harness struct {
	t     *testing.T
	nodes []*testNode
}

type testNode struct {
	id     string
	addr   raft.ServerAddress
	raft   *raft.Raft
	fsm    *FSM
	trans  *raft.InmemTransport
	logs   *raft.InmemStore
	stable *raft.InmemStore
	snaps  *raft.InmemSnapshotStore
	config *raft.Config
}

func testRaftConfig(id string) *raft.Config {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(id)
	// Short enough that a test does not wait on a production election, long
	// enough that a two-core box under the race detector does not lose
	// leadership to its own scheduler. Nothing here is ASSERTED against a
	// timing -- these only decide how long a test waits for a condition.
	c.HeartbeatTimeout = 200 * time.Millisecond
	c.ElectionTimeout = 200 * time.Millisecond
	c.LeaderLeaseTimeout = 100 * time.Millisecond
	c.CommitTimeout = 10 * time.Millisecond
	c.Logger = hclog.NewNullLogger()
	return c
}

func newHarness(t *testing.T, n int) *harness {
	t.Helper()
	h := &harness{t: t}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("gateway-%d", i+1)
		addr, trans := raft.NewInmemTransport("")
		h.nodes = append(h.nodes, &testNode{
			id: id, addr: addr, trans: trans,
			fsm:    NewFSM(nil),
			logs:   raft.NewInmemStore(),
			stable: raft.NewInmemStore(),
			snaps:  raft.NewInmemSnapshotStore(),
			config: testRaftConfig(id),
		})
	}

	// Every node can reach every other. A partition test would remove one of
	// these; nothing here does.
	for _, a := range h.nodes {
		for _, b := range h.nodes {
			if a != b {
				a.trans.Connect(b.addr, b.trans)
			}
		}
	}

	// One node bootstraps, with the whole member list. The others start with
	// an empty log and learn the configuration from the leader, which is why
	// no node needs to be told where to join.
	servers := make([]raft.Server, 0, n)
	for _, nd := range h.nodes {
		servers = append(servers, raft.Server{ID: raft.ServerID(nd.id), Address: nd.addr})
	}
	first := h.nodes[0]
	if err := raft.BootstrapCluster(first.config, first.logs, first.stable, first.snaps, first.trans,
		raft.Configuration{Servers: servers}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	for _, nd := range h.nodes {
		r, err := raft.NewRaft(nd.config, nd.fsm, nd.logs, nd.stable, nd.snaps, nd.trans)
		if err != nil {
			t.Fatalf("%s: %v", nd.id, err)
		}
		nd.raft = r
	}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	for _, nd := range h.nodes {
		if nd.raft != nil {
			_ = nd.raft.Shutdown().Error()
		}
	}
}

// leader blocks until exactly one node reports itself leader, and returns it.
//
// It waits for a CONDITION rather than for a duration, and the deadline is
// only there so a broken test fails instead of hanging. No assertion in this
// file is made about how long this took.
func (h *harness) leader(timeout time.Duration) *testNode {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found *testNode
		count := 0
		for _, nd := range h.nodes {
			if nd.raft != nil && nd.raft.State() == raft.Leader {
				found, count = nd, count+1
			}
		}
		if count == 1 {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("no single leader within %s", timeout)
	return nil
}

func (h *harness) live() []*testNode {
	out := make([]*testNode, 0, len(h.nodes))
	for _, nd := range h.nodes {
		if nd.raft != nil {
			out = append(out, nd)
		}
	}
	return out
}

// propose sends one entry through the leader and waits for it to commit.
func (h *harness) propose(ldr *testNode, c Command) Applied {
	h.t.Helper()
	b, err := c.Encode()
	if err != nil {
		h.t.Fatal(err)
	}
	f := ldr.raft.Apply(b, 5*time.Second)
	if err := f.Error(); err != nil {
		h.t.Fatalf("applying %s: %v", c.Kind, err)
	}
	res, ok := f.Response().(Applied)
	if !ok {
		h.t.Fatalf("applying %s: response was %T", c.Kind, f.Response())
	}
	return res
}

// waitForAgreement blocks until every live node's state encodes to the same
// bytes, and returns those bytes.
func (h *harness) waitForAgreement(timeout time.Duration) []byte {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		agreed := true
		var want []byte
		for i, nd := range h.live() {
			b, err := nd.fsm.State().Encode()
			if err != nil {
				h.t.Fatal(err)
			}
			if i == 0 {
				want = b
				continue
			}
			if string(b) != string(want) {
				agreed = false
				last = fmt.Sprintf("%s has %s, %s has %s", h.live()[0].id, want, nd.id, b)
				break
			}
		}
		if agreed {
			return want
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.t.Fatalf("the nodes never agreed within %s: %s", timeout, last)
	return nil
}

// Committed entries reach every node, and every node turns them into the same
// bytes. This is the property the whole design rests on: if two nodes could
// serialise the same committed entries differently they would build different
// rings, and two shards would think they owned one tenant.
func TestCommittedEntriesReachEveryNodeIdentically(t *testing.T) {
	h := newHarness(t, 3)
	ldr := h.leader(10 * time.Second)

	for i, nd := range h.nodes {
		res := h.propose(ldr, Command{Kind: AddShard, Member: node(nd.id, fmt.Sprintf("127.0.0.1:909%d", i+1))})
		if !res.Changed {
			t.Fatalf("adding %s changed nothing", nd.id)
		}
	}
	h.propose(ldr, Command{Kind: SetVNodes, VNodes: 128})

	agreed := h.waitForAgreement(10 * time.Second)

	state, err := DecodeState(agreed)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Members) != 3 || state.VNodes != 128 {
		t.Fatalf("agreed state = %s", agreed)
	}
	// Three memberships and one ring-config change, all of which could move a
	// tenant.
	if state.RingEpoch != 4 {
		t.Fatalf("ring epoch = %d after 3 adds and a vnode change", state.RingEpoch)
	}
}

// A proposer learns that its entry was refused. The refusal is deterministic,
// so the replicas stay identical -- what differs is only that the caller is
// told no.
func TestARefusedEntryIsReportedBackAndChangesNothing(t *testing.T) {
	h := newHarness(t, 3)
	ldr := h.leader(10 * time.Second)

	h.propose(ldr, Command{Kind: SetStoreGen, Gen: 7})
	before := h.waitForAgreement(10 * time.Second)

	b, err := (Command{Kind: SetStoreGen, Gen: 6}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	f := ldr.raft.Apply(b, 5*time.Second)
	if err := f.Error(); err != nil {
		t.Fatalf("the entry itself should commit; only its application is refused: %v", err)
	}
	res, ok := f.Response().(Applied)
	if !ok {
		t.Fatalf("response was %T", f.Response())
	}
	if res.Err == nil {
		t.Fatal("a store generation below the committed one was applied")
	}

	after := h.waitForAgreement(10 * time.Second)
	if string(after) != string(before) {
		t.Fatalf("a refused entry changed the state:\n before %s\n after  %s", before, after)
	}
}

// Losing the leader elects another one, and everything committed before the
// loss is still there afterwards.
//
// The election is REPORTED, never asserted: how long hashicorp/raft takes to
// notice a dead leader is a property of its timers and of the machine, and a
// test that gates on it is a test that fails on a busy runner for no reason.
// What is asserted is that a leader exists again and that the state survived.
func TestLosingTheLeaderKeepsTheCommittedState(t *testing.T) {
	h := newHarness(t, 3)
	ldr := h.leader(10 * time.Second)

	for i := 1; i <= 3; i++ {
		h.propose(ldr, Command{Kind: AddShard, Member: node(fmt.Sprintf("gateway-%d", i), fmt.Sprintf("127.0.0.1:909%d", i))})
	}
	h.propose(ldr, Command{Kind: SetStoreGen, Gen: 5})
	before := h.waitForAgreement(10 * time.Second)

	killed := ldr.id
	start := time.Now()
	if err := ldr.raft.Shutdown().Error(); err != nil {
		t.Fatal(err)
	}
	ldr.raft = nil

	next := h.leader(30 * time.Second)
	t.Logf("leader %s lost; %s took over after %s", killed, next.id, time.Since(start).Round(time.Millisecond))
	if next.id == killed {
		t.Fatal("the killed node is still reporting itself leader")
	}

	after := h.waitForAgreement(10 * time.Second)
	if string(after) != string(before) {
		t.Fatalf("losing the leader changed the committed state:\n before %s\n after  %s", before, after)
	}

	// The new leader can still commit, which is the difference between an
	// election that happened and a cluster that merely stopped.
	res := h.propose(next, Command{Kind: BumpPolicyGen})
	if res.Err != nil || res.State.PolicyGen != 1 {
		t.Fatalf("the new leader could not commit: %+v", res)
	}
}

// A node that restarts reads its own log back and reaches the state it had,
// which is what makes a rolling restart a non-event rather than a rebuild.
func TestARestartedNodeRecoversItsState(t *testing.T) {
	h := newHarness(t, 3)
	ldr := h.leader(10 * time.Second)

	h.propose(ldr, Command{Kind: AddShard, Member: node("gateway-1", "127.0.0.1:9091")})
	h.propose(ldr, Command{Kind: AddShard, Member: node("gateway-2", "127.0.0.1:9092")})
	h.propose(ldr, Command{Kind: SetStoreGen, Gen: 11})
	want := h.waitForAgreement(10 * time.Second)

	// Restart a follower over the same stores, with a fresh state machine.
	var victim *testNode
	for _, nd := range h.nodes {
		if nd.id != ldr.id {
			victim = nd
			break
		}
	}
	if err := victim.raft.Shutdown().Error(); err != nil {
		t.Fatal(err)
	}
	victim.fsm = NewFSM(nil)
	r, err := raft.NewRaft(victim.config, victim.fsm, victim.logs, victim.stable, victim.snaps, victim.trans)
	if err != nil {
		t.Fatal(err)
	}
	victim.raft = r

	got := h.waitForAgreement(20 * time.Second)
	if string(got) != string(want) {
		t.Fatalf("a restarted node reached a different state:\n want %s\n got  %s", want, got)
	}
}

// A snapshot is the only thing a node joining after the log was truncated ever
// sees, so what it writes has to be exactly what Restore reads.
func TestASnapshotRestoresByteIdenticalState(t *testing.T) {
	f := NewFSM(nil)
	for _, c := range []Command{
		{Kind: AddShard, Member: node("gateway-2", "127.0.0.1:9092")},
		{Kind: AddShard, Member: node("gateway-1", "127.0.0.1:9091")},
		{Kind: SetVNodes, VNodes: 128},
		{Kind: BumpPolicyGen},
		{Kind: SetStoreGen, Gen: 3},
	} {
		b, err := c.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if res, ok := f.Apply(&raft.Log{Data: b}).(Applied); !ok || res.Err != nil {
			t.Fatalf("%s: %+v", c.Kind, res)
		}
	}

	want, err := f.State().Encode()
	if err != nil {
		t.Fatal(err)
	}

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	snap.Release()

	var adopted State
	other := NewFSM(func(s State) { adopted = s })
	if err := other.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	got, err := other.State().Encode()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("a snapshot did not restore what it captured:\n want %s\n got  %s", want, got)
	}
	// Restoring is a membership change like any other: whoever is serving
	// requests has to be told, or the node keeps routing on the ring it had
	// before it was caught up.
	if len(adopted.Members) != 2 || adopted.StoreGen != 3 {
		t.Fatalf("restore did not report the new state: %+v", adopted)
	}
}

// Every applied entry that changed anything reaches the request path. A
// membership that was committed and never adopted is a node routing on a ring
// the cluster has already replaced.
func TestEveryChangeIsHandedToTheRequestPath(t *testing.T) {
	var seen []State
	f := NewFSM(func(s State) { seen = append(seen, s) })

	apply := func(c Command) Applied {
		t.Helper()
		b, err := c.Encode()
		if err != nil {
			t.Fatal(err)
		}
		res, ok := f.Apply(&raft.Log{Data: b}).(Applied)
		if !ok {
			t.Fatalf("response was not Applied")
		}
		return res
	}

	apply(Command{Kind: AddShard, Member: node("gateway-1", "127.0.0.1:9091")})
	apply(Command{Kind: AddShard, Member: node("gateway-1", "127.0.0.1:9091")}) // no change
	apply(Command{Kind: SetStoreGen, Gen: 4})
	if res := apply(Command{Kind: SetStoreGen, Gen: 2}); res.Err == nil { // refused
		t.Fatal("a lower store generation was applied")
	}
	if res := apply(Command{Kind: "nonsense"}); res.Err == nil {
		t.Fatal("an unknown entry was applied")
	}

	if len(seen) != 2 {
		t.Fatalf("the request path was told about %d changes, want 2 (%+v)", len(seen), seen)
	}
}

// memSink stands in for the snapshot store's sink.
type memSink struct{ bytes.Buffer }

func (s *memSink) Close() error  { return nil }
func (s *memSink) ID() string    { return "test" }
func (s *memSink) Cancel() error { return nil }

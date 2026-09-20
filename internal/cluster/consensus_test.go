package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

// A cluster of Consensus nodes over in-memory transports.
//
// Each node also carries the View its request path would read, wired through
// OnApply exactly as cmd/gateway wires it, so these tests assert on what a
// request would actually see rather than on what the log contains.
type consensusCluster struct {
	t     *testing.T
	peers []Peer
	nodes map[string]*Consensus
	views map[string]*View

	trans map[string]*raft.InmemTransport

	mu       sync.Mutex
	rejected map[string]int
}

// kill takes a node away the way a machine does: it cuts the node off from its
// peers FIRST, so that the graceful leadership transfer Close performs cannot
// succeed, and only then stops it.
//
// Without the disconnect this would be a handover, not a failure -- Close
// transfers leadership when it holds it, so the cluster would never have to
// notice anything was missing and the test would be measuring a rolling
// restart while claiming to measure a crash.
func (c *consensusCluster) kill(id string) {
	c.t.Helper()
	c.trans[id].DisconnectAll()
	for _, other := range c.peers {
		if other.ID != id {
			c.trans[other.ID].Disconnect(raft.ServerAddress(peerAddr(c.peers, id)))
		}
	}
	_ = c.nodes[id].Close()
}

func peerAddr(peers []Peer, id string) string {
	for _, p := range peers {
		if p.ID == id {
			return p.RaftAddr
		}
	}
	return ""
}

func startCluster(t *testing.T, n int) *consensusCluster {
	t.Helper()
	c := &consensusCluster{
		t:        t,
		nodes:    map[string]*Consensus{},
		views:    map[string]*View{},
		trans:    map[string]*raft.InmemTransport{},
		rejected: map[string]int{},
	}

	for i := 1; i <= n; i++ {
		c.peers = append(c.peers, Peer{
			ID:       fmt.Sprintf("gateway-%d", i),
			Addr:     fmt.Sprintf("127.0.0.1:191%02d", i),
			RaftAddr: fmt.Sprintf("127.0.0.1:192%02d", i),
		})
	}

	// Build every transport first, then connect them, then start the nodes:
	// a node that started before its peers existed would campaign into a void.
	trans := c.trans
	for _, p := range c.peers {
		_, tr := raft.NewInmemTransport(raft.ServerAddress(p.RaftAddr))
		trans[p.ID] = tr
	}
	for _, a := range c.peers {
		for _, b := range c.peers {
			if a.ID != b.ID {
				trans[a.ID].Connect(raft.ServerAddress(b.RaftAddr), trans[b.ID])
			}
		}
	}

	members := make([]ring.Node, 0, len(c.peers))
	for _, p := range c.peers {
		members = append(members, ring.Node{ID: p.ID, Addr: p.Addr})
	}

	for _, p := range c.peers {
		// The configured ring is what this node routes on until the log
		// replaces it, which is what cmd/gateway does.
		view, err := New(p.ID, members, 128)
		if err != nil {
			t.Fatal(err)
		}
		c.views[p.ID] = view

		id := p.ID
		node, err := StartConsensus(ConsensusOptions{
			NodeID:             p.ID,
			Peers:              c.peers,
			VNodes:             128,
			Transport:          trans[p.ID],
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			CommitTimeout:      10 * time.Millisecond,
			RaftLogger:         hclog.NewNullLogger(),
			OnApply: func(s State) {
				if err := view.Adopt(s); err != nil {
					c.mu.Lock()
					c.rejected[id]++
					c.mu.Unlock()
				}
			},
		})
		if err != nil {
			t.Fatalf("%s: %v", p.ID, err)
		}
		c.nodes[p.ID] = node
	}

	t.Cleanup(func() {
		for _, node := range c.nodes {
			_ = node.Close()
		}
	})
	return c
}

func (c *consensusCluster) leader(timeout time.Duration) *Consensus {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found *Consensus
		count := 0
		for _, node := range c.nodes {
			if node.IsLeader() {
				found, count = node, count+1
			}
		}
		if count == 1 {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %s", timeout)
	return nil
}

func (c *consensusCluster) follower(notID string) *Consensus {
	c.t.Helper()
	for id, node := range c.nodes {
		if id != notID && !node.IsLeader() {
			return node
		}
	}
	c.t.Fatal("no follower")
	return nil
}

func (c *consensusCluster) rejections(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rejected[id]
}

// waitUntil polls a condition. It is a CONDITION, never a duration: the
// deadline exists so a broken test fails instead of hanging, and no assertion
// in this file is made about how long anything took.
func (c *consensusCluster) waitUntil(timeout time.Duration, what string, fn func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("%s did not happen within %s", what, timeout)
}

// Every node computes the same bootstrapper from the same member list, so
// there is no flag an operator can set on two nodes and end up with two
// clusters -- and the membership the leader seeds reaches every node's ring.
func TestOneNodeBootstrapsAndEveryRingEndsUpTheSame(t *testing.T) {
	c := startCluster(t, 3)
	ldr := c.leader(15 * time.Second)

	if got := bootstrapper(c.peers); got != "gateway-1" {
		t.Fatalf("bootstrapper = %q, want the id that sorts first", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := ldr.Seed(ctx); err != nil {
		t.Fatal(err)
	}

	c.waitUntil(20*time.Second, "every node applying the seeded membership", func() bool {
		for _, node := range c.nodes {
			if len(node.State().Members) != 3 {
				return false
			}
		}
		return true
	})

	// The rings agree, which is the only thing that makes forwarding mean
	// anything: three nodes that computed different rings would each forward
	// a tenant somewhere else.
	for i := 0; i < 5000; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		var want string
		for _, p := range c.peers {
			owner, _, ok := c.views[p.ID].Owner(tenant)
			if !ok {
				t.Fatalf("%s has no ring", p.ID)
			}
			if want == "" {
				want = owner.ID
				continue
			}
			if owner.ID != want {
				t.Fatalf("%q: %s says %s, another says %s", tenant, p.ID, owner.ID, want)
			}
		}
	}
}

// After the first membership commits, the log is the truth. Seeding again
// would let a node that still holds a stale configuration file put back a
// shard the cluster had removed.
func TestSeedingOnlyEverFillsAnEmptyState(t *testing.T) {
	c := startCluster(t, 3)
	ldr := c.leader(15 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := ldr.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	c.waitUntil(20*time.Second, "the seed committing", func() bool { return len(ldr.State().Members) == 3 })
	before := ldr.State()

	if err := ldr.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	after := ldr.State()
	if after.RingEpoch != before.RingEpoch || len(after.Members) != len(before.Members) {
		t.Fatalf("seeding a non-empty state changed it: epoch %d then %d", before.RingEpoch, after.RingEpoch)
	}
}

// A follower that is asked to commit says who can. Every caller that gets this
// has the same next move, so refusing without naming the leader would make
// every write path guess.
func TestAFollowerRefusesAndNamesTheLeader(t *testing.T) {
	c := startCluster(t, 3)
	ldr := c.leader(15 * time.Second)

	f := c.follower(ldr.self)
	_, err := f.Propose(context.Background(), Command{Kind: BumpPolicyGen})

	var notLeader *NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("a follower's proposal failed with %v, want a NotLeaderError", err)
	}
	if notLeader.LeaderID != ldr.self {
		t.Fatalf("the follower named %q as leader, the leader is %q", notLeader.LeaderID, ldr.self)
	}
	if notLeader.LeaderAddr == "" {
		t.Fatal("the leader was named but not addressed, so nothing could forward to it")
	}
}

// Removing a shard reaches every remaining node's ring, and the tenants it
// held are redistributed identically everywhere.
func TestRemovingAShardReachesEveryRemainingRing(t *testing.T) {
	c := startCluster(t, 3)
	ldr := c.leader(15 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := ldr.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	c.waitUntil(20*time.Second, "the seed committing everywhere", func() bool {
		for _, node := range c.nodes {
			if len(node.State().Members) != 3 {
				return false
			}
		}
		return true
	})

	// Pick a shard that is not the leader, so this is a membership change
	// rather than an election.
	var victim string
	for _, p := range c.peers {
		if p.ID != ldr.self {
			victim = p.ID
			break
		}
	}
	owned := tenantsOwnedBy(t, c.views[ldr.self], victim, 2000)
	if len(owned) == 0 {
		t.Fatalf("%s owned none of 2000 tenants, so removing it would prove nothing", victim)
	}

	if _, err := ldr.Propose(ctx, Command{Kind: RemoveShard, ID: victim}); err != nil {
		t.Fatal(err)
	}
	c.waitUntil(20*time.Second, "the removal reaching every node", func() bool {
		for _, node := range c.nodes {
			if node.State().Has(victim) {
				return false
			}
		}
		return true
	})

	// The survivors agree about where the removed shard's tenants went.
	survivors := []string{}
	for _, p := range c.peers {
		if p.ID != victim {
			survivors = append(survivors, p.ID)
		}
	}
	for _, tenant := range owned {
		var want string
		for _, id := range survivors {
			owner, _, ok := c.views[id].Owner(tenant)
			if !ok {
				t.Fatalf("%s has no ring", id)
			}
			if owner.ID == victim {
				t.Fatalf("%s still routes %q to the removed shard", id, tenant)
			}
			if want == "" {
				want = owner.ID
			} else if owner.ID != want {
				t.Fatalf("%q: %s says %s, another survivor says %s", tenant, id, owner.ID, want)
			}
		}
	}

	// The removed node is told about a membership it is not in. It refuses it
	// and keeps answering on the ring it had, which is what a shard being
	// drained should do until it is actually stopped -- adopting would leave
	// it forwarding every request away, including its own tenants.
	if c.rejections(victim) == 0 {
		t.Fatalf("%s adopted a membership it was not in", victim)
	}
	if !c.views[victim].Ring().Has(victim) {
		t.Fatalf("%s dropped itself from its own ring", victim)
	}
}

// The milestone's own question: losing the leader elects another one and
// leaves the ring exactly as it was.
//
// Every assertion here is an EVENT, never a latency. How long hashicorp/raft
// takes to notice a dead leader is a property of its timers and of the machine
// the test ran on; the election time is reported so a reader can see it, and
// asserted on by nothing.
func TestLosingTheLeaderElectsAnotherAndLeavesTheRingAlone(t *testing.T) {
	c := startCluster(t, 3)
	ldr := c.leader(15 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := ldr.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	c.waitUntil(20*time.Second, "the seed committing everywhere", func() bool {
		for _, node := range c.nodes {
			if len(node.State().Members) != 3 {
				return false
			}
		}
		return true
	})

	survivor := c.follower(ldr.self)
	before := ownership(t, c.views[survivor.self], 3000)
	killed := ldr.self

	start := time.Now()
	if err := ldr.Close(); err != nil {
		t.Fatal(err)
	}
	delete(c.nodes, killed)

	next := c.leader(30 * time.Second)
	t.Logf("leader %s lost; %s took over after %s", killed, next.self, time.Since(start).Round(time.Millisecond))
	if next.self == killed {
		t.Fatal("the killed node is still leading")
	}

	// Losing the leader is not a membership change: nobody committed one, so
	// no tenant moved. A ring that shifted here would mean an election had
	// rearranged the keyspace, which is exactly what must not happen.
	after := ownership(t, c.views[survivor.self], 3000)
	for tenant, owner := range before {
		if after[tenant] != owner {
			t.Fatalf("an election moved %q from %s to %s", tenant, owner, after[tenant])
		}
	}

	// The new leader can commit, which is what separates an election that
	// happened from a cluster that merely stopped.
	res, err := next.Propose(ctx, Command{Kind: BumpPolicyGen})
	if err != nil {
		t.Fatalf("the new leader could not commit: %v", err)
	}
	if res.State.PolicyGen != 1 {
		t.Fatalf("policy generation = %d after one bump", res.State.PolicyGen)
	}
}

func ownership(t *testing.T, v *View, n int) map[string]string {
	t.Helper()
	out := make(map[string]string, n)
	for i := 0; i < n; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		owner, _, ok := v.Owner(tenant)
		if !ok {
			t.Fatal("the view has no ring")
		}
		out[tenant] = owner.ID
	}
	return out
}

func tenantsOwnedBy(t *testing.T, v *View, id string, n int) []string {
	t.Helper()
	var out []string
	for tenant, owner := range ownership(t, v, n) {
		if owner == id {
			out = append(out, tenant)
		}
	}
	return out
}

// A node that is not in its own peer list would campaign in a cluster it is
// not a member of.
func TestConsensusRefusesAConfigurationItIsNotIn(t *testing.T) {
	peers := []Peer{{ID: "gateway-1", Addr: "127.0.0.1:19101", RaftAddr: "127.0.0.1:19201"}}
	if _, err := StartConsensus(ConsensusOptions{NodeID: "gateway-9", Peers: peers}); err == nil {
		t.Fatal("a node outside its own peer list started")
	}
	if _, err := StartConsensus(ConsensusOptions{NodeID: "gateway-1"}); err == nil {
		t.Fatal("a node with no peers started")
	}
	if _, err := StartConsensus(ConsensusOptions{Peers: peers}); err == nil {
		t.Fatal("a node with no id started")
	}
}

// The milestone's own assertion: traffic keeps being admitted across the loss
// of the leader, and the quota still comes out exactly.
//
// This is the test that separates "consensus governs membership" from
// "consensus is in the way". The limiter is shared and its clock NEVER
// advances, so the quota is a pure count with no refill and the expected
// number is exact rather than approximate -- an exact-count assertion that
// could refill underneath itself is not an assertion.
//
// The kill is triggered by the Nth ADMISSION, not by a timer, so it lands at
// the same point in the workload on a fast machine and a slow one. And it is a
// real kill: the node is cut off from its peers before it is stopped, because
// Close transfers leadership when it holds it and a graceful handover is not
// what this is about.
//
// Which assertion is load-bearing is worth saying, because the obvious one is
// not. The exact quota would hold in a cluster with no consensus at all --
// counters are authoritative wherever they live, which is the whole design.
// What this test alone can show is that no two nodes disagreed about ownership
// while there was no leader, that admissions continued, and that no ring moved
// without a membership change. The evidence that keeping consensus off the
// request path MATTERS is the control in scripts/kill-leader.sh, which puts it
// back on and requires this scenario to break.
func TestTheRingKeepsAdmittingWhileTheLeaderIsLost(t *testing.T) {
	const (
		quota    = 300
		killAt   = 100
		attempts = 600
		workers  = 4
	)

	c := startCluster(t, 3)
	ldr := c.leader(15 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := ldr.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	c.waitUntil(20*time.Second, "the seed committing everywhere", func() bool {
		for _, node := range c.nodes {
			if len(node.State().Members) != 3 {
				return false
			}
		}
		return true
	})

	epochBefore := c.views[ldr.self].Epoch()
	killedID := ldr.self
	tenant := "failover-tenant"

	// A frozen clock: nothing refills, so the quota is a count.
	clock := limiter.NewFakeClock(time.Unix(1700000000, 0))
	mem := limiter.NewMemory(limiter.WithClock(clock))
	limits := []limiter.Limit{{
		Name:      "per-hour",
		Algorithm: limiter.AlgorithmTokenBucket,
		Count:     quota,
		Period:    time.Hour,
		Burst:     quota,
	}}

	var (
		sent       atomic.Int64
		admitted   atomic.Int64
		afterKill  atomic.Int64
		failed     atomic.Int64
		disagreed  atomic.Int64
		noRing     atomic.Int64
		killedFlag atomic.Bool
		killOnce   sync.Once
		wg         sync.WaitGroup
	)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sent.Add(1) <= attempts {
				// Every node has to name the same owner for the whole run,
				// including while there is no leader. A disagreement here is
				// two shards believing they coordinate one tenant.
				var want string
				for _, p := range c.peers {
					owner, _, ok := c.views[p.ID].Owner(tenant)
					if !ok {
						noRing.Add(1)
						continue
					}
					if want == "" {
						want = owner.ID
					} else if owner.ID != want {
						disagreed.Add(1)
					}
				}

				res, err := mem.Check(context.Background(), limiter.Request{
					Tenant: tenant, Limits: limits, Cost: 1,
				})
				if err != nil {
					failed.Add(1)
					continue
				}
				if !res.Allowed {
					continue
				}
				if killedFlag.Load() {
					afterKill.Add(1)
				}
				if admitted.Add(1) == killAt {
					killOnce.Do(func() {
						c.kill(killedID)
						killedFlag.Store(true)
					})
				}
			}
		}()
	}
	wg.Wait()
	delete(c.nodes, killedID)

	t.Logf("%d admitted of %d attempts, %d of them after the leader died",
		admitted.Load(), attempts, afterKill.Load())

	if disagreed.Load() != 0 {
		t.Errorf("the nodes disagreed about who owns %q %d times during the transition",
			tenant, disagreed.Load())
	}
	if noRing.Load() != 0 {
		t.Errorf("a node had no ring %d times during the transition", noRing.Load())
	}
	if failed.Load() != 0 {
		t.Errorf("%d checks failed outright; losing the leader is not supposed to reach the request path",
			failed.Load())
	}
	if got := admitted.Load(); got != quota {
		t.Errorf("admitted %d against a quota of %d across a leader loss", got, quota)
	}
	if afterKill.Load() == 0 {
		t.Error("nothing was admitted after the kill, so the transition was never exercised")
	}

	// A leader again, and not the dead one.
	next := c.leader(30 * time.Second)
	t.Logf("leader %s was killed; %s took over", killedID, next.self)
	if next.self == killedID {
		t.Fatal("the killed node is still leading")
	}

	// Nobody committed a membership change, so no tenant may have moved.
	for id, node := range c.nodes {
		if got := c.views[id].Epoch(); got != epochBefore {
			t.Errorf("%s moved to ring epoch %d without a membership change (was %d)", id, got, epochBefore)
		}
		if len(node.State().Members) != 3 {
			t.Errorf("%s lost a member without one being removed: %d left", id, len(node.State().Members))
		}
	}
}

// A node that restarts over its own log recovers what it committed, and does
// NOT bootstrap a second time.
//
// Every other test in this file runs on in-memory stores, which means
// raft.HasExistingState is always false and the branch that guards
// re-bootstrapping is never taken. That branch is the one standing between a
// rolling restart and a node deciding it is a brand new cluster, so it needs a
// test that actually writes a log to disk.
//
// Mutation to check this against: delete `!existing &&` from StartConsensus and
// this test fails, because raft refuses to bootstrap a store that already has
// state.
func TestANodeRestartsOverItsOwnLogWithoutBootstrappingAgain(t *testing.T) {
	dir := t.TempDir()
	peer := Peer{ID: "gateway-1", Addr: "127.0.0.1:19101", RaftAddr: "127.0.0.1:19201"}

	start := func() *Consensus {
		t.Helper()
		// A fresh transport each time: the old one belongs to the process that
		// just stopped, which is the point of a restart.
		_, trans := raft.NewInmemTransport(raft.ServerAddress(peer.RaftAddr))
		c, err := StartConsensus(ConsensusOptions{
			NodeID:             peer.ID,
			Peers:              []Peer{peer},
			VNodes:             128,
			Dir:                dir,
			Transport:          trans,
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
			CommitTimeout:      10 * time.Millisecond,
			RaftLogger:         hclog.NewNullLogger(),
		})
		if err != nil {
			t.Fatalf("starting over %s: %v", dir, err)
		}
		return c
	}

	waitLeader := func(c *Consensus) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.WaitForLeader(ctx); err != nil {
			t.Fatal(err)
		}
	}

	first := start()
	waitLeader(first)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	if err := first.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Propose(ctx, Command{Kind: SetStoreGen, Gen: 17}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Propose(ctx, Command{Kind: BumpPolicyGen}); err != nil {
		t.Fatal(err)
	}
	cancel()

	before, err := first.State().Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	// The log is on disk now. Coming back up must read it rather than decide
	// this is a new cluster.
	second := start()
	t.Cleanup(func() { _ = second.Close() })
	waitLeader(second)

	// Winning an election is not the same as having replayed the log. A node
	// is leader as soon as it has the votes; its state machine catches up
	// afterwards, and on a loaded machine that gap is wide enough to read an
	// empty state through. So wait for the CONDITION -- the state matching --
	// rather than for leadership, which is a different fact.
	var after []byte
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		after, err = second.State().Encode()
		if err != nil {
			t.Fatal(err)
		}
		if string(after) == string(before) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if string(after) != string(before) {
		t.Fatalf("a restart did not recover what was committed within 30s:\n before %s\n after  %s", before, after)
	}
	if got := second.State().StoreGen; got != 17 {
		t.Fatalf("the store generation came back as %d, want 17", got)
	}
	if got := second.State().PolicyGen; got != 1 {
		t.Fatalf("the policy generation came back as %d, want 1", got)
	}

	// And seeding again is still a no-op, because the log already has members.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	if err := second.Seed(ctx2); err != nil {
		t.Fatal(err)
	}
	if again, _ := second.State().Encode(); string(again) != string(before) {
		t.Fatal("seeding after a restart changed the committed state")
	}
}

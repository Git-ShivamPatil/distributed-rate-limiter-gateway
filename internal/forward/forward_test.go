package forward_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/cluster"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/forward"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/grpcapi"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

type fixedSource struct {
	p   policy.Policy
	err error
}

func (f *fixedSource) Lookup(context.Context, string) (policy.Policy, error) {
	if f.err != nil {
		return policy.Policy{}, f.err
	}
	return f.p, nil
}

func bucketPolicy(count int64, period time.Duration) policy.Policy {
	return policy.Policy{
		Name: "shared",
		Limits: []limiter.Limit{{
			Name: "per-period", Algorithm: limiter.AlgorithmTokenBucket,
			Count: count, Period: period, Burst: count,
		}},
	}
}

// twoNodes builds node A and node B over one limiter, with A's ring pointing
// every tenant at B. B serves gRPC over an in-memory listener.
//
// One limiter between them is not a shortcut: it is the deployment. Every node
// shares one Redis, and that is exactly why a forward failing is survivable.
func twoNodes(t *testing.T, src policy.Source, opts ...forward.Option) (*decide.Service, *decide.Service, *forward.Client) {
	t.Helper()

	shared := limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0))))

	// Node B: the owner.
	nodeB := decide.New(shared, src, "node-b")
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	grpcapi.New(nodeB, slog.New(slog.DiscardHandler), grpcapi.WithTrustedTenantField(true)).Register(server)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})

	dialer := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	})
	all := append([]forward.Option{
		forward.WithDialOptions(dialer, grpc.WithTransportCredentials(insecure.NewCredentials())),
		forward.WithPoolSize(2),
	}, opts...)
	client := forward.New(all...)
	t.Cleanup(func() { _ = client.Close() })

	// Node A: knows only that node-b owns everything.
	view := &fixedView{self: "node-a", owner: ring.Node{ID: "node-b", Addr: "passthrough://bufnet"}}
	nodeA := decide.New(shared, src, "node-a",
		decide.WithRing(view, client), decide.WithLogger(slog.New(slog.DiscardHandler)))

	return nodeA, nodeB, client
}

// fixedView sends every tenant to one node.
type fixedView struct {
	self  string
	owner ring.Node
}

func (v *fixedView) Owner(string) (ring.Node, bool, bool) {
	return v.owner, v.owner.ID == v.self, true
}
func (v *fixedView) Self() string { return v.self }

// A request that arrives at a node which does not own the tenant is decided by
// the node that does, and the answer says so.
func TestForwardedCheckIsAnsweredByTheOwner(t *testing.T) {
	nodeA, _, client := twoNodes(t, &fixedSource{p: bucketPolicy(20, time.Minute)})

	out, err := nodeA.Decide(context.Background(), decide.Query{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Result.Allowed {
		t.Fatal("the first request was refused")
	}
	if !out.Forwarded {
		t.Fatal("the answer is not marked as forwarded")
	}
	if out.Owner != "node-b" {
		t.Fatalf("owner = %q, want node-b", out.Owner)
	}
	if out.Result.Decisions[0].Remaining != 19 {
		t.Fatalf("remaining = %d, want 19", out.Result.Decisions[0].Remaining)
	}

	forwarded, failed := client.Stats()
	if forwarded != 1 || failed != 0 {
		t.Fatalf("client stats: forwarded=%d failed=%d", forwarded, failed)
	}
	if st := nodeA.Stats(); st.Forwarded != 1 || st.Local != 0 {
		t.Fatalf("node A stats: %+v -- it should have decided nothing itself", st)
	}
}

// The whole point of the ring: wherever a request lands, the tenant spends one
// quota.
func TestOneQuotaWhicheverNodeIsAsked(t *testing.T) {
	nodeA, nodeB, _ := twoNodes(t, &fixedSource{p: bucketPolicy(20, time.Minute)})
	ctx := context.Background()

	allowed := 0
	for i := 0; i < 30; i++ {
		target := nodeA // the non-owner, which forwards
		if i%2 == 1 {
			target = nodeB // the owner, deciding locally
		}
		out, err := target.Decide(ctx, decide.Query{Tenant: "acme"})
		if err != nil {
			t.Fatal(err)
		}
		if out.Result.Allowed {
			allowed++
		}
	}
	if allowed != 20 {
		t.Fatalf("%d of 30 requests were admitted across two nodes against one 20-token bucket", allowed)
	}
}

// An answer ABOUT the tenant travels back intact. Turning a peer's "no such
// tenant" into a local retry would hide a real 404; turning an unreachable
// peer into one would invent it.
func TestDecisionErrorsComeBackFromTheOwner(t *testing.T) {
	src := &fixedSource{err: fmt.Errorf("%w: %q", policy.ErrTenantNotFound, "ghost")}
	nodeA, _, _ := twoNodes(t, src)

	_, err := nodeA.Decide(context.Background(), decide.Query{Tenant: "ghost"})
	if !errors.Is(err, policy.ErrTenantNotFound) {
		t.Fatalf("err = %v, want ErrTenantNotFound from the owner", err)
	}
}

func TestImpossibleCostComesBackAsSuch(t *testing.T) {
	nodeA, _, _ := twoNodes(t, &fixedSource{p: bucketPolicy(5, time.Minute)})
	_, err := nodeA.Decide(context.Background(), decide.Query{Tenant: "acme", Cost: 50})
	if !errors.Is(err, limiter.ErrCostExceedsCapacity) {
		t.Fatalf("err = %v, want ErrCostExceedsCapacity", err)
	}
}

// An owner that cannot be reached costs a hop, not a quota: the request is
// decided here instead, against the same counter store.
func TestUnreachableOwnerFallsBackToLocal(t *testing.T) {
	shared := limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0))))
	src := &fixedSource{p: bucketPolicy(20, time.Minute)}

	// A peer address nothing is listening on.
	client := forward.New(forward.WithTimeout(200 * time.Millisecond))
	defer func() { _ = client.Close() }()

	view := &fixedView{self: "node-a", owner: ring.Node{ID: "node-b", Addr: "127.0.0.1:1"}}
	nodeA := decide.New(shared, src, "node-a",
		decide.WithRing(view, client), decide.WithLogger(slog.New(slog.DiscardHandler)))

	out, err := nodeA.Decide(context.Background(), decide.Query{Tenant: "acme"})
	if err != nil {
		t.Fatalf("a request was failed rather than decided locally: %v", err)
	}
	if !out.Result.Allowed {
		t.Fatal("the local decision refused a request inside the quota")
	}
	if out.Forwarded {
		t.Fatal("the answer claims to have been forwarded")
	}
	if st := nodeA.Stats(); st.FellBack != 1 || st.Local != 1 {
		t.Fatalf("stats = %+v, want one fallback and one local decision", st)
	}

	// And the quota is still one quota: the local decision consumed from the
	// same store the owner would have used.
	allowed := 1
	for i := 0; i < 30; i++ {
		out, err := nodeA.Decide(context.Background(), decide.Query{Tenant: "acme"})
		if err != nil {
			t.Fatal(err)
		}
		if out.Result.Allowed {
			allowed++
		}
	}
	if allowed != 20 {
		t.Fatalf("%d requests admitted against a 20-token bucket while falling back", allowed)
	}
}

// A forwarded request is never forwarded again. Two nodes with different views
// of the ring would otherwise bounce it between them until a deadline expired.
func TestAForwardedRequestIsNotForwardedAgain(t *testing.T) {
	shared := limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0))))
	src := &fixedSource{p: bucketPolicy(20, time.Minute)}

	counting := &countingForwarder{}
	// A view that points at somebody else no matter what, which is what a
	// disagreement looks like from inside one node.
	view := &fixedView{self: "node-a", owner: ring.Node{ID: "node-b", Addr: "127.0.0.1:1"}}
	node := decide.New(shared, src, "node-a", decide.WithRing(view, counting))

	out, err := node.Decide(context.Background(), decide.Query{Tenant: "acme", Forwarded: true})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Result.Allowed {
		t.Fatal("the request was refused")
	}
	if counting.calls != 0 {
		t.Fatalf("a request already marked as forwarded was forwarded %d more times", counting.calls)
	}
}

type countingForwarder struct{ calls int }

func (c *countingForwarder) Check(context.Context, string, decide.Query) (decide.Outcome, error) {
	c.calls++
	return decide.Outcome{}, decide.ErrPeerUnavailable
}

// A peek is forwarded too, and consumes nothing on either node.
func TestForwardedPeekConsumesNothing(t *testing.T) {
	nodeA, _, _ := twoNodes(t, &fixedSource{p: bucketPolicy(5, time.Minute)})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		out, err := nodeA.Decide(ctx, decide.Query{Tenant: "acme", Peek: true})
		if err != nil {
			t.Fatal(err)
		}
		if got := out.Result.Decisions[0].Remaining; got != 5 {
			t.Fatalf("peek %d: remaining = %d, want 5", i, got)
		}
	}
}

// The real ring, rather than a fixed view: whichever node is asked, both agree
// who the owner is, which is what makes forwarding land in one place.
func TestRealRingAgreesAcrossNodes(t *testing.T) {
	memberList := []ring.Node{
		{ID: "gateway-1", Addr: "127.0.0.1:9091"},
		{ID: "gateway-2", Addr: "127.0.0.1:9092"},
		{ID: "gateway-3", Addr: "127.0.0.1:9093"},
	}

	views := make([]*cluster.View, 0, len(memberList))
	for _, m := range memberList {
		v, err := cluster.New(m.ID, memberList, ring.DefaultVNodes)
		if err != nil {
			t.Fatal(err)
		}
		views = append(views, v)
	}

	for i := 0; i < 2000; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		first, _, _ := views[0].Owner(tenant)
		selves := 0
		for _, v := range views {
			owner, isSelf, ok := v.Owner(tenant)
			if !ok || owner.ID != first.ID {
				t.Fatalf("%q: %s says the owner is %q, %s says %q",
					tenant, views[0].Self(), first.ID, v.Self(), owner.ID)
			}
			if isSelf {
				selves++
			}
		}
		if selves != 1 {
			t.Fatalf("%q: %d nodes each think they own it", tenant, selves)
		}
	}
}

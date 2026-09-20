package decide

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// fencingChecker refuses the first n calls with a fence and then answers
// normally, which is what a stale node that refreshes its policy would see.
type fencingChecker struct {
	err       error
	refuseFor int64
	calls     atomic.Int64
}

func (f *fencingChecker) Check(context.Context, limiter.Request) (limiter.Result, error) {
	if f.calls.Add(1) <= f.refuseFor {
		return limiter.Result{}, f.err
	}
	return limiter.Result{Allowed: true}, nil
}

// refreshingSource hands out an old policy until it is invalidated, then a new
// one -- a policy cache, reduced to the behaviour that matters here.
type refreshingSource struct {
	old, new      policy.Policy
	invalidations atomic.Int64
}

func (s *refreshingSource) Lookup(context.Context, string) (policy.Policy, error) {
	if s.invalidations.Load() > 0 {
		return s.new, nil
	}
	return s.old, nil
}

func (s *refreshingSource) Invalidate(string) { s.invalidations.Add(1) }

func failOpen(name string) policy.Policy {
	return policy.Policy{
		Name:        name,
		Limits:      []limiter.Limit{bucket("per-minute", 5, time.Minute)},
		FailureMode: config.FailOpen,
	}
}

// THE TRAP, and the reason the fence is a value in the reply rather than an
// error from the script.
//
// A tenant whose policy fails open is ADMITTED when the counter store cannot
// answer -- that is what fail_open means and it is correct. A fence is not
// that: the store answered, and what it said was that this node is carrying a
// policy the cluster has already replaced. Admitting on the say-so of a node
// the store has just established is out of date is precisely the
// over-admission the fence exists to prevent.
//
// So this must be REFUSED, and the failure mode must not get a vote.
func TestAFencedCheckIsRefusedEvenWhenThePolicyFailsOpen(t *testing.T) {
	fence := &limiter.FenceError{Kind: limiter.ErrPolicyStale, Sent: 4, Stored: 9}
	s := New(&fencingChecker{err: fence, refuseFor: 1000}, &source{p: failOpen("free")}, "node-a")

	out, err := s.Decide(context.Background(), Query{Tenant: "acme"})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("a fenced check on a fail_open tenant answered err=%v allowed=%v, want ErrFenced",
			err, out.Result.Allowed)
	}
	if out.Result.Allowed {
		t.Fatal("a fenced check admitted the request")
	}
	if out.Degraded != nil {
		t.Fatalf("a fence was reported as a degraded answer (%+v); it is a refusal, not an outage", out.Degraded)
	}
	if got := s.Stats().PolicyStale; got == 0 {
		t.Fatal("the fence fired without being counted; a silent fence cannot be told from one that never fires")
	}
}

// THE CONTROL for the test above. The same scenario with the fence arriving as
// an ordinary store error -- which is what it would be if the script used
// redis.error_reply -- must ADMIT, because that path honours fail_open.
//
// If this half ever starts refusing too, the test above has stopped depending
// on the distinction it is supposed to prove.
func TestAPlainStoreErrorOnTheSameTenantIsAdmitted(t *testing.T) {
	s := New(&fencingChecker{err: errors.New("dial tcp: connection refused"), refuseFor: 1000},
		&source{p: failOpen("free")}, "node-a")

	out, err := s.Decide(context.Background(), Query{Tenant: "acme"})
	if err != nil {
		t.Fatalf("a store outage on a fail_open tenant: %v", err)
	}
	if !out.Result.Allowed {
		t.Fatal("a fail_open tenant was refused during a store outage, so this control proves nothing")
	}
	if out.Degraded == nil || out.Degraded.Mode != config.FailOpen {
		t.Fatalf("the admission was not reported as degraded: %+v", out.Degraded)
	}
}

// A store reset is the other fence, and it refuses the same way -- but it is
// counted separately, because "this node's policy is old" and "this is not the
// store we thought it was" want different alerts.
func TestAStoreResetRefusesAndIsCountedSeparately(t *testing.T) {
	fence := &limiter.FenceError{Kind: limiter.ErrStoreReset, Sent: 2, Stored: 3}
	s := New(&fencingChecker{err: fence, refuseFor: 1000}, &source{p: failOpen("free")}, "node-a")

	if _, err := s.Decide(context.Background(), Query{Tenant: "acme"}); !errors.Is(err, ErrFenced) {
		t.Fatalf("a store-reset fence answered %v, want ErrFenced", err)
	}
	st := s.Stats()
	if st.StoreFenced != 1 || st.PolicyStale != 0 {
		t.Fatalf("store reset counted as store=%d policy=%d, want 1 and 0", st.StoreFenced, st.PolicyStale)
	}
}

// Being fenced on a stale policy is usually a cache that has not caught up.
// Dropping the cached copy and asking once more turns a fence into a few
// milliseconds rather than a cache lifetime of refusals.
func TestAStalePolicyIsRefreshedAndRetriedOnce(t *testing.T) {
	src := &refreshingSource{old: failOpen("old"), new: failOpen("new")}
	checker := &fencingChecker{
		err:       &limiter.FenceError{Kind: limiter.ErrPolicyStale, Sent: 1, Stored: 2},
		refuseFor: 1, // the first call is fenced, the retry succeeds
	}
	s := New(checker, src, "node-a")

	out, err := s.Decide(context.Background(), Query{Tenant: "acme"})
	if err != nil {
		t.Fatalf("the retry did not recover: %v", err)
	}
	if !out.Result.Allowed {
		t.Fatal("the retry was refused")
	}
	if got := src.invalidations.Load(); got != 1 {
		t.Fatalf("the cached policy was invalidated %d times, want exactly 1", got)
	}
	if got := checker.calls.Load(); got != 2 {
		t.Fatalf("the store was asked %d times, want 2 (the fenced call and one retry)", got)
	}
	if out.Policy.Name != "new" {
		t.Fatalf("the retry used the %q policy, so it asked again with the same stale copy", out.Policy.Name)
	}
}

// There is no third attempt. Two nodes could otherwise bounce a tenant between
// them refreshing forever, and a fence that never settles is a hang rather
// than a refusal.
func TestARetryThatIsStillFencedRefusesRatherThanLooping(t *testing.T) {
	src := &refreshingSource{old: failOpen("old"), new: failOpen("new")}
	checker := &fencingChecker{
		err:       &limiter.FenceError{Kind: limiter.ErrPolicyStale, Sent: 1, Stored: 2},
		refuseFor: 1000, // never stops fencing
	}
	s := New(checker, src, "node-a")

	if _, err := s.Decide(context.Background(), Query{Tenant: "acme"}); !errors.Is(err, ErrFenced) {
		t.Fatalf("a permanently fenced tenant answered %v, want ErrFenced", err)
	}
	if got := checker.calls.Load(); got != 2 {
		t.Fatalf("the store was asked %d times, want exactly 2 -- more means it is retrying in a loop", got)
	}
	if got := src.invalidations.Load(); got != 1 {
		t.Fatalf("the cache was invalidated %d times, want exactly 1", got)
	}
}

// genSource hands out a policy stamped with a fixed generation until it is
// invalidated, then one stamped with a newer one.
type genSource struct {
	oldGen, newGen uint64
	invalidations  atomic.Int64
}

func (s *genSource) Lookup(context.Context, string) (policy.Policy, error) {
	p := failOpen("free")
	if s.invalidations.Load() > 0 {
		p.Gen = s.newGen
		p.Limits = []limiter.Limit{bucket("per-minute", 1, time.Minute)} // the tightened one
		return p, nil
	}
	p.Gen = s.oldGen
	p.Limits = []limiter.Limit{bucket("per-minute", 100, time.Minute)} // the wide one
	return p, nil
}

func (s *genSource) Invalidate(string) { s.invalidations.Add(1) }

// A node notices on its OWN that the copy of a policy it holds predates an
// edit, without waiting to be refused by the store.
//
// That matters more here than it first appears. A tenant's checks are
// coordinated by ONE node, so the owner is the only node whose generation ever
// reaches the counter store for that tenant -- nothing else is in a position
// to contradict it. Without this, a stale OWNER enforces its stale limit
// indefinitely and the store-side fence never gets the chance to fire.
func TestANodeRefreshesWhenItsPolicyPredatesTheClustersGeneration(t *testing.T) {
	src := &genSource{oldGen: 3, newGen: 5}
	s := New(limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))),
		src, "node-a",
		WithPolicyGeneration(func() uint64 { return 5 }))

	out, err := s.Decide(context.Background(), Query{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := src.invalidations.Load(); got != 1 {
		t.Fatalf("the node refreshed %d times; it should notice its copy is behind and refresh once", got)
	}
	if out.Policy.Gen != 5 {
		t.Fatalf("the decision used generation %d, want the refreshed 5", out.Policy.Gen)
	}
	if got := out.Result.Decisions[0].Limit; got != 1 {
		t.Fatalf("the decision enforced a limit of %d, so it used the copy it already had", got)
	}
	if s.Stats().PolicyStale == 0 {
		t.Fatal("the refresh was not counted")
	}
}

// THE CONTROL. With no generation to compare against, the same node enforces
// the copy it is holding -- which is what happened before this existed, and
// what the scenario in scripts/policy-skew-test.sh measures as an
// over-admission.
func TestWithoutAClusterGenerationTheStaleCopyIsEnforced(t *testing.T) {
	src := &genSource{oldGen: 3, newGen: 5}
	s := New(limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))),
		src, "node-a") // no WithPolicyGeneration

	out, err := s.Decide(context.Background(), Query{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if got := src.invalidations.Load(); got != 0 {
		t.Fatalf("something refreshed (%d) with nothing to compare against, so the test above proves nothing", got)
	}
	if got := out.Result.Decisions[0].Limit; got != 100 {
		t.Fatalf("the control enforced a limit of %d, want the stale 100", got)
	}
}

// A source that stamps nothing is unfenced by design. Retrying against one
// would refetch on every single request, forever.
func TestAnUnstampedPolicyIsNotRefreshedForever(t *testing.T) {
	src := &genSource{oldGen: 0, newGen: 0}
	s := New(limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))),
		src, "node-a",
		WithPolicyGeneration(func() uint64 { return 9 }))

	for i := 0; i < 5; i++ {
		if _, err := s.Decide(context.Background(), Query{Tenant: "acme"}); err != nil {
			t.Fatal(err)
		}
	}
	if got := src.invalidations.Load(); got != 0 {
		t.Fatalf("an unstamped policy was invalidated %d times", got)
	}
}

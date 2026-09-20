package policy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// hookSource runs a function in the middle of a lookup, which is how a test
// makes something happen "while the query is in flight" without a sleep.
type hookSource struct {
	during func()
	calls  atomic.Int64
}

func (s *hookSource) Lookup(context.Context, string) (Policy, error) {
	s.calls.Add(1)
	if s.during != nil {
		s.during()
	}
	return Policy{
		Name:   "free",
		Limits: []limiter.Limit{{Name: "per-minute", Algorithm: limiter.AlgorithmTokenBucket, Count: 10, Period: time.Minute}},
	}, nil
}

// THE ORDERING RULE, and the whole safety of the policy fence.
//
// The generation is read BEFORE the policy is fetched. A bump that commits
// while the query is in flight belongs to an edit this read may not have seen,
// so stamping the NEW generation onto the OLD limits would present them to the
// counter store as current -- and the store would wave them through, because
// the fence trusts the number it is given. That is the one mistake the fence
// cannot catch.
//
// Reading first can only UNDER-stamp, and an under-stamped entry is fenced,
// refreshed and retried. Wrong in the safe direction.
func TestTheGenerationIsReadBeforeTheFetch(t *testing.T) {
	var gen atomic.Uint64
	gen.Store(5)

	// The bump lands while the store is being queried.
	src := &hookSource{during: func() { gen.Store(6) }}
	c := NewCache(src, WithGeneration(gen.Load), WithTTL(time.Hour))

	p, err := c.Lookup(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}

	if p.Gen != 5 {
		t.Fatalf("the entry was stamped generation %d; a policy read before the bump must not claim to be after it", p.Gen)
	}

	// The control. If this is not 6, the bump never happened and the
	// assertion above passed for the wrong reason -- it would hold just as
	// well with the read in the wrong place.
	if got := gen.Load(); got != 6 {
		t.Fatalf("the generation is %d at the end, so nothing bumped during the fetch and this test proves nothing", got)
	}
}

// The stamp is what reaches the counter store, so it has to survive the cache
// rather than being recomputed on the way out.
func TestTheStampIsHeldWithTheEntry(t *testing.T) {
	var gen atomic.Uint64
	gen.Store(3)
	src := &hookSource{}
	c := NewCache(src, WithGeneration(gen.Load), WithTTL(time.Hour))
	ctx := context.Background()

	first, err := c.Lookup(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if first.Gen != 3 {
		t.Fatalf("first lookup stamped %d", first.Gen)
	}

	// The cluster moves on, but this node has not refreshed: its cached copy
	// is still the one it fetched, and it must keep saying so. A cache that
	// reported the CURRENT generation for an OLD policy would defeat the fence
	// in exactly the way the fence exists to prevent.
	gen.Store(4)
	second, err := c.Lookup(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if second.Gen != 3 {
		t.Fatalf("a cached policy reported generation %d after the cluster moved to 4", second.Gen)
	}
	if got := src.calls.Load(); got != 1 {
		t.Fatalf("the store was queried %d times; the second lookup should have been a cache hit", got)
	}

	// Once it does refresh, it carries the new one.
	c.Invalidate("acme")
	third, err := c.Lookup(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if third.Gen != 4 {
		t.Fatalf("after invalidating, the refreshed policy carries generation %d, want 4", third.Gen)
	}
}

// With no generation supplied, every policy is unfenced. That is the
// single-node deployment, and it is what keeps every test that is not about
// fencing working unchanged.
func TestWithoutAGenerationPoliciesAreUnfenced(t *testing.T) {
	c := NewCache(&hookSource{}, WithTTL(time.Hour))
	p, err := c.Lookup(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if p.Gen != 0 {
		t.Fatalf("a cache with no generation source stamped %d", p.Gen)
	}
}

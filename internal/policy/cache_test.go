package policy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// fakeSource is a Source whose answers and failures the test controls.
type fakeSource struct {
	mu      sync.Mutex
	count   int64
	limit   int64
	err     error
	delay   time.Duration
	started chan struct{}
}

func (f *fakeSource) Lookup(ctx context.Context, tenant string) (Policy, error) {
	atomic.AddInt64(&f.count, 1)
	if f.started != nil {
		select {
		case f.started <- struct{}{}:
		default:
		}
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return Policy{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return Policy{}, f.err
	}
	return Policy{
		Name: "plan",
		Limits: []limiter.Limit{{
			Name: "per-minute", Algorithm: limiter.AlgorithmTokenBucket,
			Count: f.limit, Period: time.Minute, Burst: f.limit,
		}},
	}, nil
}

func (f *fakeSource) set(limit int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.limit, f.err = limit, err
}

func (f *fakeSource) calls() int64 { return atomic.LoadInt64(&f.count) }

// The changeover boundary: a policy edit takes effect no later than the TTL,
// and no earlier -- the old limit is what is enforced until then, which is the
// property an operator is actually relying on when they read "cache_ttl".
func TestCacheChangeoverHappensAtTheTTLBoundary(t *testing.T) {
	src := &fakeSource{limit: 20}
	now := time.Unix(1_700_000_000, 0)
	clock := func() time.Time { return now }
	c := NewCache(src, WithTTL(5*time.Second), WithCacheClock(clock))
	ctx := context.Background()

	p, err := c.Lookup(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if p.Limits[0].Count != 20 {
		t.Fatalf("first lookup returned %d, want 20", p.Limits[0].Count)
	}

	src.set(5, nil) // the operator edits the policy

	// One nanosecond before the TTL lapses, the OLD limit is still in force.
	now = now.Add(5*time.Second - time.Nanosecond)
	p, _ = c.Lookup(ctx, "acme")
	if p.Limits[0].Count != 20 {
		t.Fatalf("the new limit took effect early: got %d just before the TTL", p.Limits[0].Count)
	}
	if src.calls() != 1 {
		t.Fatalf("the store was queried %d times inside the TTL, want 1", src.calls())
	}

	// At the boundary it is re-read.
	now = now.Add(time.Nanosecond)
	p, _ = c.Lookup(ctx, "acme")
	if p.Limits[0].Count != 5 {
		t.Fatalf("the new limit had not taken effect at the TTL boundary: got %d", p.Limits[0].Count)
	}
	if src.calls() != 2 {
		t.Fatalf("store calls = %d, want 2", src.calls())
	}
}

// An explicit invalidation is what makes an admin write take effect at once on
// the node that served it, rather than at the next TTL.
func TestCacheInvalidateTakesEffectImmediately(t *testing.T) {
	src := &fakeSource{limit: 20}
	now := time.Unix(1_700_000_000, 0)
	c := NewCache(src, WithTTL(time.Hour), WithCacheClock(func() time.Time { return now }))
	ctx := context.Background()

	if p, _ := c.Lookup(ctx, "acme"); p.Limits[0].Count != 20 {
		t.Fatal("unexpected first read")
	}
	src.set(5, nil)
	c.Invalidate("acme")

	if p, _ := c.Lookup(ctx, "acme"); p.Limits[0].Count != 5 {
		t.Fatalf("after invalidation the cache still served the old policy")
	}
}

// A cold cache under load must send one query, not one per waiting request.
func TestCacheSingleflightsConcurrentMisses(t *testing.T) {
	src := &fakeSource{limit: 20, delay: 50 * time.Millisecond, started: make(chan struct{}, 1)}
	c := NewCache(src, WithTTL(time.Minute))
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Lookup(ctx, "acme"); err != nil {
				t.Errorf("lookup: %v", err)
			}
		}()
	}
	wg.Wait()

	if n := src.calls(); n != 1 {
		t.Fatalf("50 concurrent misses produced %d queries, want 1", n)
	}
}

// "No such tenant" is an answer, and caching it is what stops a flood of
// requests for an unknown tenant becoming a flood of queries.
func TestCacheRemembersUnknownTenants(t *testing.T) {
	src := &fakeSource{err: fmt.Errorf("%w: %q", ErrTenantNotFound, "ghost")}
	now := time.Unix(1_700_000_000, 0)
	c := NewCache(src, WithTTL(time.Minute), WithNegativeTTL(time.Second),
		WithCacheClock(func() time.Time { return now }))
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		if _, err := c.Lookup(ctx, "ghost"); !errors.Is(err, ErrTenantNotFound) {
			t.Fatalf("lookup %d: err = %v, want ErrTenantNotFound", i, err)
		}
	}
	if n := src.calls(); n != 1 {
		t.Fatalf("20 lookups of an unknown tenant produced %d queries, want 1", n)
	}

	// The negative entry expires sooner than a real policy would, so a tenant
	// that has just been created starts working quickly.
	now = now.Add(time.Second)
	src.set(20, nil)
	p, err := c.Lookup(ctx, "ghost")
	if err != nil {
		t.Fatalf("after the negative TTL: %v", err)
	}
	if p.Limits[0].Count != 20 {
		t.Fatal("a newly created tenant was not picked up after the negative TTL")
	}
}

// A policy store outage must not take enforcement down with it. The last known
// policy is served until StaleFor, and only then does the gateway start
// failing lookups.
func TestCacheServesStaleWhileTheStoreIsDown(t *testing.T) {
	src := &fakeSource{limit: 20}
	now := time.Unix(1_700_000_000, 0)
	c := NewCache(src, WithTTL(time.Second), WithStaleFor(time.Minute),
		WithCacheClock(func() time.Time { return now }))
	ctx := context.Background()

	if _, err := c.Lookup(ctx, "acme"); err != nil {
		t.Fatal(err)
	}

	storeDown := errors.New("dial tcp 127.0.0.1:5432: connect: connection refused")
	src.set(0, storeDown)

	// Past the TTL, but inside the stale window: the old policy is served.
	now = now.Add(30 * time.Second)
	p, err := c.Lookup(ctx, "acme")
	if err != nil {
		t.Fatalf("a store outage inside the stale window failed the lookup: %v", err)
	}
	if p.Limits[0].Count != 20 {
		t.Fatalf("served %d, want the last known 20", p.Limits[0].Count)
	}
	if c.Stats().Stale == 0 {
		t.Error("serving stale was not counted, so an operator cannot see it happening")
	}

	// Past the stale window, it stops pretending.
	now = now.Add(time.Hour)
	if _, err := c.Lookup(ctx, "acme"); err == nil {
		t.Fatal("an arbitrarily old policy was served as if it were current")
	}
}

// A failure to reach the store must not be cached as an answer, or the
// outage's first request would poison the cache for a whole TTL after
// recovery.
func TestCacheDoesNotCacheStoreFailures(t *testing.T) {
	src := &fakeSource{err: errors.New("connection refused")}
	now := time.Unix(1_700_000_000, 0)
	c := NewCache(src, WithTTL(time.Hour), WithStaleFor(0),
		WithCacheClock(func() time.Time { return now }))
	ctx := context.Background()

	if _, err := c.Lookup(ctx, "acme"); err == nil {
		t.Fatal("expected the store failure to surface")
	}
	src.set(20, nil) // the store comes back

	p, err := c.Lookup(ctx, "acme")
	if err != nil {
		t.Fatalf("the cache remembered a failure as if it were an answer: %v", err)
	}
	if p.Limits[0].Count != 20 {
		t.Fatal("unexpected policy after recovery")
	}
}

func TestCacheInvalidateAllDropsEverything(t *testing.T) {
	src := &fakeSource{limit: 20}
	c := NewCache(src, WithTTL(time.Hour))
	ctx := context.Background()

	for _, tenant := range []string{"a", "b", "c"} {
		if _, err := c.Lookup(ctx, tenant); err != nil {
			t.Fatal(err)
		}
	}
	if c.Len() != 3 {
		t.Fatalf("cache holds %d entries, want 3", c.Len())
	}
	c.InvalidateAll()
	if c.Len() != 0 {
		t.Fatalf("cache holds %d entries after InvalidateAll", c.Len())
	}
	if _, err := c.Lookup(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if n := src.calls(); n != 4 {
		t.Fatalf("store calls = %d, want 4 (three misses plus one after the flush)", n)
	}
}

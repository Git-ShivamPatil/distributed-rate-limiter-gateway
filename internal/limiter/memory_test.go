package limiter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemoryBurstThenRefuse(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	m := NewMemory(WithClock(clock))
	l := tokenBucket(20, time.Minute, 20)

	allowed, denied := 0, 0
	for i := 0; i < 30; i++ {
		res, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: []Limit{l}})
		if err != nil {
			t.Fatalf("check %d: %v", i, err)
		}
		if res.Allowed {
			allowed++
		} else {
			denied++
		}
	}
	if allowed != 20 || denied != 10 {
		t.Fatalf("30 requests against a 20-token bucket: %d allowed, %d denied; want 20 and 10", allowed, denied)
	}
}

// A refused request must not consume from the limits that would have allowed
// it. Without this, a tenant hitting a strict per-second cap would still burn
// its per-minute budget and be punished twice for one request.
func TestMemoryMultiLimitIsAllOrNothing(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	m := NewMemory(WithClock(clock))

	generous := Limit{Name: "per-minute", Algorithm: AlgorithmTokenBucket, Count: 1000, Period: time.Minute, Burst: 1000}
	strict := Limit{Name: "per-second", Algorithm: AlgorithmSlidingWindow, Count: 2, Period: time.Second}
	limits := []Limit{generous, strict}

	for i := 0; i < 2; i++ {
		res, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: limits})
		if err != nil || !res.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, res.Allowed, err)
		}
	}

	// The third is refused by the strict limit.
	res, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("a third request passed a sliding window of 2 per second")
	}
	if res.Limiting != "per-second" {
		t.Fatalf("limiting = %q, want per-second", res.Limiting)
	}

	// The generous limit must still show 998 remaining: two consumed, and
	// nothing consumed by the refused request.
	peek, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: limits, PeekOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := peek.Decisions[0].Remaining; got != 998 {
		t.Fatalf("per-minute remaining = %d, want 998 -- the refused request consumed from it", got)
	}

	// And the refusal must have REPORTED 998 too. Each limit is evaluated as
	// if it might be charged, so the generous one first computed the headroom
	// it would have had afterwards -- a counterfactual that contradicts the
	// state actually left behind. Found by the smoke test, which compared the
	// number in the body against the next request's.
	if got := res.Decisions[0].Remaining; got != 998 {
		t.Fatalf("the refusal reported per-minute remaining = %d, want 998; it reported what it WOULD have been", got)
	}
	if res.Decisions[0].RetryAfter != 0 {
		t.Fatal("a limit that allowed the request reported a retry-after")
	}
}

// Tenants do not share counters, which is what makes a noisy neighbour
// somebody else's problem.
func TestMemoryTenantsAreIsolated(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	m := NewMemory(WithClock(clock))
	l := tokenBucket(5, time.Minute, 5)

	for i := 0; i < 20; i++ { // noisy neighbour burns its own quota and then some
		_, _ = m.Check(context.Background(), Request{Tenant: "noisy", Limits: []Limit{l}})
	}
	for i := 0; i < 5; i++ {
		res, err := m.Check(context.Background(), Request{Tenant: "quiet", Limits: []Limit{l}})
		if err != nil || !res.Allowed {
			t.Fatalf("quiet tenant request %d was refused: %v", i, err)
		}
	}
}

// Concurrent traffic against one bucket admits exactly the burst -- no more
// (lost updates) and no fewer (double counting). Run with -race.
func TestMemoryConcurrentExactness(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	m := NewMemory(WithClock(clock))
	// A rate slow enough that nothing refills during the test, so the answer
	// is exactly the burst.
	l := Limit{Name: "tb", Algorithm: AlgorithmTokenBucket, Count: 1000, Period: time.Hour, Burst: 1000}

	const goroutines, each = 50, 100
	var wg sync.WaitGroup
	results := make([]int, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				res, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: []Limit{l}})
				if err != nil {
					t.Errorf("goroutine %d: %v", g, err)
					return
				}
				if res.Allowed {
					results[g]++
				}
			}
		}(g)
	}
	wg.Wait()

	total := 0
	for _, n := range results {
		total += n
	}
	if total != 1000 {
		t.Fatalf("%d of %d concurrent requests were admitted against a burst of 1000", total, goroutines*each)
	}
}

func TestMemorySweepDropsOnlyExpiredState(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	m := NewMemory(WithClock(clock))
	bucket := tokenBucket(20, time.Minute, 20)
	win := window(5, time.Second)

	if _, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: []Limit{bucket, win}}); err != nil {
		t.Fatal(err)
	}
	if m.Len() != 2 {
		t.Fatalf("live counters = %d, want 2", m.Len())
	}

	// Both still hold debt, so neither may be discarded.
	if n := m.Sweep(); n != 0 {
		t.Fatalf("sweep removed %d counters that still held debt", n)
	}

	// The window ages out first.
	clock.Advance(time.Second)
	if n := m.Sweep(); n != 1 {
		t.Fatalf("sweep removed %d, want 1 (the window)", n)
	}
	if m.Len() != 1 {
		t.Fatalf("live counters = %d, want 1", m.Len())
	}

	// The bucket only expires once it has refilled completely: one token of
	// 20 per minute is 3 seconds, so 3 seconds after the single request.
	clock.Advance(2 * time.Second)
	if n := m.Sweep(); n != 1 {
		t.Fatalf("sweep removed %d, want 1 (the refilled bucket)", n)
	}
	if m.Len() != 0 {
		t.Fatalf("live counters = %d, want 0", m.Len())
	}

	// And discarding it did not hand out free quota: the bucket was full.
	res, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: []Limit{bucket}})
	if err != nil || !res.Allowed || res.Decisions[0].Remaining != 19 {
		t.Fatalf("after sweep: allowed=%v remaining=%d err=%v; want a full bucket",
			res.Allowed, res.Decisions[0].Remaining, err)
	}
}

// Sweeping while traffic runs must not lose a counter. A tenant whose state is
// swept between the map lookup and the lock would have its writes land in an
// unreachable struct, which reads as a fresh quota.
func TestMemorySweepDuringTrafficLosesNothing(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	m := NewMemory(WithClock(clock))
	l := Limit{Name: "tb", Algorithm: AlgorithmTokenBucket, Count: 500, Period: time.Hour, Burst: 500}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.Sweep()
			}
		}
	}()

	allowed := 0
	for i := 0; i < 600; i++ {
		res, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: []Limit{l}})
		if err != nil {
			t.Fatal(err)
		}
		if res.Allowed {
			allowed++
		}
	}
	close(stop)
	wg.Wait()

	if allowed != 500 {
		t.Fatalf("%d requests admitted against a burst of 500 while sweeping concurrently", allowed)
	}
}

func TestMemoryRejectsImpossibleCost(t *testing.T) {
	m := NewMemory(WithClock(NewFakeClock(time.Unix(1_700_000_000, 0))))
	l := tokenBucket(10, time.Minute, 10)

	_, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: []Limit{l}, Cost: 11})
	if !errors.Is(err, ErrCostExceedsCapacity) {
		t.Fatalf("err = %v, want ErrCostExceedsCapacity -- waiting would never help", err)
	}
}

func TestMemoryKeyBudgetFailsClosed(t *testing.T) {
	clock := NewFakeClock(time.Unix(1_700_000_000, 0))
	m := NewMemory(WithClock(clock), WithMaxKeys(2))
	l := tokenBucket(1, time.Hour, 1)

	for _, tenant := range []string{"a", "b"} {
		if _, err := m.Check(context.Background(), Request{Tenant: tenant, Limits: []Limit{l}}); err != nil {
			t.Fatalf("tenant %s: %v", tenant, err)
		}
	}
	_, err := m.Check(context.Background(), Request{Tenant: "c", Limits: []Limit{l}})
	if !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("err = %v, want ErrTooManyKeys; the store must refuse rather than evict live counters", err)
	}
}

// An unset max_keys in the config arrives here as zero, and zero has to mean
// "the default" rather than "a store that can hold nothing".
//
// It meant the latter once. Every unit test built the limiter directly and
// passed; the first request the real binary served failed closed with "key
// budget exhausted", because main passes the config value straight through.
func TestMemoryZeroMaxKeysMeansDefault(t *testing.T) {
	m := NewMemory(WithClock(NewFakeClock(time.Unix(1_700_000_000, 0))), WithMaxKeys(0))
	l := tokenBucket(5, time.Minute, 5)

	res, err := m.Check(context.Background(), Request{Tenant: "acme", Limits: []Limit{l}})
	if err != nil {
		t.Fatalf("a limiter built with max_keys unset refused to store anything: %v", err)
	}
	if !res.Allowed {
		t.Fatal("the first request to an empty limiter was refused")
	}
}

// Editing a limit's parameters starts a fresh counter rather than
// reinterpreting the stored debt under the new rate.
func TestLimitKeyChangesWithParameters(t *testing.T) {
	a := tokenBucket(20, time.Minute, 20)
	b := tokenBucket(20, time.Minute, 5)
	if a.Key("acme") == b.Key("acme") {
		t.Fatal("two limits with different bursts share a key")
	}
	if a.Key("acme") == a.Key("globex") {
		t.Fatal("two tenants share a key")
	}
	// The tenant is braced so that all of a tenant's keys land in one Redis
	// Cluster slot, which is what lets one script touch several of them.
	if got := a.Key("acme"); got[:6] != "rl1:{a" {
		t.Fatalf("key %q does not start with the versioned, brace-wrapped tenant", got)
	}
}

package redis

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// testDB is kept away from database 0 so that running the suite never wipes
// counters a developer is watching in the compose stack.
const testDB = 15

// testClient connects to a real Redis.
//
// Not miniredis: the whole point of this package is a Lua script running
// inside Redis, and an in-process fake reimplements Lua, TIME, sorted-set
// ordering and key expiry closely enough to pass while differing exactly where
// it matters. In CI REDIS_REQUIRED=1 turns "no Redis" from a skip into a
// failure, so a green run can never mean "the interesting tests did not run".
func testClient(t *testing.T) *goredis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := goredis.NewClient(&goredis.Options{Addr: addr, DB: testDB})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		if os.Getenv("REDIS_REQUIRED") == "1" {
			t.Fatalf("REDIS_REQUIRED=1 but no Redis at %s: %v", addr, err)
		}
		t.Skipf("no Redis at %s: %v (set REDIS_REQUIRED=1 to make this a failure)", addr, err)
	}
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushing test database: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func tenantFor(t *testing.T) string {
	return "test-" + strings.NewReplacer("/", "-", "#", "-", " ", "-").Replace(t.Name())
}

func tokenBucket(name string, count int64, period time.Duration, burst int64) limiter.Limit {
	return limiter.Limit{Name: name, Algorithm: limiter.AlgorithmTokenBucket, Count: count, Period: period, Burst: burst}
}

func window(name string, count int64, period time.Duration) limiter.Limit {
	return limiter.Limit{Name: name, Algorithm: limiter.AlgorithmSlidingWindow, Count: count, Period: period}
}

// The differential test. Both implementations run the same operations against
// the same fake clock and must agree on every field of every decision.
//
// This is what makes it safe to have the algorithm written twice -- once in Go
// and once in Lua. Anything the two do differently shows up here rather than
// as a tenant being limited differently depending on which backend a
// deployment happens to be using.
func TestRedisAgreesWithTheInMemoryOracle(t *testing.T) {
	client := testClient(t)
	clock := limiter.NewFakeClock(time.Unix(1_700_000_000, 0))
	mem := limiter.NewMemory(limiter.WithClock(clock))
	red := New(client, withClock(clock))
	ctx := context.Background()

	policies := [][]limiter.Limit{
		{tokenBucket("tb", 20, time.Minute, 20)},
		{window("sw", 5, time.Second)},
		{tokenBucket("tb", 1000, time.Minute, 200), window("sw", 25, time.Second)},
		// A rate whose emission does not divide evenly, to catch rounding that
		// differs between Go and Lua.
		{tokenBucket("odd", 3, time.Second, 7)},
		// A limit tight enough that most requests are refusals.
		{tokenBucket("tight", 2, time.Minute, 2), window("tight-sw", 3, 2*time.Second)},
	}

	rng := rand.New(rand.NewSource(20260919))
	const operations = 4000
	mismatches := 0

	for i := 0; i < operations; i++ {
		policy := policies[rng.Intn(len(policies))]
		tenant := fmt.Sprintf("%s-%d", tenantFor(t), rng.Intn(4))
		cost := int64(1)
		if rng.Intn(8) == 0 {
			cost = 1 + int64(rng.Intn(3))
		}
		peek := rng.Intn(10) == 0

		req := limiter.Request{Tenant: tenant, Limits: policy, Cost: cost, PeekOnly: peek}

		gotMem, errMem := mem.Check(ctx, req)
		gotRed, errRed := red.Check(ctx, req)

		if (errMem == nil) != (errRed == nil) {
			t.Fatalf("op %d: memory err=%v redis err=%v", i, errMem, errRed)
		}
		if errMem != nil {
			continue // both rejected the request the same way
		}

		if d := diff(gotMem, gotRed); d != "" {
			mismatches++
			t.Errorf("op %d (tenant %s, cost %d, peek %v):\n%s", i, tenant, cost, peek, d)
			if mismatches > 5 {
				t.Fatal("too many mismatches")
			}
		}

		// Advance in whole microseconds: that is the resolution both
		// implementations work at, and a sub-microsecond advance would be a
		// difference in the test rather than in the code.
		switch rng.Intn(4) {
		case 0:
			clock.Advance(time.Duration(rng.Intn(1000)) * time.Microsecond)
		case 1:
			clock.Advance(time.Duration(rng.Intn(500)) * time.Millisecond)
		case 2:
			clock.Advance(time.Duration(rng.Intn(5)) * time.Second)
		}
	}
}

func diff(want, got limiter.Result) string {
	var b strings.Builder
	if want.Allowed != got.Allowed {
		fmt.Fprintf(&b, "  allowed: memory=%v redis=%v\n", want.Allowed, got.Allowed)
	}
	if want.Limiting != got.Limiting {
		fmt.Fprintf(&b, "  limiting: memory=%q redis=%q\n", want.Limiting, got.Limiting)
	}
	if len(want.Decisions) != len(got.Decisions) {
		fmt.Fprintf(&b, "  decisions: memory=%d redis=%d\n", len(want.Decisions), len(got.Decisions))
		return b.String()
	}
	for i := range want.Decisions {
		w, g := want.Decisions[i], got.Decisions[i]
		if w != g {
			fmt.Fprintf(&b, "  limit %q:\n    memory: %+v\n    redis:  %+v\n", w.Name, w, g)
		}
	}
	return b.String()
}

// Two Checkers over one Redis are two gateway replicas. They must enforce one
// quota between them, which is the entire reason this backend exists.
func TestTwoRepliasShareOneQuota(t *testing.T) {
	client := testClient(t)
	a := New(client)
	b := New(client)
	ctx := context.Background()

	l := tokenBucket("shared", 30, time.Hour, 30)
	tenant := tenantFor(t)

	allowed := 0
	for i := 0; i < 50; i++ {
		target := a
		if i%2 == 1 {
			target = b
		}
		res, err := target.Check(ctx, limiter.Request{Tenant: tenant, Limits: []limiter.Limit{l}})
		if err != nil {
			t.Fatal(err)
		}
		if res.Allowed {
			allowed++
		}
	}
	if allowed != 30 {
		t.Fatalf("two replicas admitted %d requests against a shared quota of 30; each is counting on its own", allowed)
	}
}

// The contention case: many goroutines, one counter, an exact answer.
func TestConcurrentExactness(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		limit limiter.Limit
		want  int
	}{
		{"token bucket", tokenBucket("tb", 500, time.Hour, 500), 500},
		{"sliding window", window("sw", 500, time.Hour), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checker := New(client)
			tenant := tenantFor(t)

			const goroutines, each = 50, 20 // 1000 attempts against a quota of 500
			var wg sync.WaitGroup
			counts := make([]int, goroutines)
			errs := make([]error, goroutines)

			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < each; i++ {
						res, err := checker.Check(ctx, limiter.Request{
							Tenant: tenant, Limits: []limiter.Limit{tc.limit},
						})
						if err != nil {
							errs[g] = err
							return
						}
						if res.Allowed {
							counts[g]++
						}
					}
				}(g)
			}
			wg.Wait()

			for g, err := range errs {
				if err != nil {
					t.Fatalf("goroutine %d: %v", g, err)
				}
			}
			total := 0
			for _, n := range counts {
				total += n
			}
			if total != tc.want {
				t.Fatalf("%d of %d concurrent requests admitted, want exactly %d",
					total, goroutines*each, tc.want)
			}
		})
	}
}

// Several requests inside one microsecond must each be counted.
//
// A sliding-window entry is a sorted-set member, and ZADD with a member that
// already exists updates its score instead of adding a second one. If members
// were keyed by timestamp alone, two requests in the same microsecond would
// collapse into one entry -- the window would undercount and admit more than
// the limit. The suffix in check.lua exists for this, and this test is the
// reason it can be trusted.
func TestSameMicrosecondRequestsAreAllCounted(t *testing.T) {
	client := testClient(t)
	clock := limiter.NewFakeClock(time.Unix(1_700_000_000, 0)) // never advances
	checker := New(client, withClock(clock))
	ctx := context.Background()

	l := window("sw", 10, time.Minute)
	tenant := tenantFor(t)

	allowed := 0
	for i := 0; i < 15; i++ {
		res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: []limiter.Limit{l}})
		if err != nil {
			t.Fatal(err)
		}
		if res.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("%d requests admitted in one microsecond against a window of 10", allowed)
	}

	card, err := client.ZCard(ctx, l.Key(tenant)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if card != 10 {
		t.Fatalf("the window log holds %d entries after 10 admissions; entries collapsed", card)
	}
}

// Cost > 1 writes one entry per unit, so the window stays exact.
func TestWindowCostWritesOneEntryPerUnit(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()

	l := window("sw", 10, time.Minute)
	tenant := tenantFor(t)

	res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: []limiter.Limit{l}, Cost: 4})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Allowed || res.Decisions[0].Remaining != 6 {
		t.Fatalf("cost 4 of 10: allowed=%v remaining=%d", res.Allowed, res.Decisions[0].Remaining)
	}
	card, _ := client.ZCard(ctx, l.Key(tenant)).Result()
	if card != 4 {
		t.Fatalf("the log holds %d entries after a cost-4 request, want 4", card)
	}
}

// The production constructor reads Redis's clock, not the caller's.
//
// Proven through the key's TTL: the script sets it to the instant the bucket
// refills, computed from whatever it believes "now" to be. A script using a
// wrong clock produces a TTL that is wrong by the same amount.
func TestProductionCheckerUsesRedisTime(t *testing.T) {
	client := testClient(t)
	checker := New(client) // no clock injected -- the only production shape
	ctx := context.Background()

	l := tokenBucket("tb", 20, time.Minute, 20) // one token per 3s
	tenant := tenantFor(t)

	if _, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: []limiter.Limit{l}}); err != nil {
		t.Fatal(err)
	}

	ttl, err := client.PTTL(ctx, l.Key(tenant)).Result()
	if err != nil {
		t.Fatal(err)
	}
	// One token consumed means the bucket refills in one emission interval.
	if ttl < 2500*time.Millisecond || ttl > 3500*time.Millisecond {
		t.Fatalf("the key's TTL is %s, want about 3s; the script is not reading Redis TIME", ttl)
	}
}

// An expired key and a full bucket are the same state, so expiry can never be
// a source of free quota -- it only ever discards state that meant nothing.
func TestKeyExpiresExactlyWhenTheBucketRefills(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()

	// 4 per second, burst 4: the whole bucket refills in one second.
	l := tokenBucket("fast", 4, time.Second, 4)
	tenant := tenantFor(t)

	for i := 0; i < 4; i++ {
		if _, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: []limiter.Limit{l}}); err != nil {
			t.Fatal(err)
		}
	}
	res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: []limiter.Limit{l}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("a 5th request was admitted against a burst of 4")
	}

	ttl, err := client.PTTL(ctx, l.Key(tenant)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > time.Second+50*time.Millisecond {
		t.Fatalf("TTL is %s, want at most the 1s it takes to refill the whole bucket", ttl)
	}
}

// A refused request consumes from nothing, and says so.
func TestRefusalIsAllOrNothing(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()

	generous := tokenBucket("per-hour", 1000, time.Hour, 1000)
	strict := window("strict", 2, time.Hour)
	limits := []limiter.Limit{generous, strict}
	tenant := tenantFor(t)

	for i := 0; i < 2; i++ {
		res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits})
		if err != nil || !res.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, res.Allowed, err)
		}
	}

	res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits})
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("a third request passed a window of 2")
	}
	if res.Limiting != "strict" {
		t.Fatalf("limiting = %q, want strict", res.Limiting)
	}
	// The generous limit was charged twice and not a third time -- and the
	// refusal must report 998, not the 997 it would have been.
	if got := res.Decisions[0].Remaining; got != 998 {
		t.Fatalf("the refusal reported per-hour remaining = %d, want 998", got)
	}

	peek, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, PeekOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := peek.Decisions[0].Remaining; got != 998 {
		t.Fatalf("per-hour remaining is %d after two admissions and a refusal, want 998", got)
	}
}

// A peek writes nothing at all.
func TestPeekDoesNotCreateState(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()

	l := tokenBucket("tb", 10, time.Minute, 10)
	tenant := tenantFor(t)

	res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: []limiter.Limit{l}, PeekOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Allowed || res.Decisions[0].Remaining != 10 {
		t.Fatalf("peek on an untouched bucket: allowed=%v remaining=%d", res.Allowed, res.Decisions[0].Remaining)
	}
	n, err := client.Exists(ctx, l.Key(tenant)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("a peek created a key")
	}
}

func TestCostExceedingCapacityIsAnError(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	l := tokenBucket("tb", 10, time.Minute, 10)

	_, err := checker.Check(context.Background(), limiter.Request{
		Tenant: tenantFor(t), Limits: []limiter.Limit{l}, Cost: 11,
	})
	if err == nil {
		t.Fatal("a cost larger than the bucket was accepted")
	}
}

func TestLoadAndPing(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	if err := checker.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := checker.Load(ctx); err != nil {
		t.Fatalf("the check script does not load into this Redis: %v", err)
	}
}

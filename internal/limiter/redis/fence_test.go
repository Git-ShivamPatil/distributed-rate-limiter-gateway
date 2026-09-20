package redis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// snapshotKeys records every key belonging to a tenant and its value, so a
// test can require that a refused call changed nothing at all rather than
// require that it merely looked like it did.
func snapshotKeys(t *testing.T, client *goredis.Client, tenant string) map[string]string {
	t.Helper()
	ctx := context.Background()
	out := map[string]string{}
	for _, pattern := range []string{"rl1:{" + tenant + "}:*", limiter.MetaKey(tenant)} {
		keys, err := client.Keys(ctx, pattern).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			kind, err := client.Type(ctx, k).Result()
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "hash":
				v, err := client.HGetAll(ctx, k).Result()
				if err != nil {
					t.Fatal(err)
				}
				out[k] = strings.Join([]string{v["store_gen"], v["policy_gen"]}, "/")
			case "zset":
				v, err := client.ZRangeWithScores(ctx, k, 0, -1).Result()
				if err != nil {
					t.Fatal(err)
				}
				out[k] = strings.Trim(strings.Replace(strings.TrimSpace(strings.Join(zmembers(v), ",")), " ", "", -1), ",")
			default:
				v, err := client.Get(ctx, k).Result()
				if err != nil {
					t.Fatal(err)
				}
				out[k] = v
			}
		}
	}
	return out
}

func zmembers(zs []goredis.Z) []string {
	out := make([]string, 0, len(zs))
	for _, z := range zs {
		out = append(out, z.Member.(string))
	}
	return out
}

func sameKeys(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// A fence is a VALUE in the reply, not an error reply from the script. That
// distinction is the whole point: decide.go treats an error from the store as
// "the store could not answer" and honours the policy's failure mode, so a
// fence delivered as an error would ADMIT a fail_open tenant -- which is
// exactly the request the fence exists to refuse.
func TestAFenceIsARefusalAndNotAStoreFailure(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	tenant := tenantFor(t)
	limits := []limiter.Limit{tokenBucket("tb", 100, time.Hour, 100)}

	// An up-to-date node establishes generation 7.
	if _, err := checker.Check(ctx, limiter.Request{
		Tenant: tenant, Limits: limits, PolicyGen: 7,
	}); err != nil {
		t.Fatal(err)
	}

	before := snapshotKeys(t, client, tenant)

	_, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, PolicyGen: 6})
	if !errors.Is(err, limiter.ErrPolicyStale) {
		t.Fatalf("a check carrying an older policy was answered with %v, want ErrPolicyStale", err)
	}

	var fence *limiter.FenceError
	if !errors.As(err, &fence) {
		t.Fatalf("the refusal does not say which generations disagreed: %v", err)
	}
	if fence.Sent != 6 || fence.Stored != 7 {
		t.Fatalf("fence reports sent=%d stored=%d, want 6 and 7", fence.Sent, fence.Stored)
	}

	if after := snapshotKeys(t, client, tenant); !sameKeys(before, after) {
		t.Fatalf("a fenced check changed state:\n before %v\n after  %v", before, after)
	}
}

// Every tenant's first request finds no meta. A fence that refused it would
// refuse all traffic everywhere the first time it shipped.
func TestAnAbsentFenceAdoptsAndNeverRefuses(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	tenant := tenantFor(t)
	limits := []limiter.Limit{tokenBucket("tb", 10, time.Hour, 10)}

	res, err := checker.Check(ctx, limiter.Request{
		Tenant: tenant, Limits: limits, StoreGen: 3, PolicyGen: 9,
	})
	if err != nil || !res.Allowed {
		t.Fatalf("the first request of a tenant: allowed=%v err=%v", res.Allowed, err)
	}

	meta, err := client.HGetAll(ctx, limiter.MetaKey(tenant)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if meta["store_gen"] != "3" || meta["policy_gen"] != "9" {
		t.Fatalf("the fences were not adopted: %v", meta)
	}
}

// The one that is easy to get wrong. If the stored generation only advanced on
// ADMITTED requests, a tightening could be outrun by exhausting the quota
// first: every later request is a denial, the stored generation never moves,
// and the node still carrying the old wider limit is never fenced.
func TestADeniedRequestStillAdvancesTheFence(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	tenant := tenantFor(t)
	limits := []limiter.Limit{tokenBucket("tb", 1, time.Hour, 1)}

	// Exhaust the quota at generation 4.
	if res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, PolicyGen: 4}); err != nil || !res.Allowed {
		t.Fatalf("setup: allowed=%v err=%v", res.Allowed, err)
	}
	res, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, PolicyGen: 4})
	if err != nil || res.Allowed {
		t.Fatalf("the bucket should be empty: allowed=%v err=%v", res.Allowed, err)
	}

	// A newer node arrives while the tenant is throttled. Its request is
	// DENIED on quota -- and must still move the fence.
	res, err = checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, PolicyGen: 5})
	if err != nil {
		t.Fatal(err)
	}
	if res.Allowed {
		t.Fatal("the bucket admitted a second request")
	}

	// The older node is now fenced, which is the point.
	if _, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, PolicyGen: 4}); !errors.Is(err, limiter.ErrPolicyStale) {
		t.Fatalf("a denial did not advance the fence: the stale node was answered with %v", err)
	}
}

// A peek is a question, but the generation it carries is still evidence that a
// newer one exists.
func TestAPeekAdvancesTheFenceWithoutTouchingAnyCounter(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	tenant := tenantFor(t)
	l := tokenBucket("tb", 10, time.Hour, 10)

	if _, err := checker.Check(ctx, limiter.Request{
		Tenant: tenant, Limits: []limiter.Limit{l}, PolicyGen: 2, PeekOnly: true,
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := client.Exists(ctx, l.Key(tenant)).Result(); err != nil || n != 0 {
		t.Fatalf("a peek created a counter: exists=%d err=%v", n, err)
	}
	if got, err := client.HGet(ctx, limiter.MetaKey(tenant), "policy_gen").Result(); err != nil || got != "2" {
		t.Fatalf("a peek did not record the generation it carried: %q %v", got, err)
	}
}

// A store generation that differs IN EITHER DIRECTION is a different store. A
// wipe followed by a re-mint can hand out a number below the one a surviving
// node still carries, and treating "lower" as acceptable is the silent
// full-quota amnesty the generation exists to catch.
func TestADifferentStoreIsRefusedInEitherDirection(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	tenant := tenantFor(t)
	limits := []limiter.Limit{tokenBucket("tb", 100, time.Hour, 100)}

	if _, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, StoreGen: 5}); err != nil {
		t.Fatal(err)
	}

	for _, gen := range []uint64{4, 6} {
		before := snapshotKeys(t, client, tenant)
		_, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits, StoreGen: gen})
		if !errors.Is(err, limiter.ErrStoreReset) {
			t.Fatalf("store generation %d against a stored 5 was answered with %v, want ErrStoreReset", gen, err)
		}
		if after := snapshotKeys(t, client, tenant); !sameKeys(before, after) {
			t.Fatalf("a store-reset refusal at generation %d changed state", gen)
		}
	}
}

// An expired fence and an absent fence are the same thing, and an absent fence
// adopts whatever the next caller carries. A TTL here would un-fence every
// quiet tenant on a timer.
func TestTheFenceNeverExpires(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	tenant := tenantFor(t)
	limits := []limiter.Limit{tokenBucket("tb", 10, time.Second, 10)}

	if _, err := checker.Check(ctx, limiter.Request{
		Tenant: tenant, Limits: limits, StoreGen: 1, PolicyGen: 1,
	}); err != nil {
		t.Fatal(err)
	}

	ttl, err := client.PTTL(ctx, limiter.MetaKey(tenant)).Result()
	if err != nil {
		t.Fatal(err)
	}
	// go-redis reports -1 for "exists, no expiry" and -2 for "no such key".
	if ttl != -1 {
		t.Fatalf("the meta key has a ttl of %s; an expired fence is an absent fence", ttl)
	}
}

// Clearing a tenant's counters must not clear its fences. The two live under
// different prefixes so that the scan which finds one cannot find the other --
// a property worth pinning, because the obvious key name would have made the
// counter reset silently un-fence the tenant.
func TestAFenceIsNotACounter(t *testing.T) {
	tenant := "acme"
	counters := "rl1:{" + tenant + "}:"
	if strings.HasPrefix(limiter.MetaKey(tenant), counters) {
		t.Fatalf("the meta key %q sits under the counter prefix %q, so `gatewayctl counters reset` would delete it",
			limiter.MetaKey(tenant), counters+"*")
	}
	// ...but it must still share the hash tag, or one script could not touch
	// both in a Redis Cluster.
	if !strings.Contains(limiter.MetaKey(tenant), "{"+tenant+"}") {
		t.Fatalf("the meta key %q does not carry the tenant hash tag", limiter.MetaKey(tenant))
	}
}

// Fencing is opt-in: a caller that carries no generations is never refused by
// one. That is what lets a single-node deployment, and every test that is not
// about fencing, run unchanged.
func TestZeroGenerationsAreNeverFenced(t *testing.T) {
	client := testClient(t)
	checker := New(client)
	ctx := context.Background()
	tenant := tenantFor(t)
	limits := []limiter.Limit{tokenBucket("tb", 100, time.Hour, 100)}

	// Establish generations from a fenced caller.
	if _, err := checker.Check(ctx, limiter.Request{
		Tenant: tenant, Limits: limits, StoreGen: 9, PolicyGen: 9,
	}); err != nil {
		t.Fatal(err)
	}
	// An unfenced caller still works, and does not disturb what is stored.
	if _, err := checker.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits}); err != nil {
		t.Fatalf("an unfenced caller was refused: %v", err)
	}
	meta, err := client.HGetAll(ctx, limiter.MetaKey(tenant)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if meta["store_gen"] != "9" || meta["policy_gen"] != "9" {
		t.Fatalf("an unfenced caller moved the fences: %v", meta)
	}
}

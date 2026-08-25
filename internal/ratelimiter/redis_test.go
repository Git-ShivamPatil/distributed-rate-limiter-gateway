package ratelimiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisBucket(t *testing.T, capacity, refill float64) (*RedisTokenBucket, *fakeClock) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })

	c := &fakeClock{now: time.Unix(1000, 0)}
	rb := NewRedisTokenBucket(client, capacity, refill).withClock(c)
	return rb, c
}

func TestRedisTokenBucket_AllowsBurstUpToCapacity(t *testing.T) {
	rb, _ := newTestRedisBucket(t, 5, 1)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		allowed, _, err := rb.Allow(ctx, "client-a")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d: expected allowed, got denied", i)
		}
	}

	allowed, retryAfter, err := rb.Allow(ctx, "client-a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if allowed {
		t.Fatal("expected 6th request to be denied once burst is exhausted")
	}
	if retryAfter <= 0 {
		t.Fatalf("expected positive retryAfter, got %v", retryAfter)
	}
}

func TestRedisTokenBucket_RefillsOverTime(t *testing.T) {
	rb, c := newTestRedisBucket(t, 2, 1) // 1 token/sec
	ctx := context.Background()

	rb.Allow(ctx, "client-b")
	rb.Allow(ctx, "client-b")
	if allowed, _, _ := rb.Allow(ctx, "client-b"); allowed {
		t.Fatal("expected bucket to be empty after draining capacity")
	}

	c.Advance(1 * time.Second)
	if allowed, _, _ := rb.Allow(ctx, "client-b"); !allowed {
		t.Fatal("expected one token to have refilled after 1s")
	}
}

func TestRedisTokenBucket_KeysAreIndependent(t *testing.T) {
	rb, _ := newTestRedisBucket(t, 1, 1)
	ctx := context.Background()

	if allowed, _, _ := rb.Allow(ctx, "client-x"); !allowed {
		t.Fatal("client-x should be allowed on first request")
	}
	if allowed, _, _ := rb.Allow(ctx, "client-y"); !allowed {
		t.Fatal("client-y should be independent of client-x")
	}
}

// TestRedisTokenBucket_SharedAcrossReplicas is the whole reason this
// backend exists: two RedisTokenBucket values pointed at the same Redis
// stand in for two gateway replicas, and must share one bucket per
// client rather than each keeping its own count the way TokenBucket
// would.
func TestRedisTokenBucket_SharedAcrossReplicas(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	c := &fakeClock{now: time.Unix(1000, 0)}
	replicaA := NewRedisTokenBucket(client, 2, 1).withClock(c)
	replicaB := NewRedisTokenBucket(client, 2, 1).withClock(c)
	ctx := context.Background()

	if allowed, _, _ := replicaA.Allow(ctx, "shared-client"); !allowed {
		t.Fatal("replica A: expected first request allowed")
	}
	if allowed, _, _ := replicaB.Allow(ctx, "shared-client"); !allowed {
		t.Fatal("replica B: expected second request allowed (shares capacity=2 with replica A)")
	}
	if allowed, _, _ := replicaA.Allow(ctx, "shared-client"); allowed {
		t.Fatal("replica A: expected third request denied -- the bucket is shared, not per-replica")
	}
}

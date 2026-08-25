package ratelimiter

import (
	"context"
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time         { return f.now }
func (f *fakeClock) Advance(d time.Duration) { f.now = f.now.Add(d) }

func newTestBucket(capacity, refill float64) (*TokenBucket, *fakeClock) {
	c := &fakeClock{now: time.Unix(0, 0)}
	tb := NewTokenBucket(capacity, refill).withClock(c)
	return tb, c
}

func TestTokenBucket_AllowsBurstUpToCapacity(t *testing.T) {
	tb, _ := newTestBucket(5, 1)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		allowed, _, err := tb.Allow(ctx, "client-a")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d: expected allowed, got denied", i)
		}
	}

	allowed, retryAfter, _ := tb.Allow(ctx, "client-a")
	if allowed {
		t.Fatal("expected 6th request to be denied once burst is exhausted")
	}
	if retryAfter <= 0 {
		t.Fatalf("expected positive retryAfter, got %v", retryAfter)
	}
}

func TestTokenBucket_RefillsOverTime(t *testing.T) {
	tb, c := newTestBucket(2, 1) // 1 token/sec
	ctx := context.Background()

	tb.Allow(ctx, "client-b")
	tb.Allow(ctx, "client-b")
	if allowed, _, _ := tb.Allow(ctx, "client-b"); allowed {
		t.Fatal("expected bucket to be empty after draining capacity")
	}

	c.Advance(1 * time.Second)
	if allowed, _, _ := tb.Allow(ctx, "client-b"); !allowed {
		t.Fatal("expected one token to have refilled after 1s")
	}
}

func TestTokenBucket_KeysAreIndependent(t *testing.T) {
	tb, _ := newTestBucket(1, 1)
	ctx := context.Background()

	if allowed, _, _ := tb.Allow(ctx, "client-a"); !allowed {
		t.Fatal("client-a should be allowed on first request")
	}
	if allowed, _, _ := tb.Allow(ctx, "client-a"); allowed {
		t.Fatal("client-a should be denied on second request")
	}
	if allowed, _, _ := tb.Allow(ctx, "client-c"); !allowed {
		t.Fatal("client-c should be allowed independently of client-a")
	}
}

func TestTokenBucket_NeverExceedsCapacity(t *testing.T) {
	tb, c := newTestBucket(3, 1)
	ctx := context.Background()
	tb.Allow(ctx, "client-d") // 3 -> 2 tokens

	c.Advance(1 * time.Hour) // plenty of time to refill past capacity

	allowedCount := 0
	for i := 0; i < 5; i++ {
		if allowed, _, _ := tb.Allow(ctx, "client-d"); allowed {
			allowedCount++
		}
	}
	if allowedCount != 3 {
		t.Fatalf("expected exactly 3 allowed requests (capacity), got %d", allowedCount)
	}
}

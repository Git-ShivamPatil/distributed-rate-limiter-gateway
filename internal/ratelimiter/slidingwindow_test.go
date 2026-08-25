package ratelimiter

import (
	"context"
	"testing"
	"time"
)

func newTestSlidingWindow(limit int, window time.Duration) (*SlidingWindowLog, *fakeClock) {
	c := &fakeClock{now: time.Unix(0, 0)}
	sw := NewSlidingWindowLog(limit, window).withClock(c)
	return sw, c
}

func TestSlidingWindowLog_AllowsUpToLimit(t *testing.T) {
	sw, _ := newTestSlidingWindow(3, time.Second)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		allowed, _, err := sw.Allow(ctx, "client-a")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !allowed {
			t.Fatalf("request %d: expected allowed, got denied", i)
		}
	}

	if allowed, retryAfter, _ := sw.Allow(ctx, "client-a"); allowed || retryAfter <= 0 {
		t.Fatalf("expected 4th request denied with positive retryAfter, got allowed=%v retryAfter=%v", allowed, retryAfter)
	}
}

func TestSlidingWindowLog_ExpiresOldHits(t *testing.T) {
	sw, c := newTestSlidingWindow(2, time.Second)
	ctx := context.Background()

	sw.Allow(ctx, "client-b")
	sw.Allow(ctx, "client-b")
	if allowed, _, _ := sw.Allow(ctx, "client-b"); allowed {
		t.Fatal("expected 3rd request denied within the same window")
	}

	c.Advance(1100 * time.Millisecond) // past the 1s window
	if allowed, _, _ := sw.Allow(ctx, "client-b"); !allowed {
		t.Fatal("expected request allowed once the earlier hits aged out")
	}
}

func TestSlidingWindowLog_KeysAreIndependent(t *testing.T) {
	sw, _ := newTestSlidingWindow(1, time.Second)
	ctx := context.Background()

	if allowed, _, _ := sw.Allow(ctx, "client-a"); !allowed {
		t.Fatal("client-a should be allowed on first request")
	}
	if allowed, _, _ := sw.Allow(ctx, "client-c"); !allowed {
		t.Fatal("client-c should be independent of client-a")
	}
}

// TestSlidingWindowLog_NoDoubleRateAtBoundary is the classic interview
// point comparing sliding window log against a naive fixed window: a
// fixed window resets its counter every tick, so a client can send N
// requests right before the tick and N more right after -- 2x the
// intended rate for a moment. A sliding window log must not allow that.
func TestSlidingWindowLog_NoDoubleRateAtBoundary(t *testing.T) {
	sw, c := newTestSlidingWindow(2, time.Second)
	ctx := context.Background()

	sw.Allow(ctx, "client-d")
	c.Advance(900 * time.Millisecond)
	sw.Allow(ctx, "client-d")

	// Both hits are still within 1s of themselves -- a 3rd request 50ms
	// later must be denied, unlike a fixed window that would have just
	// reset its counter at the 1s tick.
	c.Advance(50 * time.Millisecond)
	if allowed, _, _ := sw.Allow(ctx, "client-d"); allowed {
		t.Fatal("expected request denied -- both prior hits are still within the trailing window")
	}
}

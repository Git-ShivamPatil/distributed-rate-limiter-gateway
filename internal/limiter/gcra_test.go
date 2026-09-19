package limiter

import (
	"testing"
	"time"
)

func tokenBucket(count int64, period time.Duration, burst int64) Limit {
	return Limit{Name: "tb", Algorithm: AlgorithmTokenBucket, Count: count, Period: period, Burst: burst}
}

// A cold bucket admits exactly its burst and not one more.
func TestGCRABurstIsExact(t *testing.T) {
	l := tokenBucket(20, time.Minute, 20)
	now := time.Unix(1_700_000_000, 0)

	var tat time.Time
	for i := 1; i <= 20; i++ {
		var d Decision
		tat, d = gcraCheck(l, tat, now, 1)
		if !d.Allowed {
			t.Fatalf("request %d of the burst was refused", i)
		}
		if want := int64(20 - i); d.Remaining != want {
			t.Fatalf("request %d: remaining = %d, want %d", i, d.Remaining, want)
		}
	}

	before := tat
	tat, d := gcraCheck(l, tat, now, 1)
	if d.Allowed {
		t.Fatal("the 21st request was admitted; the burst is not exact")
	}
	if !tat.Equal(before) {
		t.Fatal("a refused request moved the theoretical arrival time, so it consumed quota")
	}
	// 20 per minute is one token per 3s, and the bucket is empty.
	if want := 3 * time.Second; d.RetryAfter != want {
		t.Fatalf("retry after = %s, want %s", d.RetryAfter, want)
	}
	if d.Remaining != 0 {
		t.Fatalf("remaining = %d, want 0", d.Remaining)
	}
}

// Quota accrues continuously rather than in steps at a window boundary.
func TestGCRARefillIsContinuous(t *testing.T) {
	l := tokenBucket(20, time.Minute, 20)
	now := time.Unix(1_700_000_000, 0)

	var tat time.Time
	for i := 0; i < 20; i++ {
		tat, _ = gcraCheck(l, tat, now, 1)
	}

	// One emission interval short of a token: still refused.
	now = now.Add(3*time.Second - time.Nanosecond)
	tatAfter, d := gcraCheck(l, tat, now, 1)
	if d.Allowed {
		t.Fatal("a token was handed out a nanosecond early")
	}
	if d.RetryAfter != time.Nanosecond {
		t.Fatalf("retry after = %s, want 1ns", d.RetryAfter)
	}

	// One nanosecond later, exactly one token exists.
	now = now.Add(time.Nanosecond)
	tatAfter, d = gcraCheck(l, tat, now, 1)
	if !d.Allowed {
		t.Fatal("the refilled token was not handed out")
	}
	if _, d2 := gcraCheck(l, tatAfter, now, 1); d2.Allowed {
		t.Fatal("two tokens were handed out when only one had accrued")
	}
}

// A long idle period refills to the burst and no further: idle time does not
// accumulate into a bigger burst.
func TestGCRAIdleDoesNotAccumulate(t *testing.T) {
	l := tokenBucket(20, time.Minute, 20)
	now := time.Unix(1_700_000_000, 0)

	var tat time.Time
	for i := 0; i < 20; i++ {
		tat, _ = gcraCheck(l, tat, now, 1)
	}

	now = now.Add(24 * time.Hour)
	allowed := 0
	for i := 0; i < 100; i++ {
		var d Decision
		tat, d = gcraCheck(l, tat, now, 1)
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 20 {
		t.Fatalf("after a day idle the bucket admitted %d, want exactly the burst of 20", allowed)
	}
}

// Cost is charged in whole units and an unaffordable cost changes nothing.
func TestGCRACost(t *testing.T) {
	l := tokenBucket(10, time.Second, 10)
	now := time.Unix(1_700_000_000, 0)

	tat, d := gcraCheck(l, time.Time{}, now, 4)
	if !d.Allowed || d.Remaining != 6 {
		t.Fatalf("cost 4 against 10: allowed=%v remaining=%d, want true/6", d.Allowed, d.Remaining)
	}

	before := tat
	tat, d = gcraCheck(l, tat, now, 7)
	if d.Allowed {
		t.Fatal("cost 7 was admitted with 6 units left")
	}
	if !tat.Equal(before) {
		t.Fatal("the refused cost-7 request still consumed quota")
	}
	if _, d = gcraCheck(l, tat, now, 6); !d.Allowed {
		t.Fatal("cost 6 was refused with exactly 6 units left")
	}
}

// A peek reports the same headroom a request would see, and consumes nothing.
func TestGCRAPeekConsumesNothing(t *testing.T) {
	l := tokenBucket(5, time.Second, 5)
	now := time.Unix(1_700_000_000, 0)

	tat, _ := gcraCheck(l, time.Time{}, now, 2)
	for i := 0; i < 10; i++ {
		after, d := gcraCheck(l, tat, now, 0)
		if !after.Equal(tat) {
			t.Fatal("a peek moved the theoretical arrival time")
		}
		if d.Remaining != 3 {
			t.Fatalf("peek remaining = %d, want 3", d.Remaining)
		}
	}
}

// The stored state stops meaning anything at its expiry, which is what makes
// discarding the key safe. Past that instant the bucket is full either way.
func TestGCRAExpiryEqualsFullBucket(t *testing.T) {
	l := tokenBucket(20, time.Minute, 20)
	now := time.Unix(1_700_000_000, 0)

	var tat time.Time
	for i := 0; i < 20; i++ {
		tat, _ = gcraCheck(l, tat, now, 1)
	}
	expiry := gcraExpiry(tat)
	if !expiry.Equal(now.Add(60 * time.Second)) {
		t.Fatalf("expiry = %s, want now+60s (the time to refill 20 at 20/min)", expiry.Sub(now))
	}

	at := expiry
	_, kept := gcraCheck(l, tat, at, 1)
	_, dropped := gcraCheck(l, time.Time{}, at, 1)
	if kept.Allowed != dropped.Allowed || kept.Remaining != dropped.Remaining {
		t.Fatalf("at the expiry instant the state matters: kept=%+v dropped=%+v", kept, dropped)
	}
}

// Rounding is conservative: the effective rate never exceeds the configured one.
func TestGCRAEmissionRoundsUp(t *testing.T) {
	// 3 per second does not divide evenly into nanoseconds.
	l := tokenBucket(3, time.Second, 3)
	if got, want := l.emission(), 333_333_334*time.Nanosecond; got != want {
		t.Fatalf("emission = %s, want %s (rounded up, so the rate is never faster than asked)", got, want)
	}
	if l.emission()*3 < time.Second {
		t.Fatal("three emission intervals are shorter than the period: the effective rate is too fast")
	}
}

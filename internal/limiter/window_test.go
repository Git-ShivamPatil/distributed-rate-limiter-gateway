package limiter

import (
	"testing"
	"time"
)

func window(count int64, period time.Duration) Limit {
	return Limit{Name: "sw", Algorithm: AlgorithmSlidingWindow, Count: count, Period: period}
}

func TestWindowCountIsExact(t *testing.T) {
	l := window(5, time.Second)
	now := time.Unix(1_700_000_000, 0)

	var log []time.Time
	for i := 1; i <= 5; i++ {
		var d Decision
		log, d = windowCheck(l, log, now, 1)
		if !d.Allowed {
			t.Fatalf("request %d was refused inside the limit", i)
		}
		if want := int64(5 - i); d.Remaining != want {
			t.Fatalf("request %d: remaining = %d, want %d", i, d.Remaining, want)
		}
	}

	before := len(log)
	log, d := windowCheck(l, log, now, 1)
	if d.Allowed {
		t.Fatal("the 6th request in the window was admitted")
	}
	if len(log) != before {
		t.Fatal("a refused request was written into the log")
	}
	if d.RetryAfter != time.Second {
		t.Fatalf("retry after = %s, want 1s", d.RetryAfter)
	}
}

// This is the reason the algorithm is a log and not a fixed-window counter.
//
// A fixed window that resets on the second boundary lets a tenant send Count
// just before the reset and Count just after -- 2*Count inside one window of
// Period. Here that is impossible by construction.
func TestWindowHasNoBoundaryBurst(t *testing.T) {
	const count = 5
	l := window(count, time.Second)
	start := time.Unix(1_700_000_000, 0)

	// Spend the whole allowance at the very end of a notional window.
	now := start.Add(999 * time.Millisecond)
	var log []time.Time
	for i := 0; i < count; i++ {
		log, _ = windowCheck(l, log, now, 1)
	}

	// A fixed-window counter would reset here and admit Count more.
	now = start.Add(time.Second)
	admitted := 0
	for i := 0; i < count; i++ {
		var d Decision
		log, d = windowCheck(l, log, now, 1)
		if d.Allowed {
			admitted++
		}
	}
	if admitted != 0 {
		t.Fatalf("%d requests were admitted just after a window boundary; the window is not sliding", admitted)
	}

	// Only once the first entries age out does room appear, one at a time.
	now = start.Add(999*time.Millisecond + time.Second + time.Nanosecond)
	log, d := windowCheck(l, log, now, 1)
	if !d.Allowed {
		t.Fatal("no room appeared after the oldest entries aged out")
	}
	if _, d2 := windowCheck(l, log, now, count); d2.Allowed {
		t.Fatal("the whole allowance was admitted again after a single entry aged out")
	}
}

// The window is half-open: an entry at exactly now-Period has left it.
func TestWindowIsHalfOpen(t *testing.T) {
	l := window(1, time.Second)
	now := time.Unix(1_700_000_000, 0)

	log, _ := windowCheck(l, nil, now, 1)

	if _, d := windowCheck(l, log, now.Add(time.Second-time.Nanosecond), 1); d.Allowed {
		t.Fatal("an entry one nanosecond short of the period had already aged out")
	}
	if _, d := windowCheck(l, log, now.Add(time.Second), 1); !d.Allowed {
		t.Fatal("an entry at exactly now-period was still counted as inside the window")
	}
}

func TestWindowRetryAfterPointsAtTheRightEntry(t *testing.T) {
	l := window(3, 10*time.Second)
	base := time.Unix(1_700_000_000, 0)

	var log []time.Time
	log, _ = windowCheck(l, log, base, 1)
	log, _ = windowCheck(l, log, base.Add(2*time.Second), 1)
	log, _ = windowCheck(l, log, base.Add(4*time.Second), 1)

	// Asking for one unit at t=5s must wait for the oldest (t=0) to age out.
	now := base.Add(5 * time.Second)
	_, d := windowCheck(l, log, now, 1)
	if d.Allowed {
		t.Fatal("a 4th request was admitted into a window of 3")
	}
	if want := 5 * time.Second; d.RetryAfter != want {
		t.Fatalf("retry after = %s, want %s (the oldest entry leaves at t=10s)", d.RetryAfter, want)
	}

	// Asking for three units has to wait for the third-oldest (t=4s).
	_, d3 := windowCheck(l, log, now, 3)
	if want := 9 * time.Second; d3.RetryAfter != want {
		t.Fatalf("retry after for cost 3 = %s, want %s (room for 3 exists once the t=4s entry leaves)", d3.RetryAfter, want)
	}
}

func TestWindowExpiryEqualsEmptyLog(t *testing.T) {
	l := window(3, time.Second)
	now := time.Unix(1_700_000_000, 0)

	log, _ := windowCheck(l, nil, now, 3)
	expiry := windowExpiry(l, log)
	if !expiry.Equal(now.Add(time.Second)) {
		t.Fatalf("expiry = %s, want newest entry + period", expiry.Sub(now))
	}

	_, kept := windowCheck(l, log, expiry, 3)
	_, dropped := windowCheck(l, nil, expiry, 3)
	if kept.Allowed != dropped.Allowed || kept.Remaining != dropped.Remaining {
		t.Fatalf("at the expiry instant the log still matters: kept=%+v dropped=%+v", kept, dropped)
	}
}

func TestWindowPeekConsumesNothing(t *testing.T) {
	l := window(4, time.Second)
	now := time.Unix(1_700_000_000, 0)

	log, _ := windowCheck(l, nil, now, 2)
	for i := 0; i < 5; i++ {
		after, d := windowCheck(l, log, now, 0)
		if len(after) != len(log) {
			t.Fatal("a peek changed the log")
		}
		if d.Remaining != 2 {
			t.Fatalf("peek remaining = %d, want 2", d.Remaining)
		}
	}
}

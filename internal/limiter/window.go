package limiter

import "time"

// The sliding window keeps one timestamp per admitted unit of cost and counts
// the ones still inside the trailing window. That is more expensive than a
// counter -- O(Count) memory per key, which is why Limit.Validate caps Count
// for this algorithm -- and it buys exactness at the window boundary.
//
// A fixed-window counter resets at a wall-clock instant, so a tenant with a
// limit of N can send N just before the reset and N just after: 2N inside one
// window of Period. The "sliding window counter" approximation weights the
// previous window to smooth that out, but it still assumes traffic was evenly
// spread inside the previous window, and so it admits more than N in some
// windows and fewer in others. A log assumes nothing: at most Count units are
// admitted in ANY window of Period. That is what makes it the right choice for
// the strict per-endpoint policies, and the wrong choice for a high-rate
// tenant-wide quota.
//
// The window is half-open: an entry at exactly now-Period has aged out.

// windowCheck evaluates cost against the log, which must be sorted ascending.
//
// It returns the log to store and the decision. On refusal the log is returned
// pruned but otherwise unchanged -- a refused request leaves no trace.
func windowCheck(l Limit, log []time.Time, now time.Time, cost int64) ([]time.Time, Decision) {
	cutoff := now.Add(-l.Period)

	// Entries are appended in time order, so everything that has aged out is a
	// prefix and one scan finds where the live part starts.
	drop := 0
	for drop < len(log) && !log[drop].After(cutoff) {
		drop++
	}
	log = log[drop:]

	d := Decision{Name: l.Name, Limit: l.Count}
	if len(log) > 0 {
		d.ResetAfter = log[0].Add(l.Period).Sub(now)
	}

	used := int64(len(log))
	remaining := l.Count - used
	if remaining < 0 {
		remaining = 0
	}

	if cost == 0 { // peek
		d.Allowed = true
		d.Remaining = remaining
		return log, d
	}

	if used+cost > l.Count {
		// Enough entries have to age out to make room for cost. The one that
		// matters is the (used+cost-Count)-th oldest; when it leaves the
		// window there is exactly enough space.
		idx := used + cost - l.Count - 1
		d.Allowed = false
		d.Remaining = remaining
		d.RetryAfter = log[idx].Add(l.Period).Sub(now)
		return log, d
	}

	for i := int64(0); i < cost; i++ {
		log = append(log, now)
	}
	d.Allowed = true
	d.Remaining = l.Count - int64(len(log))
	d.ResetAfter = log[0].Add(l.Period).Sub(now)
	return log, d
}

// windowExpiry is the instant the log stops carrying information: when its
// newest entry ages out, an empty log and an absent key are the same state.
func windowExpiry(l Limit, log []time.Time) time.Time {
	if len(log) == 0 {
		return time.Time{}
	}
	return log[len(log)-1].Add(l.Period)
}

package limiter

import "time"

// GCRA -- the generic cell rate algorithm -- is a token bucket with the state
// turned inside out. Instead of storing (tokens, last_refill) and recomputing
// the refill on every read, it stores one timestamp: the theoretical arrival
// time (TAT) of the next request that would conform exactly.
//
// The correspondence is exact, not approximate:
//
//	tokens(t) = (tolerance - (tat - t)) / emission
//
// A bucket is full when tat <= t, which is also the moment the state stops
// meaning anything and can be discarded -- which is what makes key expiry safe
// rather than a source of free quota. See gcraExpiry.
//
//	emission  = Period / Count, rounded up  (time for one unit of quota)
//	tolerance = emission * Capacity         (how far ahead of now the TAT may sit)
//
// A request of cost c is admitted when tat + c*emission - tolerance <= now.

// gcraCheck evaluates cost against the stored theoretical arrival time.
//
// A zero tat means "no state", which is identical to a full bucket. It returns
// the TAT to store (unchanged when the request is refused, because a refused
// request must not consume quota) and the decision.
func gcraCheck(l Limit, tat, now time.Time, cost int64) (time.Time, Decision) {
	emission := l.emission()
	tolerance := l.tolerance()
	capacity := l.Capacity()

	// An empty or stale TAT is a full bucket.
	if tat.Before(now) {
		tat = now
	}

	d := Decision{Name: l.Name, Limit: capacity}

	// Headroom before this request, which is what a refusal reports.
	headroom := func(t time.Time) int64 {
		free := tolerance - t.Sub(now)
		if free < 0 {
			return 0
		}
		return int64(free / emission)
	}

	if cost == 0 { // a peek consumes nothing and always "allows"
		d.Allowed = true
		d.Remaining = headroom(tat)
		d.ResetAfter = tat.Sub(now)
		return tat, d
	}

	next := tat.Add(emission * time.Duration(cost))
	allowAt := next.Add(-tolerance)

	if allowAt.After(now) {
		d.Allowed = false
		d.Remaining = headroom(tat)
		d.RetryAfter = allowAt.Sub(now)
		d.ResetAfter = tat.Sub(now)
		return tat, d
	}

	d.Allowed = true
	d.Remaining = headroom(next)
	d.ResetAfter = next.Sub(now)
	return next, d
}

// gcraExpiry is the instant at which the stored state stops carrying
// information -- the moment the bucket is full again.
//
// Discarding the key at exactly this instant is not an approximation and not a
// memory-pressure compromise: an absent key and a full bucket are the same
// state. Both the in-memory store and the Redis script expire on this rule, so
// neither can hand out quota by forgetting.
func gcraExpiry(tat time.Time) time.Time { return tat }

package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// SlidingWindowLog is an in-memory rate limiter that tracks the exact
// timestamp of every request in the trailing window. Compared to
// TokenBucket it's more accurate right at window boundaries -- a token
// bucket can allow a burst, then a drought, then another burst, while a
// sliding window log enforces "at most N requests in any trailing
// window" exactly. The cost is remembering every timestamp instead of
// one float per client.
type SlidingWindowLog struct {
	mu     sync.Mutex
	hits   map[string][]time.Time
	limit  int
	window time.Duration
	clock  clock
}

// NewSlidingWindowLog creates a limiter that allows at most limit
// requests in any trailing window of time, per key.
func NewSlidingWindowLog(limit int, window time.Duration) *SlidingWindowLog {
	return &SlidingWindowLog{
		hits:   make(map[string][]time.Time),
		limit:  limit,
		window: window,
		clock:  realClock{},
	}
}

// withClock swaps in a fake clock -- used only by tests.
func (s *SlidingWindowLog) withClock(c clock) *SlidingWindowLog {
	s.clock = c
	return s
}

func (s *SlidingWindowLog) Allow(_ context.Context, key string) (bool, time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()
	cutoff := now.Add(-s.window)

	hits := s.hits[key]
	kept := hits[:0] // filter in place -- drop timestamps that aged out
	for _, t := range hits {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}

	if len(kept) >= s.limit {
		retryAfter := kept[0].Add(s.window).Sub(now)
		s.hits[key] = kept
		return false, retryAfter, nil
	}

	kept = append(kept, now)
	s.hits[key] = kept
	return true, 0, nil
}

package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// clock abstracts time so tests can advance it deterministically instead
// of calling time.Sleep and slowing the suite down.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type bucketState struct {
	tokens     float64
	lastRefill time.Time
}

// TokenBucket is an in-memory, per-process rate limiter: fast, zero
// external dependencies, safe for concurrent use. Its one limitation is
// exactly why RedisTokenBucket will exist later -- state lives only in
// this process's memory, so two replicas of the gateway do NOT share a
// limit for the same client.
type TokenBucket struct {
	mu       sync.Mutex
	buckets  map[string]*bucketState
	capacity float64 // max tokens a bucket holds -- the burst size
	refill   float64 // tokens added per second
	clock    clock
}

// NewTokenBucket creates an in-memory limiter. capacity is the maximum
// burst size; refillPerSecond is the steady-state allowed rate.
func NewTokenBucket(capacity, refillPerSecond float64) *TokenBucket {
	return &TokenBucket{
		buckets:  make(map[string]*bucketState),
		capacity: capacity,
		refill:   refillPerSecond,
		clock:    realClock{},
	}
}

// withClock swaps in a fake clock -- used only by tests.
func (tb *TokenBucket) withClock(c clock) *TokenBucket {
	tb.clock = c
	return tb
}

func (tb *TokenBucket) Allow(_ context.Context, key string) (bool, time.Duration, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := tb.clock.Now()
	b, ok := tb.buckets[key]
	if !ok {
		b = &bucketState{tokens: tb.capacity, lastRefill: now}
		tb.buckets[key] = b
	}

	if elapsed := now.Sub(b.lastRefill).Seconds(); elapsed > 0 {
		b.tokens = min(tb.capacity, b.tokens+elapsed*tb.refill)
		b.lastRefill = now
	}

	if b.tokens >= 1 {
		b.tokens--
		return true, 0, nil
	}

	deficit := 1 - b.tokens
	wait := time.Duration(deficit / tb.refill * float64(time.Second))
	return false, wait, nil
}

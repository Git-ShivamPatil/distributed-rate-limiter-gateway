package limiter

import (
	"sync"
	"time"
)

// Clock is the time source a limiter reads.
//
// Which clock a limiter reads is a correctness property, not a detail. The
// in-memory limiter reads this process's clock, which is right because it is
// also the only process that can see its own counters. The Redis limiter must
// NOT read this one: several gateway replicas share those counters, their
// clocks drift apart, and a sliding window evaluated against two different
// "now"s admits a different number of requests depending on which replica a
// request lands on. The Redis limiter reads Redis's own TIME instead, inside
// the script, so every replica gets the same answer to "what time is it".
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// RealClock reads the wall clock of this process.
func RealClock() Clock { return realClock{} }

// FakeClock is a manually advanced clock. Tests use it so that a suite about
// refill behaviour does not have to sleep through the refill.
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock starts a fake clock at t.
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{now: t} }

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance moves the clock forward by d.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Package limiter decides whether a request fits inside a tenant's quota.
//
// Two algorithms sit behind one interface:
//
//   - token_bucket, expressed as GCRA (the generic cell rate algorithm). A
//     bucket of `Burst` tokens refilling at `Count` tokens per `Period` is
//     exactly equivalent to a single timestamp -- the theoretical arrival time
//     of the next conforming request -- which is why the whole state is one
//     integer instead of a (tokens, last_refill) pair that has to be written
//     back on every read.
//   - sliding_window, a log of the timestamps in the trailing window. It is
//     exact: at most Count requests in ANY window of Period, including across
//     a window boundary, which is the thing a fixed-window counter gets wrong.
//
// Both are implemented twice -- once in Go, once in Lua inside Redis -- and the
// Go implementation is the oracle the Lua one is differentially tested against.
// That is only possible because both read their clock from the same place and
// both derive their keys from this file.
package limiter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// Algorithm names a limiting strategy. The string values are part of the
// config file, the database schema and the gRPC API, so they do not change.
type Algorithm string

const (
	AlgorithmTokenBucket   Algorithm = "token_bucket"
	AlgorithmSlidingWindow Algorithm = "sliding_window"
)

// Bounds on what a policy may ask for. These are not taste: MaxPeriod and
// MaxCount together keep every intermediate value in the GCRA arithmetic
// inside int64 nanoseconds, and MaxWindowCount keeps a sliding-window log from
// costing more memory than the quota it enforces is worth.
const (
	MaxPeriod      = 24 * time.Hour
	MaxCount       = 100_000_000
	MaxWindowCount = 100_000
)

// Limit is one quota rule: Count units per Period, with Burst as the maximum
// instantaneous burst for a token bucket.
type Limit struct {
	// Name identifies the rule within a tenant's policy, and is part of the
	// storage key -- two rules with the same name share a counter.
	Name      string        `json:"name" yaml:"name"`
	Algorithm Algorithm     `json:"algorithm" yaml:"algorithm"`
	Count     int64         `json:"count" yaml:"count"`
	Period    time.Duration `json:"period" yaml:"period"`
	// Burst is the token bucket capacity. Zero means "same as Count", which
	// makes `100 per minute` behave the way most people read it. Unused by
	// sliding_window, where the window itself bounds the burst.
	Burst int64 `json:"burst,omitempty" yaml:"burst,omitempty"`
}

// Validate reports why a limit cannot be enforced, if it cannot.
func (l Limit) Validate() error {
	if l.Name == "" {
		return fmt.Errorf("limit: name is empty")
	}
	if strings.ContainsAny(l.Name, ":{}") {
		return fmt.Errorf("limit %q: name must not contain ':', '{' or '}' -- those delimit the storage key", l.Name)
	}
	switch l.Algorithm {
	case AlgorithmTokenBucket, AlgorithmSlidingWindow:
	default:
		return fmt.Errorf("limit %q: unknown algorithm %q (want %q or %q)",
			l.Name, l.Algorithm, AlgorithmTokenBucket, AlgorithmSlidingWindow)
	}
	if l.Count <= 0 {
		return fmt.Errorf("limit %q: count must be > 0, got %d", l.Name, l.Count)
	}
	if l.Count > MaxCount {
		return fmt.Errorf("limit %q: count %d exceeds the maximum of %d", l.Name, l.Count, MaxCount)
	}
	if l.Period <= 0 {
		return fmt.Errorf("limit %q: period must be > 0, got %s", l.Name, l.Period)
	}
	if l.Period > MaxPeriod {
		return fmt.Errorf("limit %q: period %s exceeds the maximum of %s", l.Name, l.Period, MaxPeriod)
	}
	if l.Burst < 0 {
		return fmt.Errorf("limit %q: burst must be >= 0, got %d", l.Name, l.Burst)
	}
	if l.Burst > MaxCount {
		return fmt.Errorf("limit %q: burst %d exceeds the maximum of %d", l.Name, l.Burst, MaxCount)
	}
	switch l.Algorithm {
	case AlgorithmTokenBucket:
		if l.Period/time.Duration(l.Count) < time.Nanosecond {
			return fmt.Errorf("limit %q: %d per %s is faster than one token per nanosecond", l.Name, l.Count, l.Period)
		}
	case AlgorithmSlidingWindow:
		if l.Burst != 0 && l.Burst != l.Count {
			return fmt.Errorf("limit %q: sliding_window has no separate burst -- the window bounds it; leave burst unset", l.Name)
		}
		if l.Count > MaxWindowCount {
			return fmt.Errorf("limit %q: sliding_window keeps one timestamp per request, so count %d exceeds the maximum of %d; use token_bucket for rates this high",
				l.Name, l.Count, MaxWindowCount)
		}
	}
	return nil
}

// Capacity is the largest cost this limit can ever admit in one request.
func (l Limit) Capacity() int64 {
	if l.Algorithm == AlgorithmSlidingWindow {
		return l.Count
	}
	if l.Burst > 0 {
		return l.Burst
	}
	return l.Count
}

// Quantum is the resolution every limiter works at.
//
// One microsecond, and not one nanosecond, because the Redis implementation
// does its arithmetic in Lua, whose numbers are float64. A nanosecond Unix
// timestamp is about 1.8e18 and float64 stops representing consecutive
// integers above 2^53 (9.0e15), so nanosecond timestamps in Lua would be
// silently rounded -- the sort of error that shows up as a rate limiter that
// is mysteriously 3% generous. Microsecond timestamps are exact until the year
// 287396, and both implementations use the same unit so their arithmetic
// agrees bit for bit. That is what makes them differentially testable.
const Quantum = time.Microsecond

// Emission is the time one unit of quota takes to accrue.
//
// It rounds UP to the next whole quantum, deliberately. Rounding down would
// make the effective rate faster than the configured one, and a limiter that
// admits more than it was told to is a worse failure than one that admits
// fractionally fewer. The cost is that a limit whose period does not divide
// evenly by its count is enforced slightly slowly: 45,000 per second wants
// 22.22us per token and gets 23us, which is 3.4% under. Limits at that rate
// belong to the benchmark rather than to a tenant, and the direction of the
// error is the safe one.
func (l Limit) Emission() time.Duration {
	n := time.Duration(l.Count)
	e := l.Period / n
	if l.Period%n != 0 {
		e++
	}
	if r := e % Quantum; r != 0 {
		e += Quantum - r
	}
	if e < Quantum {
		e = Quantum
	}
	return e
}

// Tolerance is the GCRA delay-variation tolerance: how far ahead of now the
// theoretical arrival time may sit before a request is refused. It is exactly
// Capacity worth of emission intervals, which is what makes the burst exact --
// and why it is derived from Emission rather than rounded separately.
func (l Limit) Tolerance() time.Duration {
	return l.Emission() * time.Duration(l.Capacity())
}

// fingerprint distinguishes counters whose parameters differ.
//
// It is part of the key, so editing a policy starts a fresh counter rather than
// reinterpreting the old one -- a stored GCRA timestamp means "this many
// emission intervals of debt", and the emission interval is exactly what a
// policy edit changes. Carrying it over would silently rescale the debt.
func (l Limit) fingerprint() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d|%d", l.Algorithm, l.Count, l.Period.Nanoseconds(), l.Capacity())))
	return hex.EncodeToString(sum[:4])
}

// Key is where this limit's counter lives for one tenant.
//
// The tenant is wrapped in braces so that every key belonging to a tenant
// hashes to the same Redis Cluster slot. A check evaluates several limits in
// one script, and a multi-key script may only touch one slot.
func (l Limit) Key(tenant string) string {
	return "rl1:{" + tenant + "}:" + l.Name + ":" + l.fingerprint()
}

// Decision is the outcome for a single limit.
type Decision struct {
	Name      string `json:"name"`
	Allowed   bool   `json:"allowed"`
	Limit     int64  `json:"limit"`
	Remaining int64  `json:"remaining"`
	// RetryAfter is how long until this limit would admit the same cost. Zero
	// when the limit allowed the request.
	RetryAfter time.Duration `json:"retry_after"`
	// ResetAfter is how long until this limit is fully replenished.
	ResetAfter time.Duration `json:"reset_after"`
}

// Request is one admission question.
type Request struct {
	Tenant string
	Limits []Limit
	// Cost is how many units this request consumes. Zero is read as one; a
	// peek (cost 0) is expressed by PeekOnly instead, so that a caller cannot
	// silently get a free request by omitting the field.
	Cost int64
	// PeekOnly reports what would happen without consuming anything. It is how
	// GetQuota answers, and it never mutates state.
	PeekOnly bool
}

// EffectiveCost is the cost after the zero-means-one rule.
func (r Request) EffectiveCost() int64 {
	if r.PeekOnly {
		return 0
	}
	if r.Cost <= 0 {
		return 1
	}
	return r.Cost
}

// Result is the outcome of evaluating every limit in a Request.
//
// Evaluation is all-or-nothing: if any limit refuses, nothing is consumed from
// any of them. A request that is going to be rejected must not leave a dent in
// the quota it did not get to use.
type Result struct {
	Allowed bool
	// Decisions holds one entry per limit, in the order they were given.
	Decisions []Decision
	// Limiting names the limit that refused, and is empty when allowed. When
	// several refuse it is the one with the longest RetryAfter, because that is
	// the one the caller actually has to wait for.
	Limiting string
}

// RetryAfter is how long the caller should wait, zero when allowed.
func (r Result) RetryAfter() time.Duration {
	var worst time.Duration
	for _, d := range r.Decisions {
		if !d.Allowed && d.RetryAfter > worst {
			worst = d.RetryAfter
		}
	}
	return worst
}

// Tightest is the decision with the least headroom, which is the one whose
// numbers belong in the X-RateLimit-* headers.
func (r Result) Tightest() (Decision, bool) {
	if len(r.Decisions) == 0 {
		return Decision{}, false
	}
	best := r.Decisions[0]
	for _, d := range r.Decisions[1:] {
		if d.Remaining < best.Remaining {
			best = d
		}
	}
	return best, true
}

// Checker answers admission questions. Every backend -- in-memory, Redis,
// forwarded to another node -- satisfies this, so the HTTP and gRPC layers
// never learn which one is deciding.
type Checker interface {
	Check(ctx context.Context, req Request) (Result, error)
}

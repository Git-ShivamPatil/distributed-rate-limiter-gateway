// Package redis keeps rate-limit counters in Redis, so that every gateway
// replica enforces one quota instead of one each.
//
// The whole decision is one script and one round trip. The script is in
// check.lua next door; this file builds its arguments and reads its answer,
// and deliberately contains no limiting logic of its own -- two
// implementations of the algorithm, one in Go and one in Lua, would drift.
// The Go implementation that does exist (limiter.Memory) is the oracle this
// one is differentially tested against.
package redis

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

//go:embed check.lua
var checkSource string

// Checker evaluates limits against Redis.
type Checker struct {
	client goredis.UniversalClient
	script *goredis.Script

	// clock is a test seam and nothing else. When it is nil -- which is the
	// only way the production constructor can leave it -- the script reads
	// Redis's own TIME, which is the entire point of this backend: replicas
	// with drifting clocks must not disagree about what "now" is.
	clock limiter.Clock
}

// Option configures a Checker.
type Option func(*Checker)

// New builds a Checker over an existing client.
func New(client goredis.UniversalClient, opts ...Option) *Checker {
	c := &Checker{client: client, script: goredis.NewScript(checkSource)}
	for _, o := range opts {
		o(c)
	}
	return c
}

// withClock makes the script use a caller-supplied clock instead of Redis
// TIME. It is unexported so that only this package's own tests can reach it:
// the differential test has to drive both implementations from one fake clock,
// and nothing outside may hand the limiter a gateway's wall clock.
func withClock(c limiter.Clock) Option {
	return func(ch *Checker) { ch.clock = c }
}

// Ping reports whether Redis is reachable, for readiness checks.
func (c *Checker) Ping(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	return nil
}

// Load pre-loads the script so the first request does not pay for it, and
// fails loudly at startup if this Redis cannot run it at all.
func (c *Checker) Load(ctx context.Context) error {
	if err := c.script.Load(ctx, c.client).Err(); err != nil {
		return fmt.Errorf("redis: loading check script: %w", err)
	}
	return nil
}

// Check evaluates every limit in the request in one atomic script.
func (c *Checker) Check(ctx context.Context, req limiter.Request) (limiter.Result, error) {
	cost := req.EffectiveCost()
	for _, l := range req.Limits {
		if err := l.Validate(); err != nil {
			return limiter.Result{}, err
		}
		if cost > l.Capacity() {
			return limiter.Result{}, fmt.Errorf("%w: cost %d against limit %q of %d",
				limiter.ErrCostExceedsCapacity, cost, l.Name, l.Capacity())
		}
	}
	if len(req.Limits) == 0 {
		return limiter.Result{Allowed: true}, nil
	}

	peek := 0
	if req.PeekOnly {
		peek = 1
	}
	override := int64(0)
	if c.clock != nil {
		override = c.clock.Now().UnixMicro()
	}

	keys := make([]string, 0, len(req.Limits))
	argv := make([]any, 0, 3+4*len(req.Limits))
	argv = append(argv, cost, peek, override)

	for _, l := range req.Limits {
		keys = append(keys, l.Key(req.Tenant))
		switch l.Algorithm {
		case limiter.AlgorithmTokenBucket:
			argv = append(argv, "tb", micros(l.Emission()), micros(l.Tolerance()), l.Capacity())
		case limiter.AlgorithmSlidingWindow:
			argv = append(argv, "sw", micros(l.Period), l.Count, l.Count)
		default:
			return limiter.Result{}, fmt.Errorf("redis: unknown algorithm %q", l.Algorithm)
		}
	}

	raw, err := c.script.Run(ctx, c.client, keys, argv...).Result()
	if err != nil {
		return limiter.Result{}, fmt.Errorf("redis: check: %w", err)
	}
	return parseResult(raw, req.Limits)
}

func micros(d time.Duration) int64 { return int64(d / time.Microsecond) }

// ErrMalformedReply means the script answered in a shape this code does not
// understand, which is a bug in one of the two rather than a limit decision.
var ErrMalformedReply = errors.New("redis: malformed reply from check script")

func parseResult(raw any, limits []limiter.Limit) (limiter.Result, error) {
	values, ok := raw.([]any)
	if !ok {
		return limiter.Result{}, fmt.Errorf("%w: %T", ErrMalformedReply, raw)
	}
	want := 1 + 4*len(limits)
	if len(values) != want {
		return limiter.Result{}, fmt.Errorf("%w: %d values for %d limits, want %d",
			ErrMalformedReply, len(values), len(limits), want)
	}

	allowedAll, err := toInt(values[0])
	if err != nil {
		return limiter.Result{}, err
	}

	res := limiter.Result{
		Allowed:   allowedAll == 1,
		Decisions: make([]limiter.Decision, 0, len(limits)),
	}
	for i, l := range limits {
		base := 1 + i*4
		allowed, err := toInt(values[base])
		if err != nil {
			return limiter.Result{}, err
		}
		remaining, err := toInt(values[base+1])
		if err != nil {
			return limiter.Result{}, err
		}
		retry, err := toInt(values[base+2])
		if err != nil {
			return limiter.Result{}, err
		}
		reset, err := toInt(values[base+3])
		if err != nil {
			return limiter.Result{}, err
		}
		res.Decisions = append(res.Decisions, limiter.Decision{
			Name:       l.Name,
			Allowed:    allowed == 1,
			Limit:      l.Capacity(),
			Remaining:  remaining,
			RetryAfter: time.Duration(retry) * time.Microsecond,
			ResetAfter: time.Duration(reset) * time.Microsecond,
		})
	}
	if !res.Allowed {
		res.Limiting = worstRefusal(res.Decisions)
	}
	return res, nil
}

func worstRefusal(ds []limiter.Decision) string {
	var name string
	worst := time.Duration(-1)
	for _, d := range ds {
		if !d.Allowed && d.RetryAfter > worst {
			worst, name = d.RetryAfter, d.Name
		}
	}
	return name
}

func toInt(v any) (int64, error) {
	switch n := v.(type) {
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	case string: // some clients surface integers as strings
		var parsed int64
		if _, err := fmt.Sscanf(n, "%d", &parsed); err != nil {
			return 0, fmt.Errorf("%w: %q is not a number", ErrMalformedReply, n)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("%w: %T is not a number", ErrMalformedReply, v)
	}
}

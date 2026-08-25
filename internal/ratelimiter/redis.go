package ratelimiter

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucketScript runs the whole read-refill-consume cycle as a single
// atomic operation inside Redis. That atomicity is the entire point: run
// this logic as separate GET/SET calls from Go instead, and two gateway
// replicas can both read "1 token left" at the same instant, both decide
// to allow, and the client gets two requests through a bucket that only
// had room for one.
//
// KEYS[1] = bucket key
// ARGV[1] = capacity      (max tokens / burst size)
// ARGV[2] = refill rate   (tokens per second)
// ARGV[3] = now           (unix seconds, float)
// ARGV[4] = ttl seconds   (idle buckets expire instead of leaking memory)
//
// Redis converts a Lua number returned to the client into an integer
// reply, truncating any fraction -- so the token count is returned as a
// string to keep its precision.
const tokenBucketScript = `
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local ttl      = tonumber(ARGV[4])

local data   = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * refill)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call("HMSET", key, "tokens", tostring(tokens), "ts", tostring(now))
redis.call("EXPIRE", key, ttl)

return {allowed, tostring(tokens)}
`

// RedisTokenBucket is a token-bucket limiter backed by Redis, so every
// replica of the gateway enforces one shared limit per client instead of
// each replica counting independently. It satisfies the same Limiter
// interface as TokenBucket, so the middleware doesn't know or care which
// one it's talking to.
type RedisTokenBucket struct {
	client   *redis.Client
	script   *redis.Script
	capacity float64
	refill   float64
	ttl      time.Duration
	prefix   string
	clock    clock
}

// NewRedisTokenBucket creates a Redis-backed limiter with the same
// capacity/refillPerSecond semantics as NewTokenBucket.
func NewRedisTokenBucket(client *redis.Client, capacity, refillPerSecond float64) *RedisTokenBucket {
	return &RedisTokenBucket{
		client:   client,
		script:   redis.NewScript(tokenBucketScript),
		capacity: capacity,
		refill:   refillPerSecond,
		ttl:      10 * time.Minute,
		prefix:   "ratelimit:",
		clock:    realClock{},
	}
}

// withClock swaps in a fake clock -- used only by tests.
func (r *RedisTokenBucket) withClock(c clock) *RedisTokenBucket {
	r.clock = c
	return r
}

func (r *RedisTokenBucket) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	now := float64(r.clock.Now().UnixNano()) / 1e9

	res, err := r.script.Run(ctx, r.client,
		[]string{r.prefix + key},
		r.capacity, r.refill, now, int(r.ttl.Seconds()),
	).Result()
	if err != nil {
		return false, 0, fmt.Errorf("ratelimiter: redis script: %w", err)
	}

	vals, ok := res.([]interface{})
	if !ok || len(vals) != 2 {
		return false, 0, fmt.Errorf("ratelimiter: unexpected script result: %v", res)
	}

	allowedRaw, _ := vals[0].(int64)
	tokensStr, _ := vals[1].(string)
	tokensLeft, _ := strconv.ParseFloat(tokensStr, 64)

	if allowedRaw == 1 {
		return true, 0, nil
	}

	deficit := 1 - tokensLeft
	if deficit < 0 {
		deficit = 0
	}
	wait := time.Duration(deficit / r.refill * float64(time.Second))
	return false, wait, nil
}

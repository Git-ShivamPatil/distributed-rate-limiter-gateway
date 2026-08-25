// Package ratelimiter implements rate-limiting algorithms behind one
// shared interface, so the HTTP layer never needs to know which
// algorithm -- or later, which storage backend -- is actually deciding.
package ratelimiter

import (
	"context"
	"time"
)

// Limiter is the interface every algorithm satisfies. Code that enforces
// limits (the HTTP middleware) depends only on this, so swapping the
// algorithm, or moving state into Redis down the line, never touches
// that code.
type Limiter interface {
	// Allow reports whether the request identified by key is permitted
	// right now. When it is not, retryAfter estimates how long the
	// caller should wait before trying again.
	Allow(ctx context.Context, key string) (allowed bool, retryAfter time.Duration, err error)
}

package middleware

import (
	"log"
	"net"
	"net/http"
	"strconv"

	"distributed-rate-limiter/internal/ratelimiter"
)

// clientKey extracts the identity a request is rate-limited by: an API
// key if the caller sent one, otherwise their IP address.
func clientKey(r *http.Request) string {
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return "key:" + apiKey
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}

// RateLimit wraps next, rejecting requests over the limit with 429 and a
// Retry-After header. It depends only on the Limiter interface, so
// swapping the algorithm -- or later, moving state into Redis -- never
// requires a change here.
func RateLimit(limiter ratelimiter.Limiter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := clientKey(r)
		allowed, retryAfter, err := limiter.Allow(r.Context(), key)
		if err != nil {
			// Fail open: a broken limiter backend shouldn't take the
			// whole gateway down. This matters more once state moves to
			// Redis and the limiter has a network dependency of its own.
			log.Printf("ratelimiter: check failed, failing open: %v", err)
			next.ServeHTTP(w, r)
			return
		}
		if !allowed {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error":"rate limit exceeded"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"distributed-rate-limiter/internal/middleware"
	"distributed-rate-limiter/internal/ratelimiter"
)

func main() {
	limiter := buildLimiter()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	handler := middleware.RateLimit(limiter, mux)

	addr := envOr("GATEWAY_ADDR", ":8080")
	srv := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	log.Printf("gateway listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

// buildLimiter picks the rate limiter from env vars: RATE_LIMIT_BACKEND
// chooses where state lives (memory, single instance only -- or redis,
// shared across replicas); RATE_LIMIT_ALGORITHM chooses token bucket vs
// sliding window for the in-memory backend. The Redis backend is token
// bucket only for now -- a Redis-backed sliding window (via a sorted
// set) is a natural follow-up but isn't built yet.
func buildLimiter() ratelimiter.Limiter {
	capacity := envFloat("RATE_LIMIT_CAPACITY", 20)
	refill := envFloat("RATE_LIMIT_REFILL_PER_SEC", 5)

	if envOr("RATE_LIMIT_BACKEND", "memory") == "redis" {
		return buildRedisLimiter(capacity, refill)
	}

	switch envOr("RATE_LIMIT_ALGORITHM", "token_bucket") {
	case "sliding_window":
		limit := envInt("RATE_LIMIT_MAX_REQUESTS", 20)
		window := envDuration("RATE_LIMIT_WINDOW", time.Second)
		log.Printf("rate limiter: sliding window log (%d req / %s)", limit, window)
		return ratelimiter.NewSlidingWindowLog(limit, window)
	default:
		log.Printf("rate limiter: in-memory token bucket (capacity=%.0f, refill=%.0f/s)", capacity, refill)
		return ratelimiter.NewTokenBucket(capacity, refill)
	}
}

// buildRedisLimiter connects to Redis and fails fast (instead of failing
// on the first request) if it's unreachable, so a misconfigured
// REDIS_ADDR shows up immediately in the startup logs.
func buildRedisLimiter(capacity, refill float64) ratelimiter.Limiter {
	addr := envOr("REDIS_ADDR", "localhost:6379")
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"), // empty means no auth
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("could not connect to redis at %s: %v", addr, err)
	}

	log.Printf("rate limiter: redis token bucket (%s, capacity=%.0f, refill=%.0f/s)", addr, capacity, refill)
	return ratelimiter.NewRedisTokenBucket(client, capacity, refill)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

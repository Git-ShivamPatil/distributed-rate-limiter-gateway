# Distributed Rate Limiter & API Gateway

> **Status: in active development.** The performance figures on the portfolio site
> (`45K req/s · <8ms p99`) are **targets, not measurements.** Nothing has been
> benchmarked yet. See [CLAIMS.md](CLAIMS.md).

Full milestone plan and progress tracking: `Desktop/Projects/PROGRAM.md` → section
`02 · Distributed Rate Limiter & API Gateway`.


A rate-limiting API gateway in Go, built incrementally as a portfolio project.

## Roadmap

- [x] Core algorithms: token bucket + sliding window log, both behind one `Limiter` interface
- [x] HTTP middleware -- per-client keying (API key or IP), 429 + Retry-After on denial
- [x] Redis-backed limiter -- shared state across gateway replicas, atomic via a Lua script
- [ ] PostgreSQL -- per-client limit configs, audit log
- [ ] gRPC API alongside REST
- [ ] Consistent hashing + Raft leader election for shard ownership and failover
- [ ] AKS deployment, Prometheus/Grafana
- [ ] React dashboard for live traffic/quota
- [ ] Load test with k6 -- req/sec and p99 latency

## Design

`internal/ratelimiter.Limiter` is a one-method interface (`Allow`). Three
implementations satisfy it today:

- **Token bucket** (in-memory) -- refills continuously from elapsed real
  time, allows short bursts up to a capacity. Cheap: one float and one
  timestamp per client. Each gateway replica keeps its own count.
- **Sliding window log** (in-memory) -- keeps every request timestamp in
  the trailing window. More accurate at window boundaries (no
  burst-then-drought pattern a fixed window can have), at the cost of
  remembering every hit instead of one number. Also per-replica.
- **Redis token bucket** -- same algorithm as the in-memory version, but
  state lives in Redis, so every replica shares one bucket per client.
  The refill-and-consume step runs as a single Lua script inside Redis,
  so two replicas can't both read "1 token left" and both let a request
  through -- the read, refill, and decrement happen as one atomic
  operation instead of three round trips.

The HTTP middleware depends only on `Limiter`, so switching which one is
active is a config change in `main.go`, not a code change.

## Running

```bash
go mod tidy
go test ./... -v
```

`go-redis` needs a fairly recent Go release. If `go mod tidy` downloads
an extra Go toolchain version the first time, that's expected -- it's Go
automatically satisfying that requirement, not an error.

In-memory backend (default):

```bash
go run ./cmd/gateway
```

Redis backend:

```bash
docker compose up -d redis
```

```powershell
$env:RATE_LIMIT_BACKEND = "redis"
$env:REDIS_ADDR = "localhost:6379"
go run ./cmd/gateway
```

```bash
# macOS/Linux
RATE_LIMIT_BACKEND=redis REDIS_ADDR=localhost:6379 go run ./cmd/gateway
```

Or the whole stack with Docker:

```bash
docker compose up --build
```

## Config

| Var | Default | Meaning |
|---|---|---|
| `GATEWAY_ADDR` | `:8080` | listen address |
| `RATE_LIMIT_BACKEND` | `memory` | `memory` or `redis` |
| `RATE_LIMIT_ALGORITHM` | `token_bucket` | `token_bucket` or `sliding_window` (memory backend only) |
| `REDIS_ADDR` | `localhost:6379` | Redis address (redis backend only) |
| `REDIS_PASSWORD` | (empty) | Redis auth password, if any |
| `RATE_LIMIT_CAPACITY` | `20` | token bucket burst size |
| `RATE_LIMIT_REFILL_PER_SEC` | `5` | token bucket steady-state rate |
| `RATE_LIMIT_MAX_REQUESTS` | `20` | sliding window: max requests per window |
| `RATE_LIMIT_WINDOW` | `1s` | sliding window: window size |

## Verifying the limit

PowerShell:

```powershell
1..25 | ForEach-Object { curl.exe -s -o NUL -w "%{http_code}\n" http://localhost:8080/ }
```

macOS/Linux:

```bash
for i in $(seq 1 25); do curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/; done
```

First ~20 requests return `200` (the burst capacity), the rest `429`
until tokens/window allow more.

## Proving state is actually shared (Redis backend)

Run two gateways against the same Redis and alternate requests between
them -- if they were still counting independently (like the in-memory
backend), each would allow its own ~20 before limiting. Sharing one
Redis bucket, the combined total across both should cap at ~20.

```powershell
docker compose up -d redis
$env:RATE_LIMIT_BACKEND = "redis"; $env:GATEWAY_ADDR = ":8080"; go run ./cmd/gateway
# in a second terminal:
$env:RATE_LIMIT_BACKEND = "redis"; $env:GATEWAY_ADDR = ":8081"; go run ./cmd/gateway
# in a third terminal:
1..25 | ForEach-Object { $port = 8080 + ($_ % 2); curl.exe -s -o NUL -w "%{http_code} (port $port)\n" http://localhost:$port/ }
```

Postgres is defined in `docker-compose.yml` for an upcoming milestone
but isn't wired into the gateway yet.

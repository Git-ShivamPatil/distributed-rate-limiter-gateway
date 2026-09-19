<div align="center">

# Distributed Rate Limiter & API Gateway

**Multi-tenant gateway · token-bucket and sliding-window quotas · consistent-hash shard ring · Raft election over shard failure**

![status](https://img.shields.io/badge/status-in_development-111111?style=flat-square)
![progress](https://img.shields.io/badge/milestones-4_of_9-4a4a4a?style=flat-square)
![licence](https://img.shields.io/badge/licence-MIT-767676?style=flat-square)

![Go](https://img.shields.io/badge/Go-1.27--000000?style=flat-square&logo=go&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-000000?style=flat-square&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-000000?style=flat-square&logo=postgresql&logoColor=white)

[Case study](https://www.shivamsfolio.com/projects/distributed-rate-limiter-api-gateway) · [Claims ledger](CLAIMS.md) · [Series](#part-of-a-series)

</div>

---

> [!IMPORTANT]
> **4 of 9 milestones complete.** It is a gateway now, not only a limiter: it authenticates, routes, limits and proxies, and answers the same questions over gRPC. Replicas sharing one Redis enforce **one** quota between them; tenants, policies and API keys live in Postgres and can be changed while it runs. `45K req/s · <8ms p99` is a target, not a measurement; nothing is benchmarked yet. Every number lands in [CLAIMS.md](CLAIMS.md) first, with its commit, host and caveat.

## Problem

Keep per-tenant quotas accurate across replicas while a noisy neighbour, a lost shard or a rebalance is in progress.

## Run what exists today

```bash
docker compose up -d redis postgres && make migrate                   # 1. data plane + schema
go run ./cmd/echo &                                                   # 2. an upstream
go run ./cmd/gateway --node gateway-1 --config ./configs/local.yaml   # 3. a node
curl -si -H 'X-Tenant-ID: acme' localhost:8080/v1/check               # 4. a decision
curl -si -H 'X-Tenant-ID: acme' localhost:8080/api/echo/hello         # 5. the data path
grpcurl -plaintext -d '{"tenant_id":"acme"}' localhost:9090 \
  ratelimit.v1.LimiterService/Check                                   # 6. the same, over gRPC
```

Thirty of those curls against the shipped 20-token bucket return exactly twenty
`200`s and then ten `429`s, each carrying `Retry-After` and the `X-RateLimit-*`
headers. The gateway refuses to start if it cannot reach Redis, because a
limiter that silently stops limiting is worse than one that will not boot.

| | |
|---|---|
| `--node` | this node's identity — from milestone 5 it decides which tenants it owns |
| `--config` | the YAML above; an unknown key is a startup error, not a silent no-op |
| `--http-addr` | overrides the listen address for a second local node |
| `GET /v1/check` | the decision: `200` or `429`, every evaluated limit in the body |
| `GET /healthz` `/readyz` | is this process up, and can it still reach its stores |
| `GET /api/...` | the data path: authenticate, limit, then proxy to the upstream |
| `:9090` | `Check`, `GetQuota` and a decision stream; reflection is on |
| `/admin/v1/...` | tenants, policies and API keys, behind an admin token |
| `gatewayctl` | `migrate up`, `apikey create`, `counters reset` |

**Two algorithms.** A token bucket, stored as GCRA — one timestamp rather than a
`(tokens, last_refill)` pair, which makes a full bucket and an absent key the
same state, so key expiry can never hand out quota. And an exact sliding-window
log: at most N requests in *any* window, including across a boundary where a
fixed-window counter admits 2N. A policy may carry both, and a request refused
by either consumes from neither.

**One quota across replicas.** The whole decision is a single Lua script and a
single round trip, and it reads Redis's own `TIME` rather than any gateway's
clock — replicas drift, and a sliding window evaluated against two different
"now"s admits a different number of requests depending on which replica
answers.

**What proves it.** `scripts/dual-node-quota.sh` runs two gateway processes
against one Redis and requires the combined admissions to equal the quota
*exactly*; then it runs the same scenario with per-process counters as a
control, which is **required to over-admit**. A check that cannot fail when the
mechanism is removed is not evidence that the mechanism works. The Lua script
is also differentially tested against the Go implementation over several
thousand random operations — both driven by one clock, required to agree on
every field of every decision.

**Policies are rows, not config.** `make migrate` creates the schema and seeds
the two tenants the commands above use. Named policies, per-endpoint rules,
tenants and API keys are all editable through `/admin/v1` while the gateway
runs, and every node keeps them in an in-process cache, so a check costs no
database round trip. A policy store that goes down freezes policy at the last
known version rather than stopping enforcement.

**An edit has to reach the replicas that did not serve it.** The schema's
triggers publish on a Postgres channel and every node listens, so a change
lands in milliseconds; the cache TTL is the bound only when that listener is
down. `scripts/policy-reload-test.sh` proves it across two nodes whose caches
hold for an hour — then runs the same scenario with notifications switched off,
where the second node **must** keep enforcing the old policy. Without that
half, the first half would only show that something changed, not why.

**Identity.** An API key (stored only as its SHA-256) or an HS256 bearer token
names the tenant; the admin API has its own token. No secret appears in a
config file — each is named by environment variable, and a test fails the build
if the shipped config ever starts carrying one.

**One decision, two surfaces.** REST and gRPC translate; neither decides. Both
call one service inside the process, because two surfaces that each
implemented "look up the policy, pick the limits, check them, decide what a
store failure means" would eventually answer differently — and the difference
would show up as a tenant being limited by one caller and not the other. What
each surface owns is its own shape: status codes and headers on one side,
gRPC codes and a server stream on the other.

**The limiter runs in front of the proxy, not beside it.** A refused request
never reaches the upstream, so a tenant over its quota costs the backend
nothing. An admitted one arrives with `X-Tenant-ID` set and the caller's own
credentials **removed** — an upstream trusts the gateway, not the client — and
the smoke test asserts both halves against a real echo service. Per-endpoint
rules apply here using the method and path the gateway can actually see, which
is why `POST /api/orders` can be capped separately from everything else.

## Architecture

```mermaid
%%{init: {'theme':'neutral'}}%%
flowchart LR
    N0(["Client traffic"])
    N1["Gateway replicas"]
    N2["Limiter shard ring"]
    N3[("Redis + Postgres")]
    N4(["Grafana + React"])

    N0 --> N1 --> N2 <--> N3
    N2 --> N4
```

| Stage | | Detail |
|---|---|---|
| **Client traffic** | `in` | REST / gRPC |
| **Gateway replicas** | `work` | auth · routing · policy · proxy |
| **Limiter shard ring** | `work` | consistent hashing with virtual nodes · Raft over membership |
| **Redis + Postgres** | `state` | atomic Lua counters · tenant policies |
| **Grafana + React** | `out` | RED metrics · live per-tenant headroom |

Raft governs **membership and ring ownership only**, never per-request counters — per-request consensus would destroy the latency budget. The consequence is stated rather than hidden: quota is not strictly consistent across a rebalance window.

## Scope

**Dual limiting strategies.** Token-bucket for smooth burst control, an exact sliding-window log for stricter endpoint policies. Both are single-round-trip atomic Lua, so two gateway processes sharing one Redis enforce one quota, not two.

**Resilient control plane.** Consistent-hash sharding keeps a tenant on a stable shard; Raft promotes a replacement leader on failure, with quota accuracy preserved across the rebalance.

**Observable delivery.** gRPC and REST expose decisions and quota status; Prometheus feeds Grafana and a live React dashboard.

## Roadmap

`[███████████░░░░░░░░░░░░░] 4/9` — ticked only when the verification step passes, not when the code is written.

- [x] **M1 · Skeleton, config, single-node token bucket that says 429** — one process enforces an in-memory per-tenant bucket over REST, on the advertised config path and flags.
- [x] **M2 · Redis-backed token bucket and sliding window** — both strategies as single-round-trip atomic Lua; two processes sharing one Redis enforce one quota.
- [x] **M3 · Postgres policy store, tenant auth, hot-reloading cache** — per-tenant and per-endpoint policies in Postgres, served from an in-process cache; `make migrate` works as advertised.
- [x] **M4 · gRPC contract and the actual gateway data path** — authenticates, routes, applies policy and proxies upstream; same decisions over gRPC.
- [ ] **M5 · Consistent-hash ring with cross-node forwarding** — a tenant always lands on the same shard; a node that does not own it forwards over gRPC.
- [ ] **M6 · Raft membership and leader election** — ring ownership survives a node dying; enforcement continues with quota accuracy across the rebalance.
- [ ] **M7 · Prometheus, Grafana, live React dashboard** — every decision observable; per-tenant headroom exactly as advertised.
- [ ] **M8 · Benchmark harness and honest tuning** — a defensible throughput and p99 on real hardware, methodology written down, or the claim corrected.
- [ ] **M9 · Kubernetes deployment and chaos under load** — the whole stack on a cluster, surviving a pod deletion mid-load.

The benchmark needs a **separate Linux load-generation host**: k6 runs a JS VM per VU and will steal the cores it is measuring, and the figure is an aggregate across 3–4 replicas, not per-replica. Both go in the report or the number is doing work it did not earn.

## Stack

`Go 1.27` `gRPC (buf + protoc-gen-go-grpc)` `chi` `Redis 7 (Lua, go-redis v9)` `PostgreSQL 16 (pgx + golang-migrate)` `hashicorp/raft + raft-boltdb` `consistent hashing with virtual nodes` `Prometheus + Grafana` `React 18 + Vite + TypeScript` `Docker Compose` `Kubernetes (Helm, verified on kind in CI)` `k6 (constant-arrival-rate) + wrk2` `GitHub Actions`

## Commands

Beyond what runs today, the shape the finished project is aiming for:

```bash
docker compose up -d redis postgres && make migrate                     # 1. data plane
go run ./cmd/gateway --node gateway-1 --config ./configs/local.yaml     # 2. a shard
cd dashboard && npm install && npm run dev                              # 3. dashboard
k6 run tests/rate-limit.js && kubectl get pods -n gateway               # 4. the envelope
```

## Part of a series

Seven systems projects, built one at a time. This is **#2**.

| # | Project | Repo |
|---|---|---|
| 01 | Low-Latency Market Data & Order Entry Stack | [`low-latency-market-data-stack`](https://github.com/Git-ShivamPatil/low-latency-market-data-stack) |
| 02 | **Distributed Rate Limiter & API Gateway** *(you are here)* | — |
| 03 | Agentic AI Orchestration Platform | [`agentic-orchestration-platform`](https://github.com/Git-ShivamPatil/agentic-orchestration-platform) |
| 04 | High-Performance LLM Inference Server | [`rust-llm-inference-server`](https://github.com/Git-ShivamPatil/rust-llm-inference-server) |
| 05 | Secure Banking System | [`fabric-banking-platform`](https://github.com/Git-ShivamPatil/fabric-banking-platform) |
| 06 | Online Examination System | [`online-examination-system`](https://github.com/Git-ShivamPatil/online-examination-system) |
| 07 | Secure RAG with RBAC, Guardrails & Monitoring | [`secure-rag-rbac`](https://github.com/Git-ShivamPatil/secure-rag-rbac) |

Plus [`bfsi-lending-lakehouse`](https://github.com/Git-ShivamPatil/bfsi-lending-lakehouse) — a medallion lakehouse shipped outside the series. All published on [shivamsfolio.com](https://www.shivamsfolio.com/projects).

## Licence

[MIT](LICENSE) © Shivam Patil

---

<div align="center">

**[Case study](https://www.shivamsfolio.com/projects/distributed-rate-limiter-api-gateway)** · **[All projects](https://www.shivamsfolio.com/projects)** · **[Contact](https://www.shivamsfolio.com/contact)**

</div>

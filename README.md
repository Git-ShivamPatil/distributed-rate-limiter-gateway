<div align="center">

# Distributed Rate Limiter & API Gateway

**Multi-tenant gateway · token-bucket and sliding-window quotas · consistent-hash shard ring · Raft election over shard failure**

![status](https://img.shields.io/badge/status-in_development-111111?style=flat-square)
![progress](https://img.shields.io/badge/milestones-6_of_9-4a4a4a?style=flat-square)
![licence](https://img.shields.io/badge/licence-MIT-767676?style=flat-square)

![Go](https://img.shields.io/badge/Go-1.27-000000?style=flat-square&logo=go&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-000000?style=flat-square&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-000000?style=flat-square&logo=postgresql&logoColor=white)

[Case study](https://www.shivamsfolio.com/projects/distributed-rate-limiter-api-gateway) · [Claims ledger](CLAIMS.md) · [Series](#part-of-a-series)

</div>

---

> [!IMPORTANT]
> **6 of 9 milestones complete.** A gateway, not only a limiter: it authenticates, routes, limits and proxies, and answers over REST and gRPC. Replicas sharing one Redis enforce **one** quota between them, a consistent-hash ring decides which node coordinates each tenant, and policies live in Postgres and can be changed while it runs. Membership is committed through Raft, losing the leader under load is an election rather than an outage, and a tightened limit cannot be outrun by a node still holding the old one. `45K req/s · <8ms p99` is a target, not a measurement; nothing is benchmarked yet. Every number lands in [CLAIMS.md](CLAIMS.md) first, with its commit, host and caveat.

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
| `GET /v1/cluster` | this node's view of the ring, who owns a given tenant, where it sits in the log, and whether it is refusing everything because the counter store changed |
| `/admin/v1/...` | tenants, policies and API keys, behind an admin token |
| `/admin/v1/cluster/members` | the committed membership, and the two calls that change it |
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

### The ring, and what it is not for

```bash
scripts/cluster-up.sh 3        # three nodes, one ring, one Redis
scripts/show-ownership.sh acme # every node has to name the same owner
scripts/cluster-down.sh
```

A tenant is coordinated by one node. Ownership is a consistent hash of the
**tenant id alone** — not of (tenant, limit), which would spread load better
and quietly give a tenant with three limits three owners — with 256 virtual
nodes per member so the shares come out even. Removing one of N nodes remaps
fewer than 1.5/N of the keyspace, and a key owned by a node that stayed never
moves; both are property tests over 100,000 synthetic tenants. Changing a
node's *address* moves nothing at all, because the ring hashes ids.

**What the ring does not do is decide admissions.** Counters live in Redis and
every admission is backed by an atomic script there, so two nodes briefly
disagreeing about who owns a tenant cannot over-admit — the worst it costs is
a hop. That is deliberate, and it is what makes losing a node a latency event
rather than a capacity gap: a forward that fails is decided locally instead,
against the same store, and the fallback is counted so an operator can see the
ring is unhealthy.

It also means one claim needs two pieces of evidence, which
`scripts/cluster-quota-test.sh` keeps separate: that three nodes admit
**exactly** one quota between them (which would still hold with the ring
removed), and that forwarding **actually happened** (which is the only thing
that shows the ring is doing anything). The answer names the owner and says it
was forwarded; the nodes' own counters have to agree.

### Membership, under consensus

```bash
scripts/cluster-up.sh 3        # three nodes, one ring, one Redis, one Raft cluster
scripts/kill-leader.sh         # kill the leader mid-flight; the quota still comes out exactly
curl -sH "X-Admin-Token: $GATEWAY_ADMIN_TOKEN" localhost:18101/admin/v1/cluster/members
```

The member list in the config file is the **seed**, not the truth. Once a
membership has committed, the log is the truth and the file is never consulted
again -- otherwise a node that came back holding a stale file would put back a
shard the cluster had removed.

**Nobody is told to join.** The node whose id sorts first creates the cluster
with every configured peer already a voter; the rest start with an empty log
and learn the configuration from the leader. Every node computes the same
answer from the same list, so there is no flag an operator can set on two nodes
and end up with two clusters -- and a node that already has a log never
bootstraps again, whatever the file says.

**The initial ring arrives as one entry**, not as one entry per member. Seeding
a member at a time would have every node build a ring out of however much had
arrived so far -- a one-member ring, then a two-member ring, each of them a
real ring that really routes -- and the vnode count would land after the
members, so the ring would be built at the default width and then rebuilt at
the configured one, moving nearly every tenant. That was a real bug, caught by
the test that asserts an election moves nothing.

**The epoch advances only when a tenant could have moved.** Adding or removing
a shard advances it; changing a member's *address* does not, because the ring
hashes ids and a member that moved host owns exactly what it owned before.
Advancing a fencing token for a change that fences nothing is how a rolling
deploy becomes a takeover storm.

**Losing the leader is an election, not an outage.** `scripts/kill-leader.sh`
runs continuous traffic against three nodes, `SIGKILL`s the leader at a fixed
request number -- a deterministic trigger, never a timer -- and requires every
request to a surviving node to be answered with a decision, and the total
admitted to equal the quota *exactly*. The election time is **reported and
asserted on by nothing**: how long raft takes to notice a dead leader is a
property of its timers and of the machine.

**What proves that matters** is the control, because the quota assertion would
also hold in a cluster with no consensus at all. The same scenario runs against
a build made with `-tags faultinject` that puts consensus **on** the request
path, where the same scenario is **required** to fail -- and it does: 78 of 100
requests answer with neither a decision nor a refusal, against 0 in the clean
run. Be precise about what that shows. Traffic goes to the two survivors, which
are followers, and a follower cannot commit -- so the broken build starts
failing at the first request rather than at the kill. What it demonstrates is
that per-request consensus is unworkable at all, which is the reason the design
keeps it off the path; it is not a measurement of the election itself. CI also asserts the fault seam is absent from a production
binary *and present in the fault build*, so the check cannot pass by having
been renamed.

**What consensus deliberately does not do is notice a dead node.** There is no
failure detector anywhere in this design. A shard that dies does not leave the
ring by itself; something has to commit the removal through
`/admin/v1/cluster/members`. That is safe rather than convenient: losing a
shard was already a latency event and not a capacity gap, because a failed
forward is decided locally against the same Redis and counted. Membership
changes are graceful and commanded, and the README would rather say so than
imply a liveness mechanism that is not there.

### Two fences, and why it takes two

Not every node is up to date at the same instant, and two of those gaps are
over-admissions rather than inconveniences.

**A limit that tightened.** Postgres `LISTEN/NOTIFY` tells every node that a
policy changed, but it gives no ordering across them: a node whose listener is
down, or whose cache has not lapsed, is holding the old limit and has no way to
know. It supplies that limit to a script which trusts its caller, and for the
whole skew window the cluster enforces `max(old, new)` -- which for a tightening
is an over-admission nobody sees. A policy edit therefore mints the next
generation through the log, and the cache stamps that number onto the copy it
fetched. The stamp is read **before** the fetch, not after: a bump that commits
while the query is in flight belongs to an edit that read may not have seen, so
stamping the new number onto the old limits would present them as current --
the one mistake a fence cannot catch, because it trusts the number it is given.
Reading first can only under-stamp, and an under-stamped copy is refused,
refreshed and retried.

Two mechanisms act on it, and they cover different halves:

1. **The node checks its own copy** against the newest generation the cluster
   has minted, and refreshes before enforcing anything older.
2. **Redis refuses a check** carrying a generation below the one already
   enforced for that tenant.

It needs both, and the reason is this project's own architecture rather than
anything in the literature. A tenant's checks are coordinated by **one** node,
so the owner is the only node whose generation ever reaches the counter store
for that tenant -- nothing else is in a position to contradict it. The
self-check is what covers a stale owner in the steady state; the store-side
fence is what covers a node whose idea of the newest generation is itself out
of date, because it is cut off from the log but not from Redis. That was not
the plan going in: the scenario below was written expecting the store-side
fence alone to do it, and it admitted 300 against a limit of 100 until the
self-check was added.

```bash
scripts/policy-skew-test.sh            # 99 admitted against a tightened 100
scripts/policy-skew-test.sh --control  # 300, with no generation minted
```

The pinned node has to **own** the tenant, and the script picks one it does --
a node that does not own a tenant forwards its checks to the node that does,
which is not stale, so the scenario would have passed while testing nothing.

**A counter store that is not the one we think it is.** Redis has no identity
that survives being emptied: a store that was flushed and came back looks
exactly like a store nobody has used yet, and every tenant silently gets a
fresh quota. The most likely way to lose everything does not even change
Redis's `run_id` -- the test measures this rather than assuming it -- so a
detector watching only that would see a perfectly healthy store. The marker is
a generation minted once per lifetime of the store, with the candidate always
one above the highest the cluster has ever committed, so the high-water mark
lives somewhere the flush cannot reach and the generation after a wipe always
outranks the one before it.

Detection is two conditions, either of which REFUSES: the generation key is
absent, or its value differs from the committed one. A changed run id is a
third signal and it does **not** refuse -- a store that came back with its data
intact is a restart, not a replacement, and latching there would turn every
Redis restart into an outage. It is logged, and a restart that also lost the
data is caught by the absent key rather than by the run id. When one of the two
does fire, this node refuses **every** tenant, not only the ones it has seen. The
generation names the whole store and the probe fires before it could know which
tenants were touched; refusing only the tenants that happen to check in next
would leave every quiet one un-fenced and make the size of the amnesty a
function of traffic rather than a constant. The cost is real and worth saying
out loud: a `FLUSHDB` on a shared Redis takes the gateway to `503` until the
cluster agrees on a new generation.

```bash
scripts/store-wipe-test.sh            # detected in ~400ms, 0 admitted while blocked
scripts/store-wipe-test.sh --control  # unnoticed, and the quota comes back in silence
```

It is a **limitation test**. Nothing survives losing every counter; what it
shows is that the loss is detected, refused while it lasts, counted, and
bounded by the detection window rather than by luck.

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
| **Limiter shard ring** | `work` | consistent hashing with virtual nodes · Raft-committed membership |
| **Redis + Postgres** | `state` | atomic Lua counters · tenant policies |
| **Grafana + React** | `out` | RED metrics · live per-tenant headroom |

Raft governs **membership and ring ownership only**, never per-request counters — per-request consensus would destroy the latency budget. The consequence is stated rather than hidden: quota is not strictly consistent across a rebalance window.

## What the fences do not cover

- **Redis is one unreplicated instance**, and the log does not change that. The
  generation makes losing it *visible*; it does not make it survivable.
- **A single node is unfenced**, because zero means unfenced everywhere in this
  system. The command the case study publishes runs one node against one Redis,
  where there is nobody to be out of step with.
- **An admin write is two commits, and they are not atomic.** The edit goes to
  Postgres first and the generation is minted second, so a crash between them
  leaves that one edit live but un-fenced -- the behaviour this gateway had
  before fences existed. The other order would mint a generation for an edit
  that never happened.
- **A policy edit is minted through the leader.** A write that lands on a
  follower answers `409` naming the leader, and says plainly that the edit was
  written and is not yet fenced.
- **`check.lua` still ships a clock-override branch** for the differential test
  against the Go oracle. It is unreachable from the production constructor, and
  it should be a build-time split rather than a runtime argument.
- **Nothing here exercises boltdb recovery from an unclean stop.** Every
  restart the tests perform is a clean one.

## Scope

**Dual limiting strategies.** Token-bucket for smooth burst control, an exact sliding-window log for stricter endpoint policies. Both are single-round-trip atomic Lua, so two gateway processes sharing one Redis enforce one quota, not two.

**Resilient control plane.** Consistent-hash sharding keeps a tenant on a stable shard; Raft promotes a replacement leader on failure, with quota accuracy preserved across the rebalance.

**Observable delivery.** gRPC and REST expose decisions and quota status; Prometheus feeds Grafana and a live React dashboard.

## Roadmap

`[████████████████░░░░░░░░] 6/9` — ticked only when the verification step passes, not when the code is written.

- [x] **M1 · Skeleton, config, single-node token bucket that says 429** — one process enforces an in-memory per-tenant bucket over REST, on the advertised config path and flags.
- [x] **M2 · Redis-backed token bucket and sliding window** — both strategies as single-round-trip atomic Lua; two processes sharing one Redis enforce one quota.
- [x] **M3 · Postgres policy store, tenant auth, hot-reloading cache** — per-tenant and per-endpoint policies in Postgres, served from an in-process cache; `make migrate` works as advertised.
- [x] **M4 · gRPC contract and the actual gateway data path** — authenticates, routes, applies policy and proxies upstream; same decisions over gRPC.
- [x] **M5 · Consistent-hash ring with cross-node forwarding** — a tenant always lands on the same shard; a node that does not own it forwards over gRPC.
- [x] **M6 · Raft membership and leader election** — membership and ring configuration are committed through Raft; killing the leader under load is an election, not an outage; and a tightened limit cannot be outrun by a node still holding the old one.
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

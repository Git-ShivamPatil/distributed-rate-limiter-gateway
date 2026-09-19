package limiter

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// ErrCostExceedsCapacity is returned when a request asks for more units than a
// limit could ever admit. It is a client error, not a refusal: waiting will
// never help, so answering 429 with a Retry-After would be a lie.
var ErrCostExceedsCapacity = errors.New("limiter: cost exceeds the limit's capacity")

// ErrTooManyKeys is returned when the in-memory store is full. It fails the
// check closed rather than evicting a live counter, because evicting one is
// indistinguishable from handing out a fresh quota.
var ErrTooManyKeys = errors.New("limiter: in-memory key budget exhausted")

const memoryShards = 64

// DefaultMaxKeys bounds the in-memory store. Keys are (tenant x limit), so a
// legitimate deployment needs a handful per tenant; the bound exists because
// the check API can be pointed at an arbitrary tenant id, and a limiter that
// allocates on unknown input is a memory-exhaustion bug waiting for traffic.
const DefaultMaxKeys = 1 << 20

// Memory is a per-process limiter.
//
// Its counters live in this process's heap, so two replicas do NOT share a
// quota -- each enforces the whole limit independently, and N replicas admit N
// times what was configured. That is exactly the limitation the Redis backend
// exists to fix, and it is why this one is for a single node and for tests.
//
// Within one process it is exact, and it is the oracle the Redis
// implementation is differentially tested against.
type Memory struct {
	clock   Clock
	maxKeys int

	shards [memoryShards]memShard

	keysMu sync.Mutex
	keys   int
}

type memShard struct {
	mu      sync.Mutex
	tenants map[string]*tenantState
}

// tenantState holds every counter for one tenant behind one mutex.
//
// One lock per tenant, rather than one per key, is what makes a multi-limit
// check atomic: the whole set is evaluated and then committed with no other
// goroutine interleaving between the two halves. Tenants never contend with
// each other, which is the property that matters under a noisy neighbour.
type tenantState struct {
	mu      sync.Mutex
	gcra    map[string]time.Time
	windows map[string]*windowState
	// detached marks state that the sweeper has removed from the shard map.
	// A goroutine holding a stale pointer must not write into it: those writes
	// would be unreachable, and an unreachable counter is a free quota.
	detached bool
}

type windowState struct {
	log    []time.Time
	expiry time.Time // newest entry + period; the log's own TTL
}

// MemoryOption configures a Memory limiter.
type MemoryOption func(*Memory)

// WithClock replaces the time source. Tests use it; production does not.
func WithClock(c Clock) MemoryOption { return func(m *Memory) { m.clock = c } }

// WithMaxKeys bounds how many (tenant, limit) counters may exist at once.
// Zero or less means DefaultMaxKeys, so that an unset config field asks for
// the default rather than for a store that can hold nothing.
func WithMaxKeys(n int) MemoryOption { return func(m *Memory) { m.maxKeys = n } }

// NewMemory builds an in-memory limiter. It starts no goroutines: call Sweep
// periodically to discard expired counters.
func NewMemory(opts ...MemoryOption) *Memory {
	m := &Memory{clock: RealClock(), maxKeys: DefaultMaxKeys}
	for _, o := range opts {
		o(m)
	}
	if m.maxKeys <= 0 {
		m.maxKeys = DefaultMaxKeys
	}
	for i := range m.shards {
		m.shards[i].tenants = make(map[string]*tenantState)
	}
	return m
}

func (m *Memory) shardFor(tenant string) *memShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenant))
	return &m.shards[h.Sum32()%memoryShards]
}

// lockTenant returns a tenant's state with its mutex held, retrying if the
// sweeper detached the state between the lookup and the lock.
func (m *Memory) lockTenant(tenant string) *tenantState {
	s := m.shardFor(tenant)
	for {
		s.mu.Lock()
		ts, ok := s.tenants[tenant]
		if !ok {
			ts = &tenantState{gcra: make(map[string]time.Time), windows: make(map[string]*windowState)}
			s.tenants[tenant] = ts
		}
		s.mu.Unlock()

		ts.mu.Lock()
		if !ts.detached {
			return ts
		}
		ts.mu.Unlock() // swept out from under us; look it up again
	}
}

func (m *Memory) addKeys(n int) int {
	m.keysMu.Lock()
	defer m.keysMu.Unlock()
	m.keys += n
	return m.keys
}

// Len reports how many counters are live.
func (m *Memory) Len() int {
	m.keysMu.Lock()
	defer m.keysMu.Unlock()
	return m.keys
}

// Check evaluates every limit in the request and consumes from all of them or
// from none.
func (m *Memory) Check(_ context.Context, req Request) (Result, error) {
	cost := req.EffectiveCost()
	for _, l := range req.Limits {
		if err := l.Validate(); err != nil {
			return Result{}, err
		}
		if cost > l.Capacity() {
			return Result{}, fmt.Errorf("%w: cost %d against limit %q of %d", ErrCostExceedsCapacity, cost, l.Name, l.Capacity())
		}
	}
	if len(req.Limits) == 0 {
		return Result{Allowed: true}, nil
	}

	// Make room before taking any lock: the sweeper walks every shard, so
	// calling it while holding one would deadlock.
	if m.Len()+len(req.Limits) > m.maxKeys {
		m.Sweep()
	}

	now := m.clock.Now()
	ts := m.lockTenant(req.Tenant)
	defer ts.mu.Unlock()

	// Pass one decides and mutates nothing; pass two commits, and only if
	// every limit allowed. That is what makes a refused request cost nothing.
	type pending struct {
		key    string
		limit  Limit
		newTAT time.Time
		newLog *windowState
		fresh  bool
	}

	result := Result{Allowed: true, Decisions: make([]Decision, 0, len(req.Limits))}
	staged := make([]pending, 0, len(req.Limits))
	fresh := 0

	for _, l := range req.Limits {
		key := l.Key(req.Tenant)
		switch l.Algorithm {
		case AlgorithmTokenBucket:
			tat, existed := ts.gcra[key]
			nt, d := gcraCheck(l, tat, now, cost)
			result.Decisions = append(result.Decisions, d)
			staged = append(staged, pending{key: key, limit: l, newTAT: nt, fresh: !existed})
			if !existed {
				fresh++
			}
			if !d.Allowed {
				result.Allowed = false
			}
		case AlgorithmSlidingWindow:
			ws, existed := ts.windows[key]
			var log []time.Time
			if existed {
				log = ws.log
			}
			nl, d := windowCheck(l, log, now, cost)
			result.Decisions = append(result.Decisions, d)
			staged = append(staged, pending{
				key:   key,
				limit: l,
				newLog: &windowState{
					log:    nl,
					expiry: windowExpiry(l, nl),
				},
				fresh: !existed,
			})
			if !existed {
				fresh++
			}
			if !d.Allowed {
				result.Allowed = false
			}
		}
	}

	if !result.Allowed {
		// Nothing was consumed, so nothing may be reported as consumed.
		//
		// Each limit was evaluated on the assumption it might be charged, so a
		// limit that would have allowed the request reported the headroom it
		// would have had afterwards. Since the request is refused, that number
		// is a counterfactual, and it contradicts the state left behind: a
		// caller reading `remaining: 994` and then retrying successfully would
		// see 995. Re-read the ones that allowed, consuming nothing.
		for i := range result.Decisions {
			if !result.Decisions[i].Allowed {
				continue // the refusing limit already reports its true state
			}
			p := staged[i]
			switch p.limit.Algorithm {
			case AlgorithmTokenBucket:
				_, peek := gcraCheck(p.limit, ts.gcra[p.key], now, 0)
				result.Decisions[i].Remaining = peek.Remaining
				result.Decisions[i].ResetAfter = peek.ResetAfter
			case AlgorithmSlidingWindow:
				var log []time.Time
				if ws, ok := ts.windows[p.key]; ok {
					log = ws.log
				}
				_, peek := windowCheck(p.limit, log, now, 0)
				result.Decisions[i].Remaining = peek.Remaining
				result.Decisions[i].ResetAfter = peek.ResetAfter
			}
		}
		result.Limiting = limitingName(result.Decisions)
		return result, nil
	}
	if req.PeekOnly {
		return result, nil
	}
	if fresh > 0 && m.Len()+fresh > m.maxKeys {
		return Result{}, ErrTooManyKeys
	}

	added := 0
	for _, p := range staged {
		switch p.limit.Algorithm {
		case AlgorithmTokenBucket:
			ts.gcra[p.key] = p.newTAT
		case AlgorithmSlidingWindow:
			ts.windows[p.key] = p.newLog
		}
		if p.fresh {
			added++
		}
	}
	if added > 0 {
		m.addKeys(added)
	}
	return result, nil
}

// limitingName picks the refusal the caller has to wait longest for.
func limitingName(ds []Decision) string {
	var name string
	worst := time.Duration(-1)
	for _, d := range ds {
		if !d.Allowed && d.RetryAfter > worst {
			worst, name = d.RetryAfter, d.Name
		}
	}
	return name
}

// Sweep discards every counter whose state has become indistinguishable from
// no state at all -- a full bucket, or a window with nothing left in it -- and
// reports how many it removed.
//
// This is not cache eviction. Dropping a key that still holds debt would hand
// its tenant a fresh quota; dropping one at its expiry instant cannot, because
// the two states are identical. The Redis backend sets its key TTL from the
// same rule, so both backends forget at the same instant.
func (m *Memory) Sweep() int {
	now := m.clock.Now()
	removed := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for tenant, ts := range s.tenants {
			ts.mu.Lock()
			for k, tat := range ts.gcra {
				if !gcraExpiry(tat).After(now) {
					delete(ts.gcra, k)
					removed++
				}
			}
			for k, ws := range ts.windows {
				if !ws.expiry.After(now) {
					delete(ts.windows, k)
					removed++
				}
			}
			if len(ts.gcra) == 0 && len(ts.windows) == 0 {
				ts.detached = true
				delete(s.tenants, tenant)
			}
			ts.mu.Unlock()
		}
		s.mu.Unlock()
	}
	if removed > 0 {
		m.addKeys(-removed)
	}
	return removed
}

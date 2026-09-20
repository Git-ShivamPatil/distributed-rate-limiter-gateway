package policy

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Cache keeps policies in process so the request path does not pay for a
// database round trip.
//
// Three behaviours matter more than the caching itself:
//
//   - Singleflight. A cold cache under load would otherwise send one query per
//     in-flight request for the same tenant, which is how a policy store falls
//     over exactly when traffic arrives.
//   - Negative caching. A tenant that does not exist is also an answer, and
//     caching it is what stops a flood of requests for an unknown tenant from
//     becoming a flood of queries. It is cached for a shorter time, because a
//     tenant that was just created should start working quickly.
//   - Serve-stale. If the store is unreachable, an entry past its TTL is served
//     anyway, up to StaleFor. A policy store outage then freezes policy at the
//     last known version instead of taking enforcement down with it -- the
//     alternative is a gateway that refuses everything because a database it
//     only needs for CONFIGURATION is briefly unavailable.
type Cache struct {
	source  Source
	ttl     time.Duration
	negTTL  time.Duration
	stale   time.Duration
	clock   func() time.Time
	genOf   func() uint64
	group   singleflight.Group
	mu      sync.RWMutex
	entries map[string]*entry
	stats   CacheStats
}

type entry struct {
	policy    Policy
	err       error // a cached ErrTenantNotFound / ErrTenantDisabled
	fetchedAt time.Time
}

// CacheStats is what the metrics endpoint and the tests read.
type CacheStats struct {
	Hits        int64
	Misses      int64
	Stale       int64
	Invalidated int64
}

// CacheOption configures a Cache.
type CacheOption func(*Cache)

// WithTTL sets how long a policy is served before it is refreshed.
func WithTTL(d time.Duration) CacheOption { return func(c *Cache) { c.ttl = d } }

// WithNegativeTTL sets how long "no such tenant" is remembered.
func WithNegativeTTL(d time.Duration) CacheOption { return func(c *Cache) { c.negTTL = d } }

// WithStaleFor sets how long a stale entry may still be served when the store
// cannot be reached. Zero disables serving stale.
func WithStaleFor(d time.Duration) CacheOption { return func(c *Cache) { c.stale = d } }

// WithGeneration supplies the policy generation the cluster has committed.
//
// The cache stamps it onto every entry it fetches, and the stamp is what the
// counter store fences against. Without it a node's policies are unfenced,
// which is the single-node case and what every test that is not about fencing
// gets.
func WithGeneration(fn func() uint64) CacheOption {
	return func(c *Cache) { c.genOf = fn }
}

// WithCacheClock replaces the clock. Tests use it; production does not.
func WithCacheClock(fn func() time.Time) CacheOption { return func(c *Cache) { c.clock = fn } }

// NewCache wraps a Source.
func NewCache(source Source, opts ...CacheOption) *Cache {
	c := &Cache{
		source:  source,
		ttl:     5 * time.Second,
		negTTL:  time.Second,
		stale:   5 * time.Minute,
		clock:   time.Now,
		genOf:   func() uint64 { return 0 },
		entries: make(map[string]*entry),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Lookup returns a tenant's policy, from cache when it can.
func (c *Cache) Lookup(ctx context.Context, tenant string) (Policy, error) {
	now := c.clock()

	c.mu.RLock()
	e, ok := c.entries[tenant]
	c.mu.RUnlock()

	if ok && !c.expired(e, now) {
		c.bump(&c.stats.Hits)
		return e.policy, e.err
	}

	// One query per tenant no matter how many requests are waiting on it.
	v, err, _ := c.group.Do(tenant, func() (any, error) {
		// Another goroutine may have refreshed it while this one queued.
		c.mu.RLock()
		cur, ok := c.entries[tenant]
		c.mu.RUnlock()
		if ok && !c.expired(cur, c.clock()) {
			return cur, nil
		}

		// The generation is read BEFORE the fetch, and that ordering is the
		// whole safety of the fence.
		//
		// A bump that commits while this query is in flight belongs to a
		// policy this read may not have seen. Reading afterwards would stamp
		// the NEW generation onto the OLD limits, and the store would wave
		// them through as current -- the one mistake the fence cannot catch,
		// because it trusts the number it is given. Reading first can only
		// under-stamp, and an under-stamped entry is fenced, refreshed and
		// retried. Wrong in the safe direction, every time.
		gen := c.genOf()

		p, lookupErr := c.source.Lookup(ctx, tenant)
		if lookupErr != nil && !isAnswer(lookupErr) {
			return nil, lookupErr // a store failure, not an answer about the tenant
		}
		p.Gen = gen
		fresh := &entry{policy: p, err: lookupErr, fetchedAt: c.clock()}
		c.mu.Lock()
		c.entries[tenant] = fresh
		c.mu.Unlock()
		return fresh, nil
	})

	if err != nil {
		// The store is unreachable. An entry we already hold is better than
		// refusing to answer, as long as it is not arbitrarily old.
		if ok && c.stale > 0 && now.Sub(e.fetchedAt) <= c.stale {
			c.bump(&c.stats.Stale)
			return e.policy, e.err
		}
		return Policy{}, err
	}

	c.bump(&c.stats.Misses)
	fresh := v.(*entry)
	return fresh.policy, fresh.err
}

// isAnswer reports whether an error is a fact about the tenant rather than a
// failure to find out. Facts are cacheable; failures are not.
func isAnswer(err error) bool {
	return errors.Is(err, ErrTenantNotFound) || errors.Is(err, ErrTenantDisabled)
}

func (c *Cache) expired(e *entry, now time.Time) bool {
	ttl := c.ttl
	if e.err != nil {
		ttl = c.negTTL
	}
	return now.Sub(e.fetchedAt) >= ttl
}

// Invalidate drops one tenant, so a write through the admin API takes effect
// on this node immediately rather than when the TTL happens to lapse.
func (c *Cache) Invalidate(tenant string) {
	c.mu.Lock()
	delete(c.entries, tenant)
	c.stats.Invalidated++
	c.mu.Unlock()
}

// InvalidateAll drops everything. A policy changed rather than a tenant: the
// cache is keyed by tenant, and finding which tenants use a policy would be
// another query at exactly the moment the store is being written to.
func (c *Cache) InvalidateAll() {
	c.mu.Lock()
	n := len(c.entries)
	c.entries = make(map[string]*entry, n)
	c.stats.Invalidated += int64(n)
	c.mu.Unlock()
}

// Stats returns a snapshot.
func (c *Cache) Stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stats
}

// Len reports how many entries are held.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

func (c *Cache) bump(field *int64) {
	c.mu.Lock()
	*field++
	c.mu.Unlock()
}

package policy

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NotifyChannel is the Postgres channel the schema's triggers publish on.
const NotifyChannel = "policy_change"

// Listener keeps this node's cache in step with policy edits made elsewhere.
//
// Without it, a policy changed through one replica takes effect on that
// replica at once and on the others whenever their TTL happens to lapse -- so
// for up to a TTL the cluster enforces two different policies for one tenant,
// and which one a request gets depends on which replica answered. The triggers
// in the schema publish on one channel; this subscribes and drops the affected
// entries.
//
// It is an optimisation over the TTL, not a correctness requirement: if the
// listener is down, entries still expire. That is why a failure here is logged
// and retried rather than fatal.
type Listener struct {
	pool  *pgxpool.Pool
	cache *Cache
	log   *slog.Logger

	// retry bounds how fast a reconnect loop spins when Postgres is down.
	minBackoff, maxBackoff time.Duration
}

// NewListener builds a listener over a pool and the cache it refreshes.
func NewListener(pool *pgxpool.Pool, cache *Cache, log *slog.Logger) *Listener {
	if log == nil {
		log = slog.Default()
	}
	return &Listener{pool: pool, cache: cache, log: log,
		minBackoff: 250 * time.Millisecond, maxBackoff: 10 * time.Second}
}

// Run subscribes until the context is cancelled, reconnecting as needed.
func (l *Listener) Run(ctx context.Context) {
	backoff := l.minBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		if err := l.listen(ctx); err != nil && ctx.Err() == nil {
			l.log.Warn("policy change listener dropped; retrying",
				"err", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > l.maxBackoff {
				backoff = l.maxBackoff
			}
			continue
		}
		backoff = l.minBackoff
	}
}

func (l *Listener) listen(ctx context.Context) error {
	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return err
	}

	// Anything published while this node was disconnected was missed, and
	// there is no way to ask Postgres what it was. Dropping everything is the
	// only safe assumption: the alternative is serving an edit-shaped hole
	// until each entry's TTL lapses.
	l.cache.InvalidateAll()
	l.log.Info("listening for policy changes", "channel", NotifyChannel)

	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		l.apply(n.Payload)
	}
}

// apply turns a notification payload into an invalidation.
//
// Payloads are "tenant:<id>" or "policy:<name>". A tenant edit invalidates
// that tenant; a policy edit invalidates everything, because the cache is
// keyed by tenant and finding which tenants use a policy would mean querying
// the store at exactly the moment it is being written to.
func (l *Listener) apply(payload string) {
	kind, name, ok := strings.Cut(payload, ":")
	if !ok {
		l.log.Warn("unrecognised policy change payload", "payload", payload)
		l.cache.InvalidateAll()
		return
	}
	switch kind {
	case "tenant":
		l.cache.Invalidate(name)
		l.log.Debug("policy cache invalidated for tenant", "tenant", name)
	case "policy":
		l.cache.InvalidateAll()
		l.log.Debug("policy cache flushed", "policy", name)
	default:
		l.log.Warn("unrecognised policy change kind", "payload", payload)
		l.cache.InvalidateAll()
	}
}

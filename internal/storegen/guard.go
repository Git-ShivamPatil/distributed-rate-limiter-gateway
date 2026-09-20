// Package storegen watches whether the counter store is still the one the
// cluster agreed on, and refuses to decide when it is not.
//
// The problem it exists for: Redis has no identity that survives being
// emptied. A store that was flushed and came back looks exactly like a store
// nobody has used yet -- every bucket full, every window empty -- and a
// gateway that cannot tell the difference hands every tenant a fresh quota and
// reports nothing. The generation is the marker that makes it visible, and
// this is what acts on it.
//
// It fails closed for the WHOLE NODE rather than for the tenants it has seen.
// The generation names the whole store, and the out-of-band probe fires before
// it could know which tenants were touched; refusing only the tenants that
// happen to check in next would leave every quiet one un-fenced and make the
// size of the amnesty a function of traffic rather than a constant. The cost
// is real and worth stating: a FLUSHDB on a shared Redis takes this gateway to
// 503 until the cluster commits the new generation.
package storegen

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ErrStoreChanged is what a latched guard refuses with.
var ErrStoreChanged = errors.New("storegen: the counter store is not the one the cluster agreed on")

// Store is what the counter store has to be able to tell us.
type Store interface {
	// Identity reads the store's current generation and its run id.
	Identity(ctx context.Context) (gen uint64, runID string, err error)
	// MintStoreGeneration installs candidate if the store has no generation,
	// and returns whichever is in force afterwards.
	MintStoreGeneration(ctx context.Context, candidate uint64) (uint64, error)
}

// Options configures a Guard.
type Options struct {
	Store Store
	// Committed reports the generation the cluster has agreed on. Zero means
	// the cluster has agreed on none, which is the state a fresh deployment
	// starts in -- and the guard never latches there, because there are no
	// counters from an older generation to protect.
	Committed func() uint64
	// Propose commits a generation through the log. Only the leader can, and a
	// follower's failure here is expected rather than exceptional: it waits
	// for the leader to do it.
	Propose func(ctx context.Context, gen uint64) error
	// Interval is how often the store is probed. It bounds how long a wipe can
	// go unnoticed, which is the published size of the hole.
	Interval time.Duration
	Logger   *slog.Logger
}

// Guard latches closed when the store's identity stops matching the one the
// cluster committed.
type Guard struct {
	opts Options

	mu      sync.RWMutex
	blocked bool
	reason  string
	runID   string
	seen    uint64 // the generation last read from the store

	denials int64
	latches int64
}

// DefaultInterval is how often the store is probed when nothing says
// otherwise. It is the bound on how long a wipe goes unnoticed, and it is
// short because that bound is a number this project publishes.
const DefaultInterval = time.Second

// New builds a guard. It does not probe; call Reconcile or Run.
func New(opts Options) *Guard {
	if opts.Interval <= 0 {
		opts.Interval = DefaultInterval
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Committed == nil {
		opts.Committed = func() uint64 { return 0 }
	}
	return &Guard{opts: opts}
}

// Check is what the decision path calls before it answers anything. A non-nil
// error refuses the request, whatever the tenant's failure mode says: the
// store has told us it is not the one we think it is, and admitting on the
// strength of counters that may have been reset is the amnesty this exists to
// prevent.
func (g *Guard) Check() error {
	g.mu.RLock()
	blocked, reason := g.blocked, g.reason
	g.mu.RUnlock()
	if !blocked {
		return nil
	}
	g.mu.Lock()
	g.denials++
	g.mu.Unlock()
	return fmt.Errorf("%w: %s", ErrStoreChanged, reason)
}

// Fence latches the guard closed from the in-band path -- the check script
// itself reporting that a tenant's counters belong to a different generation.
//
// The same latch as the out-of-band probe, deliberately. One store, one
// answer: a node that fenced one tenant because the store was replaced has no
// business serving the next one as though it had not been.
func (g *Guard) Fence(reason string) {
	g.latch(reason)
}

func (g *Guard) latch(reason string) {
	g.mu.Lock()
	first := !g.blocked
	g.blocked, g.reason = true, reason
	if first {
		g.latches++
	}
	g.mu.Unlock()
	if first {
		g.opts.Logger.Error("refusing every tenant: the counter store is not the one the cluster agreed on",
			"reason", reason)
	}
}

func (g *Guard) clear() {
	g.mu.Lock()
	was := g.blocked
	g.blocked, g.reason = false, ""
	g.mu.Unlock()
	if was {
		g.opts.Logger.Info("the counter store matches the generation the cluster agreed on; serving again")
	}
}

// Stats is what the cluster endpoint reports.
type Stats struct {
	// Blocked says this node is refusing every tenant.
	Blocked bool   `json:"blocked"`
	Reason  string `json:"reason,omitempty"`
	// Committed is what the cluster agreed on; Observed is what the store says.
	Committed uint64 `json:"committed"`
	Observed  uint64 `json:"observed"`
	RunID     string `json:"run_id,omitempty"`
	// Denials counts requests refused by this guard, and Latches how many
	// times it has closed. A guard that fires silently cannot be told from one
	// that never fires.
	Denials int64 `json:"denials"`
	Latches int64 `json:"latches"`
}

// Stats reports what the guard has seen.
func (g *Guard) Stats() Stats {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return Stats{
		Blocked:   g.blocked,
		Reason:    g.reason,
		Committed: g.opts.Committed(),
		Observed:  g.seen,
		RunID:     g.runID,
		Denials:   g.denials,
		Latches:   g.latches,
	}
}

// Reconcile probes the store once and brings the two views back into line.
//
// It is called on a timer and on every reconnect, and it is the only place the
// latch opens.
func (g *Guard) Reconcile(ctx context.Context) error {
	gen, runID, err := g.opts.Store.Identity(ctx)
	if err != nil {
		// A store we cannot read is an OUTAGE, not a reset. The tenant's
		// failure mode already decides what an unreachable store means, and
		// latching here would turn every network blip into a cluster-wide
		// refusal that outlives it.
		return err
	}

	g.mu.Lock()
	previous := g.runID
	g.runID, g.seen = runID, gen
	g.mu.Unlock()

	committed := g.opts.Committed()

	// Nothing has been agreed yet, so there is nothing older to protect. Mint
	// a name for this store and tell the cluster, but never refuse: zero means
	// unfenced everywhere else in this system and it means it here too.
	if committed == 0 {
		return g.adopt(ctx, gen, 1)
	}

	restarted := previous != "" && runID != "" && previous != runID
	switch {
	case gen == 0:
		// The key is gone. This is the FLUSHALL case, and it is the most
		// likely one -- which is why the run id cannot be the only signal: a
		// flush does not change it.
		g.latch("the store has no generation, so its counters were cleared")
	case gen != committed:
		g.latch(fmt.Sprintf("the store is generation %d and the cluster agreed on %d", gen, committed))
	case restarted:
		// Same generation, new process: the store came back with its data, so
		// the counters are the ones we think they are. Worth a line, not a
		// refusal.
		g.opts.Logger.Warn("the counter store restarted but kept its generation",
			"generation", gen, "was", previous, "now", runID)
		g.clear()
		return nil
	default:
		g.clear()
		return nil
	}

	// Latched. Try to name the store so the cluster can agree on it again --
	// always ABOVE the highest generation ever committed, so the number after
	// a wipe outranks the one a zombie still carries.
	return g.adopt(ctx, gen, committed+1)
}

// adopt mints a generation when the store has none and commits whatever is in
// force through the log.
func (g *Guard) adopt(ctx context.Context, observed, candidate uint64) error {
	inForce := observed
	if observed == 0 {
		minted, err := g.opts.Store.MintStoreGeneration(ctx, candidate)
		if err != nil {
			return err
		}
		inForce = minted
		g.mu.Lock()
		g.seen = minted
		g.mu.Unlock()
	}

	if inForce == g.opts.Committed() {
		g.clear()
		return nil
	}
	if g.opts.Propose == nil {
		return nil
	}
	if err := g.opts.Propose(ctx, inForce); err != nil {
		// A follower cannot commit, and that is not a fault: it stays latched
		// until the leader does it and its own state machine applies it.
		g.opts.Logger.Debug("could not commit the store generation here", "generation", inForce, "err", err)
		return nil
	}
	return nil
}

// Run reconciles on a timer until the context ends.
func (g *Guard) Run(ctx context.Context) {
	t := time.NewTicker(g.opts.Interval)
	defer t.Stop()
	for {
		if err := g.Reconcile(ctx); err != nil && ctx.Err() == nil {
			g.opts.Logger.Debug("could not read the counter store's identity", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

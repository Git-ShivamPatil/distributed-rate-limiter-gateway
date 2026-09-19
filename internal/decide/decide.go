// Package decide is the one place an admission question is answered.
//
// REST and gRPC both call it. That is the point: two surfaces that each
// implemented "look up the policy, pick the limits, check them, decide what a
// store failure means" would eventually answer differently, and the difference
// would show up as a tenant being limited by one caller and not the other.
// The surfaces keep their own shapes -- status codes, headers, streams -- and
// share the decision.
package decide

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/events"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

// ErrNoTenant means the request named no tenant at all.
var ErrNoTenant = errors.New("decide: no tenant named")

// ErrPeerUnavailable means the node that owns a tenant could not be reached or
// could not answer.
//
// It belongs to the Forwarder contract rather than to any implementation of
// it, because what the caller does about it -- decide locally instead -- is a
// property of this package's design, not of how the forward was attempted.
var ErrPeerUnavailable = errors.New("decide: the owning node did not answer")

// ErrStoreUnavailable means the counter store could not decide and the policy
// fails closed. It is deliberately not "refused": the caller is within its
// quota as far as anybody knows, and a 429 would be a lie about why.
var ErrStoreUnavailable = errors.New("decide: counter store unavailable")

// Query is one admission question.
type Query struct {
	Tenant string
	// Method and Path describe the request being asked about, so that
	// per-endpoint rules can apply. Empty means only tenant-wide limits.
	Method string
	Path   string
	Cost   int64
	// Peek reports what would happen and consumes nothing.
	Peek bool
	// Forwarded marks a question that another node has already sent here.
	// It is never forwarded again: two nodes with momentarily different views
	// of the ring would otherwise bounce it between them.
	Forwarded bool
}

// ClusterView answers "who coordinates this tenant".
type ClusterView interface {
	// Owner returns the coordinating node, whether it is this one, and whether
	// there is a ring at all.
	Owner(tenant string) (node ring.Node, isSelf bool, ok bool)
	Self() string
}

// Forwarder asks another node to decide.
type Forwarder interface {
	Check(ctx context.Context, addr string, q Query) (Outcome, error)
}

// Degraded records that an answer was given without the counter store
// agreeing -- the only case where a decision is not authoritative.
type Degraded struct {
	Reason string
	Mode   string
}

// Outcome is the answer, with everything either surface needs to render it.
type Outcome struct {
	Result   limiter.Result
	Policy   policy.Policy
	Degraded *Degraded
	// Owner names the node that coordinates this tenant, when there is a ring.
	Owner string
	// Forwarded reports that the answer came from that node rather than this
	// one. It is in the response because "which node decided this" is the
	// first question when two replicas disagree.
	Forwarded bool
}

// Stats counts what the ring is doing, for metrics and for tests that need to
// prove a forward actually happened rather than assuming it.
type Stats struct {
	Local     int64
	Forwarded int64
	// FellBack counts decisions made here after the owner could not be
	// reached. It is the number that says whether the ring is healthy.
	FellBack int64
}

// Service answers admission questions.
type Service struct {
	checker  limiter.Checker
	policies policy.Source
	node     string
	hub      *events.Hub
	clock    func() time.Time
	log      *slog.Logger

	cluster   ClusterView
	forwarder Forwarder

	local     atomic.Int64
	forwarded atomic.Int64
	fellBack  atomic.Int64
}

// Option configures a Service.
type Option func(*Service)

// WithHub publishes every decision to a hub for streaming subscribers.
func WithHub(h *events.Hub) Option { return func(s *Service) { s.hub = h } }

// WithClock replaces the timestamp source on published decisions.
func WithClock(fn func() time.Time) Option { return func(s *Service) { s.clock = fn } }

// WithLogger sets where fallbacks are reported.
func WithLogger(l *slog.Logger) Option { return func(s *Service) { s.log = l } }

// WithRing makes this node route a tenant's checks to the node that owns it.
//
// Both halves are required: a view with no forwarder could only report
// ownership, and a forwarder with no view would not know where to send
// anything.
func WithRing(view ClusterView, fwd Forwarder) Option {
	return func(s *Service) { s.cluster, s.forwarder = view, fwd }
}

// New builds the service.
func New(checker limiter.Checker, policies policy.Source, node string, opts ...Option) *Service {
	s := &Service{checker: checker, policies: policies, node: node, clock: time.Now, log: slog.Default()}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Stats reports the split between local and forwarded decisions.
func (s *Service) Stats() Stats {
	return Stats{
		Local:     s.local.Load(),
		Forwarded: s.forwarded.Load(),
		FellBack:  s.fellBack.Load(),
	}
}

// Node reports which node this is, which every answer carries.
func (s *Service) Node() string { return s.node }

// Decide answers one question.
//
// The errors it returns are the ones a surface has to distinguish, and they
// are distinct on purpose: an unknown tenant, a disabled tenant, an impossible
// cost and an unreachable store want four different answers and four
// different alerts. Anything else is a refusal, which is a normal Outcome
// rather than an error.
func (s *Service) Decide(ctx context.Context, q Query) (Outcome, error) {
	if q.Tenant == "" {
		return Outcome{}, ErrNoTenant
	}

	if out, handled, err := s.maybeForward(ctx, q); handled {
		return out, err
	}

	pol, err := s.policies.Lookup(ctx, q.Tenant)
	if err != nil {
		// policy.ErrTenantNotFound and ErrTenantDisabled pass through as
		// themselves; a store failure passes through as itself too, because
		// "we could not find out" is not "there is no such tenant".
		return Outcome{}, err
	}

	limits := pol.LimitsFor(q.Method, q.Path)

	res, err := s.checker.Check(ctx, limiter.Request{
		Tenant:   q.Tenant,
		Limits:   limits,
		Cost:     q.Cost,
		PeekOnly: q.Peek,
	})
	if err != nil {
		if errors.Is(err, limiter.ErrCostExceedsCapacity) {
			return Outcome{}, err
		}
		// The store could not decide. What that means is a policy decision:
		// refusing takes the tenant down with the store, admitting stops
		// enforcing for the duration. Either way the answer says so.
		if pol.FailsClosed() {
			return Outcome{Policy: pol}, fmt.Errorf("%w: %v", ErrStoreUnavailable, err)
		}
		out := Outcome{
			Policy:   pol,
			Result:   limiter.Result{Allowed: true},
			Degraded: &Degraded{Reason: err.Error(), Mode: config.FailOpen},
		}
		s.publish(q, out)
		return out, nil
	}

	out := Outcome{Result: res, Policy: pol}
	if s.cluster != nil {
		if owner, _, ok := s.cluster.Owner(q.Tenant); ok {
			out.Owner = owner.ID
		}
	}
	s.local.Add(1)
	if !q.Peek {
		s.publish(q, out)
	}
	return out, nil
}

// maybeForward sends the question to the tenant's owner when that is not this
// node. It reports handled=true when the caller should return what it gives
// back.
//
// A failed forward is NOT an error. Counters live in Redis and every admission
// is backed by an atomic script there, so deciding locally reaches the answer
// the owner would have reached; the only thing lost is the coordination the
// ring exists to provide. That is why losing an owner is a latency event
// rather than a capacity gap -- and it is the reason a rebalance in this
// design cannot over-admit.
func (s *Service) maybeForward(ctx context.Context, q Query) (Outcome, bool, error) {
	if s.cluster == nil || s.forwarder == nil || q.Forwarded {
		return Outcome{}, false, nil
	}
	owner, isSelf, ok := s.cluster.Owner(q.Tenant)
	if !ok || isSelf {
		return Outcome{}, false, nil
	}

	fwd := q
	fwd.Forwarded = true
	out, err := s.forwarder.Check(ctx, owner.Addr, fwd)
	if err == nil {
		s.forwarded.Add(1)
		out.Owner = owner.ID
		out.Forwarded = true
		// The owner published this decision to its own stream; publishing it
		// again here would double-count every forwarded request.
		return out, true, nil
	}

	if errors.Is(err, ErrPeerUnavailable) {
		s.fellBack.Add(1)
		s.log.Warn("the owning node did not answer; deciding locally",
			"tenant", q.Tenant, "owner", owner.ID, "addr", owner.Addr, "err", err)
		return Outcome{}, false, nil
	}
	// Anything else is an answer ABOUT the tenant -- unknown, disabled, a cost
	// no quota could admit -- and must be passed through rather than retried.
	return Outcome{}, true, err
}

func (s *Service) publish(q Query, out Outcome) {
	if s.hub == nil {
		return
	}
	cost := q.Cost
	if cost <= 0 {
		cost = 1
	}
	s.hub.Publish(events.Decision{
		Tenant:   q.Tenant,
		Policy:   out.Policy.Name,
		Allowed:  out.Result.Allowed,
		Limiting: out.Result.Limiting,
		Node:     s.node,
		Cost:     cost,
		At:       s.clock(),
	})
}

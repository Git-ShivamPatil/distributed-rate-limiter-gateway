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
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/events"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// ErrNoTenant means the request named no tenant at all.
var ErrNoTenant = errors.New("decide: no tenant named")

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
}

// Service answers admission questions.
type Service struct {
	checker  limiter.Checker
	policies policy.Source
	node     string
	hub      *events.Hub
	clock    func() time.Time
}

// Option configures a Service.
type Option func(*Service)

// WithHub publishes every decision to a hub for streaming subscribers.
func WithHub(h *events.Hub) Option { return func(s *Service) { s.hub = h } }

// WithClock replaces the timestamp source on published decisions.
func WithClock(fn func() time.Time) Option { return func(s *Service) { s.clock = fn } }

// New builds the service.
func New(checker limiter.Checker, policies policy.Source, node string, opts ...Option) *Service {
	s := &Service{checker: checker, policies: policies, node: node, clock: time.Now}
	for _, o := range opts {
		o(s)
	}
	return s
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
	if !q.Peek {
		s.publish(q, out)
	}
	return out, nil
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

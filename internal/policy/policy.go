// Package policy answers "what limits apply to this tenant's request".
//
// It is deliberately separate from the limiter: the limiter knows how to count,
// and this knows what the rules are. From M3 the rules live in Postgres behind
// a cache, and the only thing that changes is which Source the gateway is
// given -- the request path does not learn about it.
package policy

import (
	"context"
	"errors"
	"fmt"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// ErrTenantNotFound means the tenant has no policy and there is no default.
//
// It is an error rather than an empty limit set on purpose. An unknown tenant
// that silently gets no limits is the failure mode where a typo in a tenant id
// turns into unlimited traffic.
var ErrTenantNotFound = errors.New("policy: no policy for tenant")

// Policy is the rule set for one tenant.
//
// A Policy returned by a Source is READ-ONLY. Lookup happens on the request
// path, so it hands back the stored value rather than allocating a copy of the
// limits every time; a caller that edits one edits what the next request sees.
type Policy struct {
	// Name is the policy's name, not the tenant's -- several tenants can share
	// one named policy, which is how a plan ("free", "pro") is expressed.
	Name string
	// Limits all have to admit a request for it to be admitted.
	Limits []limiter.Limit
	// FailureMode decides what happens when the counter store is unreachable.
	FailureMode string
}

// FailsClosed reports whether an unreachable counter store should refuse.
func (p Policy) FailsClosed() bool { return p.FailureMode != config.FailOpen }

// Source resolves a tenant to its policy.
type Source interface {
	Lookup(ctx context.Context, tenant string) (Policy, error)
}

// Static is a Source backed by the configuration file. It is the bootstrap
// source, the one tests use, and the reason the gateway can run with no
// database at all.
type Static struct {
	byTenant map[string]Policy
	fallback *Policy
}

// NewStatic builds a Source from the policies section of a config file.
func NewStatic(c config.Policies) (*Static, error) {
	s := &Static{byTenant: make(map[string]Policy, len(c.Tenants))}

	build := func(name string, p config.Policy) Policy {
		limits := make([]limiter.Limit, len(p.Limits))
		copy(limits, p.Limits)
		return Policy{Name: name, Limits: limits, FailureMode: p.FailureMode}
	}

	for tenant, policyName := range c.Tenants {
		p, ok := c.Named[policyName]
		if !ok {
			return nil, fmt.Errorf("policy: tenant %q names undefined policy %q", tenant, policyName)
		}
		s.byTenant[tenant] = build(policyName, p)
	}
	if c.Default != "" {
		p, ok := c.Named[c.Default]
		if !ok {
			return nil, fmt.Errorf("policy: default names undefined policy %q", c.Default)
		}
		d := build(c.Default, p)
		s.fallback = &d
	}
	return s, nil
}

// Lookup returns the tenant's policy, or the default, or ErrTenantNotFound.
func (s *Static) Lookup(_ context.Context, tenant string) (Policy, error) {
	if p, ok := s.byTenant[tenant]; ok {
		return p, nil
	}
	if s.fallback != nil {
		return *s.fallback, nil
	}
	return Policy{}, fmt.Errorf("%w: %q", ErrTenantNotFound, tenant)
}

// Tenants lists the tenants with an explicit policy, for the admin API and the
// dashboard. The default policy's implicit tenants cannot be listed, because
// they are only known once they send traffic.
func (s *Static) Tenants() []string {
	out := make([]string, 0, len(s.byTenant))
	for t := range s.byTenant {
		out = append(out, t)
	}
	return out
}

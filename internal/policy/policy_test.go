package policy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

func limits(name string, count int64) []limiter.Limit {
	return []limiter.Limit{{
		Name:      name,
		Algorithm: limiter.AlgorithmTokenBucket,
		Count:     count,
		Period:    time.Minute,
		Burst:     count,
	}}
}

func policies() config.Policies {
	return config.Policies{
		Named: map[string]config.Policy{
			"free": {Limits: limits("per-minute", 20)},
			"pro":  {Limits: limits("per-minute", 1000), FailureMode: config.FailOpen},
		},
		Tenants: map[string]string{"acme": "free", "globex": "pro"},
	}
}

func TestStaticLookup(t *testing.T) {
	s, err := NewStatic(policies())
	if err != nil {
		t.Fatal(err)
	}

	p, err := s.Lookup(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "free" || len(p.Limits) != 1 || p.Limits[0].Count != 20 {
		t.Fatalf("acme resolved to %+v, want the free policy", p)
	}
	if !p.FailsClosed() {
		t.Error("a policy with no failure_mode must fail closed; an unenforced limit is indistinguishable from no limit")
	}

	pro, err := s.Lookup(context.Background(), "globex")
	if err != nil {
		t.Fatal(err)
	}
	if pro.FailsClosed() {
		t.Error("globex asked to fail open and does not")
	}
}

// Without a default, an unknown tenant is an error rather than an empty limit
// set -- a typo in a tenant id must not mean unlimited traffic.
func TestStaticUnknownTenant(t *testing.T) {
	s, err := NewStatic(policies())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lookup(context.Background(), "nobody"); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("err = %v, want ErrTenantNotFound", err)
	}
}

func TestStaticDefaultAppliesToUnknownTenants(t *testing.T) {
	cfg := policies()
	cfg.Default = "free"
	s, err := NewStatic(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Lookup(context.Background(), "nobody")
	if err != nil {
		t.Fatalf("unexpected error with a default configured: %v", err)
	}
	if p.Name != "free" {
		t.Fatalf("default resolved to %q, want free", p.Name)
	}
}

func TestStaticRejectsDanglingReferences(t *testing.T) {
	cfg := policies()
	cfg.Tenants["ghost"] = "does-not-exist"
	if _, err := NewStatic(cfg); err == nil {
		t.Error("a tenant pointing at an undefined policy was accepted")
	}

	cfg = policies()
	cfg.Default = "does-not-exist"
	if _, err := NewStatic(cfg); err == nil {
		t.Error("a default pointing at an undefined policy was accepted")
	}
}

// A Source's policies must not alias the config they were built from.
//
// Lookup deliberately does NOT deep-copy on every call -- that would be an
// allocation per request on the hot path -- so a returned Policy is read-only
// by contract. What must hold instead is that the source owns its own copy, so
// that editing the config afterwards cannot silently re-rate live tenants.
func TestStaticDoesNotAliasTheConfig(t *testing.T) {
	cfg := policies()
	s, err := NewStatic(cfg)
	if err != nil {
		t.Fatal(err)
	}

	cfg.Named["free"].Limits[0].Count = 1
	cfg.Tenants["acme"] = "pro"

	p, _ := s.Lookup(context.Background(), "acme")
	if p.Name != "free" || p.Limits[0].Count != 20 {
		t.Fatalf("editing the config changed a built source: got %+v", p)
	}
}

// Two tenants sharing a named policy must not share one slice of limits, or a
// future per-tenant override would edit both.
func TestStaticTenantsDoNotShareLimitStorage(t *testing.T) {
	cfg := policies()
	cfg.Tenants["initech"] = "free"
	s, err := NewStatic(cfg)
	if err != nil {
		t.Fatal(err)
	}

	a, _ := s.Lookup(context.Background(), "acme")
	b, _ := s.Lookup(context.Background(), "initech")
	a.Limits[0].Count = 1
	if b.Limits[0].Count != 20 {
		t.Fatal("two tenants on one named policy share the same limit storage")
	}
}

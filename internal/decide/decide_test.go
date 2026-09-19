package decide

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/events"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

type source struct {
	p   policy.Policy
	err error
}

func (s *source) Lookup(context.Context, string) (policy.Policy, error) {
	if s.err != nil {
		return policy.Policy{}, s.err
	}
	return s.p, nil
}

type failingChecker struct{ err error }

func (f failingChecker) Check(context.Context, limiter.Request) (limiter.Result, error) {
	return limiter.Result{}, f.err
}

func bucket(name string, count int64, period time.Duration) limiter.Limit {
	return limiter.Limit{Name: name, Algorithm: limiter.AlgorithmTokenBucket,
		Count: count, Period: period, Burst: count}
}

func memService(t *testing.T, p policy.Policy, opts ...Option) *Service {
	t.Helper()
	mem := limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0))))
	return New(mem, &source{p: p}, "node-a", opts...)
}

func TestDecideConsumesAndRefuses(t *testing.T) {
	s := memService(t, policy.Policy{Name: "free", Limits: []limiter.Limit{bucket("per-minute", 3, time.Minute)}})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		out, err := s.Decide(ctx, Query{Tenant: "acme"})
		if err != nil || !out.Result.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, out.Result.Allowed, err)
		}
	}
	out, err := s.Decide(ctx, Query{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Result.Allowed {
		t.Fatal("a fourth request passed a limit of 3")
	}
	if out.Result.Limiting != "per-minute" {
		t.Fatalf("limiting = %q", out.Result.Limiting)
	}
}

func TestDecideRequiresATenant(t *testing.T) {
	s := memService(t, policy.Policy{Name: "free", Limits: []limiter.Limit{bucket("l", 1, time.Minute)}})
	if _, err := s.Decide(context.Background(), Query{}); !errors.Is(err, ErrNoTenant) {
		t.Fatalf("err = %v, want ErrNoTenant", err)
	}
}

// The errors a surface has to tell apart pass through unchanged.
func TestDecidePassesStoreErrorsThrough(t *testing.T) {
	notFound := fmt.Errorf("%w: %q", policy.ErrTenantNotFound, "ghost")
	disabled := fmt.Errorf("%w: %q", policy.ErrTenantDisabled, "off")

	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"unknown tenant", notFound, policy.ErrTenantNotFound},
		{"disabled tenant", disabled, policy.ErrTenantDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := New(limiter.NewMemory(), &source{err: tc.err}, "node-a")
			_, err := s.Decide(context.Background(), Query{Tenant: "x"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecideRejectsImpossibleCost(t *testing.T) {
	s := memService(t, policy.Policy{Name: "free", Limits: []limiter.Limit{bucket("per-minute", 5, time.Minute)}})
	_, err := s.Decide(context.Background(), Query{Tenant: "acme", Cost: 6})
	if !errors.Is(err, limiter.ErrCostExceedsCapacity) {
		t.Fatalf("err = %v, want ErrCostExceedsCapacity", err)
	}
}

// The fail-open/fail-closed seam, which is a policy decision rather than a
// technical one -- and either way the answer says it was not authoritative.
func TestDecideStoreFailure(t *testing.T) {
	storeDown := errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")
	limits := []limiter.Limit{bucket("per-minute", 5, time.Minute)}

	t.Run("closed refuses with a distinct error", func(t *testing.T) {
		s := New(failingChecker{storeDown},
			&source{p: policy.Policy{Name: "free", Limits: limits, FailureMode: config.FailClosed}}, "node-a")
		_, err := s.Decide(context.Background(), Query{Tenant: "acme"})
		if !errors.Is(err, ErrStoreUnavailable) {
			t.Fatalf("err = %v, want ErrStoreUnavailable", err)
		}
	})

	t.Run("open admits and says so", func(t *testing.T) {
		s := New(failingChecker{storeDown},
			&source{p: policy.Policy{Name: "free", Limits: limits, FailureMode: config.FailOpen}}, "node-a")
		out, err := s.Decide(context.Background(), Query{Tenant: "acme"})
		if err != nil {
			t.Fatal(err)
		}
		if !out.Result.Allowed {
			t.Fatal("a fail-open policy refused")
		}
		if out.Degraded == nil || out.Degraded.Mode != config.FailOpen {
			t.Fatalf("degraded = %+v, want the answer marked unenforced", out.Degraded)
		}
	})
}

// An endpoint rule applies only to the requests it describes, and only when
// the caller said which request it is asking about.
func TestDecideAppliesEndpointRules(t *testing.T) {
	p := policy.Policy{
		Name: "rules",
		Limits: []limiter.Limit{
			bucket("wide", 1000, time.Minute),
			bucket("writes", 2, time.Hour),
		},
		Matches: []policy.Match{{}, {Method: http.MethodPost, PathPrefix: "/api/orders"}},
	}
	s := memService(t, p)
	ctx := context.Background()

	out, err := s.Decide(ctx, Query{Tenant: "acme", Method: http.MethodPost, Path: "/api/orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Result.Decisions) != 2 {
		t.Fatalf("a matching write saw %d limits, want 2", len(out.Result.Decisions))
	}

	out, err = s.Decide(ctx, Query{Tenant: "acme", Method: http.MethodGet, Path: "/api/orders"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Result.Decisions) != 1 {
		t.Fatalf("a read saw %d limits, want 1", len(out.Result.Decisions))
	}

	out, err = s.Decide(ctx, Query{Tenant: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Result.Decisions) != 1 {
		t.Fatalf("an undescribed request saw %d limits, want only the tenant-wide one", len(out.Result.Decisions))
	}
}

func TestDecidePublishesToTheHub(t *testing.T) {
	hub := events.NewHub(16)
	ch, stop := hub.Subscribe(context.Background(), events.Filter{})
	defer stop()

	s := memService(t, policy.Policy{Name: "free", Limits: []limiter.Limit{bucket("per-minute", 1, time.Minute)}},
		WithHub(hub), WithClock(func() time.Time { return time.Unix(1_700_000_000, 0) }))
	ctx := context.Background()

	if _, err := s.Decide(ctx, Query{Tenant: "acme", Cost: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Decide(ctx, Query{Tenant: "acme"}); err != nil {
		t.Fatal(err)
	}

	first := <-ch
	if !first.Allowed || first.Tenant != "acme" || first.Node != "node-a" || first.Policy != "free" {
		t.Fatalf("first decision = %+v", first)
	}
	second := <-ch
	if second.Allowed || second.Limiting != "per-minute" {
		t.Fatalf("second decision = %+v, want a refusal naming the limit", second)
	}
}

// A peek changes nothing, so it is not a decision and must not appear in the
// stream -- otherwise a dashboard's own polling would show up as traffic.
func TestDecidePeekIsNotPublished(t *testing.T) {
	hub := events.NewHub(16)
	ch, stop := hub.Subscribe(context.Background(), events.Filter{})
	defer stop()

	s := memService(t, policy.Policy{Name: "free", Limits: []limiter.Limit{bucket("per-minute", 5, time.Minute)}},
		WithHub(hub))

	if _, err := s.Decide(context.Background(), Query{Tenant: "acme", Peek: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case d := <-ch:
		t.Fatalf("a peek was published as a decision: %+v", d)
	case <-time.After(200 * time.Millisecond):
	}
}

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load("../../configs/local.yaml")
	if err != nil {
		t.Fatalf("loading configs/local.yaml: %v", err)
	}
	return cfg
}

func newTestServer(t *testing.T, clock limiter.Clock) (*Server, *limiter.Memory) {
	t.Helper()
	cfg := testConfig(t)
	policies, err := policy.NewStatic(cfg.Policies)
	if err != nil {
		t.Fatal(err)
	}
	mem := limiter.NewMemory(limiter.WithClock(clock))
	return New(cfg, mem, policies, nil), mem
}

func get(t *testing.T, h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// The milestone's own verification step, as a test: 30 requests against the
// shipped config's 20-token bucket are 20 allowed then 10 refused.
func TestCheckBurstMatchesTheAdvertisedNumbers(t *testing.T) {
	srv, _ := newTestServer(t, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	codes := map[int]int{}
	for i := 0; i < 30; i++ {
		rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme"})
		codes[rec.Code]++
	}
	if codes[http.StatusOK] != 20 || codes[http.StatusTooManyRequests] != 10 {
		t.Fatalf("got %d x 200 and %d x 429, want 20 and 10 (config: 20 tokens per minute)",
			codes[http.StatusOK], codes[http.StatusTooManyRequests])
	}
}

func TestCheckHeadersAndBody(t *testing.T) {
	srv, _ := newTestServer(t, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "20" {
		t.Errorf("X-RateLimit-Limit = %q, want 20", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "19" {
		t.Errorf("X-RateLimit-Remaining = %q, want 19", got)
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("an allowed request carried Retry-After %q", got)
	}

	var body checkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Allowed || body.Tenant != "acme" || body.Policy != "free" || body.Node != "gateway-1" {
		t.Errorf("body = %+v, want an allowed decision for acme on policy free from gateway-1", body)
	}
	if len(body.Limits) != 1 || body.Limits[0].Name != "per-minute" {
		t.Errorf("body names %d limits, want 1 called per-minute", len(body.Limits))
	}

	// Exhaust the bucket and check the refusal's shape.
	for i := 0; i < 19; i++ {
		get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme"})
	}
	rec = get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme"})
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("code = %d, want 429", rec.Code)
	}
	// A token arrives every 3 seconds, so Retry-After rounds up to 3.
	if got := rec.Header().Get("Retry-After"); got != "3" {
		t.Errorf("Retry-After = %q, want 3", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", got)
	}
	body = checkResponse{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Allowed || body.Limiting != "per-minute" || body.RetryAfterMS != 3000 {
		t.Errorf("refusal body = %+v, want allowed=false limiting=per-minute retry_after_ms=3000", body)
	}
}

// Both limits of the pro policy are evaluated, and the tighter one is the one
// the headers describe.
func TestCheckEvaluatesEveryLimit(t *testing.T) {
	srv, _ := newTestServer(t, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "globex"})
	var body checkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Limits) != 2 {
		t.Fatalf("pro tenant saw %d limits, want 2", len(body.Limits))
	}
	// per-second (25) is tighter than per-minute (200 burst), so it sets the
	// headers even though both allowed.
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "25" {
		t.Errorf("X-RateLimit-Limit = %q, want 25 (the tighter limit)", got)
	}

	// The 25-per-second window refuses before the per-minute bucket does.
	for i := 0; i < 24; i++ {
		get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "globex"})
	}
	rec = get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "globex"})
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("code = %d, want 429 from the per-second window", rec.Code)
	}
	body = checkResponse{}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Limiting != "per-second" {
		t.Errorf("limiting = %q, want per-second", body.Limiting)
	}
}

func TestCheckCost(t *testing.T) {
	srv, _ := newTestServer(t, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme", CostHeader: "5"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "15" {
		t.Errorf("after cost 5 of 20: remaining = %q, want 15", got)
	}

	// A cost larger than the bucket can ever hold is a client error: no amount
	// of waiting would make it succeed, so 429 with a Retry-After would lie.
	rec = get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme", CostHeader: "21"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("cost 21 against a 20-token bucket: code = %d, want 400", rec.Code)
	}
	var e errorResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	if e.Code != "cost_too_large" {
		t.Errorf("error code = %q, want cost_too_large", e.Code)
	}

	for _, bad := range []string{"0", "-1", "abc"} {
		rec = get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme", CostHeader: bad})
		if rec.Code != http.StatusBadRequest {
			t.Errorf("cost %q: code = %d, want 400", bad, rec.Code)
		}
	}
}

func TestCheckRejectsMissingTenant(t *testing.T) {
	srv, _ := newTestServer(t, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/v1/check", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", rec.Code)
	}
	var e errorResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	if e.Code != "tenant_missing" {
		t.Errorf("error code = %q, want tenant_missing", e.Code)
	}
}

// An unknown tenant with no default policy is refused rather than waved
// through: a typo in a tenant id must not mean unlimited traffic.
func TestCheckUnknownTenantWithoutDefault(t *testing.T) {
	cfg := testConfig(t)
	cfg.Policies.Default = "" // remove the catch-all
	policies, err := policy.NewStatic(cfg.Policies)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))), policies, nil)

	rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "who-is-this"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", rec.Code)
	}
	var e errorResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	if e.Code != "tenant_unknown" {
		t.Errorf("error code = %q, want tenant_unknown", e.Code)
	}
}

func TestCheckTenantHeaderCanBeDistrusted(t *testing.T) {
	cfg := testConfig(t)
	cfg.CheckAPI.TrustTenantHeader = false
	policies, _ := policy.NewStatic(cfg.Policies)
	srv := New(cfg, limiter.NewMemory(), policies, nil)

	rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400 when the header is not trusted and no auth is configured", rec.Code)
	}
}

// failingChecker stands in for an unreachable counter store.
type failingChecker struct{ err error }

func (f failingChecker) Check(context.Context, limiter.Request) (limiter.Result, error) {
	return limiter.Result{}, f.err
}

// The fail-open/fail-closed seam. Both answers say they are not authoritative,
// because a caller cannot otherwise tell an enforced 200 from an unenforced one.
func TestCheckStoreFailure(t *testing.T) {
	storeDown := errors.New("dial tcp 127.0.0.1:6379: connect: connection refused")

	t.Run("closed refuses", func(t *testing.T) {
		cfg := testConfig(t)
		policies, _ := policy.NewStatic(cfg.Policies)
		srv := New(cfg, failingChecker{storeDown}, policies, nil)

		rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme"})
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("code = %d, want 503", rec.Code)
		}
		var body checkResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body.Allowed || body.Degraded == nil || body.Degraded.Mode != config.FailClosed {
			t.Fatalf("body = %+v, want a refusal marked degraded/closed", body)
		}
	})

	t.Run("open admits but says so", func(t *testing.T) {
		cfg := testConfig(t)
		free := cfg.Policies.Named["free"]
		free.FailureMode = config.FailOpen
		cfg.Policies.Named["free"] = free
		policies, _ := policy.NewStatic(cfg.Policies)
		srv := New(cfg, failingChecker{storeDown}, policies, nil)

		rec := get(t, srv.Handler(), "/v1/check", map[string]string{TenantHeader: "acme"})
		if rec.Code != http.StatusOK {
			t.Fatalf("code = %d, want 200", rec.Code)
		}
		var body checkResponse
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if !body.Allowed || body.Degraded == nil || body.Degraded.Mode != config.FailOpen {
			t.Fatalf("body = %+v, want an admission marked degraded/open", body)
		}
	})
}

func TestHealthz(t *testing.T) {
	srv, _ := newTestServer(t, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))
	rec := get(t, srv.Handler(), "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
}

func TestSecondsCeilRoundsUp(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want int64
	}{
		{0, 0},
		{-time.Second, 0},
		{time.Nanosecond, 1}, // never round a wait down to zero
		{time.Second, 1},
		{time.Second + time.Nanosecond, 2},
		{2500 * time.Millisecond, 3},
	}
	for _, c := range cases {
		if got := secondsCeil(c.in); got != c.want {
			t.Errorf("secondsCeil(%s) = %d, want %d", c.in, got, c.want)
		}
	}
}

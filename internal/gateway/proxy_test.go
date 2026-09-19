package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// upstream records what the gateway forwarded to it.
type upstream struct {
	server *httptest.Server
	hits   atomic.Int64
	last   atomic.Value // lastRequest
}

type lastRequest struct {
	Method  string
	Path    string
	Tenant  string
	Auth    string
	APIKey  string
	Forward string
}

func newUpstream(t *testing.T) *upstream {
	t.Helper()
	u := &upstream{}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		u.last.Store(lastRequest{
			Method:  r.Method,
			Path:    r.URL.Path,
			Tenant:  r.Header.Get(UpstreamTenantHeader),
			Auth:    r.Header.Get("Authorization"),
			APIKey:  r.Header.Get("X-API-Key"),
			Forward: r.Header.Get("X-Forwarded-For"),
		})
		w.Header().Set("X-Upstream", "test")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"path": r.URL.Path})
	}))
	t.Cleanup(u.server.Close)
	return u
}

func (u *upstream) lastReq(t *testing.T) lastRequest {
	t.Helper()
	v, _ := u.last.Load().(lastRequest)
	return v
}

func proxyServer(t *testing.T, routes []config.Route, clock limiter.Clock) *Server {
	t.Helper()
	cfg := testConfig(t)
	cfg.Routes = routes
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}
	policies, err := policy.NewStatic(cfg.Policies)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := NewProxy(cfg.Routes, nil)
	if err != nil {
		t.Fatal(err)
	}
	mem := limiter.NewMemory(limiter.WithClock(clock))
	return New(cfg, decide.New(mem, policies, cfg.Node.ID), nil, WithProxy(proxy))
}

func TestProxyForwardsAnAdmittedRequest(t *testing.T) {
	up := newUpstream(t)
	srv := proxyServer(t, []config.Route{{
		Name: "echo", PathPrefix: "/api/echo", Upstream: up.server.URL, StripPrefix: "/api",
	}}, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/api/echo/hello", map[string]string{TenantHeader: "acme"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body)
	}
	if up.hits.Load() != 1 {
		t.Fatalf("upstream saw %d requests, want 1", up.hits.Load())
	}

	got := up.lastReq(t)
	// /api/echo/hello with /api stripped reaches the upstream as /echo/hello.
	if got.Path != "/echo/hello" {
		t.Errorf("upstream path = %q, want /echo/hello", got.Path)
	}
	if got.Tenant != "acme" {
		t.Errorf("upstream tenant header = %q, want acme", got.Tenant)
	}
	if got.Forward == "" {
		t.Error("upstream received no X-Forwarded-For")
	}

	// The limiter's answer travels with the proxied response.
	if v := rec.Header().Get("X-RateLimit-Limit"); v != "20" {
		t.Errorf("X-RateLimit-Limit = %q, want 20", v)
	}
	if v := rec.Header().Get("X-RateLimit-Remaining"); v != "19" {
		t.Errorf("X-RateLimit-Remaining = %q, want 19", v)
	}
	if v := rec.Header().Get("X-Upstream"); v != "test" {
		t.Errorf("the upstream's own headers were lost: X-Upstream = %q", v)
	}
}

// A refused request must never reach the upstream. That is the reason the
// limiter is in front of it.
func TestProxyRefusalNeverReachesTheUpstream(t *testing.T) {
	up := newUpstream(t)
	srv := proxyServer(t, []config.Route{{
		Name: "echo", PathPrefix: "/api/echo", Upstream: up.server.URL,
	}}, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	allowed, refused := 0, 0
	for i := 0; i < 30; i++ {
		rec := get(t, srv.Handler(), "/api/echo", map[string]string{TenantHeader: "acme"})
		switch rec.Code {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			refused++
			if rec.Header().Get("Retry-After") == "" {
				t.Fatal("a proxied refusal carried no Retry-After")
			}
			var body checkResponse
			_ = json.Unmarshal(rec.Body.Bytes(), &body)
			if body.Route != "echo" {
				t.Errorf("the refusal does not name the route it was for: %+v", body)
			}
		default:
			t.Fatalf("unexpected code %d", rec.Code)
		}
	}
	if allowed != 20 || refused != 10 {
		t.Fatalf("%d allowed and %d refused, want 20 and 10", allowed, refused)
	}
	if up.hits.Load() != 20 {
		t.Fatalf("the upstream saw %d requests; it must see only the 20 admitted ones", up.hits.Load())
	}
}

// The upstream trusts the gateway, not the client, so the caller's credentials
// stop here.
func TestProxyDoesNotForwardCallerCredentials(t *testing.T) {
	up := newUpstream(t)
	srv := proxyServer(t, []config.Route{{
		Name: "echo", PathPrefix: "/api/echo", Upstream: up.server.URL,
	}}, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/api/echo", map[string]string{
		TenantHeader:    "acme",
		"Authorization": "Bearer a-token-the-upstream-must-not-see",
		"X-API-Key":     "rlk_secret",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	got := up.lastReq(t)
	if got.Auth != "" {
		t.Errorf("the caller's Authorization header reached the upstream: %q", got.Auth)
	}
	if got.APIKey != "" {
		t.Errorf("the caller's API key reached the upstream: %q", got.APIKey)
	}
	if got.Tenant != "acme" {
		t.Errorf("the upstream was not told which tenant this is: %q", got.Tenant)
	}
}

// The longest matching prefix wins, so the order of the config never decides.
func TestProxyLongestPrefixWins(t *testing.T) {
	general := newUpstream(t)
	special := newUpstream(t)
	srv := proxyServer(t, []config.Route{
		{Name: "general", PathPrefix: "/api", Upstream: general.server.URL},
		{Name: "special", PathPrefix: "/api/echo/special", Upstream: special.server.URL},
	}, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	get(t, srv.Handler(), "/api/echo/special/thing", map[string]string{TenantHeader: "globex"})
	get(t, srv.Handler(), "/api/other", map[string]string{TenantHeader: "globex"})

	if special.hits.Load() != 1 {
		t.Errorf("the more specific route saw %d requests, want 1", special.hits.Load())
	}
	if general.hits.Load() != 1 {
		t.Errorf("the general route saw %d requests, want 1", general.hits.Load())
	}
}

func TestProxyUnroutedPathIsGatewayNotFound(t *testing.T) {
	up := newUpstream(t)
	srv := proxyServer(t, []config.Route{{
		Name: "echo", PathPrefix: "/api/echo", Upstream: up.server.URL,
	}}, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/nothing/here", map[string]string{TenantHeader: "acme"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", rec.Code)
	}
	if up.hits.Load() != 0 {
		t.Fatal("an unrouted path reached an upstream")
	}
}

// An upstream that is down is a 502, not a 429: it is not the client's fault
// and it is not a rate-limit decision.
func TestProxyDeadUpstreamIsBadGateway(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := dead.URL
	dead.Close() // nothing is listening now

	srv := proxyServer(t, []config.Route{{
		Name: "dead", PathPrefix: "/api/dead", Upstream: addr,
	}}, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	rec := get(t, srv.Handler(), "/api/dead", map[string]string{TenantHeader: "acme"})
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("code = %d, want 502", rec.Code)
	}
	var e errorResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &e)
	if e.Code != "upstream_unavailable" {
		t.Errorf("error code = %q, want upstream_unavailable", e.Code)
	}
}

// The proxy path applies endpoint rules using the real method and path, which
// it can see -- unlike the decision API, which is told about them.
func TestProxyAppliesEndpointRules(t *testing.T) {
	up := newUpstream(t)
	cfg := testConfig(t)
	cfg.Routes = []config.Route{{Name: "api", PathPrefix: "/api", Upstream: up.server.URL}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	// A tenant whose policy has a generous tenant-wide limit and a strict rule
	// on one endpoint.
	source := &fixedSource{p: policy.Policy{
		Name: "rules",
		Limits: []limiter.Limit{
			{Name: "wide", Algorithm: limiter.AlgorithmTokenBucket, Count: 1000, Period: time.Minute, Burst: 1000},
			{Name: "writes", Algorithm: limiter.AlgorithmTokenBucket, Count: 2, Period: time.Hour, Burst: 2},
		},
		Matches: []policy.Match{{}, {Method: http.MethodPost, PathPrefix: "/api/orders"}},
	}}
	mem := limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0))))
	proxy, err := NewProxy(cfg.Routes, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, decide.New(mem, source, cfg.Node.ID), nil, WithProxy(proxy))

	post := func(path string) int {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.Header.Set(TenantHeader, "any")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec.Code
	}

	// Two writes are allowed, the third is not.
	for i := 0; i < 2; i++ {
		if code := post("/api/orders"); code != http.StatusOK {
			t.Fatalf("write %d: code = %d", i, code)
		}
	}
	if code := post("/api/orders"); code != http.StatusTooManyRequests {
		t.Fatalf("the third write: code = %d, want 429 from the endpoint rule", code)
	}
	// A different endpoint is unaffected: the rule did not apply to it.
	if code := post("/api/reads"); code != http.StatusOK {
		t.Fatalf("a request to another endpoint: code = %d, want 200", code)
	}
}

// fixedSource returns one policy for every tenant.
type fixedSource struct{ p policy.Policy }

func (f *fixedSource) Lookup(_ context.Context, _ string) (policy.Policy, error) {
	return f.p, nil
}

func TestProxyStripPrefixEdgeCases(t *testing.T) {
	up := newUpstream(t)
	srv := proxyServer(t, []config.Route{{
		Name: "echo", PathPrefix: "/api/echo", Upstream: up.server.URL, StripPrefix: "/api/echo",
	}}, limiter.NewFakeClock(time.Unix(1_700_000_000, 0)))

	// Stripping the whole prefix must leave a valid path, not an empty one.
	rec := get(t, srv.Handler(), "/api/echo", map[string]string{TenantHeader: "globex"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body)
	}
	if got := up.lastReq(t).Path; got != "/" {
		t.Errorf("upstream path = %q, want / when the whole prefix is stripped", got)
	}

	rec = get(t, srv.Handler(), "/api/echo/deep/path", map[string]string{TenantHeader: "globex"})
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Body.String())
	}
	if got := up.lastReq(t).Path; got != "/deep/path" {
		t.Errorf("upstream path = %q, want /deep/path", got)
	}
}

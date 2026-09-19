package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// fakeAdminStore records what the handlers asked it to do.
type fakeAdminStore struct {
	mu       sync.Mutex
	tenants  []policy.TenantRecord
	policies []policy.PolicyRecord
	keys     []string
	revoked  []string
	err      error
}

func (f *fakeAdminStore) Tenants(context.Context) ([]policy.TenantRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tenants, f.err
}

func (f *fakeAdminStore) UpsertTenant(_ context.Context, t policy.TenantRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.tenants = append(f.tenants, t)
	return nil
}

func (f *fakeAdminStore) DeleteTenant(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	for i, t := range f.tenants {
		if t.ID == id {
			f.tenants = append(f.tenants[:i], f.tenants[i+1:]...)
			return nil
		}
	}
	return policy.ErrTenantNotFound
}

func (f *fakeAdminStore) Policies(context.Context) ([]policy.PolicyRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.policies, f.err
}

func (f *fakeAdminStore) UpsertPolicy(_ context.Context, rec policy.PolicyRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.policies = append(f.policies, rec)
	return nil
}

func (f *fakeAdminStore) CreateAPIKey(_ context.Context, tenant, _ string, hash []byte, prefix string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if len(hash) != 32 {
		return errShortHash
	}
	f.keys = append(f.keys, tenant+"/"+prefix)
	return nil
}

func (f *fakeAdminStore) RevokeAPIKey(_ context.Context, tenant, prefix string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.revoked = append(f.revoked, tenant+"/"+prefix)
	return nil
}

var errShortHash = &shortHashError{}

type shortHashError struct{}

func (*shortHashError) Error() string { return "the stored hash is not a sha-256" }

const testAdminToken = "test-admin-token"

func adminServer(t *testing.T, store AdminStore, token string) (*Server, *policy.Cache) {
	t.Helper()
	cfg := testConfig(t)
	policies, err := policy.NewStatic(cfg.Policies)
	if err != nil {
		t.Fatal(err)
	}
	cache := policy.NewCache(policies, policy.WithTTL(time.Hour))
	opts := []Option{WithCache(cache)}
	if store != nil {
		opts = append(opts, WithAdmin(store, auth.NewAdminToken(token)))
	}
	srv := New(cfg, limiter.NewMemory(), cache, nil, opts...)
	return srv, cache
}

func adminRequest(t *testing.T, srv *Server, method, path, token string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = int64(buf.Len())
	}
	if token != "" {
		req.Header.Set("X-Admin-Token", token)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

// Without a writable store there is nowhere to put an edit, and accepting one
// would mean losing it.
func TestAdminRefusesWithoutAWritableStore(t *testing.T) {
	srv, _ := adminServer(t, nil, testAdminToken)
	rec := adminRequest(t, srv, http.MethodGet, "/admin/v1/tenants", testAdminToken, nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("code = %d, want 501", rec.Code)
	}
}

// A management surface with no credential is worse than no management surface.
func TestAdminRefusesWithoutAToken(t *testing.T) {
	srv, _ := adminServer(t, &fakeAdminStore{}, "")
	rec := adminRequest(t, srv, http.MethodGet, "/admin/v1/tenants", "anything", nil)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("code = %d, want 501 when no admin token is configured", rec.Code)
	}
}

func TestAdminRequiresTheRightToken(t *testing.T) {
	srv, _ := adminServer(t, &fakeAdminStore{}, testAdminToken)

	for _, tc := range []struct {
		name  string
		token string
		want  int
	}{
		{"no token", "", http.StatusUnauthorized},
		{"wrong token", "not-the-token", http.StatusUnauthorized},
		{"right token", testAdminToken, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := adminRequest(t, srv, http.MethodGet, "/admin/v1/tenants", tc.token, nil)
			if rec.Code != tc.want {
				t.Fatalf("code = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func TestAdminCreatesATenantAndInvalidatesTheCache(t *testing.T) {
	store := &fakeAdminStore{}
	srv, cache := adminServer(t, store, testAdminToken)

	// Warm the cache so the invalidation has something to drop.
	if _, err := cache.Lookup(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}
	before := cache.Stats().Invalidated

	rec := adminRequest(t, srv, http.MethodPost, "/admin/v1/tenants", testAdminToken,
		map[string]any{"id": "acme", "name": "Acme", "policy": "free"})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body)
	}
	if len(store.tenants) != 1 || store.tenants[0].ID != "acme" {
		t.Fatalf("store received %+v", store.tenants)
	}
	if cache.Stats().Invalidated == before {
		t.Fatal("writing a tenant did not invalidate this node's cache, so the edit would not take effect until the TTL lapsed")
	}
}

func TestAdminValidatesInput(t *testing.T) {
	srv, _ := adminServer(t, &fakeAdminStore{}, testAdminToken)

	cases := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
	}{
		{"tenant without a policy", http.MethodPost, "/admin/v1/tenants",
			map[string]any{"id": "acme"}, http.StatusBadRequest},
		{"unknown field", http.MethodPost, "/admin/v1/tenants",
			map[string]any{"id": "acme", "policy": "free", "plan": "gold"}, http.StatusBadRequest},
		{"policy with no limits", http.MethodPut, "/admin/v1/policies/empty",
			map[string]any{"limits": []any{}}, http.StatusBadRequest},
		{"limit with a zero count", http.MethodPut, "/admin/v1/policies/bad",
			map[string]any{"limits": []any{
				map[string]any{"name": "l", "algorithm": "token_bucket", "count": 0, "period_ms": 1000},
			}}, http.StatusBadRequest},
		{"unknown algorithm", http.MethodPut, "/admin/v1/policies/bad",
			map[string]any{"limits": []any{
				map[string]any{"name": "l", "algorithm": "leaky_faucet", "count": 1, "period_ms": 1000},
			}}, http.StatusBadRequest},
		{"two limits with one name", http.MethodPut, "/admin/v1/policies/bad",
			map[string]any{"limits": []any{
				map[string]any{"name": "l", "algorithm": "token_bucket", "count": 1, "period_ms": 1000},
				map[string]any{"name": "l", "algorithm": "token_bucket", "count": 2, "period_ms": 1000},
			}}, http.StatusBadRequest},
		{"bad match method", http.MethodPut, "/admin/v1/policies/bad",
			map[string]any{"limits": []any{
				map[string]any{"name": "l", "algorithm": "token_bucket", "count": 1, "period_ms": 1000, "method": "FETCH"},
			}}, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := adminRequest(t, srv, tc.method, tc.path, testAdminToken, tc.body)
			if rec.Code != tc.want {
				t.Fatalf("code = %d, want %d; body = %s", rec.Code, tc.want, rec.Body)
			}
		})
	}
}

func TestAdminUpsertPolicyRoundTrip(t *testing.T) {
	store := &fakeAdminStore{}
	srv, cache := adminServer(t, store, testAdminToken)
	if _, err := cache.Lookup(context.Background(), "acme"); err != nil {
		t.Fatal(err)
	}

	rec := adminRequest(t, srv, http.MethodPut, "/admin/v1/policies/gold", testAdminToken,
		map[string]any{
			"failure_mode": "closed",
			"limits": []any{
				map[string]any{"name": "per-minute", "algorithm": "token_bucket", "count": 500, "period_ms": 60000, "burst": 100},
				map[string]any{"name": "writes", "algorithm": "sliding_window", "count": 10, "period_ms": 1000,
					"method": "POST", "path_prefix": "/api/orders"},
			},
		})
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body)
	}
	if len(store.policies) != 1 {
		t.Fatalf("store received %d policies", len(store.policies))
	}
	got := store.policies[0]
	if got.Name != "gold" {
		t.Errorf("name = %q, want gold (from the URL, not the body)", got.Name)
	}
	if len(got.Limits) != 2 {
		t.Fatalf("got %d limits", len(got.Limits))
	}
	if got.Limits[0].Period != time.Minute {
		t.Errorf("period = %s, want 1m from period_ms 60000", got.Limits[0].Period)
	}
	if got.Matches[1].Method != "POST" || got.Matches[1].PathPrefix != "/api/orders" {
		t.Errorf("endpoint rule lost its match: %+v", got.Matches[1])
	}
	// A policy can be shared, so the whole cache goes.
	if cache.Len() != 0 {
		t.Errorf("cache still holds %d entries after a policy write", cache.Len())
	}
}

// The secret is returned exactly once, and what reaches the store is a hash.
func TestAdminCreatesAKey(t *testing.T) {
	store := &fakeAdminStore{}
	srv, _ := adminServer(t, store, testAdminToken)

	rec := adminRequest(t, srv, http.MethodPost, "/admin/v1/tenants/acme/keys", testAdminToken,
		map[string]any{"label": "laptop"})
	if rec.Code != http.StatusCreated {
		t.Fatalf("code = %d, body = %s", rec.Code, rec.Body)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(body["secret"], "rlk_") {
		t.Fatalf("secret = %q", body["secret"])
	}
	if body["prefix"] != body["secret"][:auth.KeyPrefixLen] {
		t.Error("the returned prefix does not match the secret")
	}
	if len(store.keys) != 1 || store.keys[0] != "acme/"+body["prefix"] {
		t.Fatalf("store received %v", store.keys)
	}

	// Two calls must not produce the same key.
	rec2 := adminRequest(t, srv, http.MethodPost, "/admin/v1/tenants/acme/keys", testAdminToken, nil)
	var body2 map[string]string
	_ = json.Unmarshal(rec2.Body.Bytes(), &body2)
	if body2["secret"] == body["secret"] {
		t.Fatal("two key creations returned the same secret")
	}
}

func TestAdminDeleteTenant(t *testing.T) {
	store := &fakeAdminStore{tenants: []policy.TenantRecord{{ID: "acme", Policy: "free"}}}
	srv, _ := adminServer(t, store, testAdminToken)

	rec := adminRequest(t, srv, http.MethodDelete, "/admin/v1/tenants/acme", testAdminToken, nil)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("code = %d", rec.Code)
	}
	if len(store.tenants) != 0 {
		t.Fatal("the tenant was not deleted")
	}

	rec = adminRequest(t, srv, http.MethodDelete, "/admin/v1/tenants/ghost", testAdminToken, nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("deleting an unknown tenant: code = %d, want 404", rec.Code)
	}
}

func TestAdminStoreErrorsMapToStatuses(t *testing.T) {
	store := &fakeAdminStore{err: policy.ErrConflict}
	srv, _ := adminServer(t, store, testAdminToken)

	rec := adminRequest(t, srv, http.MethodPost, "/admin/v1/tenants", testAdminToken,
		map[string]any{"id": "acme", "policy": "nope"})
	if rec.Code != http.StatusConflict {
		t.Fatalf("code = %d, want 409", rec.Code)
	}
}

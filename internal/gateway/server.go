// Package gateway is the HTTP surface: the decision API, health endpoints and
// -- from M4 -- the proxy data path.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// TenantHeader names the tenant a decision is asked about.
const TenantHeader = "X-Tenant-ID"

// CostHeader lets one request consume several units of quota -- a batch
// endpoint charging per item, for instance.
const CostHeader = "X-RateLimit-Cost"

// Server serves the gateway's HTTP API.
type Server struct {
	cfg      config.Config
	checker  limiter.Checker
	policies policy.Source
	log      *slog.Logger
	router   chi.Router
	ready    func(context.Context) error

	authn      *auth.Authenticator
	admin      AdminStore
	adminToken *auth.AdminToken
	cache      *policy.Cache
}

// Option configures a Server.
type Option func(*Server)

// WithAuth supplies the authenticator that resolves a tenant from credentials.
func WithAuth(a *auth.Authenticator) Option { return func(s *Server) { s.authn = a } }

// WithAdmin enables the management API over a writable store, behind a token.
func WithAdmin(store AdminStore, token *auth.AdminToken) Option {
	return func(s *Server) { s.admin, s.adminToken = store, token }
}

// WithCache lets admin writes invalidate this node's policy cache immediately,
// rather than waiting for the TTL on the node that served the write.
func WithCache(c *policy.Cache) Option { return func(s *Server) { s.cache = c } }

// WithReadiness supplies the check behind /readyz -- typically a round trip to
// the counter store. It is deliberately separate from /healthz: liveness asks
// whether this process is working, readiness asks whether it can enforce
// anything, and a load balancer that cannot tell them apart will either keep
// restarting a healthy gateway or keep sending traffic to one that is failing
// every check closed.
func WithReadiness(fn func(context.Context) error) Option {
	return func(s *Server) { s.ready = fn }
}

// New builds the server and its routes.
func New(cfg config.Config, checker limiter.Checker, policies policy.Source, log *slog.Logger, opts ...Option) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, checker: checker, policies: policies, log: log}
	for _, o := range opts {
		o(s)
	}
	if s.ready == nil {
		s.ready = func(context.Context) error { return nil }
	}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", s.handleHealth)
	r.Get("/readyz", s.handleReady)
	r.Route("/v1", func(r chi.Router) {
		r.Get("/check", s.handleCheck)
		r.Post("/check", s.handleCheck)
	})
	s.mountAdmin(r)

	s.router = r
	return s
}

// Handler is the server's http.Handler.
func (s *Server) Handler() http.Handler { return s.router }

// Node reports this node's id, which every response carries so that a reader
// can tell which replica answered.
func (s *Server) Node() string { return s.cfg.Node.ID }

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}` + "\n"))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.ready(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "not ready",
			"reason": err.Error(),
			"node":   s.cfg.Node.ID,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "node": s.cfg.Node.ID})
}

// checkResponse is the decision API's body. It names every limit that was
// evaluated, not just the one that refused, because "which of my limits am I
// closest to" is the question a caller actually has.
type checkResponse struct {
	Allowed      bool            `json:"allowed"`
	Tenant       string          `json:"tenant"`
	Policy       string          `json:"policy"`
	Node         string          `json:"node"`
	Limits       []limitResponse `json:"limits"`
	Limiting     string          `json:"limiting,omitempty"`
	RetryAfterMS int64           `json:"retry_after_ms,omitempty"`
	Degraded     *degradedReason `json:"degraded,omitempty"`
}

type limitResponse struct {
	Name         string `json:"name"`
	Limit        int64  `json:"limit"`
	Remaining    int64  `json:"remaining"`
	ResetMS      int64  `json:"reset_ms"`
	RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
	Allowed      bool   `json:"allowed"`
}

// degradedReason appears when a request was admitted or refused without the
// counter store agreeing -- the one case where the answer is not authoritative.
type degradedReason struct {
	Reason string `json:"reason"`
	Mode   string `json:"mode"`
}

type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorResponse{Error: msg, Code: code})
}

// handleCheck answers "is this request inside the tenant's quota", consuming
// quota when it is.
func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	tenant, err := s.resolveTenant(r)
	if err != nil {
		switch {
		case errors.Is(err, auth.ErrBadCredentials):
			writeError(w, http.StatusUnauthorized, "bad_credentials", "the credentials presented are not valid")
		case errors.Is(err, auth.ErrNoCredentials):
			w.Header().Set("WWW-Authenticate", `Bearer realm="gateway"`)
			writeError(w, http.StatusUnauthorized, "no_credentials", "an API key or bearer token is required")
		default:
			writeError(w, http.StatusBadRequest, "tenant_missing", err.Error())
		}
		return
	}

	cost, err := requestCost(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_cost", err.Error())
		return
	}

	pol, err := s.policies.Lookup(r.Context(), tenant)
	if err != nil {
		switch {
		case errors.Is(err, policy.ErrTenantNotFound):
			writeError(w, http.StatusNotFound, "tenant_unknown",
				fmt.Sprintf("no policy for tenant %q", tenant))
		case errors.Is(err, policy.ErrTenantDisabled):
			// Distinct from unknown on purpose: a disabled tenant is a
			// decision somebody made, and it should not read as a lost record.
			writeError(w, http.StatusForbidden, "tenant_disabled",
				fmt.Sprintf("tenant %q is disabled", tenant))
		default:
			s.log.Error("policy lookup failed", "tenant", tenant, "err", err)
			writeError(w, http.StatusServiceUnavailable, "policy_unavailable", "policy store unavailable")
		}
		return
	}

	// An endpoint rule needs to know which endpoint. The decision API takes
	// that as query parameters, because the request it is being asked about is
	// not the request carrying the question.
	method := r.URL.Query().Get("method")
	path := r.URL.Query().Get("path")
	limits := pol.LimitsFor(method, path)

	res, err := s.checker.Check(r.Context(), limiter.Request{
		Tenant: tenant,
		Limits: limits,
		Cost:   cost,
	})
	if err != nil {
		s.writeCheckFailure(w, tenant, pol, err)
		return
	}

	s.writeDecision(w, tenant, pol, res, nil)
}

// writeCheckFailure answers when the counter store could not decide.
//
// This is the fail-open/fail-closed seam, and it is a policy decision rather
// than a technical one: refusing means a Redis outage takes the tenant's
// traffic down with it, admitting means the limit silently stops existing for
// the duration. The mode is per policy, the default is closed, and either way
// the response says the answer was not authoritative.
func (s *Server) writeCheckFailure(w http.ResponseWriter, tenant string, pol policy.Policy, err error) {
	if errors.Is(err, limiter.ErrCostExceedsCapacity) {
		writeError(w, http.StatusBadRequest, "cost_too_large", err.Error())
		return
	}

	s.log.Error("limiter check failed", "tenant", tenant, "policy", pol.Name,
		"failure_mode", pol.FailureMode, "err", err)

	if pol.FailsClosed() {
		writeJSON(w, http.StatusServiceUnavailable, checkResponse{
			Allowed:  false,
			Tenant:   tenant,
			Policy:   pol.Name,
			Node:     s.cfg.Node.ID,
			Degraded: &degradedReason{Reason: err.Error(), Mode: config.FailClosed},
		})
		return
	}
	writeJSON(w, http.StatusOK, checkResponse{
		Allowed:  true,
		Tenant:   tenant,
		Policy:   pol.Name,
		Node:     s.cfg.Node.ID,
		Degraded: &degradedReason{Reason: err.Error(), Mode: config.FailOpen},
	})
}

func (s *Server) writeDecision(w http.ResponseWriter, tenant string, pol policy.Policy, res limiter.Result, degraded *degradedReason) {
	body := checkResponse{
		Allowed:  res.Allowed,
		Tenant:   tenant,
		Policy:   pol.Name,
		Node:     s.cfg.Node.ID,
		Limits:   make([]limitResponse, 0, len(res.Decisions)),
		Limiting: res.Limiting,
		Degraded: degraded,
	}
	for _, d := range res.Decisions {
		body.Limits = append(body.Limits, limitResponse{
			Name:         d.Name,
			Limit:        d.Limit,
			Remaining:    d.Remaining,
			ResetMS:      d.ResetAfter.Milliseconds(),
			RetryAfterMS: d.RetryAfter.Milliseconds(),
			Allowed:      d.Allowed,
		})
	}

	SetRateLimitHeaders(w.Header(), res)

	if res.Allowed {
		writeJSON(w, http.StatusOK, body)
		return
	}
	body.RetryAfterMS = res.RetryAfter().Milliseconds()
	writeJSON(w, http.StatusTooManyRequests, body)
}

// SetRateLimitHeaders writes the X-RateLimit-* family from a decision.
//
// The numbers describe the tightest limit, because that is the one the caller
// will hit first; Retry-After describes the longest wait, because that is when
// the request would actually be admitted. Retry-After is in whole seconds per
// RFC 9110 and is rounded UP -- rounding down would invite a retry that is
// still too early.
func SetRateLimitHeaders(h http.Header, res limiter.Result) {
	if d, ok := res.Tightest(); ok {
		h.Set("X-RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
		h.Set("X-RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
		h.Set("X-RateLimit-Reset", strconv.FormatInt(secondsCeil(d.ResetAfter), 10))
	}
	if !res.Allowed {
		h.Set("Retry-After", strconv.FormatInt(secondsCeil(res.RetryAfter()), 10))
	}
}

func secondsCeil(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	s := int64(d / time.Second)
	if d%time.Second != 0 {
		s++
	}
	return s
}

// resolveTenant decides which tenant a request speaks for.
//
// Credentials win over the header. The header is for a decision API called by
// infrastructure that has already established who the caller is -- an ingress
// asking "may this through?" -- and trusting it is a deliberate config choice,
// because anything that can reach the endpoint can otherwise name any tenant
// and spend its quota.
func (s *Server) resolveTenant(r *http.Request) (string, error) {
	if s.authn != nil && s.authn.Enabled() {
		tenant, err := s.authn.Tenant(r.Context(), r)
		switch {
		case err == nil:
			return tenant, nil
		case errors.Is(err, auth.ErrBadCredentials):
			// Presented and wrong is never waved through, whatever the header
			// says: falling back to the header here would make a bad key a way
			// of becoming somebody else.
			return "", err
		}
	}
	if s.cfg.CheckAPI.TrustTenantHeader {
		if t := r.Header.Get(TenantHeader); t != "" {
			return t, nil
		}
		return "", fmt.Errorf("%s header is required", TenantHeader)
	}
	if s.authn != nil && s.authn.Enabled() {
		return "", auth.ErrNoCredentials
	}
	return "", fmt.Errorf("check_api.trust_tenant_header is off and no authentication is configured")
}

func requestCost(r *http.Request) (int64, error) {
	raw := r.Header.Get(CostHeader)
	if raw == "" {
		raw = r.URL.Query().Get("cost")
	}
	if raw == "" {
		return 1, nil
	}
	cost, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cost %q is not a number", raw)
	}
	if cost < 1 {
		return 0, fmt.Errorf("cost must be >= 1, got %d", cost)
	}
	return cost, nil
}

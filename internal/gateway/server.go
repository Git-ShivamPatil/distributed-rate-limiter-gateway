// Package gateway is the HTTP surface: the decision API, the proxy data path,
// the admin API and the health endpoints.
//
// None of the limiting logic lives here. Every admission question goes to
// internal/decide, which is the same service the gRPC surface calls, so the
// two cannot drift into answering differently.
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
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/cluster"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
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
	cfg     config.Config
	decider *decide.Service
	log     *slog.Logger
	router  chi.Router
	ready   func(context.Context) error

	proxy      *Proxy
	cluster    *cluster.View
	authn      *auth.Authenticator
	admin      AdminStore
	adminToken *auth.AdminToken
	cache      *policy.Cache
	consensus  func() cluster.Stats
	control    ClusterController
}

// Option configures a Server.
type Option func(*Server)

// WithAuth supplies the authenticator that resolves a tenant from credentials.
func WithAuth(a *auth.Authenticator) Option { return func(s *Server) { s.authn = a } }

// WithProxy enables the data path for the configured routes.
func WithProxy(p *Proxy) Option { return func(s *Server) { s.proxy = p } }

// WithCluster supplies the ring view that /v1/cluster reports.
func WithCluster(v *cluster.View) Option { return func(s *Server) { s.cluster = v } }

// WithConsensus makes /v1/cluster report where this node sits in the log:
// which node is leading, and how far behind this one is.
//
// It is reported on the same endpoint as the ring on purpose. When a node is
// routing a tenant somewhere unexpected, the two questions are always asked
// together -- what does this node think the membership is, and is it caught up
// enough for that to mean anything.
func WithConsensus(stats func() cluster.Stats) Option {
	return func(s *Server) { s.consensus = stats }
}

// WithClusterControl enables the membership endpoints, which are the only
// part of the admin API that appends to the replicated log rather than writing
// a row.
func WithClusterControl(c ClusterController) Option {
	return func(s *Server) { s.control = c }
}

// WithAdmin enables the management API over a writable store, behind a token.
func WithAdmin(store AdminStore, token *auth.AdminToken) Option {
	return func(s *Server) { s.admin, s.adminToken = store, token }
}

// WithCache lets admin writes invalidate this node's policy cache immediately,
// rather than waiting for the TTL on the node that served the write.
func WithCache(c *policy.Cache) Option { return func(s *Server) { s.cache = c } }

// WithReadiness supplies the check behind /readyz -- typically a round trip to
// the stores. It is deliberately separate from /healthz: liveness asks whether
// this process is working, readiness asks whether it can enforce anything, and
// a load balancer that cannot tell them apart will either keep restarting a
// healthy gateway or keep sending traffic to one that is failing every check.
func WithReadiness(fn func(context.Context) error) Option {
	return func(s *Server) { s.ready = fn }
}

// New builds the server and its routes.
func New(cfg config.Config, decider *decide.Service, log *slog.Logger, opts ...Option) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{cfg: cfg, decider: decider, log: log}
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
		r.Get("/cluster", s.handleCluster)
	})
	s.mountAdmin(r)

	// The data path is mounted per configured prefix rather than as a
	// catch-all, so a request to an unrouted path is a 404 from the gateway
	// instead of a proxy error from somewhere downstream.
	if s.proxy != nil {
		for _, rc := range cfg.Routes {
			r.Handle(rc.PathPrefix, http.HandlerFunc(s.handleProxy))
			r.Handle(rc.PathPrefix+"/*", http.HandlerFunc(s.handleProxy))
		}
	}

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

// handleCluster reports this node's view of the ring, and optionally who owns
// a particular tenant.
//
// It exists so that "which node owns acme" has one answer a person can ask for
// rather than being inferred from logs -- and so the same question can be put
// to every node, which is how a disagreement is spotted.
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"self":    s.cfg.Node.ID,
			"members": []any{},
			"note":    "this node is not in a ring; it answers for every tenant itself",
		})
		return
	}

	body := map[string]any{"cluster": s.cluster.Describe()}
	if tenant := r.URL.Query().Get("tenant"); tenant != "" {
		owner, isSelf, ok := s.cluster.Owner(tenant)
		if ok {
			body["tenant"] = tenant
			body["owner"] = owner.ID
			body["owner_addr"] = owner.Addr
			body["owner_is_self"] = isSelf
		}
	}
	if s.consensus != nil {
		body["raft"] = s.consensus()
	}
	if s.decider != nil {
		st := s.decider.Stats()
		body["decisions"] = map[string]int64{
			"local":     st.Local,
			"forwarded": st.Forwarded,
			"fell_back": st.FellBack,
		}
	}
	writeJSON(w, http.StatusOK, body)
}

// checkResponse is the decision API's body. It names every limit that was
// evaluated, not just the one that refused, because "which of my limits am I
// closest to" is the question a caller actually has.
type checkResponse struct {
	Allowed bool   `json:"allowed"`
	Tenant  string `json:"tenant"`
	Policy  string `json:"policy"`
	// Node is the node that ANSWERED, which is this one.
	Node string `json:"node"`
	// Owner is the node that coordinates this tenant, and Forwarded says
	// whether the answer came from there. Both are in the response because
	// "which node decided this" is the first question when two replicas seem
	// to disagree.
	Owner        string          `json:"owner,omitempty"`
	Forwarded    bool            `json:"forwarded,omitempty"`
	Route        string          `json:"route,omitempty"`
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
		s.writeAuthError(w, err)
		return
	}

	cost, err := requestCost(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_cost", err.Error())
		return
	}

	q := r.URL.Query()
	out, err := s.decider.Decide(r.Context(), decide.Query{
		Tenant: tenant,
		// An endpoint rule needs to know which endpoint. The decision API
		// takes that as parameters, because the request it is being asked
		// about is not the request carrying the question.
		Method: q.Get("method"),
		Path:   q.Get("path"),
		Cost:   cost,
		Peek:   q.Get("peek") == "1" || q.Get("peek") == "true",
	})
	if err != nil {
		s.writeDecideError(w, tenant, err)
		return
	}

	body := s.responseFor(tenant, out)
	SetRateLimitHeaders(w.Header(), out.Result)
	if out.Result.Allowed {
		writeJSON(w, http.StatusOK, body)
		return
	}
	writeJSON(w, http.StatusTooManyRequests, body)
}

func (s *Server) responseFor(tenant string, out decide.Outcome) checkResponse {
	body := checkResponse{
		Allowed:   out.Result.Allowed,
		Tenant:    tenant,
		Policy:    out.Policy.Name,
		Node:      s.cfg.Node.ID,
		Owner:     out.Owner,
		Forwarded: out.Forwarded,
		Limits:    make([]limitResponse, 0, len(out.Result.Decisions)),
		Limiting:  out.Result.Limiting,
	}
	for _, d := range out.Result.Decisions {
		body.Limits = append(body.Limits, limitResponse{
			Name:         d.Name,
			Limit:        d.Limit,
			Remaining:    d.Remaining,
			ResetMS:      d.ResetAfter.Milliseconds(),
			RetryAfterMS: d.RetryAfter.Milliseconds(),
			Allowed:      d.Allowed,
		})
	}
	if !out.Result.Allowed {
		body.RetryAfterMS = out.Result.RetryAfter().Milliseconds()
	}
	if out.Degraded != nil {
		body.Degraded = &degradedReason{Reason: out.Degraded.Reason, Mode: out.Degraded.Mode}
	}
	return body
}

// writeAuthError answers a request whose caller could not be identified.
func (s *Server) writeAuthError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, auth.ErrBadCredentials):
		writeError(w, http.StatusUnauthorized, "bad_credentials", "the credentials presented are not valid")
	case errors.Is(err, auth.ErrNoCredentials):
		w.Header().Set("WWW-Authenticate", `Bearer realm="gateway"`)
		writeError(w, http.StatusUnauthorized, "no_credentials", "an API key or bearer token is required")
	default:
		writeError(w, http.StatusBadRequest, "tenant_missing", err.Error())
	}
}

// writeDecideError maps a decision failure onto a status.
//
// The four cases are distinct because they want four different answers and
// four different alerts: an unknown tenant is a caller mistake, a disabled one
// is somebody's decision, an impossible cost can never succeed, and an
// unreachable store is an outage that the tenant's policy decides the meaning
// of.
func (s *Server) writeDecideError(w http.ResponseWriter, tenant string, err error) {
	switch {
	case errors.Is(err, policy.ErrTenantNotFound):
		writeError(w, http.StatusNotFound, "tenant_unknown", fmt.Sprintf("no policy for tenant %q", tenant))
	case errors.Is(err, policy.ErrTenantDisabled):
		writeError(w, http.StatusForbidden, "tenant_disabled", fmt.Sprintf("tenant %q is disabled", tenant))
	case errors.Is(err, limiter.ErrCostExceedsCapacity):
		// Not 429: waiting will never help, so a Retry-After would be a lie.
		writeError(w, http.StatusBadRequest, "cost_too_large", err.Error())
	case errors.Is(err, decide.ErrNoTenant):
		writeError(w, http.StatusBadRequest, "tenant_missing", err.Error())
	case errors.Is(err, decide.ErrStoreUnavailable):
		s.log.Error("counter store unavailable", "tenant", tenant, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, checkResponse{
			Allowed:  false,
			Tenant:   tenant,
			Node:     s.cfg.Node.ID,
			Degraded: &degradedReason{Reason: err.Error(), Mode: config.FailClosed},
		})
	default:
		s.log.Error("policy lookup failed", "tenant", tenant, "err", err)
		writeError(w, http.StatusServiceUnavailable, "policy_unavailable", "policy store unavailable")
	}
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
// because anything that can reach the endpoint could otherwise name any tenant
// and spend its quota.
func (s *Server) resolveTenant(r *http.Request) (string, error) {
	if s.authn != nil && s.authn.Enabled() {
		tenant, err := s.authn.Tenant(r.Context(), r)
		switch {
		case err == nil:
			return tenant, nil
		case errors.Is(err, auth.ErrBadCredentials):
			// Presented and wrong is never waved through, whatever the header
			// says: falling back here would make a bad key a way of becoming
			// somebody else.
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

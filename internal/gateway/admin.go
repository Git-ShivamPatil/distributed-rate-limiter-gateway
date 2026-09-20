package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// AdminStore is the write side of the policy store. Only the admin API holds
// one; the request path has a read-only Source and cannot change policy.
type AdminStore interface {
	Tenants(ctx context.Context) ([]policy.TenantRecord, error)
	UpsertTenant(ctx context.Context, t policy.TenantRecord) error
	DeleteTenant(ctx context.Context, id string) error
	Policies(ctx context.Context) ([]policy.PolicyRecord, error)
	UpsertPolicy(ctx context.Context, rec policy.PolicyRecord) error
	CreateAPIKey(ctx context.Context, tenant, label string, hash []byte, prefix string) error
	RevokeAPIKey(ctx context.Context, tenant, prefix string) error
}

// limitDTO is how a limit crosses the admin API.
//
// It is not limiter.Limit: that type carries a time.Duration, which JSON
// renders as a bare nanosecond count, and an API that asks an operator to send
// 60000000000 will eventually be sent 60000 by somebody who meant a minute.
type limitDTO struct {
	Name       string `json:"name"`
	Algorithm  string `json:"algorithm"`
	Count      int64  `json:"count"`
	PeriodMS   int64  `json:"period_ms"`
	Burst      int64  `json:"burst,omitempty"`
	Method     string `json:"method,omitempty"`
	PathPrefix string `json:"path_prefix,omitempty"`
}

type policyDTO struct {
	Name        string     `json:"name"`
	FailureMode string     `json:"failure_mode,omitempty"`
	Description string     `json:"description,omitempty"`
	Limits      []limitDTO `json:"limits"`
}

func (d policyDTO) toRecord(name string) policy.PolicyRecord {
	rec := policy.PolicyRecord{
		Name:        name,
		FailureMode: d.FailureMode,
		Description: d.Description,
	}
	for _, l := range d.Limits {
		rec.Limits = append(rec.Limits, limiter.Limit{
			Name:      l.Name,
			Algorithm: limiter.Algorithm(l.Algorithm),
			Count:     l.Count,
			Period:    time.Duration(l.PeriodMS) * time.Millisecond,
			Burst:     l.Burst,
		})
		rec.Matches = append(rec.Matches, policy.Match{Method: l.Method, PathPrefix: l.PathPrefix})
	}
	return rec
}

func policyToDTO(rec policy.PolicyRecord) policyDTO {
	d := policyDTO{Name: rec.Name, FailureMode: rec.FailureMode, Description: rec.Description}
	for i, l := range rec.Limits {
		dto := limitDTO{
			Name:      l.Name,
			Algorithm: string(l.Algorithm),
			Count:     l.Count,
			PeriodMS:  l.Period.Milliseconds(),
			Burst:     l.Burst,
		}
		if i < len(rec.Matches) {
			if m := rec.Matches[i]; !m.Universal() {
				dto.Method, dto.PathPrefix = m.Method, m.PathPrefix
			}
		}
		d.Limits = append(d.Limits, dto)
	}
	return d
}

// mountAdmin wires the management surface.
//
// Every route is behind the admin token, and the whole tree answers 501 when
// there is no writable store -- a gateway reading policies from its config
// file has nowhere to put an edit, and pretending otherwise would accept a
// write and lose it.
func (s *Server) mountAdmin(r chi.Router) {
	r.Route("/admin/v1", func(r chi.Router) {
		// Two groups, because the two halves of the admin API need different
		// things to exist. Editing a tenant needs a writable policy store;
		// changing the membership needs a replicated log and no database at
		// all, and a gateway that has one but not the other should serve the
		// half it can rather than neither.
		r.Group(func(r chi.Router) {
			r.Use(s.requireAdmin)

			r.Get("/tenants", s.adminListTenants)
			r.Post("/tenants", s.adminUpsertTenant)
			r.Put("/tenants/{id}", s.adminUpsertTenant)
			r.Delete("/tenants/{id}", s.adminDeleteTenant)
			r.Post("/tenants/{id}/keys", s.adminCreateKey)
			r.Delete("/tenants/{id}/keys/{prefix}", s.adminRevokeKey)

			r.Get("/policies", s.adminListPolicies)
			r.Put("/policies/{name}", s.adminUpsertPolicy)
		})

		r.Group(func(r chi.Router) {
			r.Use(s.requireAdminToken)
			r.Route("/cluster", s.mountClusterAdmin)
		})
	})
}

// requireAdminToken is the credential check alone.
func (s *Server) requireAdminToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.adminToken == nil || !s.adminToken.Configured() {
			writeError(w, http.StatusNotImplemented, "admin_disabled",
				"the admin API needs an admin token; nothing is served without one")
			return
		}
		if err := s.adminToken.Check(r); err != nil {
			status := http.StatusUnauthorized
			if errors.Is(err, auth.ErrNoCredentials) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="gateway-admin"`)
			}
			writeError(w, status, "unauthorised", "admin credentials are required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireAdmin is the credential check plus a store to write to.
func (s *Server) requireAdmin(next http.Handler) http.Handler {
	return s.requireAdminToken(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.admin == nil {
			writeError(w, http.StatusNotImplemented, "admin_disabled",
				"the admin API needs a writable policy store (policy.store: postgres)")
			return
		}
		next.ServeHTTP(w, r)
	}))
}

// invalidate drops the tenant from this node's cache so an edit takes effect
// here immediately. Other nodes hear about it through Postgres NOTIFY; this is
// what covers the node that served the write, and what covers every node when
// the listener is down.
func (s *Server) invalidate(tenant string) {
	if s.cache == nil {
		return
	}
	if tenant == "" {
		s.cache.InvalidateAll()
		return
	}
	s.cache.Invalidate(tenant)
}

func (s *Server) adminListTenants(w http.ResponseWriter, r *http.Request) {
	tenants, err := s.admin.Tenants(r.Context())
	if err != nil {
		s.adminError(w, err, "listing tenants")
		return
	}
	if tenants == nil {
		tenants = []policy.TenantRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenants": tenants})
}

func (s *Server) adminUpsertTenant(w http.ResponseWriter, r *http.Request) {
	var body policy.TenantRecord
	if err := decodeJSON(w, r, &body); err != nil {
		return
	}
	if id := chi.URLParam(r, "id"); id != "" {
		body.ID = id
	}
	if body.ID == "" || body.Policy == "" {
		writeError(w, http.StatusBadRequest, "invalid_tenant", "id and policy are required")
		return
	}
	if body.Name == "" {
		body.Name = body.ID
	}
	if err := s.admin.UpsertTenant(r.Context(), body); err != nil {
		s.adminError(w, err, "writing tenant "+body.ID)
		return
	}
	s.invalidate(body.ID)
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) adminDeleteTenant(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.admin.DeleteTenant(r.Context(), id); err != nil {
		s.adminError(w, err, "deleting tenant "+id)
		return
	}
	s.invalidate(id)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) adminCreateKey(w http.ResponseWriter, r *http.Request) {
	tenant := chi.URLParam(r, "id")
	var body struct {
		Label string `json:"label"`
	}
	// A key with no body is fine; only a malformed one is an error.
	if r.ContentLength > 0 {
		if err := decodeJSON(w, r, &body); err != nil {
			return
		}
	}

	key, err := auth.GenerateKey()
	if err != nil {
		s.adminError(w, err, "generating a key")
		return
	}
	if err := s.admin.CreateAPIKey(r.Context(), tenant, body.Label, key.Hash, key.Prefix); err != nil {
		s.adminError(w, err, "storing a key for "+tenant)
		return
	}
	// The only time the secret exists outside the caller's hands. The store
	// holds its hash, so this response cannot be reconstructed later.
	writeJSON(w, http.StatusCreated, map[string]string{
		"tenant": tenant,
		"secret": key.Secret,
		"prefix": key.Prefix,
		"note":   "store this now; it is not recoverable",
	})
}

func (s *Server) adminRevokeKey(w http.ResponseWriter, r *http.Request) {
	tenant, prefix := chi.URLParam(r, "id"), chi.URLParam(r, "prefix")
	if err := s.admin.RevokeAPIKey(r.Context(), tenant, prefix); err != nil {
		s.adminError(w, err, "revoking a key for "+tenant)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) adminListPolicies(w http.ResponseWriter, r *http.Request) {
	records, err := s.admin.Policies(r.Context())
	if err != nil {
		s.adminError(w, err, "listing policies")
		return
	}
	out := make([]policyDTO, 0, len(records))
	for _, rec := range records {
		out = append(out, policyToDTO(rec))
	}
	writeJSON(w, http.StatusOK, map[string]any{"policies": out})
}

func (s *Server) adminUpsertPolicy(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var body policyDTO
	if err := decodeJSON(w, r, &body); err != nil {
		return
	}
	rec := body.toRecord(name)
	if err := rec.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_policy", err.Error())
		return
	}
	if err := s.admin.UpsertPolicy(r.Context(), rec); err != nil {
		s.adminError(w, err, "writing policy "+name)
		return
	}
	// A policy can be shared by many tenants, and finding which would mean
	// querying the store at the moment it is being written to.
	s.invalidate("")
	writeJSON(w, http.StatusOK, policyToDTO(rec))
}

// adminError maps a store error onto a status code.
func (s *Server) adminError(w http.ResponseWriter, err error, what string) {
	switch {
	case errors.Is(err, policy.ErrTenantNotFound):
		writeError(w, http.StatusNotFound, "not_found", err.Error())
	case errors.Is(err, policy.ErrConflict):
		writeError(w, http.StatusConflict, "conflict", err.Error())
	default:
		s.log.Error("admin request failed", "what", what, "err", err)
		writeError(w, http.StatusInternalServerError, "store_error", what+" failed")
	}
}

// decodeJSON reads a request body strictly, answering 400 on anything it does
// not understand rather than silently ignoring it.
func decodeJSON(w http.ResponseWriter, r *http.Request, into any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", err.Error())
		return err
	}
	return nil
}

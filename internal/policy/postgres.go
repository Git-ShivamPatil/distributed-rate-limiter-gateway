package policy

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// ErrTenantDisabled means the tenant exists and has been switched off. It is
// deliberately distinct from ErrTenantNotFound: "we turned them off" and "we
// have never heard of them" want different answers and different alerts.
var ErrTenantDisabled = errors.New("policy: tenant is disabled")

// ErrConflict means a write lost a race or violated a constraint that the
// caller could have avoided -- a duplicate id, or a policy that does not exist.
var ErrConflict = errors.New("policy: conflicting write")

// Postgres is the policy store of record.
//
// It is read on a cache miss rather than on every request: policies change
// rarely and are read constantly, so the request path must not depend on a
// database round trip. See Cache.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres wraps an existing pool.
func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

// Pool exposes the underlying pool for health checks and the notify listener.
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

// Lookup reads one tenant's policy, limits and all.
func (p *Postgres) Lookup(ctx context.Context, tenant string) (Policy, error) {
	const q = `
SELECT t.policy_name, p.failure_mode, t.disabled_at IS NOT NULL,
       l.name, l.algorithm, l.count, l.period_ms, l.burst, l.match_method, l.match_prefix
  FROM tenants t
  JOIN policies p ON p.name = t.policy_name
  LEFT JOIN policy_limits l ON l.policy_name = p.name
 WHERE t.id = $1
 ORDER BY l.name`

	rows, err := p.pool.Query(ctx, q, tenant)
	if err != nil {
		return Policy{}, fmt.Errorf("policy: querying tenant %q: %w", tenant, err)
	}
	defer rows.Close()

	var (
		found    bool
		disabled bool
		out      Policy
	)
	for rows.Next() {
		var (
			policyName  string
			failureMode string
			name        *string
			algorithm   *string
			count       *int64
			periodMS    *int64
			burst       *int64
			method      *string
			prefix      *string
		)
		if err := rows.Scan(&policyName, &failureMode, &disabled,
			&name, &algorithm, &count, &periodMS, &burst, &method, &prefix); err != nil {
			return Policy{}, fmt.Errorf("policy: scanning tenant %q: %w", tenant, err)
		}
		found = true
		out.Name = policyName
		out.FailureMode = failureMode
		// A LEFT JOIN against a policy with no limits yields one row of NULLs.
		if name == nil {
			continue
		}
		out.Limits = append(out.Limits, limiter.Limit{
			Name:      *name,
			Algorithm: limiter.Algorithm(*algorithm),
			Count:     *count,
			Period:    time.Duration(*periodMS) * time.Millisecond,
			Burst:     *burst,
		})
		out.Matches = append(out.Matches, Match{Method: *method, PathPrefix: *prefix})
	}
	if err := rows.Err(); err != nil {
		return Policy{}, fmt.Errorf("policy: reading tenant %q: %w", tenant, err)
	}
	if !found {
		return Policy{}, fmt.Errorf("%w: %q", ErrTenantNotFound, tenant)
	}
	if disabled {
		return Policy{}, fmt.Errorf("%w: %q", ErrTenantDisabled, tenant)
	}
	return out, nil
}

// TenantForKey resolves an API key to its tenant.
//
// The key is looked up by the SHA-256 of the presented secret, because the
// store holds hashes: a leaked database dump must not be a set of working
// credentials. The hash is also what makes the lookup a single indexed read
// rather than a scan of candidates.
func (p *Postgres) TenantForKey(ctx context.Context, presented string) (string, error) {
	sum := sha256.Sum256([]byte(presented))
	const q = `
SELECT k.tenant_id
  FROM api_keys k
  JOIN tenants t ON t.id = k.tenant_id
 WHERE k.key_hash = $1 AND k.revoked_at IS NULL`

	var tenant string
	err := p.pool.QueryRow(ctx, q, sum[:]).Scan(&tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrUnknownKey
	}
	if err != nil {
		return "", fmt.Errorf("policy: resolving api key: %w", err)
	}
	return tenant, nil
}

// Tenants lists tenants for the admin API and the dashboard.
func (p *Postgres) Tenants(ctx context.Context) ([]TenantRecord, error) {
	const q = `
SELECT id, name, policy_name, disabled_at IS NOT NULL, created_at
  FROM tenants ORDER BY id`
	rows, err := p.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("policy: listing tenants: %w", err)
	}
	defer rows.Close()

	var out []TenantRecord
	for rows.Next() {
		var t TenantRecord
		if err := rows.Scan(&t.ID, &t.Name, &t.Policy, &t.Disabled, &t.CreatedAt); err != nil {
			return nil, fmt.Errorf("policy: scanning tenant: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpsertTenant creates or repoints a tenant.
func (p *Postgres) UpsertTenant(ctx context.Context, t TenantRecord) error {
	const q = `
INSERT INTO tenants (id, name, policy_name, disabled_at)
VALUES ($1, $2, $3, CASE WHEN $4::bool THEN now() ELSE NULL END)
ON CONFLICT (id) DO UPDATE
   SET name = EXCLUDED.name,
       policy_name = EXCLUDED.policy_name,
       disabled_at = EXCLUDED.disabled_at`
	_, err := p.pool.Exec(ctx, q, t.ID, t.Name, t.Policy, t.Disabled)
	if err != nil {
		return wrapWrite(err, "upserting tenant "+t.ID)
	}
	return nil
}

// DeleteTenant removes a tenant and, by cascade, its API keys.
func (p *Postgres) DeleteTenant(ctx context.Context, id string) error {
	tag, err := p.pool.Exec(ctx, `DELETE FROM tenants WHERE id = $1`, id)
	if err != nil {
		return wrapWrite(err, "deleting tenant "+id)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %q", ErrTenantNotFound, id)
	}
	return nil
}

// UpsertPolicy writes a policy and replaces its limits atomically.
//
// Replacing rather than merging is deliberate: a PUT that left an old limit
// behind because the new body did not mention it is how a tenant ends up
// enforced by a rule nobody can see in the request that created it.
func (p *Postgres) UpsertPolicy(ctx context.Context, rec PolicyRecord) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("policy: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
INSERT INTO policies (name, failure_mode, description)
VALUES ($1, $2, $3)
ON CONFLICT (name) DO UPDATE SET failure_mode = EXCLUDED.failure_mode, description = EXCLUDED.description`,
		rec.Name, rec.FailureMode, rec.Description); err != nil {
		return wrapWrite(err, "upserting policy "+rec.Name)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM policy_limits WHERE policy_name = $1`, rec.Name); err != nil {
		return wrapWrite(err, "clearing limits of "+rec.Name)
	}
	for i, l := range rec.Limits {
		m := Match{Method: "*"}
		if i < len(rec.Matches) {
			m = rec.Matches[i]
		}
		if m.Method == "" {
			m.Method = "*"
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO policy_limits (policy_name, name, algorithm, count, period_ms, burst, match_method, match_prefix)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			rec.Name, l.Name, string(l.Algorithm), l.Count, l.Period.Milliseconds(), l.Burst,
			m.Method, m.PathPrefix); err != nil {
			return wrapWrite(err, "inserting limit "+l.Name+" of "+rec.Name)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("policy: commit: %w", err)
	}
	return nil
}

// Policies lists every policy with its limits.
func (p *Postgres) Policies(ctx context.Context) ([]PolicyRecord, error) {
	const q = `
SELECT p.name, p.failure_mode, p.description,
       l.name, l.algorithm, l.count, l.period_ms, l.burst, l.match_method, l.match_prefix
  FROM policies p
  LEFT JOIN policy_limits l ON l.policy_name = p.name
 ORDER BY p.name, l.name`
	rows, err := p.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("policy: listing policies: %w", err)
	}
	defer rows.Close()

	byName := map[string]*PolicyRecord{}
	var order []string
	for rows.Next() {
		var (
			name, failureMode, description string
			lname, algorithm               *string
			count, periodMS, burst         *int64
			method, prefix                 *string
		)
		if err := rows.Scan(&name, &failureMode, &description,
			&lname, &algorithm, &count, &periodMS, &burst, &method, &prefix); err != nil {
			return nil, fmt.Errorf("policy: scanning policy: %w", err)
		}
		rec, ok := byName[name]
		if !ok {
			rec = &PolicyRecord{Name: name, FailureMode: failureMode, Description: description}
			byName[name] = rec
			order = append(order, name)
		}
		if lname == nil {
			continue
		}
		rec.Limits = append(rec.Limits, limiter.Limit{
			Name:      *lname,
			Algorithm: limiter.Algorithm(*algorithm),
			Count:     *count,
			Period:    time.Duration(*periodMS) * time.Millisecond,
			Burst:     *burst,
		})
		rec.Matches = append(rec.Matches, Match{Method: *method, PathPrefix: *prefix})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]PolicyRecord, 0, len(order))
	for _, n := range order {
		out = append(out, *byName[n])
	}
	return out, nil
}

// CreateAPIKey stores the hash of a key and returns the record.
//
// The plaintext never reaches this layer's storage: the caller generates it,
// shows it to the operator once, and hands this function the hash.
func (p *Postgres) CreateAPIKey(ctx context.Context, tenant, label string, hash []byte, prefix string) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO api_keys (tenant_id, key_hash, prefix, label) VALUES ($1, $2, $3, $4)`,
		tenant, hash, prefix, label)
	if err != nil {
		return wrapWrite(err, "creating api key for "+tenant)
	}
	return nil
}

// RevokeAPIKey marks a key unusable, keeping the row so the audit trail
// survives the revocation.
func (p *Postgres) RevokeAPIKey(ctx context.Context, tenant, prefix string) error {
	tag, err := p.pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = now() WHERE tenant_id = $1 AND prefix = $2 AND revoked_at IS NULL`,
		tenant, prefix)
	if err != nil {
		return wrapWrite(err, "revoking api key")
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: no live key with prefix %q for tenant %q", ErrTenantNotFound, prefix, tenant)
	}
	return nil
}

// Ping reports whether the store is reachable.
func (p *Postgres) Ping(ctx context.Context) error {
	if err := p.pool.Ping(ctx); err != nil {
		return fmt.Errorf("policy: postgres: %w", err)
	}
	return nil
}

// wrapWrite turns the constraint violations a caller could have avoided into
// ErrConflict, so the admin API can answer 409 instead of 500.
func wrapWrite(err error, what string) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505", "23503", "23514", "23502": // unique, fk, check, not-null
			return fmt.Errorf("%w: %s: %s", ErrConflict, what, pgErr.Message)
		}
	}
	return fmt.Errorf("policy: %s: %w", what, err)
}

// TenantRecord is a tenant as the admin API sees it.
type TenantRecord struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Policy    string    `json:"policy"`
	Disabled  bool      `json:"disabled"`
	CreatedAt time.Time `json:"created_at,omitzero"`
}

// PolicyRecord is a policy as the admin API sees it.
type PolicyRecord struct {
	Name        string          `json:"name"`
	FailureMode string          `json:"failure_mode"`
	Description string          `json:"description,omitempty"`
	Limits      []limiter.Limit `json:"limits"`
	// Matches is parallel to Limits: entry i says which requests limit i
	// applies to. A zero Match applies to everything.
	Matches []Match `json:"matches,omitempty"`
}

// Validate checks a policy record before it reaches the database, so the API
// can answer with a sentence rather than a constraint name.
func (r PolicyRecord) Validate() error {
	if r.Name == "" {
		return errors.New("policy: name is empty")
	}
	switch r.FailureMode {
	case "", config.FailClosed, config.FailOpen:
	default:
		return fmt.Errorf("policy %q: failure_mode %q is not %q or %q",
			r.Name, r.FailureMode, config.FailClosed, config.FailOpen)
	}
	if len(r.Limits) == 0 {
		return fmt.Errorf("policy %q: has no limits; a policy that limits nothing should not exist", r.Name)
	}
	seen := map[string]bool{}
	for _, l := range r.Limits {
		if err := l.Validate(); err != nil {
			return fmt.Errorf("policy %q: %w", r.Name, err)
		}
		if seen[l.Name] {
			return fmt.Errorf("policy %q: two limits named %q would share one counter", r.Name, l.Name)
		}
		seen[l.Name] = true
	}
	for _, m := range r.Matches {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("policy %q: %w", r.Name, err)
		}
	}
	return nil
}

// Package config loads the gateway's configuration file.
//
// The file is the same one the case study tells a reader to run
// (`--config ./configs/local.yaml`), so its shape is part of the project's
// public surface. Two rules follow from that: every field has a documented
// default, and an unknown field is an error rather than a silently ignored
// typo -- a rate limit that did not apply because a key was misspelled is the
// worst kind of quiet failure.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// Config is the whole configuration file.
type Config struct {
	Node     Node        `yaml:"node"`
	Limiter  Limiter     `yaml:"limiter"`
	Redis    Redis       `yaml:"redis"`
	Postgres Postgres    `yaml:"postgres"`
	Policies Policies    `yaml:"policies"`
	Policy   PolicyStore `yaml:"policy"`
	Auth     Auth        `yaml:"auth"`
	CheckAPI CheckAPI    `yaml:"check_api"`
}

// PolicyStore selects where policies come from and how long they are cached.
type PolicyStore struct {
	// Store is "static" (the policies section of this file) or "postgres".
	Store string `yaml:"store"`
	// CacheTTL is how long a policy is served before being re-read. It is the
	// upper bound on how long a policy change takes to reach a replica that
	// did not serve the write -- unless ListenForChanges is on, in which case
	// it is the bound only when the listener is down.
	CacheTTL time.Duration `yaml:"cache_ttl"`
	// NegativeCacheTTL is how long "no such tenant" is remembered. Shorter,
	// because a tenant that was just created should start working quickly.
	NegativeCacheTTL time.Duration `yaml:"negative_cache_ttl"`
	// StaleFor is how long a cached policy may still be served after the store
	// becomes unreachable. A policy store outage then freezes policy at the
	// last known version rather than taking enforcement down with it.
	StaleFor time.Duration `yaml:"stale_for"`
	// ListenForChanges subscribes to Postgres NOTIFY so an edit reaches every
	// replica at once instead of when each TTL lapses.
	ListenForChanges bool `yaml:"listen_for_changes"`
}

// Auth configures tenant identity and the admin API credential.
//
// No secret is ever read from this file. Each field names an ENVIRONMENT
// VARIABLE holding the secret, because this file is committed to a public
// repository and a config format that accepts an inline secret will eventually
// be given one.
type Auth struct {
	// APIKeys enables X-API-Key / `Authorization: ApiKey` against the store.
	APIKeys bool `yaml:"api_keys"`
	// JWTSecretEnv names the env var holding the HS256 signing secret. Empty
	// disables JWT authentication.
	JWTSecretEnv   string `yaml:"jwt_secret_env"`
	JWTIssuer      string `yaml:"jwt_issuer"`
	JWTAudience    string `yaml:"jwt_audience"`
	JWTTenantClaim string `yaml:"jwt_tenant_claim"`
	// AdminTokenEnv names the env var holding the admin API credential. With
	// no token the admin API refuses every request rather than serving an
	// unauthenticated management surface.
	AdminTokenEnv string `yaml:"admin_token_env"`
}

// Node identifies this process and where it listens.
type Node struct {
	// ID names this node in logs, metrics and -- from M5 -- the hash ring. It
	// must be stable across restarts, because it is what decides which tenants
	// this node owns.
	ID       string `yaml:"id"`
	HTTPAddr string `yaml:"http_addr"`
	// ShutdownGrace bounds how long in-flight requests have to finish when the
	// process is asked to stop.
	ShutdownGrace time.Duration `yaml:"shutdown_grace"`
	// ReadTimeout and WriteTimeout bound a single request.
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// Limiter selects where counter state lives.
type Limiter struct {
	// Backend is "memory" or "redis". Memory is per-process: two replicas do
	// not share a quota, so it is for a single node and for tests.
	Backend string `yaml:"backend"`
	// SweepInterval is how often expired counters are discarded from the
	// in-memory store. It affects memory, never admission -- a counter is only
	// discarded once its state is identical to having none.
	SweepInterval time.Duration `yaml:"sweep_interval"`
	// MaxKeys bounds the in-memory store; 0 uses the package default.
	MaxKeys int `yaml:"max_keys"`
}

// Redis is the shared counter store.
type Redis struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	// PoolSize is connections per gateway process. It bounds how many checks
	// can be in flight against Redis at once.
	PoolSize int           `yaml:"pool_size"`
	Timeout  time.Duration `yaml:"timeout"`
}

// Postgres holds the policy store connection.
type Postgres struct {
	DSN string `yaml:"dsn"`
}

// Policies is the statically configured policy set.
//
// From M3 the policy store is Postgres and this becomes the bootstrap and
// test source. It stays because a limiter that cannot run without a database
// is harder to test than one that can.
type Policies struct {
	// Default applies to a tenant with no policy of its own. Empty means an
	// unknown tenant is refused rather than given a free pass.
	Default string            `yaml:"default"`
	Named   map[string]Policy `yaml:"named"`
	Tenants map[string]string `yaml:"tenants"`
}

// Policy is a named set of limits, all of which must admit a request.
type Policy struct {
	Limits []limiter.Limit `yaml:"limits"`
	// FailureMode decides what happens when the counter store cannot be
	// reached: "closed" refuses the request, "open" admits it. Closed is the
	// default because an unenforced limit is indistinguishable from no limit.
	FailureMode string `yaml:"failure_mode"`
}

// CheckAPI configures the decision endpoint.
type CheckAPI struct {
	// TrustTenantHeader lets a caller name its own tenant in X-Tenant-ID.
	// That is right for a decision API called by trusted infrastructure (an
	// ingress or a sidecar asking "may this request through?") and wrong for
	// traffic off the internet, which is why it is a switch and not a default.
	TrustTenantHeader bool `yaml:"trust_tenant_header"`
}

// FailureMode values.
const (
	FailClosed = "closed"
	FailOpen   = "open"
)

// Defaults returns the configuration a file is merged onto.
func Defaults() Config {
	return Config{
		Node: Node{
			ID:            "gateway-1",
			HTTPAddr:      ":8080",
			ShutdownGrace: 10 * time.Second,
			ReadTimeout:   5 * time.Second,
			WriteTimeout:  10 * time.Second,
		},
		Limiter: Limiter{
			Backend:       "memory",
			SweepInterval: 30 * time.Second,
		},
		Redis: Redis{
			Addr:     "127.0.0.1:6379",
			PoolSize: 64,
			Timeout:  250 * time.Millisecond,
		},
		Policy: PolicyStore{
			Store:            "static",
			CacheTTL:         5 * time.Second,
			NegativeCacheTTL: time.Second,
			StaleFor:         5 * time.Minute,
			ListenForChanges: true,
		},
		Auth: Auth{
			APIKeys:        true,
			JWTSecretEnv:   "GATEWAY_JWT_SECRET",
			JWTTenantClaim: "tid",
			AdminTokenEnv:  "GATEWAY_ADMIN_TOKEN",
		},
		CheckAPI: CheckAPI{TrustTenantHeader: true},
	}
}

// Load reads a YAML file over the defaults.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return Parse(raw)
}

// Parse reads YAML over the defaults and validates the result.
func Parse(raw []byte) (Config, error) {
	cfg := Defaults()
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // a misspelled key is an error, not a no-op
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate reports the first reason this configuration cannot be run.
func (c Config) Validate() error {
	if c.Node.ID == "" {
		return fmt.Errorf("config: node.id is empty")
	}
	if strings.ContainsAny(c.Node.ID, " \t:/") {
		return fmt.Errorf("config: node.id %q must not contain spaces, ':' or '/'", c.Node.ID)
	}
	if c.Node.HTTPAddr == "" {
		return fmt.Errorf("config: node.http_addr is empty")
	}
	switch c.Limiter.Backend {
	case "memory", "redis":
	default:
		return fmt.Errorf("config: limiter.backend %q is not \"memory\" or \"redis\"", c.Limiter.Backend)
	}
	if c.Limiter.Backend == "redis" && c.Redis.Addr == "" {
		return fmt.Errorf("config: limiter.backend is redis but redis.addr is empty")
	}
	if c.Limiter.SweepInterval < 0 {
		return fmt.Errorf("config: limiter.sweep_interval must not be negative")
	}

	switch c.Policy.Store {
	case "static", "postgres":
	default:
		return fmt.Errorf("config: policy.store %q is not \"static\" or \"postgres\"", c.Policy.Store)
	}
	if c.Policy.Store == "postgres" && c.Postgres.DSN == "" {
		return fmt.Errorf("config: policy.store is postgres but postgres.dsn is empty")
	}
	if c.Policy.CacheTTL < 0 || c.Policy.NegativeCacheTTL < 0 || c.Policy.StaleFor < 0 {
		return fmt.Errorf("config: policy cache durations must not be negative")
	}
	if c.Policy.StaleFor > 0 && c.Policy.StaleFor < c.Policy.CacheTTL {
		return fmt.Errorf("config: policy.stale_for (%s) is shorter than policy.cache_ttl (%s), so serving stale could never happen",
			c.Policy.StaleFor, c.Policy.CacheTTL)
	}
	// A secret in the config file would be a secret in the repository.
	if strings.Contains(c.Auth.JWTSecretEnv, " ") || strings.HasPrefix(c.Auth.JWTSecretEnv, "-") {
		return fmt.Errorf("config: auth.jwt_secret_env must name an environment variable, not hold a secret")
	}

	seen := map[string]bool{}
	for name, p := range c.Policies.Named {
		if name == "" {
			return fmt.Errorf("config: a policy has an empty name")
		}
		if len(p.Limits) == 0 {
			return fmt.Errorf("config: policy %q has no limits; a policy that limits nothing should not exist", name)
		}
		switch p.FailureMode {
		case "", FailClosed, FailOpen:
		default:
			return fmt.Errorf("config: policy %q: failure_mode %q is not %q or %q", name, p.FailureMode, FailClosed, FailOpen)
		}
		for i, l := range p.Limits {
			if err := l.Validate(); err != nil {
				return fmt.Errorf("config: policy %q: %w", name, err)
			}
			key := name + "/" + l.Name
			if seen[key] {
				return fmt.Errorf("config: policy %q has two limits named %q -- they would share one counter", name, l.Name)
			}
			seen[key] = true
			_ = i
		}
	}
	if c.Policies.Default != "" {
		if _, ok := c.Policies.Named[c.Policies.Default]; !ok {
			return fmt.Errorf("config: policies.default names %q, which is not defined", c.Policies.Default)
		}
	}
	for tenant, policy := range c.Policies.Tenants {
		if tenant == "" {
			return fmt.Errorf("config: a tenant has an empty id")
		}
		if strings.ContainsAny(tenant, ":{}") {
			return fmt.Errorf("config: tenant %q must not contain ':', '{' or '}' -- those delimit the storage key", tenant)
		}
		if _, ok := c.Policies.Named[policy]; !ok {
			return fmt.Errorf("config: tenant %q names policy %q, which is not defined", tenant, policy)
		}
	}
	return nil
}

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
	Cluster  Cluster     `yaml:"cluster"`
	Routes   []Route     `yaml:"routes"`
}

// Cluster is the ring this node belongs to.
//
// With no members the node is alone and answers for every tenant itself, which
// is the single-node deployment and what every test that does not care about
// the ring runs. With members, a tenant's checks are coordinated by whichever
// node the ring picks, and requests that arrive elsewhere are forwarded there.
//
// Membership is static here. Milestone 6 replaces the source with a
// Raft-committed one; the shape does not change.
type Cluster struct {
	Members []Member `yaml:"members"`
	// VNodes is how many points each node occupies on the ring. Zero uses the
	// ring package's default. Every node must agree, or they compute different
	// rings from the same membership.
	VNodes int `yaml:"vnodes"`
	// ForwardTimeout bounds one forwarded check. A forward that has not
	// answered by then has already cost more than deciding locally would have.
	ForwardTimeout time.Duration `yaml:"forward_timeout"`
	// ForwardPoolSize is how many connections are held per peer.
	ForwardPoolSize int `yaml:"forward_pool_size"`
	// Raft replaces the member list above as the source of membership. With
	// it off, this file is the truth and a node that dies leaves a stale
	// entry behind; with it on, the file is only the bootstrap seed.
	Raft Raft `yaml:"raft"`
}

// Raft governs membership and ring configuration, and nothing else.
//
// No counter and no admission decision passes through it. That is the whole
// point of the design: correctness lives in Redis behind an atomic script, so
// consensus never has to be on the request path, where one to three
// fsync-bounded round trips per check would not be a latency regression but an
// impossibility.
type Raft struct {
	// Enabled turns the log on. Off, the gateway behaves exactly as it did
	// before: membership comes from the member list and never changes while
	// the process runs.
	Enabled bool `yaml:"enabled"`
	// Dir holds the log and its snapshots. Empty keeps both in memory, which
	// loses the log on restart -- fine for a test, wrong for a deployment,
	// and so it is reported at startup rather than assumed.
	Dir string `yaml:"dir"`
	// Advertise overrides the address peers are told to reach this node on,
	// for a container that binds 0.0.0.0 and is reached on something else.
	Advertise string `yaml:"advertise"`
	// The election timers. Zero takes hashicorp/raft's defaults, which are
	// tuned for a datacenter; a local cluster of three processes on two cores
	// wants them shorter.
	HeartbeatTimeout   time.Duration `yaml:"heartbeat_timeout"`
	ElectionTimeout    time.Duration `yaml:"election_timeout"`
	LeaderLeaseTimeout time.Duration `yaml:"leader_lease_timeout"`
	CommitTimeout      time.Duration `yaml:"commit_timeout"`
}

// Member is one node of the ring.
type Member struct {
	// ID is what the ring hashes, so it must be stable across restarts. An
	// address can change without moving a single tenant.
	ID string `yaml:"id"`
	// Addr is this member's gRPC address, which is where forwards go.
	Addr string `yaml:"addr"`
	// RaftAddr is where this member's consensus traffic goes. It is separate
	// from Addr because the two carry different traffic and fail differently:
	// putting a leader election and a tenant's checks in one queue means a
	// burst of either delays the other.
	RaftAddr string `yaml:"raft_addr"`
}

// Route sends matching requests to an upstream, after the limiter has decided.
type Route struct {
	Name string `yaml:"name"`
	// PathPrefix is matched against the request path; the longest matching
	// route wins, so the order of this list never decides anything.
	PathPrefix string `yaml:"path_prefix"`
	// Upstream is an absolute URL: scheme, host, and optionally a base path.
	Upstream string `yaml:"upstream"`
	// StripPrefix is removed from the path before forwarding, so that a
	// gateway-side /api/echo can reach an upstream that serves /echo.
	StripPrefix string `yaml:"strip_prefix"`
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
	// GRPCAddr serves the Limiter service. Empty disables gRPC.
	GRPCAddr string `yaml:"grpc_addr"`
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
			GRPCAddr:      ":9090",
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

	if err := validateRoutes(c.Routes); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if err := c.validateCluster(); err != nil {
		return fmt.Errorf("config: %w", err)
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

// validateCluster rejects a ring this node could not take part in.
func (c Config) validateCluster() error {
	if len(c.Cluster.Members) == 0 {
		// Alone, which is a valid deployment -- but consensus still has to be
		// checked, because "raft on, nobody to reach it with" is exactly the
		// configuration that would otherwise slip through here.
		return c.validateRaft()
	}
	seenID := map[string]bool{}
	seenAddr := map[string]bool{}
	self := false
	for _, m := range c.Cluster.Members {
		if m.ID == "" {
			return fmt.Errorf("cluster: a member has no id")
		}
		if m.Addr == "" {
			return fmt.Errorf("cluster: member %q has no addr, so nothing could forward to it", m.ID)
		}
		if seenID[m.ID] {
			return fmt.Errorf("cluster: two members share the id %q", m.ID)
		}
		if seenAddr[m.Addr] {
			// Two members on one address means one process answering as two
			// nodes, which makes the ring's ownership meaningless.
			return fmt.Errorf("cluster: two members share the address %q", m.Addr)
		}
		seenID[m.ID], seenAddr[m.Addr] = true, true
		if m.ID == c.Node.ID {
			self = true
		}
	}
	if !self {
		return fmt.Errorf("cluster: node.id %q is not in the member list, so this node would forward every request away, including its own tenants",
			c.Node.ID)
	}
	if c.Cluster.VNodes < 0 {
		return fmt.Errorf("cluster: vnodes must not be negative")
	}
	if c.Cluster.ForwardTimeout < 0 {
		return fmt.Errorf("cluster: forward_timeout must not be negative")
	}
	return c.validateRaft()
}

// validateRaft rejects a consensus configuration this node could not join.
func (c Config) validateRaft() error {
	r := c.Cluster.Raft
	if !r.Enabled {
		return nil
	}
	if len(c.Cluster.Members) == 0 {
		return fmt.Errorf("cluster: raft.enabled with no members; consensus needs the peers it is forming a cluster with")
	}
	if r.HeartbeatTimeout < 0 || r.ElectionTimeout < 0 || r.LeaderLeaseTimeout < 0 || r.CommitTimeout < 0 {
		return fmt.Errorf("cluster: the raft timeouts must not be negative")
	}

	seen := make(map[string]string, len(c.Cluster.Members)*2)
	for _, m := range c.Cluster.Members {
		seen[m.Addr] = m.ID + " (requests)"
	}
	for _, m := range c.Cluster.Members {
		if m.RaftAddr == "" {
			// Falling back to the request address would have consensus try to
			// bind the port gRPC is already listening on: the node would fail
			// to start, and the reason would be two configuration lines apart.
			return fmt.Errorf("cluster: member %q has no raft_addr, and consensus cannot share the request port %s", m.ID, m.Addr)
		}
		if owner, taken := seen[m.RaftAddr]; taken {
			return fmt.Errorf("cluster: member %q wants %s for consensus, which %s already uses", m.ID, m.RaftAddr, owner)
		}
		seen[m.RaftAddr] = m.ID + " (consensus)"
	}
	return nil
}

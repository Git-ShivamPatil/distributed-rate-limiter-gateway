package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// The file the case study tells a reader to run has to load, validate, and mean
// what the README says it means. A test that only parses a fixture would let
// the shipped config rot.
func TestLocalConfigLoads(t *testing.T) {
	cfg, err := Load("../../configs/local.yaml")
	if err != nil {
		t.Fatalf("configs/local.yaml does not load: %v", err)
	}

	if cfg.Node.ID != "gateway-1" {
		t.Errorf("node.id = %q, want gateway-1 (the id the case study passes)", cfg.Node.ID)
	}
	if cfg.Node.HTTPAddr != ":8080" {
		t.Errorf("node.http_addr = %q, want :8080", cfg.Node.HTTPAddr)
	}
	if cfg.Node.ReadTimeout != 5*time.Second {
		t.Errorf("read_timeout = %s, want 5s -- duration strings must parse", cfg.Node.ReadTimeout)
	}

	// The shipped config is the shared, database-backed one: that is what
	// makes `docker compose up -d redis postgres && make migrate` load-bearing
	// rather than decorative.
	if cfg.Limiter.Backend != "redis" {
		t.Errorf("limiter.backend = %q, want redis", cfg.Limiter.Backend)
	}
	if cfg.Policy.Store != "postgres" {
		t.Errorf("policy.store = %q, want postgres", cfg.Policy.Store)
	}
	if cfg.Postgres.DSN == "" {
		t.Error("postgres.dsn is empty, so `make migrate` has nowhere to go")
	}
	if !cfg.Policy.ListenForChanges {
		t.Error("policy.listen_for_changes is off, so an edit would take a whole TTL to reach another replica")
	}
	if cfg.Policy.StaleFor <= cfg.Policy.CacheTTL {
		t.Errorf("stale_for %s is not longer than cache_ttl %s, so serving stale could never happen",
			cfg.Policy.StaleFor, cfg.Policy.CacheTTL)
	}
}

// The shipped config must not contain a secret. It names the environment
// variables instead, and this is the check that keeps it that way.
func TestLocalConfigHoldsNoSecrets(t *testing.T) {
	raw, err := os.ReadFile("../../configs/local.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.JWTSecretEnv == "" || cfg.Auth.AdminTokenEnv == "" {
		t.Fatal("the config does not name the environment variables its secrets come from")
	}

	text := strings.ToLower(string(raw))
	for _, needle := range []string{"jwt_secret:", "admin_token:", "password:", "secret_key"} {
		if strings.Contains(text, needle) {
			t.Errorf("configs/local.yaml contains %q, which reads like an inline secret", needle)
		}
	}
	// The Postgres DSN carries the demo password, which is fine because it is
	// the compose file's published one -- but it must be the compose one.
	if !strings.Contains(cfg.Postgres.DSN, "gateway:gateway@") {
		t.Error("the DSN's credentials are not the compose file's demonstration pair; a real one may have been committed")
	}
}

func TestDefaultsAreValid(t *testing.T) {
	if err := Defaults().Validate(); err != nil {
		t.Fatalf("the defaults do not validate: %v", err)
	}
}

// A misspelled key must fail loudly. A rate limit that did not apply because a
// field was ignored is the worst kind of silent failure.
func TestUnknownFieldIsAnError(t *testing.T) {
	_, err := Parse([]byte("node:\n  id: gateway-1\n  htpp_addr: \":8080\"\n"))
	if err == nil {
		t.Fatal("a misspelled key was accepted")
	}
	if !strings.Contains(err.Error(), "htpp_addr") {
		t.Fatalf("error does not name the offending field: %v", err)
	}
}

func TestValidationCatchesBrokenPolicies(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "tenant names a policy that does not exist",
			yaml: "policies:\n  named:\n    free:\n      limits:\n        - {name: m, algorithm: token_bucket, count: 1, period: 1s}\n  tenants:\n    acme: gold\n",
			want: `tenant "acme" names policy "gold"`,
		},
		{
			name: "default names a policy that does not exist",
			yaml: "policies:\n  default: gold\n",
			want: `policies.default names "gold"`,
		},
		{
			name: "policy with no limits",
			yaml: "policies:\n  named:\n    free:\n      failure_mode: closed\n      limits: []\n",
			want: "has no limits",
		},
		{
			name: "unknown algorithm",
			yaml: "policies:\n  named:\n    free:\n      limits:\n        - {name: m, algorithm: leaky_faucet, count: 1, period: 1s}\n",
			want: "unknown algorithm",
		},
		{
			name: "zero count",
			yaml: "policies:\n  named:\n    free:\n      limits:\n        - {name: m, algorithm: token_bucket, count: 0, period: 1s}\n",
			want: "count must be > 0",
		},
		{
			name: "two limits with one name would share a counter",
			yaml: "policies:\n  named:\n    free:\n      limits:\n        - {name: m, algorithm: token_bucket, count: 1, period: 1s}\n        - {name: m, algorithm: token_bucket, count: 2, period: 1s}\n",
			want: "two limits named",
		},
		{
			name: "sliding window with a separate burst",
			yaml: "policies:\n  named:\n    free:\n      limits:\n        - {name: m, algorithm: sliding_window, count: 5, period: 1s, burst: 50}\n",
			want: "no separate burst",
		},
		{
			name: "sliding window too large to log",
			yaml: "policies:\n  named:\n    free:\n      limits:\n        - {name: m, algorithm: sliding_window, count: 200000, period: 1s}\n",
			want: "use token_bucket for rates this high",
		},
		{
			name: "unknown failure mode",
			yaml: "policies:\n  named:\n    free:\n      failure_mode: maybe\n      limits:\n        - {name: m, algorithm: token_bucket, count: 1, period: 1s}\n",
			want: "failure_mode",
		},
		{
			name: "unknown limiter backend",
			yaml: "limiter:\n  backend: memcached\n",
			want: "limiter.backend",
		},
		{
			name: "tenant id with a key delimiter in it",
			yaml: "policies:\n  named:\n    free:\n      limits:\n        - {name: m, algorithm: token_bucket, count: 1, period: 1s}\n  tenants:\n    \"ac:me\": free\n",
			want: "must not contain",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("accepted a broken config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

// Command gatewayctl is the operational tool: schema migrations today, and
// from milestone 5 the cluster commands (ring ownership, join, leave).
//
//	gatewayctl migrate up      --config ./configs/local.yaml
//	gatewayctl migrate down 1  --config ./configs/local.yaml
//	gatewayctl migrate version --config ./configs/local.yaml
//	gatewayctl apikey create   --config ./configs/local.yaml --tenant acme
//
// The DSN comes from the same config file the gateway reads, so there is one
// answer to "which database is this" rather than two that can disagree.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "gatewayctl: %v\n", err)
		os.Exit(1)
	}
}

func usage() string {
	return `usage: gatewayctl <command> [flags]

commands:
  migrate up                 apply every pending migration
  migrate down [n]           roll back n steps (default 1; 0 rolls back all)
  migrate version            print the applied version
  apikey create              mint an API key for a tenant
  apikey revoke              revoke a key by its prefix
  counters reset             clear a tenant's live rate-limit counters

flags:
  --config <path>            the gateway config file (for postgres.dsn)
  --dsn <url>                override the DSN from the config
  --tenant <id>              tenant, for the apikey commands
  --prefix <prefix>          key prefix, for apikey revoke
  --label <text>             a note stored with a new key
`
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage())
		return errors.New("no command given")
	}

	var (
		configPath string
		dsnFlag    string
		redisFlag  string
		tenant     string
		prefix     string
		label      string
	)
	fs := flag.NewFlagSet("gatewayctl", flag.ContinueOnError)
	fs.StringVar(&configPath, "config", "", "gateway config file")
	fs.StringVar(&dsnFlag, "dsn", "", "database DSN, overriding the config")
	fs.StringVar(&redisFlag, "redis", "", "Redis address, overriding the config")
	fs.StringVar(&tenant, "tenant", "", "tenant id")
	fs.StringVar(&prefix, "prefix", "", "api key prefix")
	fs.StringVar(&label, "label", "", "note stored with a new key")
	fs.Usage = func() { fmt.Fprint(fs.Output(), usage()) }

	// Split the sub-command words from the flags, so both `migrate up --config x`
	// and `--config x migrate up` work.
	var words []string
	var flagArgs []string
	for i := 0; i < len(args); i++ {
		if len(args[i]) > 0 && args[i][0] == '-' {
			flagArgs = append(flagArgs, args[i:]...)
			break
		}
		words = append(words, args[i])
	}
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if len(words) == 0 {
		return errors.New("no command given")
	}

	dsn := dsnFlag
	redisAddr := redisFlag
	if configPath != "" {
		// Loading also validates: a config this tool accepts is one the
		// gateway will accept too.
		loaded, err := config.Load(configPath)
		if err != nil {
			return err
		}
		if dsn == "" {
			dsn = loaded.Postgres.DSN
		}
		if redisAddr == "" {
			redisAddr = loaded.Redis.Addr
		}
	}
	if dsn == "" {
		dsn = os.Getenv("GATEWAY_POSTGRES_DSN")
	}
	if redisAddr == "" {
		redisAddr = os.Getenv("GATEWAY_REDIS_ADDR")
	}
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	// Only the database commands need a DSN; counters talks to Redis.
	if dsn == "" && words[0] != "counters" {
		return errors.New("no database DSN: pass --config or --dsn, or set GATEWAY_POSTGRES_DSN")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	switch words[0] {
	case "migrate":
		return runMigrate(words[1:], dsn)
	case "apikey":
		return runAPIKey(ctx, words[1:], dsn, tenant, prefix, label)
	case "counters":
		return runCounters(ctx, words[1:], redisAddr, tenant)
	default:
		fmt.Print(usage())
		return fmt.Errorf("unknown command %q", words[0])
	}
}

// runCounters clears a tenant's live counters.
//
// This is real operational tooling: when a tenant is throttled because of a
// misconfigured policy, the fix is to correct the policy and then forget the
// debt the wrong one accrued. It is also what lets a test assert an exact
// burst against a store that outlives the test.
//
// SCAN rather than KEYS: KEYS walks the entire keyspace in one blocking call,
// which on a busy limiter's Redis is a stall of everything.
func runCounters(ctx context.Context, words []string, addr, tenant string) error {
	if len(words) == 0 || words[0] != "reset" {
		return errors.New("counters: expected `counters reset --tenant <id>`")
	}
	if tenant == "" {
		return errors.New("counters reset: --tenant is required")
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	defer func() { _ = client.Close() }()

	// The same shape internal/limiter builds: every key of one tenant shares
	// the braced tenant segment.
	// Counters only. A tenant's fences live under a different prefix on
	// purpose (see limiter.MetaKey): sweeping them up here would un-fence the
	// tenant, which is the same failure as never having fenced it.
	pattern := "rl1:{" + tenant + "}:*"
	var (
		cursor  uint64
		removed int64
	)
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 256).Result()
		if err != nil {
			return fmt.Errorf("counters reset: scanning: %w", err)
		}
		if len(keys) > 0 {
			n, err := client.Del(ctx, keys...).Result()
			if err != nil {
				return fmt.Errorf("counters reset: deleting: %w", err)
			}
			removed += n
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	fmt.Printf("reset %d counter(s) for tenant %s\n", removed, tenant)
	return nil
}

func runMigrate(words []string, dsn string) error {
	sub := "up"
	if len(words) > 0 {
		sub = words[0]
	}

	m, err := store.NewMigrator(dsn)
	if err != nil {
		return err
	}
	defer func() { _ = m.Close() }()

	switch sub {
	case "up":
		applied, err := m.Up()
		if err != nil {
			return err
		}
		version, dirty, err := m.Version()
		if err != nil {
			return err
		}
		if applied {
			fmt.Printf("migrated to version %d\n", version)
		} else {
			// Re-running must succeed: some deployments run this on every
			// start, and a command that fails when there is nothing to do
			// cannot be used that way.
			fmt.Printf("already at version %d; nothing to apply\n", version)
		}
		if dirty {
			return fmt.Errorf("schema version %d is DIRTY: a migration failed part-way and the database is in a state no migration describes", version)
		}
		return nil

	case "down":
		steps := 1
		if len(words) > 1 {
			if _, err := fmt.Sscanf(words[1], "%d", &steps); err != nil {
				return fmt.Errorf("down: %q is not a number of steps", words[1])
			}
		}
		if err := m.Down(steps); err != nil {
			return err
		}
		version, _, err := m.Version()
		if err != nil {
			return err
		}
		fmt.Printf("rolled back to version %d\n", version)
		return nil

	case "version":
		version, dirty, err := m.Version()
		if err != nil {
			return err
		}
		state := "clean"
		if dirty {
			state = "DIRTY"
		}
		fmt.Printf("version %d (%s)\n", version, state)
		return nil

	default:
		return fmt.Errorf("unknown migrate sub-command %q", sub)
	}
}

func runAPIKey(ctx context.Context, words []string, dsn, tenant, prefix, label string) error {
	if len(words) == 0 {
		return errors.New("apikey: expected create or revoke")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer pool.Close()
	repo := policy.NewPostgres(pool)

	switch words[0] {
	case "create":
		if tenant == "" {
			return errors.New("apikey create: --tenant is required")
		}
		key, err := auth.GenerateKey()
		if err != nil {
			return err
		}
		if err := repo.CreateAPIKey(ctx, tenant, label, key.Hash, key.Prefix); err != nil {
			return err
		}
		// Printed once, stored never: what the database holds is the hash.
		fmt.Printf("%s\n", key.Secret)
		fmt.Fprintf(os.Stderr, "created key %s... for tenant %s. This is the only time the secret is shown.\n",
			key.Prefix, tenant)
		return nil

	case "revoke":
		if tenant == "" || prefix == "" {
			return errors.New("apikey revoke: --tenant and --prefix are required")
		}
		if err := repo.RevokeAPIKey(ctx, tenant, prefix); err != nil {
			return err
		}
		fmt.Printf("revoked %s... for tenant %s\n", prefix, tenant)
		return nil

	default:
		return fmt.Errorf("unknown apikey sub-command %q", words[0])
	}
}

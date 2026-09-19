package policy_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/store"
)

// testDSN points at a database created for this package's run and dropped
// afterwards. Empty means no Postgres was reachable and the integration tests
// skip -- unless POSTGRES_REQUIRED is set, in which case TestMain fails the
// run rather than letting a green suite mean "the database tests did not run".
var testDSN string

const defaultDSN = "postgres://gateway:gateway@127.0.0.1:5432/gateway?sslmode=disable"

func TestMain(m *testing.M) {
	os.Exit(runSuite(m))
}

func runSuite(m *testing.M) int {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = defaultDSN
	}
	required := os.Getenv("POSTGRES_REQUIRED") == "1"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = admin.Ping(ctx)
	}
	if err != nil {
		if required {
			fmt.Fprintf(os.Stderr, "POSTGRES_REQUIRED=1 but no Postgres at %s: %v\n", dsn, err)
			return 1
		}
		fmt.Fprintf(os.Stderr, "no Postgres at %s: %v -- database tests will skip\n", dsn, err)
		return m.Run()
	}
	defer admin.Close()

	// A database of its own, so a test that drops a table cannot take a
	// developer's data with it and two runs cannot collide.
	name := fmt.Sprintf("rl_test_%d", time.Now().UnixNano()%1_000_000_000)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		fmt.Fprintf(os.Stderr, "creating test database: %v\n", err)
		return 1
	}
	defer func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dropCancel()
		if _, err := admin.Exec(dropCtx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			fmt.Fprintf(os.Stderr, "dropping test database %s: %v\n", name, err)
		}
	}()

	testDSN = withDatabase(dsn, name)

	migrator, err := store.NewMigrator(testDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "opening migrator: %v\n", err)
		return 1
	}
	if _, err := migrator.Up(); err != nil {
		fmt.Fprintf(os.Stderr, "migrating: %v\n", err)
		return 1
	}
	_ = migrator.Close()

	return m.Run()
}

func withDatabase(dsn, name string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	u.Path = "/" + name
	return u.String()
}

func testRepo(t *testing.T) (*policy.Postgres, *pgxpool.Pool) {
	t.Helper()
	if testDSN == "" {
		t.Skip("no Postgres (set POSTGRES_REQUIRED=1 to make this a failure)")
	}
	pool, err := pgxpool.New(context.Background(), testDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return policy.NewPostgres(pool), pool
}

func seedPolicy(t *testing.T, repo *policy.Postgres, name string, limits []limiter.Limit, matches []policy.Match) {
	t.Helper()
	rec := policy.PolicyRecord{Name: name, FailureMode: "closed", Limits: limits, Matches: matches}
	if err := rec.Validate(); err != nil {
		t.Fatalf("seed policy %s: %v", name, err)
	}
	if err := repo.UpsertPolicy(context.Background(), rec); err != nil {
		t.Fatalf("seed policy %s: %v", name, err)
	}
}

func bucket(name string, count int64, period time.Duration, burst int64) limiter.Limit {
	return limiter.Limit{Name: name, Algorithm: limiter.AlgorithmTokenBucket,
		Count: count, Period: period, Burst: burst}
}

// The migration seeds the two tenants the case study uses, so that the
// published commands work immediately after `make migrate`.
func TestMigrationSeedsTheDemoTenants(t *testing.T) {
	repo, _ := testRepo(t)

	p, err := repo.Lookup(context.Background(), "acme")
	if err != nil {
		t.Fatalf("the demo tenant acme is missing after migration: %v", err)
	}
	if p.Name != "free" || len(p.Limits) != 1 {
		t.Fatalf("acme resolved to %+v, want the free policy with one limit", p)
	}
	l := p.Limits[0]
	if l.Count != 20 || l.Period != time.Minute || l.Burst != 20 {
		t.Fatalf("acme's limit is %+v, want 20 per minute with a burst of 20 -- the number the case study prints", l)
	}

	globex, err := repo.Lookup(context.Background(), "globex")
	if err != nil {
		t.Fatal(err)
	}
	if len(globex.Limits) != 2 {
		t.Fatalf("globex has %d limits, want 2", len(globex.Limits))
	}
}

func TestPostgresLookup(t *testing.T) {
	repo, _ := testRepo(t)
	ctx := context.Background()
	seedPolicy(t, repo, "lookup-plan", []limiter.Limit{bucket("per-minute", 30, time.Minute, 30)}, nil)

	if err := repo.UpsertTenant(ctx, policy.TenantRecord{
		ID: "lookup-tenant", Name: "Lookup", Policy: "lookup-plan",
	}); err != nil {
		t.Fatal(err)
	}

	p, err := repo.Lookup(ctx, "lookup-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "lookup-plan" || p.Limits[0].Count != 30 {
		t.Fatalf("got %+v", p)
	}
	if !p.FailsClosed() {
		t.Error("a policy stored as closed came back as open")
	}

	if _, err := repo.Lookup(ctx, "nobody-here"); !errors.Is(err, policy.ErrTenantNotFound) {
		t.Fatalf("unknown tenant: err = %v, want ErrTenantNotFound", err)
	}
}

// A disabled tenant is refused, and refused differently from an unknown one.
func TestPostgresDisabledTenant(t *testing.T) {
	repo, _ := testRepo(t)
	ctx := context.Background()
	seedPolicy(t, repo, "disabled-plan", []limiter.Limit{bucket("per-minute", 10, time.Minute, 10)}, nil)

	if err := repo.UpsertTenant(ctx, policy.TenantRecord{
		ID: "disabled-tenant", Name: "Off", Policy: "disabled-plan", Disabled: true,
	}); err != nil {
		t.Fatal(err)
	}

	_, err := repo.Lookup(ctx, "disabled-tenant")
	if !errors.Is(err, policy.ErrTenantDisabled) {
		t.Fatalf("err = %v, want ErrTenantDisabled", err)
	}
	if errors.Is(err, policy.ErrTenantNotFound) {
		t.Fatal("a disabled tenant reads as an unknown one, so turning a tenant off looks like losing their record")
	}
}

// Writing a policy REPLACES its limits. A limit left behind because the new
// body did not mention it would enforce a rule nobody can see.
func TestPostgresUpsertPolicyReplacesLimits(t *testing.T) {
	repo, _ := testRepo(t)
	ctx := context.Background()

	seedPolicy(t, repo, "replace-plan", []limiter.Limit{
		bucket("per-minute", 100, time.Minute, 100),
		bucket("per-hour", 1000, time.Hour, 1000),
	}, nil)
	if err := repo.UpsertTenant(ctx, policy.TenantRecord{
		ID: "replace-tenant", Name: "R", Policy: "replace-plan",
	}); err != nil {
		t.Fatal(err)
	}

	seedPolicy(t, repo, "replace-plan", []limiter.Limit{
		bucket("per-minute", 50, time.Minute, 50),
	}, nil)

	p, err := repo.Lookup(ctx, "replace-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Limits) != 1 {
		t.Fatalf("policy has %d limits after a replacing write, want 1: %+v", len(p.Limits), p.Limits)
	}
	if p.Limits[0].Count != 50 {
		t.Fatalf("limit = %d, want 50", p.Limits[0].Count)
	}
}

// Endpoint rules survive the round trip and narrow the limits that apply.
func TestPostgresEndpointRules(t *testing.T) {
	repo, _ := testRepo(t)
	ctx := context.Background()

	seedPolicy(t, repo, "endpoint-plan",
		[]limiter.Limit{
			bucket("tenant-wide", 1000, time.Minute, 1000),
			bucket("writes", 10, time.Minute, 10),
		},
		[]policy.Match{
			{},
			{Method: "POST", PathPrefix: "/api/orders"},
		})
	if err := repo.UpsertTenant(ctx, policy.TenantRecord{
		ID: "endpoint-tenant", Name: "E", Policy: "endpoint-plan",
	}); err != nil {
		t.Fatal(err)
	}

	p, err := repo.Lookup(ctx, "endpoint-tenant")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Limits) != 2 || len(p.Matches) != 2 {
		t.Fatalf("got %d limits and %d matches", len(p.Limits), len(p.Matches))
	}

	cases := []struct {
		method, path string
		want         int
		why          string
	}{
		{"POST", "/api/orders", 2, "both the tenant-wide quota and the write rule apply"},
		{"GET", "/api/orders", 1, "the rule is POST-only"},
		{"POST", "/api/reads", 1, "the rule is scoped to /api/orders"},
		{"", "", 1, "a request nobody described cannot be matched against an endpoint rule"},
	}
	for _, c := range cases {
		if got := len(p.LimitsFor(c.method, c.path)); got != c.want {
			t.Errorf("%s %s: %d limits, want %d (%s)", c.method, c.path, got, c.want, c.why)
		}
	}
}

// A tenant pointing at a policy that does not exist is the caller's mistake,
// so it must be distinguishable from the database falling over.
func TestPostgresConflictsAreDistinguishable(t *testing.T) {
	repo, _ := testRepo(t)
	err := repo.UpsertTenant(context.Background(), policy.TenantRecord{
		ID: "dangling-tenant", Name: "D", Policy: "no-such-policy",
	})
	if !errors.Is(err, policy.ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

// The store holds hashes, so a key resolves but cannot be read back out.
func TestPostgresAPIKeys(t *testing.T) {
	repo, pool := testRepo(t)
	ctx := context.Background()
	seedPolicy(t, repo, "key-plan", []limiter.Limit{bucket("per-minute", 10, time.Minute, 10)}, nil)
	if err := repo.UpsertTenant(ctx, policy.TenantRecord{ID: "key-tenant", Name: "K", Policy: "key-plan"}); err != nil {
		t.Fatal(err)
	}

	key, err := auth.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateAPIKey(ctx, "key-tenant", "laptop", key.Hash, key.Prefix); err != nil {
		t.Fatal(err)
	}

	tenant, err := repo.TenantForKey(ctx, key.Secret)
	if err != nil {
		t.Fatal(err)
	}
	if tenant != "key-tenant" {
		t.Fatalf("key resolved to %q", tenant)
	}

	if _, err := repo.TenantForKey(ctx, "rlk_not-a-real-key"); !errors.Is(err, policy.ErrUnknownKey) {
		t.Fatalf("unknown key: err = %v, want ErrUnknownKey", err)
	}

	// The secret itself is nowhere in the table.
	var stored []byte
	if err := pool.QueryRow(ctx, `SELECT key_hash FROM api_keys WHERE tenant_id = $1`, "key-tenant").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) == key.Secret {
		t.Fatal("the API key is stored in the clear")
	}

	if err := repo.RevokeAPIKey(ctx, "key-tenant", key.Prefix); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.TenantForKey(ctx, key.Secret); !errors.Is(err, policy.ErrUnknownKey) {
		t.Fatal("a revoked key still authenticates")
	}
	// Revoking twice is not a silent success: the second call has nothing to do.
	if err := repo.RevokeAPIKey(ctx, "key-tenant", key.Prefix); err == nil {
		t.Fatal("revoking an already-revoked key reported success")
	}
}

// An edit made on one node reaches another node's cache without waiting for
// its TTL. This is the whole point of the NOTIFY triggers in the schema.
func TestNotifyInvalidatesAnotherNodesCache(t *testing.T) {
	repo, pool := testRepo(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seedPolicy(t, repo, "notify-plan", []limiter.Limit{bucket("per-minute", 20, time.Minute, 20)}, nil)
	if err := repo.UpsertTenant(ctx, policy.TenantRecord{ID: "notify-tenant", Name: "N", Policy: "notify-plan"}); err != nil {
		t.Fatal(err)
	}

	// A second node: its own cache, with a TTL far longer than this test, so
	// anything that reaches it can only have arrived through NOTIFY.
	cache := policy.NewCache(repo, policy.WithTTL(time.Hour))
	listener := policy.NewListener(pool, cache, slog.New(slog.DiscardHandler))
	go listener.Run(ctx)

	// Wait for the listener to be subscribed before writing, otherwise the
	// test can race past the notification it is waiting for.
	deadline := time.Now().Add(10 * time.Second)
	for {
		p, err := cache.Lookup(ctx, "notify-tenant")
		if err != nil {
			t.Fatal(err)
		}
		if p.Limits[0].Count != 20 {
			t.Fatalf("unexpected seeded limit %d", p.Limits[0].Count)
		}
		if cache.Len() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the cache never held an entry")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(250 * time.Millisecond) // let LISTEN settle

	// Edit the policy through the "other" node.
	seedPolicy(t, repo, "notify-plan", []limiter.Limit{bucket("per-minute", 5, time.Minute, 5)}, nil)

	deadline = time.Now().Add(10 * time.Second)
	for {
		p, err := cache.Lookup(ctx, "notify-tenant")
		if err != nil {
			t.Fatal(err)
		}
		if p.Limits[0].Count == 5 {
			return // the edit arrived without the TTL lapsing
		}
		if time.Now().After(deadline) {
			t.Fatal("a policy edit never reached the other node's cache; its TTL is an hour, so NOTIFY is not working")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

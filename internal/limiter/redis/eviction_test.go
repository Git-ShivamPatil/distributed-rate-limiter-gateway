package redis

import (
	"context"
	"testing"
)

// The shipped data plane, and every developer's default, is noeviction. If
// this ever fails, the check the gateway makes at startup is about to start
// refusing to boot -- which is the point, but it should be a deliberate
// change rather than a surprise.
func TestThisRedisDoesNotEvict(t *testing.T) {
	client := testClient(t)
	e := New(client).Eviction(context.Background())

	if !e.Known {
		t.Skip("this server does not answer CONFIG GET, so the policy cannot be read")
	}
	t.Logf("%s", e)
	if e.Fatal() {
		t.Fatalf("this Redis may evict counters (%s); the gateway would refuse to start against it", e)
	}
	if e.Policy != NoEviction {
		t.Errorf("maxmemory-policy is %q, not %q -- safe today only because maxmemory is %d",
			e.Policy, NoEviction, e.MaxMemory)
	}
}

// THE CONTROL, against a real server actually reconfigured to evict.
//
// A precondition nobody exercises is a precondition that drifts. This sets the
// policy and a memory limit for the length of the test, requires the verdict
// to flip to fatal, and puts both back.
func TestAServerThatMayEvictIsRefused(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	checker := New(client)

	before := checker.Eviction(ctx)
	if !before.Known {
		t.Skip("this server does not answer CONFIG GET")
	}
	if before.Fatal() {
		t.Fatalf("this server already evicts (%s), so flipping it proves nothing", before)
	}

	// Put it back whatever happens, including a panic: leaving a developer's
	// Redis set to evict would be a worse bug than the one being tested.
	t.Cleanup(func() {
		if err := client.ConfigSet(context.Background(), "maxmemory", "0").Err(); err != nil {
			t.Errorf("could not restore maxmemory: %v", err)
		}
		if err := client.ConfigSet(context.Background(), "maxmemory-policy", before.Policy).Err(); err != nil {
			t.Errorf("could not restore maxmemory-policy to %q: %v", before.Policy, err)
		}
	})

	if err := client.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err(); err != nil {
		t.Skipf("this server will not let CONFIG SET change the policy: %v", err)
	}

	// A policy alone is not enough: with maxmemory unset nothing is ever
	// evicted, so the verdict must be "latent", not "fatal". Refusing here
	// would reject a server that is actually safe.
	latent := checker.Eviction(ctx)
	if latent.Fatal() {
		t.Fatalf("allkeys-lru with maxmemory=0 was called fatal (%s); nothing can be evicted without a limit", latent)
	}
	if !latent.Latent() {
		t.Fatalf("allkeys-lru with maxmemory=0 was not flagged as latent (%s)", latent)
	}

	// Now it can actually happen.
	if err := client.ConfigSet(ctx, "maxmemory", "64mb").Err(); err != nil {
		t.Skipf("this server will not let CONFIG SET change maxmemory: %v", err)
	}
	fatal := checker.Eviction(ctx)
	if !fatal.Fatal() {
		t.Fatalf("allkeys-lru with a 64mb limit was not refused (%s).\n"+
			"  That configuration discards counters under pressure, and an evicted\n"+
			"  counter is indistinguishable from a full bucket.", fatal)
	}
	t.Logf("refused, correctly: %s", fatal)
}

// An unreadable configuration is reported as unknown -- never as safe, and
// never as fatal. Several managed providers disable CONFIG, and a limiter that
// refused to start against them would be trading a real deployment for a check
// it cannot perform anyway.
func TestAnUnreadableConfigurationIsNeitherSafeNorFatal(t *testing.T) {
	var e Eviction // the zero value is what Eviction returns when CONFIG fails

	if e.Known {
		t.Fatal("an unread configuration claims to be known")
	}
	if e.Fatal() {
		t.Fatal("an unread configuration would stop the gateway starting")
	}
	if e.Latent() {
		t.Fatal("an unread configuration was flagged as latent, which it cannot be known to be")
	}
	if got := e.String(); got == "" {
		t.Fatal("an unread configuration describes itself as nothing at all")
	}
}

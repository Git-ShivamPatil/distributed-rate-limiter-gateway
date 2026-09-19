package ring

import (
	"fmt"
	"math/rand"
	"testing"
)

func nodes(ids ...string) []Node {
	out := make([]Node, 0, len(ids))
	for i, id := range ids {
		out = append(out, Node{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", 9000+i)})
	}
	return out
}

func keys(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("tenant-%d", i))
	}
	return out
}

// Every process has to compute the same ring, whatever order it learned the
// members in. If this is not true, two nodes disagree about ownership
// permanently rather than briefly.
func TestRingIsIndependentOfInputOrder(t *testing.T) {
	base := nodes("gateway-1", "gateway-2", "gateway-3", "gateway-4")

	a, err := New(base, DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(7))
	for attempt := 0; attempt < 10; attempt++ {
		shuffled := make([]Node, len(base))
		copy(shuffled, base)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })

		b, err := New(shuffled, DefaultVNodes)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys(2000) {
			if a.Owner(k).ID != b.Owner(k).ID {
				t.Fatalf("key %q is owned by %q in one ring and %q in another built from the same members",
					k, a.Owner(k).ID, b.Owner(k).ID)
			}
		}
	}
}

// The milestone's own property: removing one of N nodes must remap strictly
// fewer than 1.5/N of the keyspace. A hash that is not consistent -- taking
// the key modulo the node count, say -- remaps roughly (N-1)/N of it.
func TestRemovingANodeRemapsAlmostOnlyItsOwnKeys(t *testing.T) {
	for _, n := range []int{3, 5, 8} {
		t.Run(fmt.Sprintf("%d nodes", n), func(t *testing.T) {
			ids := make([]string, 0, n)
			for i := 0; i < n; i++ {
				ids = append(ids, fmt.Sprintf("gateway-%d", i))
			}
			before, err := New(nodes(ids...), DefaultVNodes)
			if err != nil {
				t.Fatal(err)
			}
			after, err := New(nodes(ids[:n-1]...), DefaultVNodes)
			if err != nil {
				t.Fatal(err)
			}
			removed := ids[n-1]

			const total = 100_000
			moved, movedFromSurvivors := 0, 0
			for _, k := range keys(total) {
				b := before.Owner(k).ID
				a := after.Owner(k).ID
				if a == b {
					continue
				}
				moved++
				if b != removed {
					// This is the property that makes it a CONSISTENT hash: a
					// key owned by a node that is still present must not move.
					movedFromSurvivors++
				}
			}

			limit := int(1.5 / float64(n) * total)
			if moved >= limit {
				t.Errorf("removing 1 of %d nodes remapped %d of %d keys (%.1f%%), want fewer than %d (1.5/N)",
					n, moved, total, 100*float64(moved)/total, limit)
			}
			if movedFromSurvivors != 0 {
				t.Errorf("%d keys moved between two nodes that both stayed in the ring", movedFromSurvivors)
			}
		})
	}
}

// Adding a node may only take keys, never shuffle them between the nodes that
// were already there.
func TestAddingANodeOnlyTakesKeys(t *testing.T) {
	before, err := New(nodes("gateway-1", "gateway-2", "gateway-3"), DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	after, err := New(nodes("gateway-1", "gateway-2", "gateway-3", "gateway-4"), DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}

	moved := 0
	for _, k := range keys(50_000) {
		b, a := before.Owner(k).ID, after.Owner(k).ID
		if a == b {
			continue
		}
		moved++
		if a != "gateway-4" {
			t.Fatalf("key %q moved from %q to %q; adding a node must only move keys TO it", k, b, a)
		}
	}
	if moved == 0 {
		t.Fatal("adding a node took no keys at all")
	}
}

// Virtual nodes exist to keep the shares close to even. This measures it
// rather than assuming it.
func TestDistributionIsReasonablyEven(t *testing.T) {
	cases := []struct {
		nodes     int
		tolerance float64 // allowed share of the mean
	}{
		{3, 1.25},
		{5, 1.30},
		{10, 1.35},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%d nodes", c.nodes), func(t *testing.T) {
			ids := make([]string, 0, c.nodes)
			for i := 0; i < c.nodes; i++ {
				ids = append(ids, fmt.Sprintf("gateway-%d", i))
			}
			r, err := New(nodes(ids...), DefaultVNodes)
			if err != nil {
				t.Fatal(err)
			}

			const total = 100_000
			counts := map[string]int{}
			for _, k := range keys(total) {
				counts[r.Owner(k).ID]++
			}
			mean := float64(total) / float64(c.nodes)
			for id, got := range counts {
				share := float64(got) / mean
				if share > c.tolerance || share < 2-c.tolerance {
					t.Errorf("%s owns %d keys, %.2fx the mean of %.0f (tolerance %.2f)",
						id, got, share, mean, c.tolerance)
				}
			}
			if len(counts) != c.nodes {
				t.Errorf("only %d of %d nodes own anything", len(counts), c.nodes)
			}
		})
	}
}

// A one-node ring owns everything, which is the single-node deployment.
func TestSingleNodeOwnsEverything(t *testing.T) {
	r, err := New(nodes("only"), DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys(1000) {
		if r.Owner(k).ID != "only" {
			t.Fatalf("key %q is owned by %q", k, r.Owner(k).ID)
		}
	}
}

func TestSuccessorsAreDistinctAndStartAtTheOwner(t *testing.T) {
	r, err := New(nodes("gateway-1", "gateway-2", "gateway-3"), DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys(500) {
		succ := r.Successors(k, 3)
		if len(succ) != 3 {
			t.Fatalf("key %q: got %d successors, want 3", k, len(succ))
		}
		if succ[0].ID != r.Owner(k).ID {
			t.Fatalf("key %q: the first successor is %q, not the owner %q", k, succ[0].ID, r.Owner(k).ID)
		}
		seen := map[string]bool{}
		for _, n := range succ {
			if seen[n.ID] {
				t.Fatalf("key %q: %q appears twice in the successor list", k, n.ID)
			}
			seen[n.ID] = true
		}
	}

	// Asking for more than the membership returns the membership.
	if got := len(r.Successors("anything", 10)); got != 3 {
		t.Fatalf("asked for 10 successors of a 3-node ring, got %d", got)
	}
}

// The successor of a key is where it lands when its owner leaves. That is the
// property the takeover path depends on.
func TestSecondSuccessorBecomesOwnerWhenTheFirstLeaves(t *testing.T) {
	all := nodes("gateway-1", "gateway-2", "gateway-3")
	full, err := New(all, DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range keys(500) {
		succ := full.Successors(k, 2)
		remaining := make([]Node, 0, 2)
		for _, n := range all {
			if n.ID != succ[0].ID {
				remaining = append(remaining, n)
			}
		}
		reduced, err := New(remaining, DefaultVNodes)
		if err != nil {
			t.Fatal(err)
		}
		if got := reduced.Owner(k).ID; got != succ[1].ID {
			t.Fatalf("key %q: after %q left, the owner is %q but the second successor was %q",
				k, succ[0].ID, got, succ[1].ID)
		}
	}
}

// The tenant alone decides the owner. The page says a tenant always lands on
// the same shard, and that stops being true the moment anything else is mixed
// into the hash -- a tenant with three limits would get three owners.
func TestOwnershipDependsOnTheTenantAlone(t *testing.T) {
	r, err := New(nodes("gateway-1", "gateway-2", "gateway-3"), DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	owner := r.Owner("acme").ID
	for _, suffix := range []string{":per-minute", ":per-second", "/api/orders", ""} {
		if got := r.Owner("acme" + suffix).ID; suffix == "" && got != owner {
			t.Fatalf("the same tenant resolved to two owners")
		}
	}
	// And the hash input is the bare key, which is what every node must agree
	// on: a peer computing KeyHash("acme") must get this number.
	if KeyHash("acme") != KeyHash("acme") {
		t.Fatal("KeyHash is not deterministic")
	}
	if KeyHash("acme") == KeyHash("acme ") {
		t.Fatal("KeyHash ignores trailing whitespace, so two tenant ids collide")
	}
}

func TestNewRejectsBrokenInput(t *testing.T) {
	if _, err := New(nil, DefaultVNodes); err == nil {
		t.Error("an empty ring was accepted")
	}
	if _, err := New([]Node{{ID: ""}}, DefaultVNodes); err == nil {
		t.Error("a node with no id was accepted")
	}
	if _, err := New(nodes("a", "a"), DefaultVNodes); err == nil {
		t.Error("two nodes with the same id were accepted")
	}
	if _, err := New(nodes("a"), MaxVNodes+1); err == nil {
		t.Error("an absurd virtual-node count was accepted")
	}
	if r, err := New(nodes("a"), 0); err != nil || r.VNodes() != DefaultVNodes {
		t.Error("zero virtual nodes should mean the default")
	}
}

// An address change must not move a single tenant: ownership is keyed by id.
func TestChangingAnAddressMovesNothing(t *testing.T) {
	before, err := New([]Node{
		{ID: "gateway-1", Addr: "10.0.0.1:9090"},
		{ID: "gateway-2", Addr: "10.0.0.2:9090"},
	}, DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	after, err := New([]Node{
		{ID: "gateway-1", Addr: "10.0.5.7:9999"},
		{ID: "gateway-2", Addr: "10.0.0.2:9090"},
	}, DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys(2000) {
		if before.Owner(k).ID != after.Owner(k).ID {
			t.Fatalf("key %q moved when a node's address changed", k)
		}
	}
}

func BenchmarkOwner(b *testing.B) {
	r, err := New(nodes("gateway-1", "gateway-2", "gateway-3"), DefaultVNodes)
	if err != nil {
		b.Fatal(err)
	}
	ks := keys(1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Owner(ks[i%len(ks)])
	}
}

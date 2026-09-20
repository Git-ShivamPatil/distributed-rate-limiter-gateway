package redis

import (
	"context"
	"testing"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
)

// The production Checker passes NO clock, which is what makes the script read
// Redis's own TIME.
//
// This is asserted on the arguments rather than on behaviour, and that is not
// laziness -- it is the only assertion that can fail. Two gateways on one host
// share a clock, so no behavioural test on this machine can tell "the script
// read Redis TIME" from "the script read the caller's clock, and the callers
// agreed". Changing the production constructor to send time.Now() would leave
// every other test in this package green; it fails here.
func TestTheProductionCheckerSendsNoClock(t *testing.T) {
	c := New(nil) // no client needed: nothing is executed
	req := limiter.Request{
		Tenant: "acme",
		Limits: []limiter.Limit{tokenBucket("tb", 10, time.Minute, 10)},
	}

	_, argv, err := c.callArgs(req, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(argv) <= clockArgIndex {
		t.Fatalf("the script is called with %d arguments, too few to carry a clock", len(argv))
	}
	if got := argv[clockArgIndex]; got != int64(0) {
		t.Fatalf("a production Checker sent %v as the clock override.\n"+
			"  Anything but 0 makes the script read the CALLER's clock, and replicas drift:\n"+
			"  a sliding window evaluated against two different \"now\"s admits a different\n"+
			"  number of requests depending on which replica answered.", got)
	}

	// The control: the test seam DOES send a clock, so the assertion above is
	// distinguishing something rather than reading a field that is always 0.
	fake := limiter.NewFakeClock(time.Unix(1_700_000_000, 0))
	seamed := New(nil, withClock(fake))
	_, argv, err = seamed.callArgs(req, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := argv[clockArgIndex]; got != fake.Now().UnixMicro() {
		t.Fatalf("the test seam sent %v, want the fake clock's %d -- so the check above proves nothing",
			got, fake.Now().UnixMicro())
	}
}

// The hazard the mechanism exists for, made concrete.
//
// Two callers whose clocks differ by more than a window, against ONE Redis and
// ONE tenant. If the script honours the caller's clock -- which is exactly
// what the test seam makes it do -- the one that is ahead evicts log entries
// the other still counts, and the pair admits more than the window allows.
//
// This is the CONTROL for the test above: it shows that a caller-supplied
// clock really does break the quota, so "the production Checker sends none" is
// a claim with consequences rather than a detail.
func TestTwoCallersWithDriftingClocksOverAdmit(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	tenant := tenantFor(t)

	const inWindow = 10
	const windowFor = 10 * time.Second
	limits := []limiter.Limit{window("per-10s", inWindow, windowFor)}

	base := time.Unix(1_700_000_000, 0)
	onTime := New(client, withClock(limiter.NewFakeClock(base)))
	// The skew has to EXCEED the window, not merely be large. A caller half a
	// window ahead still counts everything the other one wrote, and the pair
	// stays inside the limit -- which is why the first draft of this test
	// passed while proving nothing. Three windows ahead, its cutoff has moved
	// past the other's entries entirely and it evicts them.
	ahead := New(client, withClock(limiter.NewFakeClock(base.Add(3*windowFor))))

	admitted := 0
	for i := 0; i < inWindow*3; i++ {
		c := onTime
		if i%2 == 1 {
			c = ahead
		}
		res, err := c.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits})
		if err != nil {
			t.Fatal(err)
		}
		if res.Allowed {
			admitted++
		}
	}

	t.Logf("two callers %s apart admitted %d against a window of %d in %s",
		3*windowFor, admitted, inWindow, windowFor)
	if admitted <= inWindow {
		t.Fatalf("drifting clocks admitted %d, which is within the window of %d.\n"+
			"  The hazard did not reproduce, so the assertion that the production\n"+
			"  Checker sends no clock is not protecting anything measurable here.", admitted, inWindow)
	}
}

// And the same scenario with the production constructor -- no clock, so both
// callers get Redis's TIME -- stays inside the window.
func TestTwoProductionCallersShareOneWindow(t *testing.T) {
	client := testClient(t)
	ctx := context.Background()
	tenant := tenantFor(t)

	const inWindow = 10
	const windowFor = 10 * time.Second
	limits := []limiter.Limit{window("per-10s", inWindow, windowFor)}

	a, b := New(client), New(client)

	admitted := 0
	started := time.Now()
	for i := 0; i < inWindow*3; i++ {
		c := a
		if i%2 == 1 {
			c = b
		}
		res, err := c.Check(ctx, limiter.Request{Tenant: tenant, Limits: limits})
		if err != nil {
			t.Fatal(err)
		}
		if res.Allowed {
			admitted++
		}
	}

	// Nothing can have aged out in the time this loop takes; if it somehow
	// did, the count below would be meaningless rather than wrong.
	if elapsed := time.Since(started); elapsed >= windowFor {
		t.Skipf("INCONCLUSIVE: the loop took %s, long enough for the window to slide", elapsed)
	}
	if admitted != inWindow {
		t.Fatalf("two production callers admitted %d against a window of %d", admitted, inWindow)
	}
}

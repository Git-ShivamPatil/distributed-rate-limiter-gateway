package cluster

import (
	"fmt"
	"sync"
	"testing"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

func members(ids ...string) []ring.Node {
	out := make([]ring.Node, 0, len(ids))
	for i, id := range ids {
		out = append(out, ring.Node{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", 9090+i)})
	}
	return out
}

func TestOwnerReportsSelf(t *testing.T) {
	v, err := New("gateway-2", members("gateway-1", "gateway-2", "gateway-3"), ring.DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	if v.Self() != "gateway-2" {
		t.Fatalf("self = %q", v.Self())
	}

	mine, theirs := 0, 0
	for i := 0; i < 3000; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		owner, isSelf, ok := v.Owner(tenant)
		if !ok {
			t.Fatalf("no owner for %q", tenant)
		}
		if isSelf != (owner.ID == "gateway-2") {
			t.Fatalf("%q: owner=%q but isSelf=%v", tenant, owner.ID, isSelf)
		}
		if isSelf {
			mine++
		} else {
			theirs++
		}
	}
	if mine == 0 || theirs == 0 {
		t.Fatalf("a third of the keyspace each: %d mine, %d theirs", mine, theirs)
	}
}

// A node that is not in its own membership list would forward every request
// away, including the ones it should answer.
func TestSelfMustBeAMember(t *testing.T) {
	if _, err := New("gateway-9", members("gateway-1", "gateway-2"), ring.DefaultVNodes); err == nil {
		t.Fatal("a node outside its own ring was accepted")
	}
	if _, err := New("", members("gateway-1"), ring.DefaultVNodes); err == nil {
		t.Fatal("a node with no id was accepted")
	}
}

// Replace swaps the whole snapshot, so a reader sees the old ring or the new
// one and never a mixture.
func TestReplaceAdvancesTheEpoch(t *testing.T) {
	v, err := New("gateway-1", members("gateway-1", "gateway-2"), ring.DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	first := v.Epoch()

	epoch, err := v.Replace(members("gateway-1", "gateway-2", "gateway-3"), ring.DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}
	if epoch <= first {
		t.Fatalf("epoch did not advance: %d then %d", first, epoch)
	}
	if v.Ring().Len() != 3 {
		t.Fatalf("the ring has %d members after adding one", v.Ring().Len())
	}

	// Replacing with a membership that excludes this node is refused rather
	// than leaving the node unable to answer for anything.
	if _, err := v.Replace(members("gateway-2", "gateway-3"), ring.DefaultVNodes); err == nil {
		t.Fatal("a membership without this node was accepted")
	}
	if v.Ring().Len() != 3 {
		t.Fatal("a refused replacement changed the ring anyway")
	}
}

// Ownership is read on every request while membership changes occasionally.
// Run with -race: this is the test that would catch a torn read.
func TestConcurrentReadsDuringReplace(t *testing.T) {
	v, err := New("gateway-1", members("gateway-1", "gateway-2"), ring.DefaultVNodes)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				owner, _, ok := v.Owner(fmt.Sprintf("tenant-%d", n%1000))
				if !ok || owner.ID == "" {
					t.Errorf("read a ring with no owner")
					return
				}
			}
		}(i)
	}

	for i := 0; i < 50; i++ {
		set := members("gateway-1", "gateway-2")
		if i%2 == 0 {
			set = members("gateway-1", "gateway-2", "gateway-3")
		}
		if _, err := v.Replace(set, ring.DefaultVNodes); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestDescribeReportsShares(t *testing.T) {
	v, err := New("gateway-1", members("gateway-1", "gateway-2", "gateway-3"), 128)
	if err != nil {
		t.Fatal(err)
	}
	d := v.Describe()
	if d.Self != "gateway-1" || d.VNodes != 128 || len(d.Members) != 3 {
		t.Fatalf("describe = %+v", d)
	}
	selves := 0
	for _, m := range d.Members {
		if m.Share != 128 {
			t.Errorf("%s holds %d ring points, want 128", m.ID, m.Share)
		}
		if m.Self {
			selves++
		}
		if m.Addr == "" {
			t.Errorf("%s has no address, so nothing could forward to it", m.ID)
		}
	}
	if selves != 1 {
		t.Fatalf("%d members are marked as self", selves)
	}
}

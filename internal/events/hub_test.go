package events

import (
	"context"
	"sync"
	"testing"
	"time"
)

func decision(tenant string, allowed bool) Decision {
	return Decision{Tenant: tenant, Allowed: allowed, Node: "test", Cost: 1, At: time.Unix(1_700_000_000, 0)}
}

func TestHubFansOutToEverySubscriber(t *testing.T) {
	h := NewHub(8)
	a, stopA := h.Subscribe(context.Background(), Filter{})
	b, stopB := h.Subscribe(context.Background(), Filter{})
	defer stopA()
	defer stopB()

	h.Publish(decision("acme", true))

	for i, ch := range []<-chan Decision{a, b} {
		select {
		case d := <-ch:
			if d.Tenant != "acme" {
				t.Fatalf("subscriber %d got %q", i, d.Tenant)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d received nothing", i)
		}
	}
}

func TestHubFilters(t *testing.T) {
	h := NewHub(8)
	only, stop := h.Subscribe(context.Background(), Filter{Tenants: []string{"acme"}, DeniedOnly: true})
	defer stop()

	h.Publish(decision("globex", false)) // wrong tenant
	h.Publish(decision("acme", true))    // allowed, and the filter wants denials
	h.Publish(decision("acme", false))   // this one

	select {
	case d := <-only:
		if d.Tenant != "acme" || d.Allowed {
			t.Fatalf("got %+v, want acme's refusal", d)
		}
	case <-time.After(time.Second):
		t.Fatal("the matching decision never arrived")
	}

	select {
	case d := <-only:
		t.Fatalf("an unmatched decision was delivered: %+v", d)
	default:
	}
}

// A subscriber that stops reading must not be able to slow the request path
// down. It misses decisions instead, and the miss is counted.
func TestHubDropsForSlowSubscribersRatherThanBlocking(t *testing.T) {
	h := NewHub(4)
	_, stop := h.Subscribe(context.Background(), Filter{}) // never read
	defer stop()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			h.Publish(decision("acme", true))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on a subscriber that was not reading")
	}

	sent, dropped, subs := h.Stats()
	if subs != 1 {
		t.Fatalf("subscribers = %d", subs)
	}
	if sent != 4 {
		t.Errorf("sent = %d, want the 4 that fit in the buffer", sent)
	}
	if dropped != 996 {
		t.Errorf("dropped = %d, want 996 -- a drop nobody counts is a drop nobody knows about", dropped)
	}
}

func TestHubUnsubscribeClosesTheChannel(t *testing.T) {
	h := NewHub(4)
	ch, stop := h.Subscribe(context.Background(), Filter{})
	stop()

	select {
	case _, open := <-ch:
		if open {
			t.Fatal("the channel delivered after unsubscribing")
		}
	case <-time.After(time.Second):
		t.Fatal("the channel was not closed")
	}

	// Publishing afterwards must not panic on the closed channel.
	h.Publish(decision("acme", true))
	if _, _, subs := h.Stats(); subs != 0 {
		t.Fatalf("subscribers = %d after unsubscribing", subs)
	}

	stop() // idempotent
}

func TestHubCancellingTheContextUnsubscribes(t *testing.T) {
	h := NewHub(4)
	ctx, cancel := context.WithCancel(context.Background())
	ch, stop := h.Subscribe(ctx, Filter{})
	defer stop()

	cancel()
	select {
	case _, open := <-ch:
		if open {
			t.Fatal("delivered after the context was cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelling the context did not unsubscribe")
	}
}

// A nil hub is usable, so the request path can carry an optional one without
// a branch at every call site.
func TestNilHubIsSafe(t *testing.T) {
	var h *Hub
	h.Publish(decision("acme", true))
	if sent, dropped, subs := h.Stats(); sent != 0 || dropped != 0 || subs != 0 {
		t.Fatal("a nil hub reported activity")
	}
}

func TestHubConcurrentPublishAndSubscribe(t *testing.T) {
	h := NewHub(64)
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ch, stop := h.Subscribe(ctx, Filter{})
			defer stop()
			for j := 0; j < 20; j++ {
				select {
				case <-ch:
				case <-time.After(50 * time.Millisecond):
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				h.Publish(decision("acme", j%3 == 0))
			}
		}()
	}
	wg.Wait()
}

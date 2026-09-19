// Package events publishes decisions to whoever is watching.
//
// It exists so that a dashboard, a debugging session or an audit stream can
// see what the gateway is deciding without any of them being able to slow it
// down. Everything here is bounded and lossy by design: a subscriber that
// cannot keep up is skipped, and the skip is counted, because the alternative
// -- blocking the request path on a slow websocket -- turns an observability
// feature into an outage.
package events

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Decision is one admission answer, flattened for publishing.
type Decision struct {
	Tenant   string    `json:"tenant"`
	Policy   string    `json:"policy"`
	Allowed  bool      `json:"allowed"`
	Limiting string    `json:"limiting,omitempty"`
	Node     string    `json:"node"`
	Cost     int64     `json:"cost"`
	At       time.Time `json:"at"`
}

// Filter narrows what a subscriber receives.
type Filter struct {
	// Tenants, when non-empty, restricts the stream to these tenants.
	Tenants []string
	// DeniedOnly restricts it to refusals.
	DeniedOnly bool
}

func (f Filter) matches(d Decision) bool {
	if f.DeniedOnly && d.Allowed {
		return false
	}
	if len(f.Tenants) == 0 {
		return true
	}
	for _, t := range f.Tenants {
		if t == d.Tenant {
			return true
		}
	}
	return false
}

// DefaultBuffer is how many decisions a subscriber may fall behind by before
// it starts missing them.
const DefaultBuffer = 256

// Hub fans decisions out to subscribers.
//
// The zero value is not usable; call NewHub. A nil *Hub is safe to Publish to,
// so the request path can carry an optional hub without a branch at every
// call site.
type Hub struct {
	mu      sync.RWMutex
	next    uint64
	subs    map[uint64]*subscription
	buffer  int
	dropped atomic.Int64
	sent    atomic.Int64
}

type subscription struct {
	ch     chan Decision
	filter Filter
}

// NewHub builds a hub whose subscribers buffer up to buffer decisions.
func NewHub(buffer int) *Hub {
	if buffer <= 0 {
		buffer = DefaultBuffer
	}
	return &Hub{subs: make(map[uint64]*subscription), buffer: buffer}
}

// Publish sends a decision to every matching subscriber.
//
// It never blocks. A full subscriber channel means that subscriber is slower
// than the traffic; the decision is dropped for it alone and counted.
func (h *Hub) Publish(d Decision) {
	if h == nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.subs) == 0 {
		return
	}
	for _, sub := range h.subs {
		if !sub.filter.matches(d) {
			continue
		}
		select {
		case sub.ch <- d:
			h.sent.Add(1)
		default:
			h.dropped.Add(1)
		}
	}
}

// Subscribe returns a channel of decisions and a function that stops it.
//
// The channel is closed by the returned cancel function or when ctx is done;
// a subscriber must call one of the two or the hub keeps publishing into a
// channel nobody reads.
func (h *Hub) Subscribe(ctx context.Context, f Filter) (<-chan Decision, func()) {
	h.mu.Lock()
	id := h.next
	h.next++
	sub := &subscription{ch: make(chan Decision, h.buffer), filter: f}
	h.subs[id] = sub
	h.mu.Unlock()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs, id)
			h.mu.Unlock()
			close(sub.ch)
		})
	}

	if ctx != nil {
		go func() {
			<-ctx.Done()
			stop()
		}()
	}
	return sub.ch, stop
}

// Stats reports what the hub has done, for the metrics endpoint and for
// telling a user of the stream that they missed something.
func (h *Hub) Stats() (sent, dropped int64, subscribers int) {
	if h == nil {
		return 0, 0, 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.sent.Load(), h.dropped.Load(), len(h.subs)
}

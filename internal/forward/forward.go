// Package forward asks another node to decide.
//
// A tenant's checks are coordinated by one node, so a request that arrives
// anywhere else is forwarded there. What matters about this package is what it
// does when forwarding FAILS: it says so in a way the caller can distinguish
// from an answer, because a peer that is unreachable must not look like a
// tenant that does not exist.
//
// The fallback is safe. Counters live in Redis and every admission is backed
// by an atomic script there, so a node that decides locally after a failed
// forward reaches the same answer the owner would have. Losing an owner costs
// a hop, not a quota.
package forward

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ratelimitv1 "github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/api/gen/ratelimit/v1"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// HopHeader marks a request that has already been forwarded once.
//
// It is what stops two nodes with different views of the ring from bouncing a
// request between them forever: a node that receives a hop decides locally
// whatever it believes about ownership.
const HopHeader = "x-ratelimit-hop"

// ErrPeerUnavailable is the signal to decide locally instead. It is defined by
// the decide package, which owns the Forwarder contract; this alias exists so
// callers of this package can name it without importing both.
var ErrPeerUnavailable = decide.ErrPeerUnavailable

// DefaultPoolSize is how many connections are opened per peer.
//
// One gRPC ClientConn multiplexes every call over a single HTTP/2 connection,
// which is efficient until it is not: concurrent streams share one TCP
// connection's window and one receive loop. A small pool spreads a busy
// gateway's forwards across several, which costs a few sockets and removes a
// head-of-line bottleneck that only appears under load.
const DefaultPoolSize = 4

// Client forwards checks to peers.
type Client struct {
	poolSize int
	timeout  time.Duration
	dialOpts []grpc.DialOption

	mu    sync.RWMutex
	peers map[string]*peer

	forwarded atomic.Int64
	failed    atomic.Int64
}

type peer struct {
	conns []*grpc.ClientConn
	next  atomic.Uint64
}

// Option configures a Client.
type Option func(*Client)

// WithPoolSize sets how many connections are kept per peer.
func WithPoolSize(n int) Option {
	return func(c *Client) {
		if n > 0 {
			c.poolSize = n
		}
	}
}

// WithTimeout bounds a single forwarded call.
//
// It is a ceiling, not a replacement for the caller's deadline: whichever is
// sooner wins. A forward that has not answered in this long has already cost
// more than deciding locally would have.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithDialOptions replaces the transport options. Tests use it to dial an
// in-memory listener.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(c *Client) { c.dialOpts = opts }
}

// New builds a forwarding client.
func New(opts ...Option) *Client {
	c := &Client{
		poolSize: DefaultPoolSize,
		timeout:  250 * time.Millisecond,
		peers:    make(map[string]*peer),
		dialOpts: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Check asks the node at addr to decide.
func (c *Client) Check(ctx context.Context, addr string, q decide.Query) (decide.Outcome, error) {
	conn, err := c.conn(addr)
	if err != nil {
		c.failed.Add(1)
		return decide.Outcome{}, fmt.Errorf("%w: dialing %s: %v", ErrPeerUnavailable, addr, err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, HopHeader, "1")

	client := ratelimitv1.NewLimiterServiceClient(conn)
	req := &ratelimitv1.CheckRequest{
		TenantId: q.Tenant,
		Cost:     q.Cost,
		Method:   q.Method,
		Path:     q.Path,
	}

	var (
		quota  *ratelimitv1.Quota
		rpcErr error
	)
	if q.Peek {
		var resp *ratelimitv1.GetQuotaResponse
		resp, rpcErr = client.GetQuota(ctx, &ratelimitv1.GetQuotaRequest{
			TenantId: q.Tenant, Method: q.Method, Path: q.Path,
		})
		if resp != nil {
			quota = resp.GetQuota()
		}
	} else {
		var resp *ratelimitv1.CheckResponse
		resp, rpcErr = client.Check(ctx, req)
		if resp != nil {
			quota = resp.GetQuota()
		}
	}
	if rpcErr != nil {
		err := translate(rpcErr, q.Tenant)
		if errors.Is(err, ErrPeerUnavailable) {
			c.failed.Add(1)
		}
		return decide.Outcome{}, err
	}

	c.forwarded.Add(1)
	return outcomeOf(quota), nil
}

// translate turns a peer's status code back into the error the local surfaces
// already know how to render.
//
// The distinction that matters is between an ANSWER about the tenant, which
// must be passed through unchanged, and a FAILURE to get one, which means
// deciding locally instead. Getting this backwards would turn an unreachable
// peer into "no such tenant" -- a 404 for a tenant that exists.
func translate(err error, tenant string) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("%w: %v", ErrPeerUnavailable, err)
	}
	switch st.Code() {
	case codes.NotFound:
		return fmt.Errorf("%w: %q", policy.ErrTenantNotFound, tenant)
	case codes.PermissionDenied:
		return fmt.Errorf("%w: %q", policy.ErrTenantDisabled, tenant)
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", limiter.ErrCostExceedsCapacity, st.Message())
	case codes.Unauthenticated:
		// The peer refused OUR credentials, which is a misconfiguration
		// between nodes rather than anything the caller did.
		return fmt.Errorf("%w: the owning node rejected this node's credentials: %s", ErrPeerUnavailable, st.Message())
	default:
		// Unavailable, DeadlineExceeded, Internal, anything else: no answer.
		// Note that a peer whose Redis is down answers Unavailable too, and
		// conflating the two is harmless here -- every node shares one Redis,
		// so deciding locally reaches the same conclusion.
		return fmt.Errorf("%w: %s: %s", ErrPeerUnavailable, st.Code(), st.Message())
	}
}

func outcomeOf(q *ratelimitv1.Quota) decide.Outcome {
	out := decide.Outcome{
		Policy: policy.Policy{Name: q.GetPolicy()},
		Result: limiter.Result{
			Allowed:  q.GetAllowed(),
			Limiting: q.GetLimiting(),
		},
	}
	for _, l := range q.GetLimits() {
		out.Result.Decisions = append(out.Result.Decisions, limiter.Decision{
			Name:       l.GetName(),
			Allowed:    l.GetAllowed(),
			Limit:      l.GetLimit(),
			Remaining:  l.GetRemaining(),
			RetryAfter: l.GetRetryAfter().AsDuration(),
			ResetAfter: l.GetResetAfter().AsDuration(),
		})
	}
	if d := q.GetDegraded(); d != nil {
		out.Degraded = &decide.Degraded{Reason: d.GetReason(), Mode: d.GetMode()}
	}
	return out
}

// conn returns a connection to a peer, opening the pool on first use.
func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	p, ok := c.peers[addr]
	c.mu.RUnlock()
	if !ok {
		var err error
		if p, err = c.dial(addr); err != nil {
			return nil, err
		}
	}
	// Round-robin across the pool. gRPC reconnects a broken connection on its
	// own, so a peer restarting does not need anything from here.
	i := p.next.Add(1)
	return p.conns[int(i)%len(p.conns)], nil
}

func (c *Client) dial(addr string) (*peer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.peers[addr]; ok {
		return p, nil // another goroutine won the race
	}

	p := &peer{conns: make([]*grpc.ClientConn, 0, c.poolSize)}
	for i := 0; i < c.poolSize; i++ {
		conn, err := grpc.NewClient(addr, c.dialOpts...)
		if err != nil {
			for _, open := range p.conns {
				_ = open.Close()
			}
			return nil, err
		}
		p.conns = append(p.conns, conn)
	}
	c.peers[addr] = p
	return p, nil
}

// Stats reports how much forwarding has happened, for metrics and tests.
func (c *Client) Stats() (forwarded, failed int64) {
	return c.forwarded.Load(), c.failed.Load()
}

// Close releases every peer connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for _, p := range c.peers {
		for _, conn := range p.conns {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	c.peers = make(map[string]*peer)
	return errors.Join(errs...)
}

package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ratelimitv1 "github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/api/gen/ratelimit/v1"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/events"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// staticSource answers for a fixed set of tenants.
type staticSource struct {
	policies map[string]policy.Policy
	disabled map[string]bool
	err      error
}

func (s *staticSource) Lookup(_ context.Context, tenant string) (policy.Policy, error) {
	if s.err != nil {
		return policy.Policy{}, s.err
	}
	if s.disabled[tenant] {
		return policy.Policy{}, fmt.Errorf("%w: %q", policy.ErrTenantDisabled, tenant)
	}
	p, ok := s.policies[tenant]
	if !ok {
		return policy.Policy{}, fmt.Errorf("%w: %q", policy.ErrTenantNotFound, tenant)
	}
	return p, nil
}

func bucketPolicy(count int64, period time.Duration) policy.Policy {
	return policy.Policy{
		Name: "test",
		Limits: []limiter.Limit{{
			Name: "per-period", Algorithm: limiter.AlgorithmTokenBucket,
			Count: count, Period: period, Burst: count,
		}},
	}
}

type fakeKeys map[string]string

func (f fakeKeys) TenantForKey(_ context.Context, presented string) (string, error) {
	if tenant, ok := f[presented]; ok {
		return tenant, nil
	}
	return "", errors.New("unknown key")
}

// dial starts the service over an in-memory connection, which keeps the test
// free of ports and of the flakiness that comes with them.
func dial(t *testing.T, src policy.Source, hub *events.Hub, opts ...Option) ratelimitv1.LimiterServiceClient {
	t.Helper()

	mem := limiter.NewMemory(limiter.WithClock(limiter.NewFakeClock(time.Unix(1_700_000_000, 0))))
	decider := decide.New(mem, src, "grpc-test", decide.WithHub(hub))

	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	New(decider, nil, opts...).Register(server)
	go func() { _ = server.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = lis.Close()
	})
	return ratelimitv1.NewLimiterServiceClient(conn)
}

func TestCheckReturnsADecision(t *testing.T) {
	client := dial(t, &staticSource{policies: map[string]policy.Policy{"acme": bucketPolicy(20, time.Minute)}},
		nil, WithTrustedTenantField(true))
	ctx := context.Background()

	resp, err := client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	q := resp.GetQuota()
	if !q.GetAllowed() {
		t.Fatal("the first request was refused")
	}
	if q.GetTenantId() != "acme" || q.GetNode() != "grpc-test" {
		t.Errorf("quota = %+v, want tenant acme from node grpc-test", q)
	}
	if len(q.GetLimits()) != 1 {
		t.Fatalf("got %d limits, want 1", len(q.GetLimits()))
	}
	if got := q.GetLimits()[0].GetRemaining(); got != 19 {
		t.Errorf("remaining = %d, want 19", got)
	}

	// Spend the rest and check the refusal's shape.
	for i := 0; i < 19; i++ {
		if _, err := client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: "acme"}); err != nil {
			t.Fatal(err)
		}
	}
	resp, err = client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: "acme"})
	if err != nil {
		t.Fatalf("a refusal must be a normal response, not an error: %v", err)
	}
	q = resp.GetQuota()
	if q.GetAllowed() {
		t.Fatal("the 21st request was admitted")
	}
	if q.GetLimiting() != "per-period" {
		t.Errorf("limiting = %q", q.GetLimiting())
	}
	if q.GetRetryAfter().AsDuration() != 3*time.Second {
		t.Errorf("retry after = %s, want 3s", q.GetRetryAfter().AsDuration())
	}
}

// GetQuota reports the same numbers without spending any of them.
func TestGetQuotaConsumesNothing(t *testing.T) {
	client := dial(t, &staticSource{policies: map[string]policy.Policy{"acme": bucketPolicy(5, time.Minute)}},
		nil, WithTrustedTenantField(true))
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		resp, err := client.GetQuota(ctx, &ratelimitv1.GetQuotaRequest{TenantId: "acme"})
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.GetQuota().GetLimits()[0].GetRemaining(); got != 5 {
			t.Fatalf("peek %d: remaining = %d, want 5 -- GetQuota is consuming", i, got)
		}
	}

	if _, err := client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: "acme"}); err != nil {
		t.Fatal(err)
	}
	resp, _ := client.GetQuota(ctx, &ratelimitv1.GetQuotaRequest{TenantId: "acme"})
	if got := resp.GetQuota().GetLimits()[0].GetRemaining(); got != 4 {
		t.Fatalf("after one Check: remaining = %d, want 4", got)
	}
}

// The codes the proto documents are the codes it returns: a caller that
// switches on them must not have to parse messages.
func TestErrorsUseTheDocumentedCodes(t *testing.T) {
	src := &staticSource{
		policies: map[string]policy.Policy{"acme": bucketPolicy(20, time.Minute), "off": bucketPolicy(1, time.Minute)},
		disabled: map[string]bool{"off": true},
	}
	client := dial(t, src, nil, WithTrustedTenantField(true))
	ctx := context.Background()

	cases := []struct {
		name string
		req  *ratelimitv1.CheckRequest
		want codes.Code
	}{
		{"unknown tenant", &ratelimitv1.CheckRequest{TenantId: "ghost"}, codes.NotFound},
		{"disabled tenant", &ratelimitv1.CheckRequest{TenantId: "off"}, codes.PermissionDenied},
		{"no tenant", &ratelimitv1.CheckRequest{}, codes.InvalidArgument},
		{"impossible cost", &ratelimitv1.CheckRequest{TenantId: "acme", Cost: 1000}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Check(ctx, tc.req)
			if status.Code(err) != tc.want {
				t.Fatalf("code = %s, want %s (err: %v)", status.Code(err), tc.want, err)
			}
		})
	}
}

// A counter store that cannot decide is UNAVAILABLE, not RESOURCE_EXHAUSTED:
// the caller is within its quota as far as anybody knows.
func TestStoreFailureIsUnavailableNotExhausted(t *testing.T) {
	src := &staticSource{policies: map[string]policy.Policy{"acme": bucketPolicy(20, time.Minute)}}
	mem := failingChecker{errors.New("connection refused")}
	decider := decide.New(mem, src, "grpc-test")

	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	New(decider, nil, WithTrustedTenantField(true)).Register(server)
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	_, err = ratelimitv1.NewLimiterServiceClient(conn).Check(context.Background(),
		&ratelimitv1.CheckRequest{TenantId: "acme"})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("code = %s, want Unavailable", got)
	}
	if status.Code(err) == codes.ResourceExhausted {
		t.Fatal("a store outage was reported as the tenant being out of quota")
	}
}

type failingChecker struct{ err error }

func (f failingChecker) Check(context.Context, limiter.Request) (limiter.Result, error) {
	return limiter.Result{}, f.err
}

// Credentials in metadata win over the tenant_id field, and a bad one is
// refused rather than falling back to the field.
func TestMetadataCredentials(t *testing.T) {
	src := &staticSource{policies: map[string]policy.Policy{
		"acme":   bucketPolicy(20, time.Minute),
		"globex": bucketPolicy(20, time.Minute),
	}}
	client := dial(t, src, nil,
		WithTrustedTenantField(true),
		WithKeyResolver(fakeKeys{"rlk_acme": "acme"}))

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "rlk_acme")
	resp, err := client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: "globex"})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetQuota().GetTenantId(); got != "acme" {
		t.Fatalf("tenant = %q, want acme: the key must win over the field", got)
	}

	bad := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "rlk_wrong")
	_, err = client.Check(bad, &ratelimitv1.CheckRequest{TenantId: "globex"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated; a bad key must not fall back to the field", status.Code(err))
	}
}

// With the field untrusted, an unauthenticated caller cannot name a tenant.
func TestUntrustedTenantFieldRequiresCredentials(t *testing.T) {
	src := &staticSource{policies: map[string]policy.Policy{"acme": bucketPolicy(20, time.Minute)}}
	client := dial(t, src, nil, WithTrustedTenantField(false), WithKeyResolver(fakeKeys{"rlk_acme": "acme"}))

	_, err := client.Check(context.Background(), &ratelimitv1.CheckRequest{TenantId: "acme"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated", status.Code(err))
	}

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-api-key", "rlk_acme")
	if _, err := client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: ""}); err != nil {
		t.Fatalf("a properly authenticated call was refused: %v", err)
	}
}

func TestStreamDecisions(t *testing.T) {
	hub := events.NewHub(64)
	src := &staticSource{policies: map[string]policy.Policy{
		"acme":   bucketPolicy(2, time.Hour),
		"globex": bucketPolicy(50, time.Hour),
	}}
	client := dial(t, src, hub, WithTrustedTenantField(true), WithHub(hub))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.StreamDecisions(ctx, &ratelimitv1.StreamDecisionsRequest{TenantIds: []string{"acme"}})
	if err != nil {
		t.Fatal(err)
	}
	// Give the subscription time to register before generating traffic.
	time.Sleep(150 * time.Millisecond)

	// Two allowed, then a refusal; plus traffic for a tenant not subscribed to.
	for i := 0; i < 3; i++ {
		if _, err := client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: "acme"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Check(ctx, &ratelimitv1.CheckRequest{TenantId: "globex"}); err != nil {
		t.Fatal(err)
	}

	var got []*ratelimitv1.Decision
	for len(got) < 3 {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("after %d decisions: %v", len(got), err)
		}
		got = append(got, msg.GetDecision())
	}

	for i, d := range got {
		if d.GetTenantId() != "acme" {
			t.Fatalf("decision %d is for %q; the filter did not hold", i, d.GetTenantId())
		}
	}
	if !got[0].GetAllowed() || !got[1].GetAllowed() {
		t.Error("the first two decisions should be admissions")
	}
	if got[2].GetAllowed() {
		t.Error("the third decision should be a refusal")
	}
	if got[2].GetLimiting() != "per-period" {
		t.Errorf("the refusal does not name the limit: %q", got[2].GetLimiting())
	}
	if got[0].GetNode() != "grpc-test" {
		t.Errorf("node = %q", got[0].GetNode())
	}
}

// Without a hub the stream is Unimplemented rather than an empty stream that
// never produces anything.
func TestStreamWithoutAHubIsUnimplemented(t *testing.T) {
	client := dial(t, &staticSource{policies: map[string]policy.Policy{}}, nil, WithTrustedTenantField(true))
	stream, err := client.StreamDecisions(context.Background(), &ratelimitv1.StreamDecisionsRequest{})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %s, want Unimplemented", status.Code(err))
	}
}

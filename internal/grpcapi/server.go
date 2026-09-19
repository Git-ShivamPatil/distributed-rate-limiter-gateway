// Package grpcapi serves the Limiter service.
//
// It holds no limiting logic: every question goes to internal/decide, which is
// the same service the REST endpoint calls. What lives here is the translation
// -- metadata to credentials, Go errors to status codes, decisions to a
// stream -- and nothing else.
package grpcapi

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ratelimitv1 "github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/api/gen/ratelimit/v1"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/events"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
)

// Server implements ratelimit.v1.LimiterService.
type Server struct {
	ratelimitv1.UnimplementedLimiterServiceServer

	decider     *decide.Service
	hub         *events.Hub
	keys        auth.KeyResolver
	trustTenant bool
	log         *slog.Logger
}

// Option configures the Server.
type Option func(*Server)

// WithKeyResolver enables API-key authentication from request metadata.
func WithKeyResolver(k auth.KeyResolver) Option { return func(s *Server) { s.keys = k } }

// WithTrustedTenantField accepts the tenant named in the request when it
// carries no credentials -- the same switch the REST endpoint has, for the
// same reason: it is right for a limiter called by trusted infrastructure and
// wrong for anything else.
func WithTrustedTenantField(trust bool) Option { return func(s *Server) { s.trustTenant = trust } }

// WithHub supplies the decision stream's source.
func WithHub(h *events.Hub) Option { return func(s *Server) { s.hub = h } }

// New builds the service.
func New(d *decide.Service, log *slog.Logger, opts ...Option) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{decider: d, log: log}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Register attaches the service to a gRPC server.
func (s *Server) Register(g *grpc.Server) {
	ratelimitv1.RegisterLimiterServiceServer(g, s)
}

// Check answers an admission question.
func (s *Server) Check(ctx context.Context, req *ratelimitv1.CheckRequest) (*ratelimitv1.CheckResponse, error) {
	tenant, err := s.tenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	out, err := s.decider.Decide(ctx, decide.Query{
		Tenant: tenant,
		Method: req.GetMethod(),
		Path:   req.GetPath(),
		Cost:   req.GetCost(),
		// A call that another node forwarded here is decided here, whatever
		// this node believes about ownership. Without that, two nodes with
		// momentarily different views of the ring would bounce it between
		// them until a deadline expired.
		Forwarded: isForwardedHop(ctx),
	})
	if err != nil {
		return nil, s.statusFor(err, tenant)
	}
	return &ratelimitv1.CheckResponse{Quota: quotaOf(tenant, s.decider.Node(), out)}, nil
}

// GetQuota reports the state without consuming any of it.
func (s *Server) GetQuota(ctx context.Context, req *ratelimitv1.GetQuotaRequest) (*ratelimitv1.GetQuotaResponse, error) {
	tenant, err := s.tenant(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	out, err := s.decider.Decide(ctx, decide.Query{
		Tenant:    tenant,
		Method:    req.GetMethod(),
		Path:      req.GetPath(),
		Peek:      true,
		Forwarded: isForwardedHop(ctx),
	})
	if err != nil {
		return nil, s.statusFor(err, tenant)
	}
	return &ratelimitv1.GetQuotaResponse{Quota: quotaOf(tenant, s.decider.Node(), out)}, nil
}

// StreamDecisions sends decisions until the caller goes away.
func (s *Server) StreamDecisions(req *ratelimitv1.StreamDecisionsRequest, stream grpc.ServerStreamingServer[ratelimitv1.StreamDecisionsResponse]) error {
	if s.hub == nil {
		return status.Error(codes.Unimplemented, "this node publishes no decision stream")
	}
	ctx := stream.Context()
	ch, stop := s.hub.Subscribe(ctx, events.Filter{
		Tenants:    req.GetTenantIds(),
		DeniedOnly: req.GetDeniedOnly(),
	})
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&ratelimitv1.StreamDecisionsResponse{
				Decision: &ratelimitv1.Decision{
					TenantId: d.Tenant,
					Policy:   d.Policy,
					Allowed:  d.Allowed,
					Limiting: d.Limiting,
					Node:     d.Node,
					Cost:     d.Cost,
					At:       timestamppb.New(d.At),
				},
			}); err != nil {
				return err
			}
		}
	}
}

// tenant decides who a call speaks for.
//
// Credentials in metadata win over the field, and a credential that is present
// and wrong is refused rather than falling back -- otherwise a bad key would
// be a way of becoming somebody else.
func (s *Server) tenant(ctx context.Context, field string) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)

	if s.keys != nil {
		if key := firstValue(md, "x-api-key", "api-key"); key != "" {
			tenant, err := s.keys.TenantForKey(ctx, key)
			if err != nil {
				return "", status.Error(codes.Unauthenticated, "the api key presented is not valid")
			}
			return tenant, nil
		}
	}
	if field != "" && s.trustTenant {
		return field, nil
	}
	if field != "" {
		return "", status.Error(codes.Unauthenticated,
			"this node does not accept an unauthenticated tenant_id; present an api key")
	}
	return "", status.Error(codes.InvalidArgument, "tenant_id is required")
}

// isForwardedHop reports whether another node sent this call.
//
// The header name is defined by the forwarding package, but reading it here
// rather than importing that package keeps the dependency pointing one way:
// forward imports decide, and the server imports neither's internals.
func isForwardedHop(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	return firstValue(md, "x-ratelimit-hop") != ""
}

func firstValue(md metadata.MD, keys ...string) string {
	for _, k := range keys {
		if v := md.Get(k); len(v) > 0 && v[0] != "" {
			return v[0]
		}
	}
	return ""
}

// statusFor maps a decision error onto the code the proto documents.
func (s *Server) statusFor(err error, tenant string) error {
	switch {
	case errors.Is(err, policy.ErrTenantNotFound):
		return status.Errorf(codes.NotFound, "no policy for tenant %q", tenant)
	case errors.Is(err, policy.ErrTenantDisabled):
		return status.Errorf(codes.PermissionDenied, "tenant %q is disabled", tenant)
	case errors.Is(err, limiter.ErrCostExceedsCapacity):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, decide.ErrNoTenant):
		return status.Error(codes.InvalidArgument, "tenant_id is required")
	case errors.Is(err, decide.ErrStoreUnavailable):
		// Not RESOURCE_EXHAUSTED: the caller is within its quota as far as
		// anybody knows. Saying otherwise would send them away to wait for a
		// reset that has nothing to do with why they were refused.
		s.log.Error("counter store unavailable", "tenant", tenant, "err", err)
		return status.Error(codes.Unavailable, "the counter store is unavailable and this policy fails closed")
	default:
		s.log.Error("check failed", "tenant", tenant, "err", err)
		return status.Error(codes.Internal, "the check could not be completed")
	}
}

func quotaOf(tenant, node string, out decide.Outcome) *ratelimitv1.Quota {
	// The node that actually decided, which is not this one when the answer
	// came back from an owner. "Which node decided this" is the first question
	// when two replicas seem to disagree.
	if out.Forwarded && out.Owner != "" {
		node = out.Owner
	}
	q := &ratelimitv1.Quota{
		Allowed:    out.Result.Allowed,
		TenantId:   tenant,
		Policy:     out.Policy.Name,
		Node:       node,
		Limiting:   out.Result.Limiting,
		RetryAfter: durationpb.New(out.Result.RetryAfter()),
	}
	for _, d := range out.Result.Decisions {
		q.Limits = append(q.Limits, &ratelimitv1.LimitState{
			Name:       d.Name,
			Limit:      d.Limit,
			Remaining:  d.Remaining,
			RetryAfter: durationpb.New(d.RetryAfter),
			ResetAfter: durationpb.New(d.ResetAfter),
			Allowed:    d.Allowed,
		})
	}
	if out.Degraded != nil {
		q.Degraded = &ratelimitv1.Degraded{Reason: out.Degraded.Reason, Mode: out.Degraded.Mode}
	}
	return q
}

// Command gateway is one node of the rate-limiting API gateway.
//
//	gateway --node gateway-1 --config ./configs/local.yaml
//
// Both `--flag value` and `--flag=value` work, because everything declarative
// -- docker compose, a Kubernetes manifest, a systemd unit -- writes the second
// form, and a binary that only accepts the first one works in every test and
// fails in every deployment.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/auth"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/cluster"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/events"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/forward"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/gateway"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/grpcapi"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	redislimiter "github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter/redis"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/ring"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	configPath string
	node       string
	httpAddr   string
	grpcAddr   string
	logLevel   string
	logFormat  string
}

func parseFlags(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.StringVar(&o.configPath, "config", "", "path to the YAML configuration file")
	fs.StringVar(&o.node, "node", "", "node id; overrides node.id from the config")
	fs.StringVar(&o.httpAddr, "http-addr", "", "HTTP listen address; overrides node.http_addr")
	fs.StringVar(&o.grpcAddr, "grpc-addr", "", "gRPC listen address; overrides node.grpc_addr")
	fs.StringVar(&o.logLevel, "log-level", "info", "debug, info, warn or error")
	fs.StringVar(&o.logFormat, "log-format", "text", "text or json")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: gateway [flags]\n\nflags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return o, nil
}

func run(args []string) error {
	opts, err := parseFlags(args)
	if err != nil {
		return err
	}

	log, err := newLogger(opts.logLevel, opts.logFormat)
	if err != nil {
		return err
	}

	cfg := config.Defaults()
	if opts.configPath != "" {
		cfg, err = config.Load(opts.configPath)
		if err != nil {
			return err
		}
	} else {
		log.Warn("no --config given; running on defaults with no policies, so every tenant is unknown")
	}
	if opts.node != "" {
		cfg.Node.ID = opts.node
	}
	if opts.httpAddr != "" {
		cfg.Node.HTTPAddr = opts.httpAddr
	}
	if opts.grpcAddr != "" {
		cfg.Node.GRPCAddr = opts.grpcAddr
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	policies, adminStore, cache, policyReady, err := buildPolicySource(ctx, cfg, log)
	if err != nil {
		return err
	}

	var (
		checker limiter.Checker
		mem     *limiter.Memory
		ready   func(context.Context) error
	)
	switch cfg.Limiter.Backend {
	case "memory":
		mem = limiter.NewMemory(limiter.WithMaxKeys(cfg.Limiter.MaxKeys))
		checker = mem
		ready = func(context.Context) error { return nil }
		log.Warn("limiter backend is memory: counters are per-process, so N replicas admit N times the quota",
			"node", cfg.Node.ID)
	case "redis":
		client := goredis.NewClient(&goredis.Options{
			Addr:         cfg.Redis.Addr,
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			PoolSize:     cfg.Redis.PoolSize,
			DialTimeout:  cfg.Redis.Timeout,
			ReadTimeout:  cfg.Redis.Timeout,
			WriteTimeout: cfg.Redis.Timeout,
		})
		defer func() { _ = client.Close() }()

		rc := redislimiter.New(client)
		// Fail at startup rather than on the first request: a gateway that
		// starts happily and then refuses everything because Redis was never
		// reachable is harder to diagnose than one that will not start.
		startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := rc.Load(startCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("%w (is the data plane up? `docker compose up -d redis postgres`)", err)
		}
		checker = rc
		ready = rc.Ping
		log.Info("limiter backend is redis: replicas sharing this Redis enforce one quota",
			"addr", cfg.Redis.Addr, "pool_size", cfg.Redis.PoolSize)
	}

	authn, adminToken := buildAuth(cfg, adminStore, log)

	// Readiness means "can this node still enforce", so it covers both stores.
	limiterReady := ready
	ready = func(ctx context.Context) error {
		if err := limiterReady(ctx); err != nil {
			return err
		}
		return policyReady(ctx)
	}

	// One decision service behind both surfaces. REST and gRPC translate; they
	// do not decide.
	hub := events.NewHub(events.DefaultBuffer)
	decideOpts := []decide.Option{decide.WithHub(hub), decide.WithLogger(log)}

	view, forwarder, err := buildRing(cfg, log)
	if err != nil {
		return err
	}
	if view != nil {
		decideOpts = append(decideOpts, decide.WithRing(view, forwarder))
		defer func() { _ = forwarder.Close() }()
	}

	decider := decide.New(checker, policies, cfg.Node.ID, decideOpts...)

	serverOpts := []gateway.Option{
		gateway.WithReadiness(ready),
		gateway.WithAuth(authn),
	}
	if cache != nil {
		serverOpts = append(serverOpts, gateway.WithCache(cache))
	}
	if adminStore != nil {
		serverOpts = append(serverOpts, gateway.WithAdmin(adminStore, adminToken))
	}
	if view != nil {
		serverOpts = append(serverOpts, gateway.WithCluster(view))
	}
	if len(cfg.Routes) > 0 {
		proxy, err := gateway.NewProxy(cfg.Routes, log)
		if err != nil {
			return err
		}
		serverOpts = append(serverOpts, gateway.WithProxy(proxy))
		log.Info("proxy routes configured", "routes", proxy.Routes())
	}
	srv := gateway.New(cfg, decider, log, serverOpts...)

	stopGRPC, err := serveGRPC(cfg, decider, hub, adminStore, log)
	if err != nil {
		return err
	}
	defer stopGRPC()

	httpSrv := &http.Server{
		Addr:         cfg.Node.HTTPAddr,
		Handler:      srv.Handler(),
		ReadTimeout:  cfg.Node.ReadTimeout,
		WriteTimeout: cfg.Node.WriteTimeout,
	}

	// Discarding expired counters is memory management, not admission: a
	// counter is only dropped once its state is identical to having none. The
	// Redis backend gets the same rule from the key TTL the script sets.
	if mem != nil && cfg.Limiter.SweepInterval > 0 {
		go sweep(ctx, mem, cfg.Limiter.SweepInterval, log)
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("gateway listening",
			"node", cfg.Node.ID, "addr", cfg.Node.HTTPAddr, "backend", cfg.Limiter.Backend,
			"policies", len(cfg.Policies.Named), "tenants", len(cfg.Policies.Tenants))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		log.Info("shutting down", "grace", cfg.Node.ShutdownGrace)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Node.ShutdownGrace)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	log.Info("stopped", "node", cfg.Node.ID)
	return nil
}

// buildRing assembles this node's view of the cluster and the client that
// forwards to its peers.
//
// A configuration with no members is a single node, which answers for every
// tenant itself. That is not a degraded mode: correctness never depended on
// the ring, only coordination does.
func buildRing(cfg config.Config, log *slog.Logger) (*cluster.View, *forward.Client, error) {
	if len(cfg.Cluster.Members) == 0 {
		log.Info("no ring configured; this node answers for every tenant itself")
		return nil, nil, nil
	}

	members := make([]ring.Node, 0, len(cfg.Cluster.Members))
	for _, m := range cfg.Cluster.Members {
		members = append(members, ring.Node{ID: m.ID, Addr: m.Addr})
	}
	view, err := cluster.New(cfg.Node.ID, members, cfg.Cluster.VNodes)
	if err != nil {
		return nil, nil, err
	}

	fwd := forward.New(
		forward.WithTimeout(cfg.Cluster.ForwardTimeout),
		forward.WithPoolSize(cfg.Cluster.ForwardPoolSize),
	)

	dist := view.Ring().Distribution()
	log.Info("ring configured",
		"members", len(members), "vnodes", view.Ring().VNodes(),
		"epoch", view.Epoch(), "own_points", dist[cfg.Node.ID])
	return view, fwd, nil
}

// serveGRPC starts the Limiter service and returns a function that stops it.
//
// Reflection is registered because the published command is a bare
// `grpcurl -plaintext ... Check`: without it a caller has to supply the .proto
// files, and a documented command that only works with extra arguments is a
// documented command that does not work.
func serveGRPC(cfg config.Config, decider *decide.Service, hub *events.Hub, store gateway.AdminStore, log *slog.Logger) (func(), error) {
	if cfg.Node.GRPCAddr == "" {
		log.Info("gRPC is disabled (node.grpc_addr is empty)")
		return func() {}, nil
	}

	lis, err := net.Listen("tcp", cfg.Node.GRPCAddr)
	if err != nil {
		return nil, fmt.Errorf("gRPC listen on %s: %w", cfg.Node.GRPCAddr, err)
	}

	opts := []grpcapi.Option{
		grpcapi.WithHub(hub),
		grpcapi.WithTrustedTenantField(cfg.CheckAPI.TrustTenantHeader),
	}
	if resolver, ok := store.(auth.KeyResolver); ok && store != nil && cfg.Auth.APIKeys {
		opts = append(opts, grpcapi.WithKeyResolver(resolver))
	}

	server := grpc.NewServer()
	grpcapi.New(decider, log, opts...).Register(server)
	reflection.Register(server)

	go func() {
		log.Info("gRPC listening", "addr", cfg.Node.GRPCAddr, "service", "ratelimit.v1.LimiterService")
		if err := server.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("gRPC server stopped", "err", err)
		}
	}()

	return func() {
		// GracefulStop waits for in-flight RPCs, including a streaming
		// subscriber that has not noticed the shutdown yet, so the caller's
		// context cancellation is what ends the stream.
		stopped := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(cfg.Node.ShutdownGrace):
			server.Stop()
		}
	}, nil
}

// buildPolicySource assembles where policies come from.
//
// Returns the read side the request path uses, the write side the admin API
// uses (nil when policies come from the config file and there is nowhere to
// write), the cache so admin writes can invalidate it, and a readiness check.
func buildPolicySource(ctx context.Context, cfg config.Config, log *slog.Logger) (
	policy.Source, gateway.AdminStore, *policy.Cache, func(context.Context) error, error,
) {
	noop := func(context.Context) error { return nil }

	switch cfg.Policy.Store {
	case "postgres":
		pool, err := pgxpool.New(ctx, cfg.Postgres.DSN)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("policy store: %w", err)
		}
		startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = pool.Ping(startCtx)
		cancel()
		if err != nil {
			pool.Close()
			return nil, nil, nil, nil, fmt.Errorf(
				"policy store unreachable: %w (is the data plane up? `docker compose up -d redis postgres && make migrate`)", err)
		}

		repo := policy.NewPostgres(pool)
		cache := policy.NewCache(repo,
			policy.WithTTL(cfg.Policy.CacheTTL),
			policy.WithNegativeTTL(cfg.Policy.NegativeCacheTTL),
			policy.WithStaleFor(cfg.Policy.StaleFor),
		)

		if cfg.Policy.ListenForChanges {
			go policy.NewListener(pool, cache, log).Run(ctx)
		}
		log.Info("policy store is postgres",
			"cache_ttl", cfg.Policy.CacheTTL, "stale_for", cfg.Policy.StaleFor,
			"listening", cfg.Policy.ListenForChanges)
		return cache, repo, cache, repo.Ping, nil

	default:
		static, err := policy.NewStatic(cfg.Policies)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		log.Info("policy store is the config file",
			"policies", len(cfg.Policies.Named), "tenants", len(cfg.Policies.Tenants))
		return static, nil, nil, noop, nil
	}
}

// buildAuth reads the credentials from the environment.
//
// Secrets never come from the config file, which is committed; the file names
// the variable and this reads it. A missing variable disables that mechanism
// and says so, rather than silently authenticating nobody.
func buildAuth(cfg config.Config, store gateway.AdminStore, log *slog.Logger) (*auth.Authenticator, *auth.AdminToken) {
	var opts []auth.Option

	if cfg.Auth.APIKeys {
		if resolver, ok := store.(auth.KeyResolver); ok && store != nil {
			opts = append(opts, auth.WithAPIKeys(resolver))
			log.Info("api key authentication enabled")
		} else {
			log.Info("api key authentication is configured but the policy store cannot resolve keys",
				"store", cfg.Policy.Store)
		}
	}
	if cfg.Auth.JWTSecretEnv != "" {
		if secret := os.Getenv(cfg.Auth.JWTSecretEnv); secret != "" {
			opts = append(opts, auth.WithJWT([]byte(secret),
				cfg.Auth.JWTIssuer, cfg.Auth.JWTAudience, cfg.Auth.JWTTenantClaim))
			log.Info("jwt authentication enabled",
				"claim", cfg.Auth.JWTTenantClaim, "secret", auth.FingerprintSecret([]byte(secret)))
		}
	}

	adminToken := auth.NewAdminToken(os.Getenv(cfg.Auth.AdminTokenEnv))
	if store != nil && !adminToken.Configured() {
		log.Warn("the admin API is disabled: no admin token is set",
			"set", cfg.Auth.AdminTokenEnv)
	}
	return auth.New(opts...), adminToken
}

func sweep(ctx context.Context, mem *limiter.Memory, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := mem.Sweep(); n > 0 {
				log.Debug("swept expired counters", "removed", n, "live", mem.Len())
			}
		}
	}
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "info":
		lv = slog.LevelInfo
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		return nil, fmt.Errorf("--log-level %q is not debug, info, warn or error", level)
	}
	opts := &slog.HandlerOptions{Level: lv}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("--log-format %q is not text or json", format)
	}
}

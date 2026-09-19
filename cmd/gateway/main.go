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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/gateway"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/limiter"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/policy"
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
	logLevel   string
	logFormat  string
}

func parseFlags(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	fs.StringVar(&o.configPath, "config", "", "path to the YAML configuration file")
	fs.StringVar(&o.node, "node", "", "node id; overrides node.id from the config")
	fs.StringVar(&o.httpAddr, "http-addr", "", "HTTP listen address; overrides node.http_addr")
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
	if err := cfg.Validate(); err != nil {
		return err
	}

	policies, err := policy.NewStatic(cfg.Policies)
	if err != nil {
		return err
	}

	mem := limiter.NewMemory(limiter.WithMaxKeys(cfg.Limiter.MaxKeys))
	var checker limiter.Checker = mem
	switch cfg.Limiter.Backend {
	case "memory":
		log.Warn("limiter backend is memory: counters are per-process, so N replicas admit N times the quota",
			"node", cfg.Node.ID)
	case "redis":
		return fmt.Errorf("limiter.backend redis is not implemented yet (milestone 2); use memory")
	}

	srv := gateway.New(cfg, checker, policies, log)

	httpSrv := &http.Server{
		Addr:         cfg.Node.HTTPAddr,
		Handler:      srv.Handler(),
		ReadTimeout:  cfg.Node.ReadTimeout,
		WriteTimeout: cfg.Node.WriteTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Discarding expired counters is memory management, not admission: a
	// counter is only dropped once its state is identical to having none.
	if cfg.Limiter.SweepInterval > 0 {
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

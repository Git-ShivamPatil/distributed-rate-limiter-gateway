package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/config"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
)

// UpstreamTenantHeader tells the upstream which tenant the gateway resolved,
// so it does not have to re-authenticate the caller.
const UpstreamTenantHeader = "X-Tenant-ID"

// Proxy forwards admitted requests to an upstream.
//
// The limiter decides first and the proxy runs second, which is the whole
// point of putting them in one process: a refused request never reaches the
// upstream, so a noisy tenant costs the backend nothing.
type Proxy struct {
	routes []*route
	log    *slog.Logger
}

type route struct {
	name        string
	prefix      string
	stripPrefix string
	target      *url.URL
	proxy       *httputil.ReverseProxy
}

// NewProxy compiles the configured routes.
func NewProxy(routes []config.Route, log *slog.Logger) (*Proxy, error) {
	if log == nil {
		log = slog.Default()
	}
	p := &Proxy{log: log}

	// One transport for every upstream. Connection reuse is the difference
	// between a proxy that adds a millisecond and one that adds a TCP
	// handshake to every request.
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns: 1024,
		// The default is 2, which quietly turns a busy gateway into a
		// connection-churning machine: every request past the second
		// concurrent one to a host opens and closes its own socket.
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}

	for _, rc := range routes {
		target, err := url.Parse(rc.Upstream)
		if err != nil {
			return nil, fmt.Errorf("route %q: upstream %q is not a URL: %w", rc.Name, rc.Upstream, err)
		}
		r := &route{
			name:        rc.Name,
			prefix:      rc.PathPrefix,
			stripPrefix: rc.StripPrefix,
			target:      target,
		}
		r.proxy = &httputil.ReverseProxy{
			Transport: transport,
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(target)
				// SetXForwarded rather than hand-built headers: it replaces
				// any X-Forwarded-For the client sent rather than appending
				// to it, so a caller cannot forge its own chain.
				pr.SetXForwarded()
				if r.stripPrefix != "" {
					trimmed := strings.TrimPrefix(pr.In.URL.Path, r.stripPrefix)
					if trimmed == "" {
						trimmed = "/"
					}
					pr.Out.URL.Path = singleJoin(target.Path, trimmed)
				}
			},
			ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
				// An upstream that is down is not the client's fault and not a
				// rate-limit decision, so it must not look like one.
				log.Error("upstream request failed",
					"route", r.name, "upstream", target.String(), "path", req.URL.Path, "err", err)
				writeError(w, http.StatusBadGateway, "upstream_unavailable",
					fmt.Sprintf("route %q could not reach its upstream", r.name))
			},
		}
		p.routes = append(p.routes, r)
	}
	return p, nil
}

func singleJoin(base, rest string) string {
	switch {
	case base == "" || base == "/":
		return rest
	case strings.HasSuffix(base, "/") && strings.HasPrefix(rest, "/"):
		return base + rest[1:]
	case !strings.HasSuffix(base, "/") && !strings.HasPrefix(rest, "/"):
		return base + "/" + rest
	default:
		return base + rest
	}
}

// Routes reports the configured route names, for logging and the admin API.
func (p *Proxy) Routes() []string {
	out := make([]string, 0, len(p.routes))
	for _, r := range p.routes {
		out = append(out, r.name)
	}
	return out
}

// match finds the route for a path. The longest matching prefix wins, so
// /api/echo/special can be routed differently from /api/echo without the
// order of the config file deciding it.
func (p *Proxy) match(path string) *route {
	var best *route
	for _, r := range p.routes {
		if !strings.HasPrefix(path, r.prefix) {
			continue
		}
		if best == nil || len(r.prefix) > len(best.prefix) {
			best = r
		}
	}
	return best
}

// handleProxy is the data path: authenticate, decide, then forward.
func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	if s.proxy == nil {
		writeError(w, http.StatusNotFound, "no_route", "this gateway has no routes configured")
		return
	}
	matched := s.proxy.match(r.URL.Path)
	if matched == nil {
		writeError(w, http.StatusNotFound, "no_route",
			fmt.Sprintf("no route matches %q", r.URL.Path))
		return
	}

	tenant, err := s.resolveTenant(r)
	if err != nil {
		s.writeAuthError(w, err)
		return
	}

	cost, err := requestCost(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_cost", err.Error())
		return
	}

	// The real method and path, not something the caller asserts: on the data
	// path the gateway can see the request it is limiting.
	out, err := s.decider.Decide(r.Context(), decide.Query{
		Tenant: tenant,
		Method: r.Method,
		Path:   r.URL.Path,
		Cost:   cost,
	})
	if err != nil {
		s.writeDecideError(w, tenant, err)
		return
	}

	SetRateLimitHeaders(w.Header(), out.Result)
	if !out.Result.Allowed {
		body := s.responseFor(tenant, out)
		body.Route = matched.name
		writeJSON(w, http.StatusTooManyRequests, body)
		return
	}

	// The upstream is told who this is, and is not told the caller's
	// credentials: it trusts the gateway, not the client.
	r.Header.Set(UpstreamTenantHeader, tenant)
	r.Header.Del("Authorization")
	r.Header.Del("X-API-Key")

	// The rate-limit headers are already on the ResponseWriter; the upstream's
	// own headers are merged in as it writes.
	matched.proxy.ServeHTTP(w, r)
}

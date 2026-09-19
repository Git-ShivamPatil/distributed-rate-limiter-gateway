package config

import (
	"strings"
	"testing"
)

// A broken route must stop the process at startup, next to every other
// configuration mistake, rather than surface as a 502 the first time somebody
// uses it.
func TestRouteValidation(t *testing.T) {
	good := Route{Name: "echo", PathPrefix: "/api/echo", Upstream: "http://127.0.0.1:9000", StripPrefix: "/api"}

	if err := validateRoutes([]Route{good}); err != nil {
		t.Fatalf("a valid route was rejected: %v", err)
	}

	cases := []struct {
		name   string
		routes []Route
		want   string
	}{
		{
			name:   "no name",
			routes: []Route{{PathPrefix: "/a", Upstream: "http://x:1"}},
			want:   "no name",
		},
		{
			name:   "duplicate names",
			routes: []Route{good, {Name: "echo", PathPrefix: "/other", Upstream: "http://x:1"}},
			want:   "two routes are named",
		},
		{
			name:   "duplicate prefixes",
			routes: []Route{good, {Name: "other", PathPrefix: "/api/echo", Upstream: "http://x:1"}},
			want:   "both claim path_prefix",
		},
		{
			name:   "prefix without a slash",
			routes: []Route{{Name: "a", PathPrefix: "api", Upstream: "http://x:1"}},
			want:   "does not start with /",
		},
		{
			name:   "relative upstream",
			routes: []Route{{Name: "a", PathPrefix: "/a", Upstream: "127.0.0.1:9000"}},
			want:   "not an absolute URL",
		},
		{
			name:   "upstream with an odd scheme",
			routes: []Route{{Name: "a", PathPrefix: "/a", Upstream: "ftp://host/x"}},
			want:   "is not http or https",
		},
		{
			// Stripping a prefix the path does not start with would send an
			// unrelated path upstream.
			name:   "strip prefix that is not a prefix",
			routes: []Route{{Name: "a", PathPrefix: "/api/echo", Upstream: "http://x:1", StripPrefix: "/v2"}},
			want:   "is not a prefix of path_prefix",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRoutes(tc.routes)
			if err == nil {
				t.Fatal("a broken route was accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

// The shipped config's route has to be one the proxy can actually serve.
func TestLocalConfigRoute(t *testing.T) {
	cfg, err := Load("../../configs/local.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Routes) == 0 {
		t.Fatal("the shipped config defines no routes, so the data path is unreachable")
	}
	if cfg.Node.GRPCAddr == "" {
		t.Error("the shipped config has no grpc_addr, so the published grpcurl command cannot work")
	}
	r := cfg.Routes[0]
	if r.PathPrefix != "/api/echo" || r.StripPrefix != "/api" {
		t.Errorf("route = %+v, want /api/echo stripping /api", r)
	}
}

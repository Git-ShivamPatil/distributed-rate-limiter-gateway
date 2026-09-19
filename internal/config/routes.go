package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// validateRoutes rejects a route the proxy could not honour.
//
// It lives in this package rather than in the gateway because a broken route
// should stop the process at startup, next to every other configuration
// mistake, rather than surface as a 502 the first time somebody uses it.
func validateRoutes(routes []Route) error {
	seen := map[string]bool{}
	prefixes := map[string]string{}

	for _, r := range routes {
		if r.Name == "" {
			return errors.New("a route has no name")
		}
		if seen[r.Name] {
			return fmt.Errorf("two routes are named %q", r.Name)
		}
		seen[r.Name] = true

		if !strings.HasPrefix(r.PathPrefix, "/") {
			return fmt.Errorf("route %q: path_prefix %q does not start with /", r.Name, r.PathPrefix)
		}
		if other, ok := prefixes[r.PathPrefix]; ok {
			// Two routes on one prefix is not a precedence question the
			// longest-match rule can settle; it is a mistake.
			return fmt.Errorf("routes %q and %q both claim path_prefix %q", other, r.Name, r.PathPrefix)
		}
		prefixes[r.PathPrefix] = r.Name

		u, err := url.Parse(r.Upstream)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("route %q: upstream %q is not an absolute URL (want e.g. http://127.0.0.1:9000)", r.Name, r.Upstream)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("route %q: upstream scheme %q is not http or https", r.Name, u.Scheme)
		}
		if r.StripPrefix != "" {
			if !strings.HasPrefix(r.StripPrefix, "/") {
				return fmt.Errorf("route %q: strip_prefix %q does not start with /", r.Name, r.StripPrefix)
			}
			if !strings.HasPrefix(r.PathPrefix, r.StripPrefix) {
				return fmt.Errorf("route %q: strip_prefix %q is not a prefix of path_prefix %q -- stripping it would send an unrelated path upstream",
					r.Name, r.StripPrefix, r.PathPrefix)
			}
		}
	}
	return nil
}

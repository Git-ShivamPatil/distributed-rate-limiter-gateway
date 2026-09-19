// Command echo is a minimal upstream: it answers with what it received.
//
// It exists so the gateway's data path can be exercised end to end without
// standing up a real service, and so the benchmark can measure the gateway
// rather than whatever was behind it. It is deliberately trivial -- one
// handler, no dependencies, no allocation beyond the response -- because an
// upstream that is itself slow makes the number in front of it meaningless.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	var (
		addr  = flag.String("addr", "127.0.0.1:9000", "listen address")
		delay = flag.Duration("delay", 0, "artificial per-request delay, for testing timeouts")
		quiet = flag.Bool("quiet", false, "do not log requests")
	)
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if *delay > 0 {
			time.Sleep(*delay)
		}
		if !*quiet {
			log.Printf("%s %s tenant=%q", r.Method, r.URL.Path, r.Header.Get("X-Tenant-ID"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream", "echo")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"upstream": "echo",
			"method":   r.Method,
			"path":     r.URL.Path,
			"query":    r.URL.RawQuery,
			// The gateway resolves the tenant and tells the upstream who it
			// is; echoing it back is what lets a test prove that happened.
			"tenant": r.Header.Get("X-Tenant-ID"),
			// Proof that the caller's credentials were NOT forwarded: an
			// upstream trusts the gateway, not the client.
			"saw_authorization": r.Header.Get("Authorization") != "",
			"saw_api_key":       r.Header.Get("X-API-Key") != "",
			"forwarded_for":     r.Header.Get("X-Forwarded-For"),
		})
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	fmt.Fprintf(os.Stderr, "echo listening on %s\n", *addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("echo: %v", err)
	}
}

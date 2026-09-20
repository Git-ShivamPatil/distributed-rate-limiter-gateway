//go:build faultinject

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/cluster"
	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/decide"
)

// FaultEnv names the environment variable that selects a fault. It is only
// read by a build made with -tags faultinject.
const FaultEnv = "GATEWAY_FAULT"

// Faults are deliberate breakages, each one the control for a specific claim.
//
// A claim whose check cannot fail when the mechanism is removed is not
// evidence, so every claim in this repository has a matching fault, and CI
// runs the suite once clean and once per fault asserting the named check goes
// RED. A fault that stops breaking its check fails the build, because that
// means the check stopped depending on the mechanism.
const (
	// FaultRaftCounters puts consensus on the request path: every decision
	// first commits an entry. It is the control for "Raft governs membership
	// and never touches a request". With it on, killing the leader has to
	// break traffic, because every request now waits on a log that has no
	// leader to append to.
	FaultRaftCounters = "raft_counters"
)

func installFaults(c *cluster.Consensus, log *slog.Logger) {
	name := os.Getenv(FaultEnv)
	if name == "" {
		return
	}
	switch name {
	case FaultRaftCounters:
		if c == nil {
			log.Error("fault needs consensus, and this node has none", "fault", name)
			os.Exit(2)
		}
		decide.FaultPreCheck = func(ctx context.Context) error {
			if _, err := c.Propose(ctx, cluster.Command{Kind: cluster.BumpPolicyGen}); err != nil {
				return fmt.Errorf("per-request consensus: %w", err)
			}
			return nil
		}
	default:
		// An unknown fault is a typo, and a typo that silently ran the clean
		// scenario would report the control as passing.
		log.Error("unknown fault", "fault", name)
		os.Exit(2)
	}
	log.Warn("FAULT INJECTED -- this build is deliberately broken and must never be deployed", "fault", name)
}

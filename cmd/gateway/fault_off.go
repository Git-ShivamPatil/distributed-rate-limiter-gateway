//go:build !faultinject

package main

import (
	"log/slog"

	"github.com/Git-ShivamPatil/distributed-rate-limiter-gateway/internal/cluster"
)

// installFaults does nothing here, and there is nothing it could be asked to
// do: the faults live in fault_on.go behind a build tag, so a shipped binary
// contains neither the code nor the switch that reaches it.
func installFaults(*cluster.Consensus, *slog.Logger) {}

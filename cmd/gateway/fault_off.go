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

// faultRemoved reports whether a named mechanism has been deliberately taken
// out. In a production build nothing has, and this is a constant false the
// compiler removes along with every branch that tests it.
func faultRemoved(string) bool { return false }

//go:build !faultinject

package decide

import "context"

// faultHook is where a fault-injection build interposes on every decision.
//
// In a production build it is exactly this: a function that returns nil and is
// inlined away. There is no variable to set, no flag to flip and no branch to
// reach from outside, which is the whole point -- a fault-injection seam that
// ships is a quota-bypass primitive waiting to be found. The injected version
// lives in fault_on.go behind a build tag, so the two cannot both exist in one
// binary.
func (s *Service) faultHook(context.Context) error { return nil }

//go:build faultinject

package decide

import "context"

// FaultPreCheck runs before every decision, and only in a build made with
// -tags faultinject.
//
// It exists to make a claim falsifiable. The design says consensus governs
// membership and never touches a request, and that per-request consensus would
// not be a latency regression but an impossibility. A test that only watches
// the real system work cannot tell the difference between "consensus is off
// the request path" and "consensus happens to be fast today". Putting it ON
// the request path and watching the same scenario break is what turns that
// sentence into a measurement.
var FaultPreCheck func(context.Context) error

func (s *Service) faultHook(ctx context.Context) error {
	if FaultPreCheck == nil {
		return nil
	}
	return FaultPreCheck(ctx)
}

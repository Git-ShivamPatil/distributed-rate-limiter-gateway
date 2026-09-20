package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// NoEviction is the only memory policy under which this limiter is correct.
const NoEviction = "noeviction"

// Eviction describes what this Redis would do to a counter under memory
// pressure.
//
// It matters more than it sounds. Every other failure this project defends
// against is loud: a store that cannot be reached returns an error, a store
// that was wiped fails a generation check, a node carrying a stale policy is
// refused. An EVICTED COUNTER is none of those. The key simply is not there
// any more, and an absent key is indistinguishable from a full bucket -- by
// design, because that is what lets a TTL expire a counter safely. So a tenant
// that was at its limit silently gets a fresh quota, nothing is logged, every
// test still passes, and the only symptom is a number in a bill.
//
// The generation fence has its own version of the same problem: losing the
// `limiter:store:gen` key alone, with the per-tenant meta keys surviving, ends
// with every existing tenant refused rather than over-admitted. Neither
// outcome is acceptable, and both are impossible under noeviction.
type Eviction struct {
	// Policy is maxmemory-policy as the server reports it. Empty when it could
	// not be read.
	Policy string
	// MaxMemory is the maxmemory setting in bytes. Zero means unlimited, and
	// under an unlimited maxmemory no key is ever evicted whatever the policy
	// says -- so the policy alone is not enough to judge this.
	MaxMemory int64
	// Evicted is evicted_keys from INFO stats: how many keys this server has
	// already discarded since it started. Non-zero is history, not prediction,
	// but it is the difference between "this could happen" and "this has been
	// happening".
	Evicted int64
	// Known is false when the server would not answer CONFIG GET, which some
	// managed providers disable. An unknown configuration is reported, never
	// treated as safe and never treated as fatal.
	Known bool
}

// Fatal reports whether this Redis can silently discard a counter.
//
// Both halves are required. A policy of allkeys-lru with maxmemory unset
// evicts nothing today, so refusing to start on the policy alone would reject
// a perfectly safe server; it is still worth a warning, because the day
// somebody sets maxmemory the limiter starts being wrong with no other sign.
func (e Eviction) Fatal() bool {
	return e.Known && e.Policy != NoEviction && e.MaxMemory > 0
}

// Latent reports a configuration that is safe now and would stop being safe
// the moment a memory limit is set.
func (e Eviction) Latent() bool {
	return e.Known && e.Policy != NoEviction && e.MaxMemory == 0
}

func (e Eviction) String() string {
	if !e.Known {
		return "maxmemory-policy unknown (the server would not answer CONFIG GET)"
	}
	return fmt.Sprintf("maxmemory-policy=%s maxmemory=%d evicted_keys=%d", e.Policy, e.MaxMemory, e.Evicted)
}

// Eviction reads whether this server may discard keys.
//
// A server that refuses CONFIG GET is reported as unknown rather than as an
// error: several managed Redis providers disable the command, and a limiter
// that will not start against them would be trading a real deployment for a
// check it cannot perform anyway.
func (c *Checker) Eviction(ctx context.Context) Eviction {
	var out Eviction

	policy, err := c.configGet(ctx, "maxmemory-policy")
	if err != nil {
		return out // unknown
	}
	limit, err := c.configGet(ctx, "maxmemory")
	if err != nil {
		return out
	}

	out.Known = true
	out.Policy = policy
	if n, convErr := strconv.ParseInt(strings.TrimSpace(limit), 10, 64); convErr == nil {
		out.MaxMemory = n
	}

	// History, best effort: a server that answered CONFIG but not INFO still
	// gives a usable verdict from the two settings above.
	if info, infoErr := c.client.Info(ctx, "stats").Result(); infoErr == nil {
		out.Evicted = statField(info, "evicted_keys")
	}
	return out
}

func (c *Checker) configGet(ctx context.Context, name string) (string, error) {
	res, err := c.client.ConfigGet(ctx, name).Result()
	if err != nil {
		return "", err
	}
	v, ok := res[name]
	if !ok {
		return "", fmt.Errorf("redis: the server did not report %s", name)
	}
	return v, nil
}

func statField(info, name string) int64 {
	for _, line := range strings.Split(info, "\n") {
		rest, ok := strings.CutPrefix(strings.TrimSpace(line), name+":")
		if !ok {
			continue
		}
		if n, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64); err == nil {
			return n
		}
	}
	return 0
}

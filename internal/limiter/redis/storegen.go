package redis

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

//go:embed storegen.lua
var storeGenSource string

// StoreGenKey names the whole store rather than any tenant, so it carries no
// hash tag. A per-tenant script may only touch one slot, which is exactly why
// this is read out of band and never from inside a check.
const StoreGenKey = "limiter:store:gen"

// Identity reads what this store currently claims to be: the generation the
// cluster minted for its lifetime, and Redis's own identifier for the running
// process.
//
// A zero generation means the key is absent, which is either a store nobody
// has used yet or one that has just been wiped -- and those are the same thing
// until the committed generation says otherwise. The run id changes on a
// restart and NOT on a FLUSHALL, which is why it cannot be the only signal:
// the most likely way to lose every counter leaves it untouched.
func (c *Checker) Identity(ctx context.Context) (uint64, string, error) {
	var gen uint64

	raw, err := c.client.Get(ctx, StoreGenKey).Result()
	switch {
	case err == goredis.Nil:
		// Absent. Left as zero.
	case err != nil:
		return 0, "", fmt.Errorf("redis: reading the store generation: %w", err)
	default:
		n, convErr := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if convErr != nil {
			return 0, "", fmt.Errorf("redis: the store generation %q is not a number", raw)
		}
		gen = n
	}

	info, err := c.client.Info(ctx, "server").Result()
	if err != nil {
		return 0, "", fmt.Errorf("redis: reading server info: %w", err)
	}
	return gen, runID(info), nil
}

// MintStoreGeneration installs candidate as this store's generation if it has
// none, and returns whichever generation is in force afterwards.
//
// It is idempotent and safe to race: Redis runs the script to completion, so
// the first caller writes and every other one reads what it wrote.
func (c *Checker) MintStoreGeneration(ctx context.Context, candidate uint64) (uint64, error) {
	if candidate == 0 {
		return 0, fmt.Errorf("redis: generation 0 is not a generation")
	}
	raw, err := c.storeGen.Run(ctx, c.client, []string{StoreGenKey}, strconv.FormatUint(candidate, 10)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: minting the store generation: %w", err)
	}
	s, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("redis: minting the store generation: the script answered %T", raw)
	}
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("redis: the minted generation %q is not a number", s)
	}
	return n, nil
}

// runID pulls run_id out of an INFO server section.
func runID(info string) string {
	for _, line := range strings.Split(info, "\n") {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(line), "run_id:"); ok {
			return strings.TrimSpace(rest)
		}
	}
	return ""
}

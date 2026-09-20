-- storegen.lua -- name this Redis, exactly once per lifetime.
--
-- The counter store has no identity of its own that survives being wiped, and
-- that is the problem this solves. A Redis that was flushed and came back
-- empty looks exactly like a Redis nobody has used yet: every bucket is full,
-- every window is empty, and every tenant silently gets a fresh quota. The
-- generation is the marker that makes the difference visible.
--
-- The candidate comes from the cluster's own log rather than from a clock or a
-- random source, and it is always one above the highest generation the cluster
-- has ever committed. That is what makes the high-water mark survive a wipe
-- that the store itself does not: the number lives somewhere the flush cannot
-- reach, so the generation after a wipe is always ABOVE the one before it, and
-- a zombie still carrying the old one is fenced rather than waved through.
--
-- Called out of band -- at startup and on reconnect, never from a check. The
-- key is deliberately NOT hash-tagged to any tenant: it names the whole store,
-- and a per-tenant script may only touch one slot.
--
-- KEYS[1]  limiter:store:gen
-- ARGV[1]  the candidate generation, as a decimal string
--
-- Returns the generation now in force, which is the candidate only when this
-- call is the one that minted it.

local current = redis.call('GET', KEYS[1])
if current then
  return current
end

-- Redis runs a script to completion with nothing interleaved, so every gateway
-- racing to mint on a freshly emptied store gets the same answer: the first
-- one writes, the rest read what it wrote. No lock, no retry, no leader.
redis.call('SET', KEYS[1], ARGV[1])
return ARGV[1]

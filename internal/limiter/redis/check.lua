-- check.lua -- evaluate every limit of one tenant, atomically, in one round trip.
--
-- Why one script rather than one per algorithm: a policy can carry several
-- limits, and they have to be all-or-nothing. Evaluating a token bucket and a
-- sliding window in two calls means a request can consume a token from the
-- first and then be refused by the second, so a tenant is charged for traffic
-- it never got to send. Redis runs a script to completion with nothing
-- interleaved, so the whole set is decided and then committed here.
--
-- Why Redis TIME rather than the caller's clock: several gateway replicas
-- share these counters and their clocks drift. A sliding window evaluated
-- against two different "now"s admits a different number of requests depending
-- on which replica a request lands on, and the drift is invisible -- every
-- local test passes. The script reads the clock of the one process that owns
-- the state.
--
-- Requires Redis 7: it replicates script effects rather than the script, which
-- is what makes a write after TIME legal.
--
-- KEYS[1]      the tenant's meta key, holding the two fences. It shares the
--              tenant hash tag but NOT the counter prefix, so that clearing a
--              tenant's counters cannot clear its fences.
-- KEYS[i + 1]  one key per limit, all sharing the tenant hash tag so a cluster
--              keeps them in one slot
--
-- ARGV[1]  store generation: which lifetime of this Redis the caller believes
--          it is talking to. 0 means the caller is not fenced.
-- ARGV[2]  policy generation: how recent the caller's copy of the policy is.
--          0 means the caller is not fenced.
-- ARGV[3]  cost, in units
-- ARGV[4]  1 to peek (decide and report, consume nothing)
-- ARGV[5]  clock override in microseconds, 0 to read Redis TIME. Only the
--          differential test against the in-memory oracle passes a value; the
--          production constructor has no way to set it.
-- then four values per limit, in the order of KEYS:
--   kind      "tb" (token bucket, GCRA) or "sw" (sliding window log)
--   a         tb: emission interval in microseconds; sw: period in microseconds
--   b         tb: tolerance in microseconds (emission * capacity); sw: count
--   capacity  the number reported as the limit
--
-- Returns a flat array whose FIRST element is a status:
--   {0, allowed, then four values per limit -- allowed, remaining,
--    retry_after_us, reset_after_us}
--   {1, stored_policy_gen}  the caller's policy is older than one already
--                           enforced here; nothing was read or written
--   {2, stored_store_gen}   this is not the store the caller's generation was
--                           minted for; nothing was read or written
--
-- The status is a VALUE and not an error reply, and that is load-bearing. A
-- caller that receives an error cannot tell a fence from an unreachable store,
-- and a tenant whose policy says fail_open would be ADMITTED by exactly the
-- check that exists to refuse it.

local store_gen = tonumber(ARGV[1])
local policy_gen = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local peek = tonumber(ARGV[4])
local override = tonumber(ARGV[5])

local STATUS_OK = 0
local STATUS_POLICY_STALE = 1
local STATUS_STORE_RESET = 2

local meta_key = KEYS[1]

-- The fences, evaluated before a single counter is read.
--
-- An ABSENT meta adopts whatever the caller carries and never refuses. It has
-- to: every tenant's first request finds no meta, and a fence that rejected it
-- would refuse all traffic everywhere the first time it shipped.
local meta = redis.call('HMGET', meta_key, 'store_gen', 'policy_gen')
local stored_store = tonumber(meta[1])
local stored_policy = tonumber(meta[2])

-- A store generation that DIFFERS IN EITHER DIRECTION is a different store.
-- Not "lower": a wipe followed by a re-mint can hand out a number below the
-- one a surviving node still carries, and treating that as acceptable is the
-- silent full-quota amnesty the generation exists to catch.
if store_gen > 0 and stored_store and stored_store ~= store_gen then
  return { STATUS_STORE_RESET, stored_store }
end

-- A policy generation BELOW the one already enforced means this caller is
-- carrying a limit the cluster has replaced. Enforcing it would put max(old,
-- new) in effect for the whole skew window, which for a tightening is the
-- over-admission nobody sees.
if policy_gen > 0 and stored_policy and policy_gen < stored_policy then
  return { STATUS_POLICY_STALE, stored_policy }
end

-- Past the fences, so record what the caller carries.
--
-- This happens on EVERY call -- denials and peeks included -- and never
-- expires. Advancing it only on admitted requests would let a tightening be
-- outrun by exhausting the quota first: every later request is a denial, the
-- stored generation never moves, and the node carrying the old wider limit is
-- never fenced. A generation is not quota, so writing one does not make a
-- refused request consume anything.
if store_gen > 0 and stored_store == nil then
  redis.call('HSET', meta_key, 'store_gen', string.format('%.0f', store_gen))
end
if policy_gen > 0 and (stored_policy == nil or policy_gen > stored_policy) then
  redis.call('HSET', meta_key, 'policy_gen', string.format('%.0f', policy_gen))
end

local now
if override > 0 then
  now = override
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000000 + tonumber(t[2])
end

local function clamp(v)
  if v < 0 then return 0 end
  return v
end

local n = #KEYS - 1
local allowed_all = 1

-- One entry per limit, holding both the "if this is admitted" numbers and the
-- "as things stand" numbers. Which pair gets reported is only known once every
-- limit has been decided: a refused request consumes nothing, so it must not
-- report the headroom it would have had.
local state = {}

for i = 1, n do
  local base = 5 + (i - 1) * 4
  local kind = ARGV[base + 1]
  local a = tonumber(ARGV[base + 2])
  local b = tonumber(ARGV[base + 3])
  local capacity = tonumber(ARGV[base + 4])
  local key = KEYS[i + 1]

  local s = { kind = kind, key = key, capacity = capacity, allowed = 1, retry = 0 }

  if kind == 'tb' then
    -- GCRA: the stored value is the theoretical arrival time of the next
    -- conforming request. tat <= now means a full bucket, which is also why an
    -- absent key and a full bucket are the same thing.
    local emission, tolerance = a, b
    local stored = redis.call('GET', key)
    local tat = now
    if stored then
      tat = tonumber(stored)
      if tat < now then tat = now end
    end

    s.remaining_now = math.floor(clamp(tolerance - (tat - now)) / emission)
    s.reset_now = clamp(tat - now)

    if cost == 0 then
      s.remaining_after, s.reset_after = s.remaining_now, s.reset_now
    else
      local new_tat = tat + emission * cost
      local allow_at = new_tat - tolerance
      if allow_at > now then
        s.allowed = 0
        s.retry = clamp(allow_at - now)
        s.remaining_after, s.reset_after = s.remaining_now, s.reset_now
      else
        s.remaining_after = math.floor(clamp(tolerance - (new_tat - now)) / emission)
        s.reset_after = clamp(new_tat - now)
        s.write = new_tat
      end
    end
  elseif kind == 'sw' then
    -- A log of one score per admitted unit. The window is half-open: an entry
    -- at exactly now-period has left it, so the live range is (cutoff, +inf].
    local period, count = a, b
    local cutoff = now - period
    local used = redis.call('ZCOUNT', key, '(' .. string.format('%.0f', cutoff), '+inf')

    s.remaining_now = clamp(count - used)
    local oldest = redis.call('ZRANGEBYSCORE', key, '(' .. string.format('%.0f', cutoff), '+inf', 'WITHSCORES', 'LIMIT', 0, 1)
    if oldest[2] then
      s.reset_now = clamp(tonumber(oldest[2]) + period - now)
    else
      s.reset_now = 0
    end

    if cost == 0 then
      s.remaining_after, s.reset_after = s.remaining_now, s.reset_now
    elseif used + cost > count then
      -- Enough entries have to age out to make room. The one that matters is
      -- the (used + cost - count)-th oldest; when it leaves, there is exactly
      -- enough space for this request.
      local idx = used + cost - count - 1
      local entry = redis.call('ZRANGEBYSCORE', key, '(' .. string.format('%.0f', cutoff), '+inf', 'WITHSCORES', 'LIMIT', idx, 1)
      s.allowed = 0
      if entry[2] then
        s.retry = clamp(tonumber(entry[2]) + period - now)
      else
        s.retry = period
      end
      s.remaining_after, s.reset_after = s.remaining_now, s.reset_now
    else
      s.remaining_after = clamp(count - used - cost)
      if oldest[2] then
        s.reset_after = clamp(tonumber(oldest[2]) + period - now)
      else
        s.reset_after = period
      end
      s.write = used -- how many live entries existed, for the member suffix
      s.cutoff = cutoff
      s.period = period
    end
  else
    return redis.error_reply('check.lua: unknown limit kind ' .. tostring(kind))
  end

  if s.allowed == 0 then allowed_all = 0 end
  state[i] = s
end

-- Expiry is set as a RELATIVE ttl rather than an absolute instant.
--
-- Both express the same rule -- the key dies exactly when its state stops
-- meaning anything -- but an absolute deadline silently assumes the clock the
-- script reads is the clock the expiry cycle compares against. In production
-- that is true. It stops being true the moment anything supplies a different
-- clock, and then every key expires on write and every counter starts empty:
-- a rate limiter that admits everything while all its tests pass.
local function ttl_ms(microseconds)
  local ms = math.ceil(microseconds / 1000)
  if ms < 1 then return 1 end
  return ms
end

if allowed_all == 1 and peek == 0 and cost > 0 then
  for i = 1, n do
    local s = state[i]
    if s.kind == 'tb' then
      -- The key expires exactly when the bucket refills, so forgetting it can
      -- never hand out quota: an expired key and a full bucket are one state.
      redis.call('SET', s.key, string.format('%.0f', s.write), 'PX', ttl_ms(s.write - now))
    else
      redis.call('ZREMRANGEBYSCORE', s.key, '-inf', string.format('%.0f', s.cutoff))
      -- Members must be unique or ZADD updates an existing one instead of
      -- adding, which would silently under-count and over-admit. Two requests
      -- can land in the same microsecond, so the suffix counts how many
      -- entries already share this score.
      local same = redis.call('ZCOUNT', s.key, string.format('%.0f', now), string.format('%.0f', now))
      for j = 1, cost do
        redis.call('ZADD', s.key, string.format('%.0f', now), string.format('%.0f', now) .. '-' .. tostring(same + j))
      end
      -- The whole log is gone one period after its newest entry.
      redis.call('PEXPIRE', s.key, ttl_ms(s.period))
    end
  end
end

local out = { STATUS_OK, allowed_all }
for i = 1, n do
  local s = state[i]
  local remaining, reset
  if allowed_all == 1 and peek == 0 and cost > 0 then
    remaining, reset = s.remaining_after, s.reset_after
  else
    remaining, reset = s.remaining_now, s.reset_now
  end
  out[#out + 1] = s.allowed
  out[#out + 1] = math.floor(remaining)
  out[#out + 1] = math.floor(s.retry)
  out[#out + 1] = math.floor(reset)
end
return out

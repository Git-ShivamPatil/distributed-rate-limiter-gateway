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
-- KEYS[i]  one key per limit, all sharing the tenant hash tag so a cluster
--          keeps them in one slot
--
-- ARGV[1]  cost, in units
-- ARGV[2]  1 to peek (decide and report, consume nothing)
-- ARGV[3]  clock override in microseconds, 0 to read Redis TIME. Only the
--          differential test against the in-memory oracle passes a value; the
--          production constructor has no way to set it.
-- then four values per limit, in the order of KEYS:
--   kind      "tb" (token bucket, GCRA) or "sw" (sliding window log)
--   a         tb: emission interval in microseconds; sw: period in microseconds
--   b         tb: tolerance in microseconds (emission * capacity); sw: count
--   capacity  the number reported as the limit
--
-- Returns a flat array: allowed, then four values per limit --
--   allowed, remaining, retry_after_us, reset_after_us

local cost = tonumber(ARGV[1])
local peek = tonumber(ARGV[2])
local override = tonumber(ARGV[3])

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

local n = #KEYS
local allowed_all = 1

-- One entry per limit, holding both the "if this is admitted" numbers and the
-- "as things stand" numbers. Which pair gets reported is only known once every
-- limit has been decided: a refused request consumes nothing, so it must not
-- report the headroom it would have had.
local state = {}

for i = 1, n do
  local base = 3 + (i - 1) * 4
  local kind = ARGV[base + 1]
  local a = tonumber(ARGV[base + 2])
  local b = tonumber(ARGV[base + 3])
  local capacity = tonumber(ARGV[base + 4])
  local key = KEYS[i]

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

local out = { allowed_all }
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

-- KEYS[1]: Rate limiter bucket key (e.g., "ratelimit:client:user_123")
-- ARGV[1]: Bucket capacity (number)
-- ARGV[2]: Refill rate per millisecond (number)
-- ARGV[3]: Tokens to consume (usually 1)
-- ARGV[4]: Current timestamp in milliseconds

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate_per_ms = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])
local now = tonumber(ARGV[4])

-- Retrieve existing bucket data
local data = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(data[1])
local last_refill = tonumber(data[2])

if tokens == nil then
    -- Initialize bucket to full capacity
    tokens = capacity
    last_refill = now
else
    -- Compute refill based on elapsed time
    local elapsed = math.max(0, now - last_refill)
    local generated = elapsed * refill_rate_per_ms
    tokens = math.min(capacity, tokens + generated)
    last_refill = now
end

local allowed = 0
local remaining = tokens

if tokens >= requested then
    allowed = 1
    tokens = tokens - requested
    remaining = tokens
end

-- Save updated values
redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)

-- Set dynamic TTL: enough time for bucket to fully refill to capacity plus buffer
local time_to_fill_ms = math.ceil(capacity / refill_rate_per_ms)
local ttl_seconds = math.ceil(time_to_fill_ms / 1000) + 60
redis.call('EXPIRE', key, ttl_seconds)

return { allowed, math.floor(remaining) }
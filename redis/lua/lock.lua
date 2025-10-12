-- key is a key to associate with the lock
-- fencing_token is a fencing token
-- ttl is a TTL of the key in milliseconds

local key = KEYS[1]
local fencing_token = ARGV[1]
local ttl = tonumber(ARGV[2])

local acquired = redis.call('SET', key, fencing_token, 'NX', 'PX', ttl)
if acquired then
  return { 1, ttl }
end

local remaining = redis.call('PTTL', key)
return { 0, remaining }

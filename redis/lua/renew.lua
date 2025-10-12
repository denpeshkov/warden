-- key is a key associated with the lock
-- fencing_token is a fencing token
-- ttl is a TTL of the key in milliseconds

local key = KEYS[1]
local fencing_token = ARGV[1]
local ttl = tonumber(ARGV[2])

if redis.call('GET', key) == fencing_token then
    redis.call('PEXPIRE', key, ttl)
    return 1
end
return 0

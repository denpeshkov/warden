-- key is a key to associate with the lock
-- fencing_token is a fencing token

local key = KEYS[1]
local fencing_token = ARGV[1]
local channel = ARGV[2]

-- Check if we own the lock
if redis.call('GET', key) == fencing_token then
    redis.call('DEL', key)
    redis.call('PUBLISH', channel, 'released')
    return 1
end
return 0

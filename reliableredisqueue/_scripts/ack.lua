local items = KEYS[1]
local unacked = KEYS[3]
local uid = ARGV[1]
local timestamp = redis.call('ZSCORE', unacked, uid)
if timestamp then
    redis.call('HDEL', items, uid)
    redis.call('ZREM', unacked, uid)
    return true
end
return false

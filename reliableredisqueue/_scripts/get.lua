local items = KEYS[1]
local ready = KEYS[2]
local unacked = KEYS[3]
local timestamp = tonumber(ARGV[1])
local max_age = timestamp - tonumber(ARGV[2])
local uids = redis.call('ZRANGEBYSCORE', unacked, 0, max_age)
if #uids > 0 then
    local unpack = unpack or table.unpack
    redis.call('LPUSH', ready, unpack(uids))
    redis.call('ZREMRANGEBYSCORE', unacked, 0, max_age)
end
-- The loop will run a single iteration most of the time.
while true do
    local uid = redis.call('LINDEX', ready, -1)
    if not uid then
        break
    end
    local item = redis.call('HGET', items, uid)
    if item then
        redis.call('ZADD', unacked, timestamp, uid)
        redis.call('RPOP', ready)
        return {uid, item}
    end
    redis.call('RPOP', ready)
end
return nil

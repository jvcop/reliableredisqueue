import time

NAMESPACE = "rrq"

_GET_SCRIPT = """
local items = KEYS[1]
local ready = KEYS[2]
local unacked = KEYS[3]
local timestamp = tonumber(ARGV[1])
local max_age = timestamp - tonumber(ARGV[2])
local uids = redis.call('ZRANGEBYSCORE', unacked, 0, max_age)
if #uids > 0 then
    redis.call('LPUSH', ready, table.unpack(uids))
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
"""

_ACK_SCRIPT = """
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
"""

_FAIL_SCRIPT = """
local ready = KEYS[2]
local unacked = KEYS[3]
local uid = ARGV[1]
local timestamp = redis.call('ZSCORE', unacked, uid)
if timestamp then
    redis.call('LPUSH', ready, uid)
    redis.call('ZREM', unacked, uid)
    return true
end
return false
"""


def _make_key(name, suffix):
    return f"{NAMESPACE}:{name}.{suffix}"


class RedisQueue:
    def __init__(self, redis, name, retry_time=30):
        self.redis = redis
        self.retry_time = retry_time
        self._items = _make_key(name, "items")
        self._ready = _make_key(name, "ready")
        self._unacked = _make_key(name, "unacked")
        self._keys = [self._items, self._ready, self._unacked]
        self._get = redis.register_script(_GET_SCRIPT)
        self._ack = redis.register_script(_ACK_SCRIPT)
        self._fail = redis.register_script(_FAIL_SCRIPT)

    def info(self):
        with self.redis.pipeline() as pipe:
            pipe.llen(self._ready)
            pipe.zcard(self._unacked)
            num_ready, num_unacked = pipe.execute()
            return {
                "total": num_ready + num_unacked,
                "ready": num_ready,
                "unacked": num_unacked,
            }

    def put(self, item, uid):
        with self.redis.pipeline() as pipe:
            pipe.hset(self._items, uid, item)
            pipe.lpush(self._ready, uid)
            pipe.execute()

    def get(self):
        timestamp = time.time()
        return self._get(keys=self._keys, args=[timestamp, self.retry_time])

    def ack(self, uid):
        return self._ack(keys=self._keys, args=[uid])

    def fail(self, uid):
        return self._fail(keys=self._keys, args=[uid])

    def purge(self):
        with self.redis.pipeline() as pipe:
            pipe.delete(self._items)
            pipe.delete(self._ready)
            pipe.delete(self._unacked)
            pipe.execute()

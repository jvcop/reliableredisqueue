import time

from . import _scripts

NAMESPACE = "rrq"


def _make_key(name, suffix):
    return f"{NAMESPACE}:{name}:{suffix}"


class RedisQueue:
    def __init__(self, redis, name, retry_time=30):
        self.redis = redis
        self.retry_time = retry_time
        self._items = _make_key(name, "items")
        self._ready = _make_key(name, "ready")
        self._unacked = _make_key(name, "unacked")
        self._keys = [self._items, self._ready, self._unacked]
        self._get = redis.register_script(_scripts.GET)
        self._ack = redis.register_script(_scripts.ACK)
        self._fail = redis.register_script(_scripts.FAIL)

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
            pipe.lpush(self._ready, uid)
            pipe.hset(self._items, uid, item)
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

import argparse
import functools
import gc
import time

from redis import Redis
from reliableredisqueue import Queue

_ROUNDS = 3


def benchmark(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        gc.disable()
        timings = []
        try:
            for _ in range(_ROUNDS):
                timings.append(func(*args, **kwargs))
        finally:
            gc.enable()

        return min(timings)

    return wrapper


@benchmark
def bench_consumer(queue, num_jobs):
    for index in range(num_jobs):
        queue.put(index)

    t0 = time.time()
    for _ in range(num_jobs):
        job = queue.get(block=False)
        queue.ack(job)

    t1 = time.time()
    queue.purge()
    return t1 - t0


@benchmark
def bench_producer(queue, num_jobs):
    t0 = time.time()
    for index in range(num_jobs):
        queue.put(index)

    t1 = time.time()
    queue.purge()
    return t1 - t0


class SimpleQueue:
    def __init__(self, redis):
        self.redis = redis
        self._ready = "rrq:baseline.ready"
        self._unacked = "rrq:baseline.unacked"

    def put(self, item):
        self.redis.lpush(self._ready, item)

    def get(self, block=False):
        assert not block
        return self.redis.rpoplpush(self._ready, self._unacked)

    def ack(self, item):
        self.redis.lrem(self._unacked, 1, item)

    def purge(self):
        with self.redis.pipeline() as pipe:
            pipe.delete(self._ready)
            pipe.delete(self._unacked)
            pipe.execute()


def cli():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("target", choices=("consumer", "producer"))
    arg_parser.add_argument("--redis-url", default="redis://localhost:6379")
    arg_parser.add_argument("--baseline", action="store_true")
    args = arg_parser.parse_args()
    if args.target == "consumer":
        func = bench_consumer
    else:
        func = bench_producer

    redis = Redis.from_url(args.redis_url)
    if args.baseline:
        queue = SimpleQueue(redis)
    else:
        queue = Queue(redis, "__bench", serializer=None)

    num_jobs = 10000
    try:
        timing = func(queue, num_jobs)
        jobs_per_sec = round(num_jobs / timing, 3)
        print(f"{jobs_per_sec} jobs/s")
    finally:
        queue.purge()


if __name__ == "__main__":
    cli()

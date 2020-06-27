import argparse
import functools
import gc
import random
import time

from redis import Redis
from reliableredisqueue import Queue

random.seed(73)

_NUM_JOBS = 10000
_NUM_ROUNDS = 3


def benchmark(func):
    @functools.wraps(func)
    def wrapper(queue, num_jobs, **kwargs):
        gc.disable()
        timings = []
        try:
            for _ in range(_NUM_ROUNDS):
                timings.append(func(queue, num_jobs, **kwargs))
        finally:
            gc.enable()
            queue.purge()

        return min(timings)

    return wrapper


@benchmark
def benchmark_consumer(queue, num_jobs, unacked_prob=0.0):
    for index in range(num_jobs):
        queue.put(index)

    rand = random.random
    t0 = time.time()
    for _ in range(num_jobs):
        job = queue.get(block=False)
        if not unacked_prob or rand() > unacked_prob:
            queue.ack(job)

    t1 = time.time()
    queue.purge()
    return t1 - t0


@benchmark
def benchmark_producer(queue, num_jobs):
    t0 = time.time()
    for index in range(num_jobs):
        queue.put(index)

    t1 = time.time()
    queue.purge()
    return t1 - t0


class SimpleQueue:
    def __init__(self, redis):
        self.redis = redis
        self._ready = "rrq:baseline:ready"
        self._unacked = "rrq:baseline:unacked"

    def put(self, item):
        self.redis.lpush(self._ready, item)

    def get(self, block=False):
        assert not block, "baseline does not support blocking"
        return self.redis.rpoplpush(self._ready, self._unacked)

    def ack(self, item):
        self.redis.lrem(self._unacked, 1, item)

    def purge(self):
        with self.redis.pipeline() as pipe:
            pipe.delete(self._ready)
            pipe.delete(self._unacked)
            pipe.execute()


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis",
        dest="redis_url",
        metavar="URL",
        default="redis://localhost:6379",
        help="URL to Redis")
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument(
        "--retry-time",
        metavar="FLOAT",
        type=float,
        default=60.0,
        help="number of seconds after which unacked jobs will be retried")
    parser.add_argument(
        "--jobs",
        dest="num_jobs",
        metavar="INT",
        type=int,
        default=_NUM_JOBS,
        help="number of jobs to enqueue/dequeue")
    subparsers = parser.add_subparsers()
    _add_consumer_command(subparsers)
    _add_producer_command(subparsers)
    args = parser.parse_args()
    args.func(args)


def _add_consumer_command(subparsers):
    parser = subparsers.add_parser("consumer")
    parser.add_argument(
        "--unacked-prob",
        metavar="FLOAT",
        type=float,
        default=0.0,
        help="probability of not acking a job")
    parser.set_defaults(func=_run_consumer_benchmark)


def _add_producer_command(subparsers):
    parser = subparsers.add_parser("producer")
    parser.set_defaults(func=_run_producer_benchmark)


def _run_consumer_benchmark(args):
    queue = _make_queue(args)
    _run_benchmark(
        benchmark_consumer,
        queue,
        args.num_jobs,
        kwargs={"unacked_prob": args.unacked_prob})


def _run_producer_benchmark(args):
    queue = _make_queue(args)
    _run_benchmark(benchmark_producer, queue, args.num_jobs)


def _make_queue(args):
    redis = Redis.from_url(args.redis_url)
    if args.baseline:
        return SimpleQueue(redis)

    return Queue(redis, "__bench", retry_time=args.retry_time, serializer=None)


def _run_benchmark(func, queue, num_jobs, kwargs=None):
    elapsed_time = func(queue, num_jobs, **(kwargs or {}))
    jobs_per_sec = round(num_jobs / elapsed_time, 3)
    print(f"{jobs_per_sec} jobs/s")


if __name__ == "__main__":
    cli()

import collections
import json
import time
import uuid

from . import _backend

Job = collections.namedtuple("Job", "uid item")


class Queue:
    def __init__(self, redis, name, retry_time=60.0, serializer=json):
        """Create a queue object with the given name.

        The following keys will be created in Redis:

        - rrq:{name}:ready
        - rrq:{name}:unacked
        - rrq:{name}:items

        retry_time must be a non-negative number or a timedelta
        object. Items retrieved from the queue that are neither acked
        nor failed in time will eventually be requeued.

        serializer will be used to serialize items added to and
        deserialize items retrieved from the queue. Set serializer to
        None if serialization/deserialization is not needed.

        """
        if hasattr(retry_time, "total_seconds"):
            retry_time = retry_time.total_seconds()

        if retry_time <= 0:
            raise ValueError("retry_time must be > 0")

        self.retry_time = retry_time
        self.serializer = serializer
        self._queue = _backend.RedisQueue(redis, name, retry_time=retry_time)

    def info(self):
        """Return a dict with statistics about the queue. This
        includes the total number of items, as well as the number of
        ready items and unacked items.

        """
        return self._queue.info()

    def put(self, item):
        """Put an item into the queue.

        item must be serializable in terms of the queue's serializer.

        """
        item_str = self._serialize(item)
        self._queue.put(item_str, _unique_id())

    def get(self, block=True, timeout=None):
        """Retrieve an item from the queue.

        By default, the call will block until an item is available. If
        timeout is a non-negative number, the call will block at most
        timeout seconds. Set block to false to return immediately.

        """
        if not block:
            return self._get()

        # N.B. Blocking commands are not allowed in Lua scripts so we
        # have to implement blocking here.
        if timeout is None:
            return self._get_no_timeout()

        return self._get_timeout(timeout)

    def ack(self, job_or_uid):
        """Acknowledge a job."""
        if hasattr(job_or_uid, "uid"):
            uid = job_or_uid.uid
        else:
            uid = job_or_uid

        self._queue.ack(uid)

    def fail(self, job_or_uid):
        """Mark a job as failed, making it available for redelivery.

        Strictly speaking, this operation is optional as the job will
        be failed implicitly after the retry time.

        """
        if hasattr(job_or_uid, "uid"):
            uid = job_or_uid.uid
        else:
            uid = job_or_uid

        self._queue.fail(uid)

    def purge(self):
        """Delete all jobs from the queue."""
        self._queue.purge()

    def _serialize(self, item):
        if self.serializer is None:
            return item

        return self.serializer.dumps(item)

    def _deserialize(self, item_str):
        if self.serializer is None:
            return item_str

        return self.serializer.loads(item_str)

    def _get(self):
        results = self._queue.get()
        if not results:
            return None

        uid, item_str = results
        item = self._deserialize(item_str)
        return Job(uid.decode("utf-8"), item)

    _block_time = 0.0005

    def _get_no_timeout(self):
        while True:
            job = self._get()
            if job is not None:
                return job

            time.sleep(self._block_time)

    def _get_timeout(self, timeout):
        if hasattr(timeout, "total_seconds"):
            timeout = timeout.total_seconds()

        if timeout < 0:
            raise ValueError("timeout must be a non-negative number")

        end = time.time() + timeout
        initial_sleep_time = self._block_time
        while True:
            remaining = end - time.time()
            if remaining <= 0:
                break

            job = self._get()
            if job is not None:
                return job

            time.sleep(min(remaining, initial_sleep_time))

        return None


def _unique_id():
    return uuid.uuid4().hex

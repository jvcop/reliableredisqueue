from datetime import timedelta
import time
import types
from unittest.mock import MagicMock

from fakeredis import FakeRedis
import pytest

from reliableredisqueue import Queue


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture
def queue(redis):
    return Queue(redis, "test")


def test_put(queue):
    queue.put({"foo": 0})
    queue.put({"bar": 1})
    assert queue.info() == {"total": 2, "ready": 2, "unacked": 0}


def test_get(queue):
    assert queue.get(block=False) is None
    queue.put({"foo": 0})
    queue.put({"bar": 1})
    foo = queue.get()
    assert foo.item == {"foo": 0}
    assert queue.info() == {"total": 2, "ready": 1, "unacked": 1}
    bar = queue.get()
    assert bar.item == {"bar": 1}
    assert queue.info() == {"total": 2, "ready": 0, "unacked": 2}


def test_ack(queue):
    queue.put({"foo": 0})
    queue.put({"bar": 1})
    foo = queue.get()
    bar = queue.get()
    queue.ack(foo)
    assert queue.info() == {"total": 1, "ready": 0, "unacked": 1}
    queue.ack(bar)
    assert queue.info() == {"total": 0, "ready": 0, "unacked": 0}


def test_ack_idempotent(queue):
    queue.put("foobar")
    foo = queue.get()
    queue.ack(foo)
    assert queue.info() == {"total": 0, "ready": 0, "unacked": 0}
    queue.ack(foo)
    assert queue.info() == {"total": 0, "ready": 0, "unacked": 0}
    queue.fail(foo)
    assert queue.get(block=False) is None
    assert queue.info() == {"total": 0, "ready": 0, "unacked": 0}


def test_ack_unknown_uid(queue):
    job = types.SimpleNamespace()
    job.uid = "01234567890"
    queue.ack(job)
    assert queue.info() == {"total": 0, "ready": 0, "unacked": 0}


def test_fail(queue):
    queue.put({"foo": 0})
    queue.put({"bar": 1})
    foo = queue.get()
    bar = queue.get()
    queue.fail(foo)
    assert queue.info() == {"total": 2, "ready": 1, "unacked": 1}
    queue.fail(bar)
    assert queue.info() == {"total": 2, "ready": 2, "unacked": 0}


def test_fail_idempotent(queue):
    queue.put("foo")
    foo = queue.get()
    queue.fail(foo)
    assert queue.info() == {"total": 1, "ready": 1, "unacked": 0}
    queue.fail(foo)
    assert queue.info() == {"total": 1, "ready": 1, "unacked": 0}
    queue.ack(foo)
    assert queue.info() == {"total": 1, "ready": 1, "unacked": 0}


def test_fail_unknown_uid(queue):
    job = types.SimpleNamespace()
    job.uid = "01234567890"
    queue.fail(job)
    assert queue.info() == {"total": 0, "ready": 0, "unacked": 0}


def test_purge(queue):
    queue.put("foobar")
    queue.purge()
    assert queue.info() == {"total": 0, "ready": 0, "unacked": 0}


@pytest.mark.parametrize("retry_time", (0, -12))
def test_invalid_retry_time(redis, retry_time):
    with pytest.raises(ValueError):
        Queue(redis, "test", retry_time=retry_time)


def test_retry_time(redis):
    queue = Queue(redis, "test", retry_time=timedelta(seconds=0.5))
    assert queue.retry_time == 0.5
    item = {"test": True}
    queue.put(item)
    job = queue.get()
    assert job.item == item
    time.sleep(0.55)
    job = queue.get()
    assert job.item == item


def test_ack_after_retry_time(redis):
    queue = Queue(redis, "test", retry_time=timedelta(seconds=0.5))
    assert queue.retry_time == 0.5
    item = {"test": True}
    queue.put(item)
    job = queue.get()
    time.sleep(0.55)
    queue.ack(job)
    # Requeue is only triggered on GET, so the ACK is effective.
    job = queue.get(block=False)
    assert job is None


def test_fail_after_retry_time(redis):
    queue = Queue(redis, "test", retry_time=timedelta(seconds=0.5))
    assert queue.retry_time == 0.5
    item = {"test": True}
    queue.put(item)
    job = queue.get()
    assert job.item == item
    time.sleep(0.55)
    queue.fail(job)
    job = queue.get(block=False)
    assert job is not None


def test_blocking(queue):
    timeout = 0.1
    queue.put({"test": True})
    job = queue.get(timeout=timeout)
    assert job is not None
    job = queue.get(timeout=timeout)
    assert job is None


def test_serializer(redis):
    item = {"test": True}
    item_as_json = b'{"test": true}'
    serializer = MagicMock()
    serializer.loads.return_value = item
    serializer.dumps.return_value = item_as_json
    queue = Queue(redis, "test", serializer=serializer)
    queue.put(item)
    serializer.dumps.assert_called_with(item)
    job = queue.get()
    serializer.loads.assert_called_with(item_as_json)
    assert job.item == item


def test_no_serializer(redis):
    item_as_json = '{"test": true}'
    queue = Queue(redis, "test", serializer=None)
    queue.put(item_as_json)
    assert queue.get().item == item_as_json.encode()

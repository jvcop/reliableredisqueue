==================
reliableredisqueue
==================

A lightweight reliable queue based on `Redis <http://www.redis.io>`_.

Project status
==============

This project is still in its infancy and breaking changes are likely. Also, optimization hasn't been a priority yet.

Usage
=====

>>> queue = Queue(redis, "demo", retry_time=30.0)
>>> queue.put("foo bar")
>>> queue.put("baz")
>>> job = queue.get()
Job(uid='30d645a822614417902e76ee7a9b1d52', item='foo bar')
>>> queue.ack(job)

Ack/Fail
========

Once processing of an item was successful, the item should be *acknowledged*, or *acked*: :code:`queue.ack(job)`. This will remove it from the queue. On the other hand, if processing failed, the item should be *failed*: :code:`queue.fail(job)`. The item will be requeued so that it can be processed again at a later time.

Strictly speaking, *failing* is not required: Items for which no acknowledgement arrived in time will eventually be failed implicitly. The time can be controlled through the queue's :code:`retry_time` argument, which defaults to 60 seconds.

Reliability
===========

Ultimately, the reliability depends on the configuration of `Redis`. That being said, this implementation tries to prevent data loss even if the power cable is unplugged in the middle of an operation.

Tests
=====

To run the tests, execute

.. code-block:: bash

    make
    make test

Benchmarks
==========

There are two kinds of benchmarks: consumer and producer. Benchmarks run multiple rounds and report the best timing.

For consumers, run:

.. code-block:: bash

    make
    bin/python benchmark/bench.py consumer

For producers, run:

.. code-block:: bash

    make
    bin/python benchmark/bench.py producer

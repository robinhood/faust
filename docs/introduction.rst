.. _introduction:

=====================
  Introducing Faust
=====================

.. include:: includes/tags.txt

**Table of Contents**

.. contents::
    :local:
    :depth: 1

What can it do?
===============

**Agents**
    Process infinite streams in a straightforward manner using
    asynchronous generators. The concept of "agents" comes from
    the actor model, and means the stream processor can execute
    concurrently on many CPU cores, and on hundreds of machines
    at the same time.

    Use regular Python syntax to process streams and reuse your favorite
    libraries:

    .. sourcecode:: python

        @app.agent()
        async def process(stream):
            async for value in stream:
                process(value)

**Tables**
    Tables are sharded dictionaries that enable stream processors
    to be stateful with persistent and durable data.

    Streams are partitioned to keep relevant data close, and can be easily
    repartitioned to achieve the topology you need.

    In this example we repartition an order stream by account id, to count
    orders in a distributed table:

    .. sourcecode:: python

        import faust

        # this model describes how message values are serialized
        # in the Kafka "orders" topic.
        class Order(faust.Record, serializer='json'):
            account_id: str
            product_id: str
            amount: int
            price: float

        orders_kafka_topic = app.topic('orders', value_type=Order)

        # our table is sharded amongst worker instances, and replicated
        # with standby copies to take over if one of the nodes fail.
        order_count_by_account = app.Table('order_count', default=int)

        @app.agent(orders_kafka_topic)
        async def process(orders: faust.Stream[Order]) -> None:
            async for order in orders.group_by(Order.account_id):
                order_count_by_account[order.account_id] += 1

    If we start multiple instances of this Faust application on many machines,
    any order with the same account id will be received by the same
    stream processing agent, so the count updates correctly in the table.

    Sharding/partitioning is an essential part of stateful stream
    processing applications, so take this into account when designing your
    system, but note that streams can also be processed in round-robin order
    so you can use Faust for event processing and as a task queue also.

**Asynchronous with** :mod:`asyncio`
    Faust takes full advantage of :mod:`asyncio` and the new :keyword:`async <async
    def>`/:keyword:`await` keywords in Python 3.6+ to run multiple stream
    processors in the same process, along with web servers and other network
    services.

    Thanks to Faust and :mod:`asyncio` you can now embed your stream processing
    topology into your existing asyncio/gevent/eventlet/Twisted/Tornado
    applications.

**Faust is...**
    .. include:: includes/introduction.txt

----------------------------------------------------

.. topic:: Faust is used for...

    .. hlist::
        :columns: 2

        - **Event Processing**

        - **Distributed Joins & Aggregations**

        - **Machine Learning**

        - **Asynchronous Tasks**

        - **Distributed Computing**

        - **Data Denormalization**

        - **Intrusion Detection**

        - **Realtime Web & Web Sockets.**

        - **and much more...**

How do I use it?
================

.. topic:: Step 1: Add events to your system

    - Was an account created? Publish to Kafka.

    - Did a user change their password? Publish to Kafka.

    - Did someone make an order, create a comment, tag something, ...?
      Publish it all to Kafka!

.. topic:: Step 2: Use Faust to process those events

    Some ideas based on the events mentioned above:

    - Send email when an order is dispatches.

    - Find orders created with no corresponding dispatch event for
      more than three consecutive days.

    - Find accounts that changed their password from a suspicious IP address.

    - Starting to get the idea?

What do I need?
==================

.. sidebar:: Version Requirements
    :subtitle: Faust version 1.0 runs on

    **Core**

    - Python 3.6 or later.
    - Kafka 0.10.1 or later.

    **Extensions**

    - RocksDB 5.0 or later, python-rocksdb

Faust requires Python 3.6 or later, and a running Kafka broker.

There's no plan to support earlier Python versions.
Please get in touch if this is something you want to work on.


Extensions
==========

+--------------+-------------+--------------------------------------------------+
| **Name**     | **Version** | **Bundle**                                       |
+--------------+-------------+--------------------------------------------------+
| rocksdb      | 5.0         | ``pip install faust[rocksdb]``                   |
+--------------+-------------+--------------------------------------------------+
| redis        | aredis 1.1  | ``pip install faust[redis]``                     |
+--------------+-------------+--------------------------------------------------+
| datadog      | 0.20.0      | ``pip install faust[datadog]``                   |
+--------------+-------------+--------------------------------------------------+
| statsd       | 3.2.1       | ``pip install faust[statsd]``                    |
+--------------+-------------+--------------------------------------------------+
| uvloop       | 0.8.1       | ``pip install faust[uvloop]``                    |
+--------------+-------------+--------------------------------------------------+
| gevent       | 1.4.0       | ``pip install faust[gevent]``                    |
+--------------+-------------+--------------------------------------------------+
| eventlet     | 1.16.0      | ``pip install faust[eventlet]``                  |
+--------------+-------------+--------------------------------------------------+

Optimizations
-------------

These can be all installed using ``pip install faust[fast]``:

+--------------+-------------+--------------------------------------------------+
| **Name**     | **Version** | **Bundle**                                       |
+--------------+-------------+--------------------------------------------------+
| aiodns       | 1.1.0       | ``pip install faust[aiodns]``                    |
+--------------+-------------+--------------------------------------------------+
| cchardet     | 1.1.0       | ``pip install faust[cchardet]``                  |
+--------------+-------------+--------------------------------------------------+
| ciso8601     | 2.1.0       | ``pip install faust[ciso8601]``                  |
+--------------+-------------+--------------------------------------------------+
| cython       | 0.9.26      | ``pip install faust[cython]``                    |
+--------------+-------------+--------------------------------------------------+
| setproctitle | 1.1.0       | ``pip install faust[setproctitle]``              |
+--------------+-------------+--------------------------------------------------+

Debugging extras
----------------

These can be all installed using ``pip install faust[debug]``:

+--------------+-------------+--------------------------------------------------+
| **Name**     | **Version** | **Bundle**                                       |
+--------------+-------------+--------------------------------------------------+
| aiomonitor   | 0.3         | ``pip install faust[aiomonitor]``                |
+--------------+-------------+--------------------------------------------------+
| setproctitle | 1.1.0       | ``pip install faust[setproctitle]``              |
+--------------+-------------+--------------------------------------------------+

.. note::

    See bundles in the :ref:`installation` instructions section of
    this document for a list of supported :pypi:`setuptools` extensions.

.. admonition:: To specify multiple extensions at the same time

    separate extensions with the comma:

    .. sourcecode:: console

        $ pip install faust[uvloop,fast,rocksdb,datadog,redis]

.. admonition:: RocksDB On MacOS Sierra

    To install :pypi:`python-rocksdb` on MacOS Sierra you need to
    specify some additional compiler flags:

    .. sourcecode:: console

        $ CFLAGS='-std=c++11 -stdlib=libc++ -mmacosx-version-min=10.10' \
            pip install -U --no-cache python-rocksdb

Design considerations
=====================

Modern Python
    Faust uses current Python 3 features such as
    :keyword:`async <async def>`/:keyword:`await` and type annotations. It's statically
    typed and verified by the `mypy`_ type checker. You can take advantage of
    type annotations when writing Faust applications, but
    this is not mandatory.

Library
    Faust is designed to be used as a library, and embeds into
    any existing Python program, while also including helpers that
    make it easy to deploy applications without boilerplate.

Supervised
    The Faust worker is built up by many different services that start
    and stop in a certain order.  These services can be managed by
    supervisors, but if encountering an irrecoverable error such
    as not recovering from a lost Kafka connections, Faust is designed
    to crash.

    For this reason Faust is designed to run inside a process supervisor
    tool such as `supervisord`_, `Circus`_, or one provided by your
    Operating System.

Extensible
    Faust abstracts away storages, serializers, and even message transports,
    to make it easy for developers to extend Faust with new capabilities,
    and integrate into your existing systems.

Lean
    The source code is short and readable and serves as a good starting point
    for anyone who wants to learn how Kafka stream processing systems work.

.. _`mypy`: http://mypy-lang.org
.. _`supervisord`: http://supervisord.org
.. _`circus`: http://circus.readthedocs.io/

.. include:: includes/resources.txt

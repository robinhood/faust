.. _intro:

=============================
 Welcome to Faust
=============================

.. include:: includes/tags.txt

**Table of Contents**

.. contents::
    :local:
    :depth: 1

What is Faust?
==============

**Streams**
    With Faust you process streams in a straightforward manner using iterators,
    and the concept of having "Agents" make writing stream processors easy.

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
        async def process(order: faust.Stream[Order]) -> None:
            async for order in orders.group_by(Order.account_id):
                order_count_by_account[order.account_id] += 1

    If we start multiple instances of this Faust application on many machines,
    any order with the same account id will be received by the same
    stream processing agent, so the count updates correctly in the table.

    Sharding and partitioning is an essential part of stateful stream
    processing applications, so take this into account when designing your
    system, but note that streams can also be processed in round-robin order
    so you can use Faust for event processing and as a task queue also.

**Asynchronous with** :mod:`asyncio`
    Faust takes advantage of :mod:`asyncio` and the new :keyword:`async <async
    def>`/:keyword:`await` keywords in Python 3.6+ to run multiple stream
    processors in the same process, along with web servers and other network
    services.

    Thanks to Faust and :mod:`asyncio` you can now embed your stream processing
    topology into your existing asyncio/gevent/eventlet/Twisted/Tornado
    applications.

**Faust is...**
    .. include:: includes/introduction.txt


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

What do I need?
===============

.. sidebar:: Version Requirements
    :subtitle: Faust version 1.0 runs on

    **Core**

    - Python 3.6 or later.
    - Kafka 0.10.1 or later.

    **Extensions**

    - RocksDB 5.0 or later, python-rocksdb

Faust requires Python 3.6 or later, and a running Kafka broker.

There's currently no plan to port Faust to earlier Python versions;
please get in touch if you want to work on this.

.. admonition:: RocksDB On MacOS Sierra

    To install :pypi:`python-rocksdb` on MacOS Sierra you need to
    specify some additional compiler flags:

    .. sourcecode:: console

        $ CFLAGS='-std=c++11 -stdlib=libc++ -mmacosx-version-min=10.10' \
            pip install -U --no-cache python-rocksdb

Extensions
----------

+--------------+-------------+--------------------------------------------------+
| **Name**     | **Version** | **Bundle**                                       |
+--------------+-------------+--------------------------------------------------+
| rocksdb      | 5.0         | ``pip install faust[rocksdb]``                   |
+--------------+-------------+--------------------------------------------------+
| statsd       | 3.2.1       | ``pip install faust[statsd]``                    |
+--------------+-------------+--------------------------------------------------+
| uvloop       | 0.8.1       | ``pip install faust[uvloop]``                    |
+--------------+-------------+--------------------------------------------------+
| aiodns       | 1.1         | ``pip install faust[fast]``                      |
+--------------+-------------+--------------------------------------------------+
| setproctitle | 1.1         | ``pip install faust[setproctitle]`` (also debug) |
+--------------+-------------+--------------------------------------------------+
| aiomonitor   | 0.3         | ``pip install faust[debug]``                     |
+--------------+-------------+--------------------------------------------------+

.. note::

    See bundles in the :ref:`installation` instructions section of
    this document for a list of supported :pypi:`setuptools` extensions.

.. admonition:: To specify multiple extensions at the same time

    separate extensions with the comma:

    .. sourcecode:: console

        $ pip install fuast[uvloop,fast,rocksdb,statsd]

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

What is Kafka?
==============================================


.. include:: includes/kafka.txt

Examples
========

.. topic:: Process events in a Kafka topic

    .. sourcecode:: python

        import faust

        orders_topic = app.topic('orders', value_serializer='json')

        @app.agent(orders_topic)
        async def process_order(orders):
            async for order in orders:
                print(order['product_id'])

.. topic:: Describe stream data using models

    .. sourcecode:: python

        from datetime import datetime
        import faust

        class Order(faust.Record, serializer='json', isodates=True):
            id: str
            user_id: str
            product_id: str
            amount: float
            price: float
            date_created: datatime = None
            date_updated: datetime = None

        orders_topic = app.topic('orders', value_type=Order)

        @app.agent(orders_topic)
        async def process_order(orders):
            async for order in orders:
                print(order.product_id)


.. topic:: Use async. I/O to perform other actions while processing the stream

    .. sourcecode:: python

        # [...]
        @app.agent(orders_topic)
        async def process_order(orders):
            session = aiohttp.ClientSession()
            async for order in orders:
                async with session.get(f'http://e.com/api/{order.id}/') as resp:
                    product_info = await request.text()
                    await session.post(
                        f'http://cache/{order.id}/', data=product_info)

.. topic:: Buffer up many events at a time

    Here we get up to 100 events within a 30 second window:

    .. sourcecode:: python

        # [...]
        async for orders_batch in orders.take(100, within=30.0):
            print(len(orders))

.. topic:: Aggregate information into a table

    .. sourcecode:: python

        orders_by_country = app.Table('orders_by_country', default=int)

        @app.agent(orders_topic)
        async def process_order(orders):
            async for order in orders.group_by(order.country_origin):
                country = order.country_origin
                orders_by_country[country] += 1
                print(f'Orders for country {country}: {orders_by_country[country]}')

.. topic:: Aggregate information using a window

    Count number of orders by country, within the last two days:

    .. sourcecode:: python

        orders_by_country = app.Table(
            'orders_by_country',
            default=int,
        ).hopping(timedelta(days=2))

        async for order in orders_topic.stream():
            orders_by_country[order.country_origin] += 1
            # values in this table are not concrete! access .current
            # for the value related to the time of the current event
            print(orders_by_country[order.country_origin].current())

.. topic:: Send something to be processed later

    TODO This is not implemented yet

    .. sourcecode:: python

        async for event in my_topic.stream():
            # forward to other topic, but only after two days
            await topic.send(event, eta=timedelta(days=2))

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

Live happily, die quickly
    Faust is programmed to crash on encountering an error such as losing
    the connection to Kafka.  That means error recovery is up to supervisor
    tools such as `supervisord`_, `Circus`_, or one provided by your Operating
    System.

Extensible
    Faust abstracts away storages, serializers, and even message transports,
    to make it easy for developers to extend Faust with new capabilities,
    and integrate into your existing systems.

Lean
    The source code is short and readable and serves as a good starting point
    for anyone who wants to learn how Kafka stream processing systems work.

Upcoming Features
=================

Some features are planned, but not available in the first version.

Joins
    Faust will soon support Table/Table, Stream/Stream, and Table/Stream
    joins.
Table HTTP API
    Query tables using the Faust HTTP server.
Delayed messages
    Send messages to be processed later, e.g. *"after two days, do something."*

.. _installation:

.. include:: includes/installation.txt

.. _`mypy`: http://mypy-lang.org

.. _`supervisord`: http://supervisord.org

.. _`circus`: http://circus.readthedocs.io/

.. include:: includes/resources.txt

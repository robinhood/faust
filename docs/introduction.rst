.. _intro:

=============================
 Introduction to Faust
=============================

.. contents::
    :local:
    :depth: 1

.. include:: includes/introduction.txt

.. topic:: Faust can be used for...

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

    - Python 3.6
    - Kafka 0.10 or later.

    **Extensions**

    - RocksDB 5.0 or later, python-rocksdb

Faust requires Python 3.6 or later, and a running Kafka broker.

There's currently no plan to port Faust to earlier Python versions,
please get in touch if this is something that you want to work on.

Extensions
----------

+------------+-------------+--------------------------------------------------+
| **Name**   | **Version** | **Bundle**                                       |
+------------+-------------+--------------------------------------------------+
| rocksdb    | 5.0         | ``pip install faust[rocksdb]``                   |
+------------+-------------+--------------------------------------------------+
| uvloop     | 0.8.0       | ``pip install faust[uvloop]``                    |
+------------+-------------+--------------------------------------------------+
| aiomonitor | 0.2.1       | ``pip install faust[debug]``                     |
+------------+-------------+--------------------------------------------------+
| aiodns     | 1.0         | ``pip install faust[fast]``                      |
+------------+-------------+--------------------------------------------------+
| fastavro   | 0.12        | ``pip install faust[fast]``                      |
+------------+-------------+--------------------------------------------------+

.. note::

    You can install multiple bundles at the same time:

    .. sourcecode:: console

        $ pip install -U faust[fast,rocksdb,uvloop]

    and also use them in requirement files:

    :file:`requirements.txt`:

    .. sourcecode:: text

        faust[fast,rocksdb,uvloop]

What is Kafka?
==============================================


.. include:: includes/kafka.txt



How do I use it?
================

.. topic:: Step 1: Add events to your system

    - Was an account created? Publish to Kafka.

    - Did someone change their password? Publish to Kafka.

    - Did someone make an order, create a comment, tag something, ...?
      Publish it all to Kafka!

.. topic:: Step 2: Use Faust to process those events

    Some ideas based around the events mentioned above:

    - Send email once an order is dispatched.

    - Find orders that were made, but no associated dispatch event
      after three days.

    - Find accounts that changed their password from a suspicious IP address.

    - Starting to get the idea?

Examples
========

.. topic:: Iterate over events in a topic

    .. sourcecode:: python

        orders_topic = app.topic('orders', value_type=Order)
        async for order in orders_topic.stream():
            print(order.product_id)

.. topic:: Asynchronously processing events in a topic

    .. sourcecode:: python

        session = aiohttp.ClientSession()
        async for order in orders_topic.stream():
            product_info = await session.get(f'http://e.com/api/{order.id}/')
            await aiohttp.post(f'http://cache/{order.id}/', data=product_info)

.. topic:: Buffer up many events at a time

    Here we get up to 100 events within a 30 second window:

    .. sourcecode:: python

        async for orders in orders_topic.stream().take(100, within=30.0):
            print(len(orders))

.. topic:: Aggregate information into a table

    .. sourcecode:: python

        orders_by_country = app.Table('orders_by_country', default=int)

        async for order in orders_topic.stream():
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

    async for event in my_topic.stream():
        # forward to other topic, but only after two days
        await topic.send(event, eta=timedelta(days=2))

Design considerations
=====================

Modern Python
    Faust uses modern Python 3 features such as ``async``/``await`` and type
    annotations. It's statically typed and verified by the `mypy`_
    type checker. You can take advantage of type annotations when writing
    Faust applications, but this is not mandatory.

Library
    Faust is designed to be used as a library, and embeds into
    any existing Python program, while also including helpers that
    make it easy to deploy applications without boilerplate.

Live happy, die hard
    Faust is programmed to crash on encountering an error such as losing
    the connection to Kafka.  This means error recovery is up to supervisor
    tools such as `supervisord`_, `Circus`_, or one provided by your Operating
    System.

Extensible
    Faust abstracts away storages, serializers and even message transports,
    to make it easy for developers to extend Faust with new capabilities,
    and integrate into your existing systems.

Upcoming Features
=================

Some features are planned, but not available in the first version

-Joins-
    Faust will soon support Table/Table, Stream/Stream, and Table/Stream
    joins.

-Table HTTP API-
    Query tables using the Faust HTTP server.

-Delayed messages-
    Send messages to be processed later, e.g. *"after two days, do something"*.

.. _`mypy`: http://mypy-lang.org

.. _`supervisord`: http://supervisord.org

.. _`circus`: http://circus.readthedocs.io/

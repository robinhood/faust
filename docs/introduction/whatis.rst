=============================
 What is Faust?
=============================

.. include:: ../includes/tags.txt

**Table of Contents**

.. contents::
    :local:
    :depth: 1

Basics
======

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
    .. include:: ../includes/introduction.txt


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

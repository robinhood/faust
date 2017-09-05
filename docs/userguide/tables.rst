.. _guide-tables:

============================================================
  Tables and Windowing
============================================================

.. contents::
    :local:
    :depth: 2

Tables
======

Basics
------

A table is a distributed in-memory dictionary. Tables are backed by a kafka
changelog topic for persistence and fault-tolerance. This allows us to replay
the changelog upon failure allowing to rebuild the state of the Table before
the fault.

Tables can be initialized as follows:

.. sourcecode:: python

    table = app.Table('totals', default=int)

Tables should be *updated within a stream iteration* in order to align the
table's partitions with the stream's partitions in order to ensure that
upon failures stream partitions are rebalanced to a different worker along
with their respective table partitions:

.. sourcecode:: python

    class Withdrawal(faust.Record):
        account: str
        amount: float

    totals = app.Table('totals', default=int)

    async for event in app.topic('withdrawals', value_type=Withdrawal).stream():
        table[event.account] += event.amount

This also ensures that producing to the changelog and committing messages
from the source happen simultaneously.

.. note::

    In Kafka 0.10.x we can have a case where changelog messages are produced
    but the source topic is not committed due to failures. This will result
    in inconsistencies in accordance with Kafka's at-least once guarantees.
    This is expected to be fixed with Kafka 0.11.0 which allows for stronger
    consistency guarantees owing to exactly-once-semantics.

Co-partitioning Tables and Streams
----------------------------------

Faust uses co-partitioning of stream partitions and their corresponding
changelog partitions in order to ensure correct distribution of stateful
processing among available clients. Therefore tables
should be sharded according to the stream. If a table needs to be sharded
differently, we should repartition the stream using :class:`~@Stream.group_by`
as follows:

.. sourcecode:: python

    withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

    country_to_total = app.Table(
        'country_to_total', default=int).tumbling(10.0, expires=10.0)

    withdrawals_stream = app.topic('withdrawals', value_type=Withdrawal).stream()
    withdrawals_by_country = withdrawals_stream.group_by(Withdrawal.country)

    async for withdrawal in withdrawals_by_country:
        country_to_total[withdrawal.country] += withdrawal.amount

Without co-partitioning Stream and Table partitions, we could end up with a
table shard ending up on a different worker than the worker processing its
corresponding Stream partition.

.. note::

    Due to this reason Table changelogs should have the same number of
    partitions as the source topic.

Table Sharding
--------------

Tables should be sharded such that the key distribution across Kafka
partitions is disjoint. This ensures that all computation for a subset of
keys happen together in the same worker process.

The following is an example of incorrect usage which may result in key
subsets across partitions being disjoint.

.. sourcecode:: python

    withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

    user_to_total = app.Table('user_to_total', default=int)
    country_to_total = app.Table(
        'country_to_total', default=int).tumbling(10.0, expires=10.0)


    @app.actor(withdrawals_topic)
    async def find_large_withdrawals(withdrawals):
        async for withdrawal in withdrawals:
            user_to_total[withdrawal.user] += withdrawal.amount
            country_to_total[withdrawal.country] += withdrawal.amount

Here the stream ``withdrawals`` is partitioned by ``Withdrawal.user`` hence the
``country_to_total`` table which is expected to partitioned by country would
end up actually being partitioned by user, resulting in the same country
being present in multiple partitions.

The above use case should be re-implemented as follows:

.. sourcecode:: python

    withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

    user_to_total = app.Table('user_to_total', default=int)
    country_to_total = app.Table(
        'country_to_total', default=int).tumbling(10.0, expires=10.0)


    @app.actor(withdrawals_topic)
    async def find_large_user_withdrawals(withdrawals):
        async for withdrawal in withdrawals:
            user_to_total[withdrawal.user] += withdrawal.amount


    @app.actor(withdrawals_topic)
    async def find_large_country_withdrawals(withdrawals):
        async for withdrawal in withdrawals.group_by(Withdrawal.country):
            country_to_total[withdrawal.country] += withdrawal.amount

Changelogging
-------------

Table updates are published to a Kafka topic for recovery upon failures. We
use Log Compaction to ensure that the changelog topic doesn't grow
exponentially, keeping the number of messages in the changelog topic ``O(number
 of keys in the table)``.

In order to publish a changelog message into Kafka for fault-tolerance the
table needs to be set explicitly. Hence, while changing values in Tables by
reference, we still need to explicitly set the value to publish to the
changelog, as shown below:

.. sourcecode:: python

    user_withdrawals = app.Table('user_withdrawals', default=list)

    async for event in app.topic('withdrawals', value_type=Withdrawal).stream():
        withdrawals = user_withdrawals[event.account]
        withdrawals.append(event.amount)
        user_withdrawals[event.account] = withdrawals

The following code would not be fault-tolerant as it would not publish to the
kafka changelog. It would still work locally but recovery upon failure would
not correctly build the state of the world before the crash.

.. sourcecode:: python

    user_withdrawals = app.Table('user_withdrawals', default=list)

    async for event in app.topic('withdrawals', value_type=Withdrawal).stream():
        withdrawals = user_withdrawals[event.account]
        withdrawals.append(event.amount)

Due to changelogging, keys and values should be serializable.

.. seealso::

    :ref:`guide-models` for more information about models and serialization.

.. note::

    Faust creates an internal changelog topic for each table. The Faust
    application should be the only client producing to the changelog topics.

.. warning::

    The most current key/value pair is serialized and published to changelog
    upon every update.

Windowing
=========

Windowing allows us to process streams while preserving state over defined
windows of time. A windowed table preserves key-value pairs according to the
configured Windowing Policy.

We support the following Window Policies:

.. class:: HoppingWindow

.. class:: TumblingWindow

How To
------

A windowed table can be defined as follows:

.. code-block:: python

    from datetime import timedelta
    views = app.Table('views', default=int).tumbling(timedelta(minutes=1),
        expires=timedelta(hours=1))

    events_topic = app.topic('events_elk', value_type=Event)

    @app.actor(events_topic)
    async def aggregate_page_views(events):
        async for event in events:
            page = event.page
            views[page] += 1
            if views[page].now() >= 10000:
                # Page is trending for current processing time window
                print('Trending now')
            if views[page].current(event) >= 10000:
                # Page would be trending in the event's time window
                print('Trending when event happend')
            if views[page].delta(timedelta(minutes=30)) > views[page].now():
                print('Less popular compared to 30 minutes back')

Out of Order Events
-------------------

Events can sometimes come out of order due to various reasons such as network
issues. Windowed Tables in Faust handle out of order events until
``expires`` seconds``. In order to handle out of order events we store separate
aggregates for each window in the last ``expires`` seconds. The space
complexity for handling out of order events is ``O(w * K)`` where ``w`` is
the number of windows in the last ``expires`` seconds and ``K`` is the number
of keys in the Table.

.. note::

    Currently we use the event timestamp for Windowing. We expect to support
    using processing time and timestamp from the message payload for Windowing.

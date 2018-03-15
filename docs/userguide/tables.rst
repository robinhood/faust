.. _guide-tables:

============================================================
  Tables and Windowing
============================================================

.. topic:: \

    *“A man sees in the world what he carries in his heart.”*

    -- Goethe, *Faust: First part*

.. contents::
    :local:
    :depth: 2

Tables
======

Basics
------

A table is a distributed in-memory dictionary, backed by a Kafka
changelog topic used for persistence and fault-tolerance. We can replay
the changelog upon network failure and node restarts, allowing us to rebuild the
state of the table as it was before the fault.

To create a table use ``app.Table``:

.. sourcecode:: python

    table = app.Table('totals', default=int)

You cannot modify a table outside of a stream operation; this means that you can
only mutate the table from within an ``async for event in stream:`` block.
We require this to align the table's partitions with the stream's, and to
ensure the source topic partitions are correctly rebalanced to a different
worker upon failure, along with any necessary table partitions.

Modifying a table outside of a stream will raise an error:

.. sourcecode:: python

    totals = app.Table('totals', default=int)

    # cannot modify table, as we are not iterating over stream
    table['foo'] += 30

This source-topic-event to table-modification-event requirement also ensures
that producing to the changelog and committing messages from the source
happen simultaneously.

.. warning::


    An abruptly terminated Faust worker can allow some changelog entries
    to go through, before having committed the source topic offsets.

    Duplicate messages may result in double-counting and other data
    consistency issues, so we are hoping to take advantage of Kafka 0.11's
    stronger consistency guarantees and new "exactly-once"-semantics features
    as soon as that is supported in a Python Kafka client.

Co-partitioning Tables and Streams
----------------------------------

When managing stream partitions and their corresponding changelog
partitions, “co-partitioning” ensures the correct distribution of stateful
processing among available clients, but one requirement is that tables and
streams must share shards.

To shard the table differently, you must first repartition the stream using
:class:`~@Stream.group_by`.

Repartion a stream:

.. sourcecode:: python

    withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

    country_to_total = app.Table(
        'country_to_total', default=int).tumbling(10.0, expires=10.0)

    withdrawals_stream = app.topic('withdrawals', value_type=Withdrawal).stream()
    withdrawals_by_country = withdrawals_stream.group_by(Withdrawal.country)

    @app.agent
    async def process_withdrawal(withdrawals):
        async for withdrawal in withdrawals.group_by(Withdrawal.country):
            country_to_total[withdrawal.country] += withdrawal.amount

If the stream and table are not co-partitioned, we could end up with a
table shard ending up on a different worker than the worker processing its
corresponding stream partition.

.. warning::

    For this reason, table changelog topics must have the same number of partitions as the
    source topic.


Table Sharding
--------------

Tables shards in Kafka must organize using a disjoint distribution of keys
so that any computation for a subset of keys always happen together in the
same worker process.

The following is an example of incorrect usage where subsets of keys are
likely to be processed by different worker processes:

.. sourcecode:: python

    withdrawals_topic = app.topic('withdrawals', key_type=str, value_type=Withdrawal)

    user_to_total = app.Table('user_to_total', default=int)
    country_to_total = app.Table(
        'country_to_total', default=int).tumbling(10.0, expires=10.0)


    @app.agent(withdrawals_topic)
    async def process_withdrawal(withdrawals):
        async for withdrawal in withdrawals:
            user_to_total[withdrawal.user] += withdrawal.amount
            country_to_total[withdrawal.country] += withdrawal.amount

Here the stream ``withdrawals`` is (implicitly) partitioned by the user ID used
as message key. So the ``country_to_total`` table, instead of being
partitioned by country name, is partitioned by the user ID. In practice,
this means that data for a country may reside on multiple partitions, and
worker instances end up with incomplete data.

To fix that reimplement your program like this, using two distinct agents
and repartition the stream by country when populating the table:

.. sourcecode:: python

    withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

    user_to_total = app.Table('user_to_total', default=int)
    country_to_total = app.Table(
        'country_to_total', default=int).tumbling(10.0, expires=10.0)


    @app.agent(withdrawals_topic)
    async def find_large_user_withdrawals(withdrawals):
        async for withdrawal in withdrawals:
            user_to_total[withdrawal.user] += withdrawal.amount


    @app.agent(withdrawals_topic)
    async def find_large_country_withdrawals(withdrawals):
        async for withdrawal in withdrawals.group_by(Withdrawal.country):
            country_to_total[withdrawal.country] += withdrawal.amount

The Changelog
-------------

Every modification to a table has a corresponding changelog update,
the changelog is used to recover data after a failure.

We store the changelog in Kafka as a topic and use log compaction
to only keep the *most recent value for a key in the log*.
Kafka periodically compacts the table, to ensure the log does not
grow beyond the number of keys in the table.

.. note::

    In production the RocksDB store allows for almost instantaneous recovery
    of tables: a worker only needs to retrieve updates missed since last time
    the instance was up.

If you change the value for a key in the table, please make sure you update
the table with the new value after:

In order to publish a changelog message into Kafka for fault-tolerance the
table needs to be set explicitly. Hence, while changing values in Tables by
reference, we still need to explicitly set the value to publish to the
changelog, as shown below:

.. sourcecode:: python

    user_withdrawals = app.Table('user_withdrawals', default=list)
    topic = app.topic('withdrawals', value_type=Withdrawal)

    async for event in topic.stream():
        # get value for key in table
        withdrawals = user_withdrawals[event.account]
        # modify the value
        withdrawals.append(event.amount)
        # write it back to the table (also updating changelog):
        user_withdrawals[event.account] = withdrawals

If you forget to do so, like in the following example, the program will
work but will have inconsistent data if a recovery is needed for any reason:

.. sourcecode:: python

    user_withdrawals = app.Table('user_withdrawals', default=list)
    topic = app.topic('withdrawals', value_type=Withdrawal)

    async for event in topic.stream():
        withdrawals = user_withdrawals[event.account]
        withdrawals.append(event.amount)
        # OOPS! Did not update the table with the new value

Due to this changelog, both table keys and values must be serializable.

.. seealso::

    - The :ref:`guide-models` guide for more information about models and
      serialization.

.. note::

    Faust creates an internal changelog topic for each table. The Faust
    application should be the only client producing to the changelog topics.

Windowing
=========

Windowing allows us to process streams while preserving state over defined
windows of time. A windowed table preserves key-value pairs according to the
configured "Windowing Policy."

We support the following policies:

.. class:: HoppingWindow

.. class:: TumblingWindow

How To
------

You can define a windowed table like this:

.. sourcecode:: python

    from datetime import timedelta
    views = app.Table('views', default=int).tumbling(
        timedelta(minutes=1),
        expires=timedelta(hours=1),
    )


Since a key can exist in multiple windows, the windowed table returns a special
wrapper for ``table[k]``, called a ``WindowSet``.

Here's an example of a windowed table in use:

.. sourcecode:: python

    events_topic = app.topic('events_elk', value_type=Event)

    @app.agent(events_topic)
    async def aggregate_page_views(events):
        async for event in events:
            page = event.page

            # increment one to all windows this event falls into.
            views[page] += 1

            if views[page].now() >= 10000:
                # Page is trending for current processing time window
                print('Trending now')
            if views[page].current(event) >= 10000:
                # Page would be trending in the event's time window
                print('Trending when event happened')
            if views[page].delta(timedelta(minutes=30)) > views[page].now():
                print('Less popular compared to 30 minutes back')


In this table, ``table[k].current()`` returns the most recent value relative
to the time of the currently processing event, and is the default behavior.
You can also make the current value relative to the current local time,
relative to a different field in the event (if it has a custom timestamp
field), or of another event.

The default behavior is "relative to current stream":

.. sourcecode:: python

    views = app.Table('views', default=int).tumbling(...).relative_to_stream()

Where ``.relative_to_stream()`` means values are selected based on the window
of the current event in the currently processing stream.

You can also use ``.relative_to_now()``: this means the window of the current
local time is used instead:

.. sourcecode:: python

    views = app.Table('views', default=int).tumbling(...).relative_to_now()

If the current event has a custom timestamp field that you want to use,
``relative_to_field(field_descriptor)`` is suited for that task::

    views = app.Table('views', default=int) \
        .tumbling(...) \
        .relative_to_field(Account.date_created)


You can override this default behavior when accessing data in the table:

.. sourcecode:: python

    @app.agent(topic)
    async def process(stream):
        async for event in stream:
            # Get latest value for key', based on the tables default
            # relative to option.
            print(table[key].current())

            # You can bypass the default relative to option, and
            # get the value closest to the current local time
            print(table[key].now())

            # Or get the value for a delta, e.g. 30 seconds ago
            print(table[key].delta(30))


"Out of Order" Events
---------------------

Kafka maintains the order of messages published to it, but when using custom
timestamp fields, relative ordering is not guaranteed.

For example, a producer can lose network connectivity while sending a batch
of messages and be forced to retry sending them later, then messages in the
topic won't be in timestamp order.

Windowed tables in Faust correctly handles such "out of order " events, at least
until the message is as old as the table expiry configuration.

.. note::

    We handle out of order events by storing separate aggregates for each
    window in the last ``expires`` seconds. The space complexity for this
    is ``O(w * K)`` where ``w`` is the number of windows in the last
    expires seconds and ``K`` is the number of keys in the table.

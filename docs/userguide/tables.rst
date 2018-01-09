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
ensure the source topic partitions are correctly rebalanced to a differento
worker upon failure, along with any necessary table partitions.

.. sourcecode:: python

    class Withdrawal(faust.Record):
        account: str
        amount: float

    totals = app.Table('totals', default=int)

    async for event in app.topic('withdrawals', value_type=Withdrawal).stream():
        table[event.account] += event.amount

This source-topic-event to table-modification-event requirement also ensures
that producing to the changelog and committing messages from the source
happen simultaneously.

.. warning::


    An abruptly terminated Faust worker can allow some changelog entries
    to go through, before having committed the source topic offsets.

    Duplicate messages may result in double-counting and other data
    consistency issues, so we are hoping to take advantage of Kafka 0.11's
    stronger consistency guarantees and new "exactly-once"-semantics features
    soon.

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

.. note::

    Due to this reason Table changelogs should have the same number of
    partitions as the source topic.

Table Sharding
--------------

Tables shards in Kafka must organize using a disjoint distribution of keys
so that any computation for a subset of keys always happen together in the
same worker process.

The following is an example of incorrect usage where subsets of keys are
likely to be processed by different worker processes:

.. sourcecode:: python

    withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

    user_to_total = app.Table('user_to_total', default=int)
    country_to_total = app.Table(
        'country_to_total', default=int).tumbling(10.0, expires=10.0)


    @app.agent(withdrawals_topic)
    async def process_withdrawal(withdrawals):
        async for withdrawal in withdrawals:
            user_to_total[withdrawal.user] += withdrawal.amount
            country_to_total[withdrawal.country] += withdrawal.amount

Here the stream ``withdrawals`` is (implicitly) partitioned by ``Withdrawal.user``,
since that's what's used as message key. Hence the ``country_to_total`` table
which is expected to be partitioned by country, would
end up actually being partitioned by user.  In practice this means
the data for a country may reside in multiple partitions and the calculations
will be wrong.

The above use case should be re-implemented as follows:

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

Changelogging
-------------

Table updates are published to a Kafka topic for recovery upon failures. We
use Log Compaction to ensure that the changelog topic doesn't grow
exponentially, keeping the number of messages in the changelog
topic ``O(n)``, where n is the number of keys in the table.

.. note::

    In production it is recommended that you use the ``rocksdb`` store,
    as that will allow for almost instantaneous recovery (only needing
    to retrieve the updates since last time the instance was up).

In order to publish a changelog message into Kafka for fault-tolerance the
table needs to be set explicitly. Hence, while changing values in Tables by
reference, we still need to explicitly set the value to publish to the
changelog, as shown below:

.. sourcecode:: python

    user_withdrawals = app.Table('user_withdrawals', default=list)
    topic = app.topic('withdrawals', value_type=Withdrawal)

    async for event in topic.stream():
        withdrawals = user_withdrawals[event.account]
        withdrawals.append(event.amount)
        user_withdrawals[event.account] = withdrawals

The following code would not be fault-tolerant as it would not publish to the
kafka changelog. It would still work locally but recovery upon failure would
not correctly build the state of the world before the crash.

.. sourcecode:: python

    user_withdrawals = app.Table('user_withdrawals', default=list)
    topic = app.topic('withdrawals', value_type=Withdrawal)

    async for event in topic.stream():
        withdrawals = user_withdrawals[event.account]
        withdrawals.append(event.amount)

Due to the table topic changelog, keys and values should be serializable.

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

.. sourcecode:: python

    from datetime import timedelta
    views = app.Table('views', default=int).tumbling(
        timedelta(minutes=1),
        expires=timedelta(hours=1),
    )


A windowed table returns a special wrapper for ``table[k]``, called a
``WindowSet``, since ``k`` can exist in multiple windows at once.


Let's show an example of a windowed table in use:


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


In this table, the time is relative to the event being currently processed,
as is the default, but you can also make the ``current()`` value be relative to
the current time, or of another event.

The default is equivalent to:

.. sourcecode:: python

    views = app.Table('views', default=int).tumbling(...).relative_to_stream()

Where ``.relative_to_stream()`` means values are selected based on the window
of the current event in the currently processing stream.

You can also use ``.relative_to_now()``, which means the window of the current
local time is used instead:

.. sourcecode:: python

    views = app.Table('views', default=int).tumbling(...).relative_to_now()

If your stream events has a different timestamp field that you would like to
use, then use ``relative_to_field(field_descriptor)``, which means the window
of a field in the current event, in the currently processing stream will be
used:

    views = app.Table('views', default=int) \
        .tumbling(...) \
        .relative_to_field(Account.date_created)

Now when accessing data in the table you can choose the ``.current()`` based
on your selected default relative to option:

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


Out of Order Events
-------------------

Events can sometimes come out of order due to various reasons such as network
issues. Windowed Tables in Faust handle out of order events until
``expires`` seconds``. In order to handle out of order events we store separate
aggregates for each window in the last ``expires`` seconds. The space
complexity for handling out of order events is ``O(w * K)`` where ``w`` is
the number of windows in the last ``expires`` seconds and ``K`` is the number
of keys in the Table.

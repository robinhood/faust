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
    consistency issues, but since version 1.5 of Faust you can
    enable a setting for strict processing guarantees.

    See the :setting:`processing_guarantee` setting for more information.

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

    withdrawals_topic = app.topic('withdrawals', key_type=str,
                                  value_type=Withdrawal)

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

.. class:: TumblingWindow

This class creates fixed-sized, non-overlapping and contiguous time intervals
to preserve key-value pairs, e.g. ``Tumbling(10)`` will create non-overlapping
10 seconds windows:

.. sourcecode:: bash

  window 1: ----------
  window 2:           ----------
  window 3:                     ----------
  window 4:                               ----------
  window 5:                                         ----------


This class is exposed as a method from the output of ``app.Table()``, it takes
a mandatory parameter ``size``, representing the window (time interval) duration
and an optional parameter ``expires``, representing the duration for which we
want to store the data (key-value pairs) allocated to each window.

.. class:: HoppingWindow

This class creates fixed-sized, overlapping time intervals to preserve key-value
pairs, e.g. ``Hopping(10, 5)`` will create overlapping 10 seconds windows. Each
window will be created every 5 seconds.

.. sourcecode:: bash

  window 1: ----------
  window 2:      ----------
  window 3:           ----------
  window 4:                ----------
  window 5:                     ----------
  window 6:                          ----------


This class is exposed as a method from the output of ``app.Table()``, it takes 2
mandatory parameters:

- ``size``, representing the window (time interval) duration.
- ``step``, representing the time interval used to create new windows.

It also takes an optional parameter ``expires``, representing the duration for
which we want to store the data (key-value pairs) allocated to each window.

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

    page_views_topic = app.topic('page_views', value_type=str)

    @app.agent(events_topic)
    async def aggregate_page_views(pages):
        # values in this streams are URLs as strings.
        async for page_url in pages:

            # increment one to all windows this page URL fall into.
            views[page_url] += 1

            if views[page_url].now() >= 10000:
                # Page is trending for current processing time window
                print('Trending now')

            if views[page_url].current() >= 10000:
                # Page would be trending in the current event's time window
                print('Trending when event happened')

            if views[page_url].value() >= 10000:
                # Page would be trending in the current event's time window
                # according to the relative time set when creating the
                # table.
                print('Trending when event happened')

            if views[page_url].delta(timedelta(minutes=30)) > views[page_url].now():
                print('Less popular compared to 30 minutes back')


In this table, ``table[k].now()`` returns the most recent value for the
current processing window, overriding the _relative_to_ option used to create
the window.

In this table, ``table[k].current()`` returns the most recent value relative
to the time of the currently processing event, overriding the _relative_to_
option used to create the window.

In this table, ``table[k].value()`` returns the most recent value relative
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
            print(table[key].value())

            # You can bypass the default relative to option, and
            # get the value closest to the event timestamp
            print(table[key].current())

            # You can bypass the default relative to option, and
            # get the value closest to the current local time
            print(table[key].now())

            # Or get the value for a delta, e.g. 30 seconds ago, relative
            # to the event timestamp
            print(table[key].delta(30))


.. note::

  We always retrieve window data based on timestamps. With tumbling windows
  there is just one window at a time, so for a given timestamp there is just
  one corresponding window. This is not the case for for hopping windows, in
  which a timestamp could be located in more than 1 window.

  At this point, when accessing data from a hopping table, we always access the
  latest window for a given timestamp and we have no way of modifying this
  behavior.

.. _windowed-table-iter:

Iterating over keys/values/items in a windowed table.
-----------------------------------------------------

.. note::

    Tables are distributed across workers, so when iterating over table
    contents you will only see the partitions assigned to the current worker.

    Iterating over all the keys in a table will require you to visit
    all workers, which is highly impractical in a production system.

    For this reason table iteration is mostly used in debugging
    and observing your system.

To iterate over the keys/items/values in windowed table you may
add the ``key_index`` option to enable support for it:

.. code-block:: python

    windowed_table = app.Table(
        'name',
        default=int,
    ).hopping(10, 5, expires=timedelta(minutes=10), key_index=True)

Adding the key index means we keep a second table as an index of the
keys present in the table. Whenever a new key is added we add the key to
the key index, similarly whenever a key is deleted we also delete it from the
index.

This enables fast iteration over the keys, items and values in the windowed
table, with the caveat that those keys may not exist in all windows.

The table iterator views (``.keys()``/``.items()``/``.values()``)
will be time-relative to the stream by default, unless you have changed
the time-relativity using the ``.relative_to_now`` or
``relative_to_timestamp`` modifiers:

.. code-block:: python

    # Show keys present relative to time of current event in stream:
    print(list(windowed_table.keys()))

    # Show items present relative to time of current event in stream:
    print(list(windowed_table.items()))

    # Show values present relative to time of current event in stream:
    print(list(windowed_table.values()))

You can also manually specify the time-relativity:

.. code-block:: python

    # Change time-relativity to current wall-clock time,
    # and show a list of items present in that window.
    print(list(windowed_table.relative_to_now().items()))

    # Get items present 30 seconds ago:
    print(list(windowed_table.relative_to_now().items().delta(30.0)))

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

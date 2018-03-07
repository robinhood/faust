======
 TODO
======

"ExactlyOnce" / Transactions (KIP-98)
=====================================

- "Attaching" should be deprecated and transactions should be used to
  send messages as we commit.

HTTP Table view
===============

Forward request to node with key

https://cwiki.apache.org/confluence/display/KAFKA/KIP-67%3A+Queryable+state+for+Kafka+Streams

User must explicitly mark tables as public for table to be exposed in HTTP
interface:

.. sourcecode:: python

    user_to_amount = app.Table('user_to_amount', public=True)

HTTP API
--------

* List of key/value pairs in the table (with pagination)

    .. sourcecode:: text

        GET localhost:6666/api/table/user_to_amount/?page=

        200 {"results": {"key", "value"}}

* -List available tables-

    .. sourcecode:: text

        GET localhost:6666/api/table/

        200 {"results": ["user_to_amount"]}

* -Get value by key-:

    .. sourcecode:: text

        GET localhost:6666/api/table/user_to_amount/key/

        200 {"key": "value"}


HTTP User interface
-------------------

If content-type is set to text/html, return HTML pages allowing the user
to browse key/value pairs.

Joins
=====

- Stream/Stream join

- Table/Table join

- Stream/Table, Table/Stream join.

See ``faust/joins.py``

API already exposed in faust.streams.Stream, but not implemented.

Buffering Ack Optimization
==========================

TODO

Ack buffer using start_offset-end_offset.

Django support builtin
======================

Automatically call `django.setup()` and friends in examples/django/faustapp/app.py
when the ``DJANGO_SETTINGS_MODULE`` environment variable is set.

Agent per partition
===================

Start one agent per partition.
Must also be able to introspect what partitions an agent is active for.

Tables
======

- Nested data-structures, like ``Mapping[str, List]``, ``Mapping[str, Set]``

    - Can be accomplished by treating the changelog as a database "transaction
      log"

    - For example, adding a new element to a Mapping of sets::

        class SubReq(faust.Record):
            topic: str

        class PubReq(faust.Record):
            topic: str
            message: str


        subscribers = app.Table('subscribers', type=set)

        @app.agent()
        async def subscribe(subscriptions: Stream[SubReq]) -> AsyncIterable[bool]:
            async for subsription in subscriptions:
                subscribers[subscription.topic].add(subscriber.account)

        @app.agent()
        async def send_to_subscribers(requests):
            async for req in requests:
                for account in subscribers[req.topic]:
                    accounts.get(account).send_message(req.message)

        @route('/(?P<topic>/send/')
        @accept_methods('POST')
        async def send_to_subscribers(request):
            await send_to_subscribers.send(PubReq(
                topic=request.POST['topic'],
                message=request.POST['message'],
            )

    - Adding an element produces the following changelog:

        .. sourcecode:: text

            KEY=topic VALUE={'action': 'add', 'value': new_member}

    - while removing an element produces the changelog:

        .. sourcecode:: text

            KEY=topic VALUE={'action': 'remove', 'value': new_member}

    - NOTE: Not sure how this would coexist with windowing, but maybe it will
            work just by the Window+key keying.


Tests
=====

Need to write more functional tests: test behavior, not coverage.

librdkafka asyncio client
=========================

Need to dive into C to add callbacks to C client so that it can be
connected to the event loop.

There are already NodeJS clients using librdkafka so this should
definitely be possible.

Look at confluent-kafka for inspiration.

Sensors
=======

- ``through()`` latency

- ``group_by()`` latency

Documentation
=============

- Topic

  - Partitioning/Sharding illustration

  - Arguments to ``app.topic``

- Agent

    - Message lifecycle

    - Manual acknowledgement (``async with event``)

    - Arguments to ``app.agent``

- Tables

    - Windowing (``value.current()``, ``Table.relative_to_stream()`` etc.)

    - Windowing illustrations

    - Changelog callbacks

    - Arguments to ``app.Table``.

- Models

    - may have forgotten something (isodates, special cases, go through code).

- Stream

    - Arguments

        - Stream from iterable/async iterable
        - Stream from channel/topic.

- Deployment

    - supervisord

    - Logging

    - Sentry

- Availability guide

    - partitioning

    - recovery

    - acknowledgements

- Go through comments in the code, some of it describes things that should
  be documented.


Typing
======

These are very very very low priority tasks, and more of a convenience if
anyone wants to learn Python typing.

- Add typing to (either .pyi header files, or fork projects):

    - aiokafka
        - kafka-python
    - aiohttp
    - avro-python3

- WeakSet missing from mypy

    Not really a task, but a note to keep checking when this is fixed
    in a future mypy version.


Workflows
=========

Things to replace Celery, maybe not in Core but in a separate library.

- Chains

- Chords/Barrier

    synchronization should be possible:
        ``chord_id = uuid(); requests = [....]``,
    then each agent forwards a completion message to an agent that keeps
    track of counts::

        chord_unlock.send(key=chord_id, value=(chord_size, callback)

     when the `chord_unlock` agent sees that ``count > chord_size``, it
     calls the callback

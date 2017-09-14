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

* List available tables

    .. sourcecode:: text

        GET localhost:6666/api/table/

        200 {"results": ["user_to_amount"]}

* List of key/value pairs in the table (with pagination)

    .. sourcecode:: text

        GET localhost:6666/api/table/user_to_amount/?page=

        200 {"results": {"key", "value"}}

* Get value by key:

    .. sourcecode:: text

        GET localhost:6666/api/table/user_to_amount/key/

        200 {"key": "value"}

* Set value for key

    .. sourcecode:: text

        PUT/POST localhost:6666/api/table/user_to_amount/key/
        form data: {"key": "value"}

        response: 200

* Delete key

    .. sourcecode:: text

        DELETE localhost:6666/api/table/user_to_amount/key/
        response: 200

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

CLI Commands
============

- ``faust table``

    Show tables from the command-line (maybe use https://robpol86.github.io/terminaltables/)

Fault Tolerance
===============

- Rebalance listener:
    https://github.com/apache/kafka/blob/4b3ea062be515bc173f6c788c4c1e14f77935aef/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamThread.java#L1264-L1342

- Partition assignor links:

    * https://github.com/apache/kafka/blob/trunk/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamPartitionAssignor.java (KafkaStream’s partition Assignor)
    * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal (Partition assignor protocol used by kafka)
    * https://github.com/dpkp/kafka-python/blob/master/kafka/coordinator/assignors/roundrobin.py (Kafka python’s roundrobin parition assignor for a simpler example of the partition assignor)
    * https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy (Sticky Partition Assignment strategy that was added recently)

- Standby Tables

    - Like standby tasks in KS

        Note: There are no standby streams, this is only for tables.

    - TopicManager consumes data from other partitions, to quickly recover if
      one of the nodes go down.

    - Probably will have to change TopicManager._topicmap to
      use ``(topic, partition)`` as key.

    - New attribute: ``Table.standby``

        Can be used for introspection only, to quickly check if a stream is
        standby, or to be used in for example ``repr(table)``.


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

        @app.actor()
        async def subscribe(subscriptions: Stream[SubReq]) -> AsyncIterable[bool]:
            async for subsription in subscriptions:
                subscribers[subscription.topic].add(subscriber.account)

        @app.actor()
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


Deployment
==========

- Daemonization

    Handled by supervisord/circus ?

- Sentry/Raven

- ``faust`` command-line tool

    DONE:

    .. sourcecode:: console

        $ faust -A examples.simple worker
        $ FAUSTAPP=examples.simple faust worker

    TODO(?):

    .. sourcecode:: console

        $ faust -A examples.simple status
        $ faust -A examples.simple ping
        $ faust -A examples.simple send topic [value [ key]]

Tests
=====

Need to write functional tests: test behavior, not coverage.

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

HTTP interface
--------------

.. sourcecode:: text

    GET localhost:6666/stats/
    Returns: general stats events processed/s, total events, commit()
    latency etc.,

    GET localhost:6666/stats/topic/mytopic/
    Stats related to topic by name.

    GET localhost:6666/stats/task/mytask/
    Stats related to task by name.

    GET localhost:6666/stats/table/mytable/
    Stats related to table by table name.

HTTP Graphs
-----------

Show graphs in realtime:  Wow factor+++ :-)

Optimize ``aiokafka``
=====================

Find out if there are any obvious optimizations that can be applied
as it's currently quite slow.

Documentation
=============

- Introduction/README

- Tutorial

- Glossary (docs/glossary.rst)

- User Guide (docs/userguide/)

    - Streams

    - Tables

    - Models

    - Availability

        - partitioning

        - recovery

        - acknowledgements

    - Sensors

    - Deployment

        * daemonization

        * uvloop vs. asyncio

        * debugging (aiomonitor)

        * logging

    - Web API

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
    then each actor forwards a completion message to an actor that keeps
    track of counts::

        chord_unlock.send(key=chord_id, value=(chord_size, callback)

     when the `chord_unlock` actor sees that ``count > chord_size``, it
     calls the callback

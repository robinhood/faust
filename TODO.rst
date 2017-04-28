======
 TODO
======

HTTP Table view
===============

Forward request to node with key

https://cwiki.apache.org/confluence/display/KAFKA/KIP-67%3A+Queryable+state+for+Kafka+Streams

User must explicitly mark tables as public for table to be exposed in HTTP
interface:

.. code-block:: python

    user_to_amount = app.table('user_to_amount', public=True)

HTTP API
--------

* List available tables

    .. code-block:: text

        GET localhost:6666/api/table/

        200 {"results": ["user_to_amount"]}

* List of key/value pairs in the table (with pagination)

    .. code-block:: text

        GET localhost:6666/api/table/user_to_amount/?page=

        200 {"results": {"key", "value"}}

* Get value by key:

    .. code-block:: text

        GET localhost:6666/api/table/user_to_amount/key/

        200 {"key": "value"}

* Set value for key

    .. code-block:: text

        PUT/POST localhost:6666/api/table/user_to_amount/key/
        form data: {"key": "value"}

        response: 200

* Delete key

    .. code-block:: text

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

See :file:`faust/joins.py`

API already exposed in faust.streams.Stream, but not implemented.

Fault Tolerance
===============

- Rebalance listener:
    https://github.com/apache/kafka/blob/4b3ea062be515bc173f6c788c4c1e14f77935aef/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamThread.java#L1264-L1342

- Partition assignor links:

    * https://github.com/apache/kafka/blob/trunk/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamPartitionAssignor.java (KafkaStream’s partition Assignor)
    * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal (Partition assignor protocol used by kafka)
    * https://github.com/dpkp/kafka-python/blob/master/kafka/coordinator/assignors/roundrobin.py (Kafka python’s roundrobin parition assignor for a simpler example of the partition assignor)
    * https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy (Sticky Partition Assignment strategy that was added recently)

- Standby Streams

    - Like standby tasks in KS

    - StreamManager consumes data from other partitions, to quickly recover if
      one of the nodes go down.

    - Probably will have to change StreamManager._topicmap to
      use ``(topic, partition)`` as key.

    - New attribute: ``Stream.standby``

        Can be used for introspection only, to quickly check if a stream is
        standby, or to be used in for example ``repr(stream)``.

    - ``App._streams`` and ``App._tables`` mapping may have to change to key
      by ``(topic, partition)`` also.

    - StreamManager uses Stream.clone to create copies of clones:

        ``x = Stream.clone(standby=True)``

Deployment
==========

- Daemonization

    Handled by supervisord/circus ?

- Sentry/Raven

- ``faust`` command-line tool

    .. code-block:: console

        $ faust -A examples.simple start
        $ FAUSTAPP=examples.simple faust start
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

Write a basic sensor interface including the following metrics:

- number of events processed/s

- number of events processed/s by topic

- number of events processed/s by task

- number of records written to table

- number of records written to table by table.

- average processing time (from event received to event acked)

- total number of events

- ``commit()`` latency

- ``through()`` latency

- ``group_by()`` latency

- ``Producer.send`` latency

HTTP interface
--------------

.. code-block:: text

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

    - Tasks

    - Streams

    - Tables

    - Models

    - High Availability

    - Serialization

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

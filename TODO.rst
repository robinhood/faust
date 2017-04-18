- HTTP Table view
    Forward request to node with key

    https://cwiki.apache.org/confluence/display/KAFKA/KIP-67%3A+Queryable+state+for+Kafka+Streams

    User must explicitly mark tables as public for table to be exposed in HTTP
    interface:

    .. code-block:: python

        user_to_amount = app.table('user_to_amount', public=True)

    HTTP API
    --------

    * List of key/value pairs in the table (with pagination)

        .. code-block:: text

            GET localhost:6666/api/table/user_to_amount/?page=
            200 {"results": {"key", "value"}}

    * Get value by key:

        GET localhost:6666/api/table/user_to_amount/key/
        200 {"key": "value"}

    * Set value for key

        PUT/POST localhost:6666/api/table/user_to_amount/key/
        form data: {"key": "value"}

        response: 200

    * Delete key

        DELETE localhost:6666/api/table/user_to_amount/key/
        response: 200

    HTTP User interface
    -------------------

    If content-type is set to text/html, return HTML pages allowing the user
    to browse key/value pairs.


- Joins

- Window

- Aggregate

    Depends on Window

- Fault tolerance

    - Rebalance listener:
        https://github.com/apache/kafka/blob/4b3ea062be515bc173f6c788c4c1e14f77935aef/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamThread.java#L1264-L1342

- Deployment

- Tests

    Need to write functional tests: test behavior, not coverage.

- librdkafka asyncio client

    port of confluent-kafka using asyncio, needs to dive into C to add
    callbacks to C client so that it can be connected to the event loop.

    There are already NodeJS clients using librdkafka so this should
    definitely be possible.

- Sensors

    Write a basic sensor interface including the following metrics:

    - number of events processed/s

    - number of events processed/s by topic

    - number of events processed/s by task

    - number of records written to table

    - number of records written to table by table.

    - average processing time (from event received to event acked)

    - total number of events

    - commit() latency

    - through() latency

    - group_by() latency

    - Producer.send latency

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

    Show graphs in realtime.

- Typing:
    - WeakSet missing from mypy
    - Typing of:

        - aiokafka
        - aiohttp
        - avro-python3

.. _guide-streams:

=================================================
 Streams - Infinite Data Structures
=================================================

.. topic:: \

    *“Everything transitory is but an image.”*

    -- Goethe, *Faust: Part II*

.. contents::
    :local:
    :depth: 2

.. module:: faust

.. currentmodule:: faust

Basics
======

A stream is an infinite async iterable, being passed messages consumed
from a channel/topic:

.. sourcecode:: python

    @app.agent(my_topic)
    async def process(stream):
        async for value in s:
            ...

The above :ref:`agent <guide-agents>` is how you usually define stream
processors in Faust, but you can also create stream objects manually
at any point with the caveat that this can trigger a Kafka rebalance
when doing so at runtime:

.. sourcecode:: python

    stream = app.stream(my_topic)  # or: my_topic.stream()
    async for value in stream:
        ...

The stream *needs to be iterated over* to be processed, it will not
be active until you do.

When iterated over the stream gives deserialized values, but you can also
iterate over key/value pairs (using
:meth:`~@Stream.items`), or raw messages (using :meth:`~@Stream.events`).

Keys and values can be bytes for manual deserialization, or :class:`~faust.Model`
instances, and this is decided by the topic's ``key_type`` and
``value_type`` argumetns.

.. seealso::

    - The :ref:`guide-channels` guide -- fore more information about
      channels and topics.

    - The :ref:`guide-models` guide -- for more information about models
      and serialization.

The easiest way to process streams is to use :ref:`agents <guide-agents>`, but
you can also create a stream manually from any topic/channel.

Here we define a model for our stream, create a stream from the "withdrawals"
topic and iterate over it:

.. sourcecode:: python

    class Withdrawal(faust.Record):
        account: str
        amount: float

    async for w in app.topic('withdrawals', value_type=Withdrawal).stream():
        print(w.amount)

Do note that the worker must be started first (or at least the app),
for this to work, and the stream iterater probably needs to be started
as an :class:`asyncio.Task`, so a more practical example is:

.. sourcecode:: python

    import faust

    class Withdrawal(faust.Record):
        account: str
        amount: float

    app = faust.App('example-app')

    withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)

    @app.task
    async def mytask():
        async for w in withdrawals_topic.stream():
            print(w.amount)

    if __name__ == '__main__':
        app.main()

You may also treat the stream as a stream of bytes values:

.. sourcecode:: python

    async for value in app.topic('messages').stream():
        # the topic description has no value_type, so values
        # are now the raw message value in bytes.
        print(repr(value))

Processors
==========

A stream can have an arbitrary number of processor callbacks
that are executed as values go through the stream.

These are usually not used in normal Faust applications, but can be useful
for libraries to extend the functionality of streams.

A processor takes a value as argument and returns a value:

.. sourcecode:: python

    def add_default_language(value: MyModel) -> MyModel:
        if not value.language:
            value.language = 'US'
        return value

    async def add_client_info(value: MyModel) -> MyModel:
        value.client = await get_http_client_info(value.account_id)
        return value

    s = app.stream(my_topic,
                   processors=[add_default_language, add_client_info])

.. note::

    Processors can be async callable, or normal callable.


Since the processors are stored in an ordered list, the processors above
will execute in order and the final value going out of the stream will be the
reduction after all processors are applied:

.. sourcecode:: pycon

    async for value in s:
        # all processors applied here so `value`
        # will be equivalent to doing:
        #   value = add_default_language(add_client_info(value))


Message Lifecycle
=================

Kafka Topics
------------

Every Faust worker instance will start a single Kafka consumer
responsible for fetching messages from all subscribed topics.

Every message in the topic have an offset number (where the
first message has an offset of zero), and we use a single offset
to track the messages that consumers do not want to see again.

The Kafka consumer commits the topic offsets every three
seconds (by default, can also be configured using the
:setting:`broker_commit_interval` setting) in a background task.

Since we only have one consumer and multiple agents can be subscribed
to the same topic, we need a smart way to track when those events
have processed so we can commit and advance the consumer group offset.

We use reference counting for this, so when you define an agent that
iterates over the topic as a stream::

   @app.agent(topic)
   async def process(stream):
       async for value in stream:
            print(value)

The act of starting that stream iterator will add the topic to
the Conductor service. This internal service is responsible for
forwarding messages received by the consumer to the streams:

.. sourcecode:: text

  [Consumer] -> [Conductor] -> [Topic] -> [Stream]


The :keyword:`async for` is what triggers this, and the agent code above
is roughly equivalent to::

   async def custom_agent(app: App, topic: Topic):
        topic_iterator = aiter(topic)
        app.topics.add(topic)  # app.topics is the Conductor
        stream = Stream(topic_iterator, app=app)
        async for value in stream:
            print(value)

If two agents use streams subscribed to the same topic::

   topic = app.topic('orders')

    @app.agent(topic)
    async def processA(stream):
         async for value in stream:
             print(f'A: {value}')

    @app.agent(topic)
     async def processB(stream):
          async for value in stream:
              print(f'B: {value}')

The Conductor will forward every message received on the "orders"
topic to both of the agents, increasing the reference count whenever
it enters an agents stream.

The reference count decreases when the event is :term:`acknowledged`,
and when it reaches zero the consumer will consider that offset as "done" and
can commit it.

Acknowledgment
~~~~~~~~~~~~~~

The acknowledgment signifies that the event processing is complete and
should not happen again.

An event is automatically acknowledged when:

- The agent stream advances to a new event (``Stream.__anext__``)
- An exception occurs in the agent during event processing.
- The application shuts down, or a rebalance is required,
  and the stream finished processing the event.

What this means is that an event is acknowledged when your agent is
finished handling it, but you can also manually control when it happens.

To manually control when the event is acknowledged, and its reference count
decreased, use ``await event.ack()``

  async for event in stream.events():
    print(event.value)
    await event.ack()

 You can also use :keyword:`async for` on the event::

    async for event in stream.events():
        async with event:
            print(event.value)
            # event acked when exiting this block

Note that the conditions in automatic acknowledgment still apply
when manually acknowledging a message.

Combining streams
=================

Streams can be combined, so that you receive values from multiple streams
in the same iteration:

.. sourcecode:: pycon

    >>> s1 = app.stream(topic1)
    >>> s2 = app.stream(topic2)
    >>> async for value in (s1 & s2):
    ...     ...

Mostly this is useful when you have two topics having the same value type, but
can be used in general.

If you have two streams that you want to process independently you should
rather start individual tasks:

.. sourcecode:: python

    @app.agent(topic1)
    async def process_stream1(stream):
        async for value in stream:
            ...


    @app.agent(topic2)
    async def process_stream2(stream):
        async for value in stream:
            ...

Operations
==========

``group_by()`` -- Repartiton the stream
---------------------------------------

The :meth:`Stream.group_by() <faust.Stream.group_by>` method repartitions the
stream by taking a "key type" as argument:

.. sourcecode:: python

    import faust

    class Order(faust.Record):
        account_id: str
        product_id: str
        amount: float
        price: float

    app = faust.App('group-by-example')
    orders_topic = app.topic('orders', value_type=Order)

    @app.agent(orders_topic)
    async def process(orders):
        async for order in orders.group_by(Order.account_id):
            ...


In the example above the "key type" is a field descriptor, and the
stream will be repartitioned by the account_id field found in the deserialized
stream value.

The new stream will be using a new intermediate topic where messages have
account ids as key, and this is the stream that the agent will finally be
iterating over.

.. note::

    ``Stream.group_by()`` returns a new stream subscribing to the intermediate
    topic of the group by operation.

Apart from field descriptors, the key type argument can also be specified as
a callable, or an async callable, so if you're not using models to describe
the data in streams you can manually extract the key used for repartitioning:

.. sourcecode:: python

    def get_order_account_id(order):
        return json.loads(order)['account_id']

    @app.agent(app.topic('order'))
    async def process(orders):
        async for order in orders.group_by(get_order_account_id):
            ...

.. seealso::

    - The :ref:`guide-models` guide -- for more information on field
      descriptors and models.

    - The :meth:`faust.Stream.group_by` method in the API reference.

``items()`` -- Iterate over keys and values
-------------------------------------------

Use :meth:`Stream.items() <faust.Stream.items>` to get access to both message
key and value at the same time:

.. sourcecode:: python

    @app.agent()
    async def process(stream):
        async for key, value in stream.items():
            ...


Note that this changes the type of what you iterate over from ``Stream`` to
``AsyncIterator``, so if you want to repartition the stream or similar,
``.items()`` need to be the last operation:

.. sourcecode:: python

    async for key, value in stream.through('foo').group_by(M.id).items():
        ...

``events()`` -- Access raw messages
-----------------------------------

Use :meth:`Stream.events() <faust.Stream.events>` to iterate over raw
``Event`` values, including access to original message payload and message
metadata:

.. sourcecode:: python

    @app.agent
    async def process(stream):
        async for event in stream.events():
            message = event.message
            topic = event.message.topic
            partition = event.message.partition
            offset = event.message.offset

            key_bytes = event.message.key
            value_bytes = event.message.value

            key_deserialized = event.key
            value_deserialized = event.value

            async with event:  # use  `async with event` for manual ack
                process(event)
                # event will be acked when this block returns.

.. seealso::

    - The :class:`faust.Event` class in the API reference -- for more
      information about events.

    - The :class:`faust.types.tuples.Message` class in the API reference -- for more
      information about the fields available in ``event.message``.


``take()`` -- Buffer up values in the stream
--------------------------------------------

Use :meth:`Stream.take() <faust.Stream.take>` to gather up multiple events in
the stream before processing them, for example to take 100 values at a time:

.. sourcecode:: python

    @app.agent()
    async def process(stream):
        async for values in stream.take(100):
            assert len(values) == 100
            print(f'RECEIVED 100 VALUES: {values}')

The problem with the above code is that it will block forever if there are 99
messages and the last hundredth message is never received.

To solve this add a ``within`` timeout so that up to 100 values will be
processed within 10 seconds:

.. sourcecode:: python

    @app.agent()
    async def process(stream):
        async for values in stream.take(100, within=10):
            print(f'RECEIVED {len(values)}: {values}')

The above code works better: if values are constantly being streamed it will
process hundreds and hundreds without delay, but if there are long periods of
time with no events received it will still process what it has gathered.

``enumerate()`` -- Count values
-------------------------------

Use :meth:`Stream.enumerate()` to keep a count of the number of values
seen so far in a stream.

This operation works exactly like the Python :func:`enumerate` function, but
for an asynchronous stream:

.. sourcecode:: python

    @app.agent()
    async def process(stream):
        async for i, value in stream.enumerate():
            ...

The count will start at zero by default, but ``enumerate`` also accepts an
optional starting point argument.

.. seealso::

    - The :func:`faust.utils.aiter.aenumerate` function -- for a general version
      of :func:`enumerate` that let you enumerate any async iterator, not just
      streams.

    - The :func:`enumerate` function in the Python standard library.

``through()`` -- Forward through another topic
----------------------------------------------

Use :meth:`Stream.through() <faust.Stream.through>` to forward every value
to a new topic, and replace the stream by subscribing to the new topic:

.. sourcecode:: python

    source_topic = app.topic('source-topic')
    destination_topic = app.topic('destination-topic')

    @app.agent()
    async def process(stream):
        async for value in stream.through(destination_topic):
            # we are now iterating over stream(destination_topic)
            print(value)

You can also specify the destination topic as a string:

.. sourcecode:: python

    # [...]
    async for value in stream.through('foo'):
        ...


Through is especially useful if you need to convert the number of partitions
in a source topic, by using an intermediate table.

If you simply want to forward a value to another topic, you can send it
manually, or use the echo recipe below:

.. sourcecode:: python

    @app.agent()
    async def process(stream):
        async for value in stream:
            await other_topic.send(value)

``echo()`` -- Repeat to one or more topics
------------------------------------------

Use :meth:`echo` to repeat values received from a stream to another
channel/topic, or many other channels/topics:

.. sourcecode:: python

    @app.agent()
    async def process(stream):
        async for event in stream.echo('other_topic'):
            ...

The operation takes one or more topics, as string topic names or :class:`topic
descriptions <@topic>`, so this also works:

.. sourcecode:: python

    source_topic = app.topic('sourcetopic')
    echo_topic1 = app.topic('source-copy-1')
    echo_topic2 = app.topic('source-copy-2')

    @app.agent(source_topic)
    async def process(stream):
        async for event in stream.echo(echo_topic1, echo_topic2):
            ...

.. seealso::

    - The :ref:`guide-channels` guide -- for more information about channels
      and topics.

.. _stream-operations:

Reference
=========

.. note::

    Do not create ``Stream`` objects directly, instead use: ``app.stream``
    to instantiate new streams.

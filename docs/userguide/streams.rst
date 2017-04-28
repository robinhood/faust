.. _guide-streams:

=================================================
 Streams
=================================================

Basics
======

A stream is an infinite async iterable, being passed messages consumed
from a topic:

.. code-block:: pycon

    s = app.stream(my_topic)
    async for event in s:
        ...

The stream *needs to be iterated over* to be processed, it will not
be active until you do.

Processors
----------

A stream can an arbitrary number of processor callbacks
that are executed as events go through the stream.

These are usually not used in normal Faust applications, but can be useful
for libraries to extend functionality in streams.

A processor takes an Event as argument and returns an Event:

.. code-block:: python

    def add_default_language(event: Event) -> Event:
        if not event.language:
            event.language = 'US'
        return event

    async def add_client_info(event: Event) -> Event:
        event.client = await get_http_client_info(event.account_id)
        return event

    s = app.stream(my_topic,
                   processors=[add_default_language, add_client_info])

.. note::

    Processors can be both async callables, and normal callables.


Since the processors are stored in an ordered list, the processors above
will execute in order, and the final value going out of the stream will be the
reduction after all processors are applied:

.. code-block:: pycon

    async for event in s:
        # all processors applied here so `event`
        # will be equivalent to doing:
        #   event = add_default_language(add_client_info(event))


S-routines
----------

A Stream can also have a special callback, called an *S-routine*, that
encapsulates a stream processing chain in a coroutine.

.. code-block:: pycon

    >>> def filter_large_withdrawals(it: AsyncIterator) -> AsyncIterator:
    ...     return (e async for e in it if e.value >= 1000.0)

    >>> s = app.stream(my_topic, filter_large_withdrawals)
    >>> for event in s:
    ...     print(s)


.. admonition:: S-routines vs coroutines

    An S-routine is really just an alternative way of defining a sending and
    receving generator.  Instead of writing that in the traditional way of:

    .. code-block:: python

        def filter_large_withdrawals():
            while 1:
                event = (yield)
                if event.value >= 1000.0:
                    yield event

    we receive messages via an infinite iterator.

S-routines are useful when joining and combining streams.

Combining streams
-----------------

Streams can be combined, so that you receive events from multiple streams
in the same iteration:

.. code-block:: pycon

    >>> s1 = app.stream(topic1)
    >>> s2 = app.stream(topic2)
    >>> async for event in (s1 & s2):
    ...     ...

Mostly this is useful when you have two topics having the same value type, but
can be used in general.

If you have two streams that you want to process independently you should
rather start individual tasks:

.. code-block:: python

    @app.task
    async def process_stream1(app):
        async for event in app.stream(topic1):
            ...


    @app.task
    async def process_stream2(app):
        async for event in app.stream(topic2):
            ...

.. _guide-streams:

=================================================
 Streams
=================================================

.. contents::
    :local:
    :depth: 2

.. module:: faust

.. currentmodule:: faust

Basics
======

A stream is an infinite async iterable, being passed messages consumed
from a topic:

.. sourcecode:: pycon

    s = app.stream(my_topic)
    async for value in s:
        ...

The stream *needs to be iterated over* to be processed, it will not
be active until you do.

When iterated over the stream gives the deserialized values in the
topic/channel, but you can also iterate over key/value pairs (using
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
you can also you can create a stream manually from any topic/channel.

Here we define a model for our stream, create a stream from the "withdrawals"
topic and iterate over it:

.. sourcecode:: python

    class Withdrawal(faust.Record):
        account: str
        amount: float

    async for w in app.topic('withdrawals', value_type=Withdrawal).stream():
        print(w.amount)

Do note that the worker must be started first (or at least the app),
for this to work.

You can also treat the stream as a stream of bytes values:

.. sourcecode:: python

    async for value in app.topic('messages').stream():
        print(value)  # <-- .value contains the bytes message value.

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
will execute in order, and the final value going out of the stream will be the
reduction after all processors are applied:

.. sourcecode:: pycon

    async for value in s:
        # all processors applied here so `value`
        # will be equivalent to doing:
        #   value = add_default_language(add_client_info(value))


Message Lifecycle
=================

NEED TO WRITE ACKS AND SUCH

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

``items()`` -- Iterate over keys and values
-------------------------------------------

``events()`` -- Access raw messages
-----------------------------------

``take()`` -- Buffer up values in the stream
--------------------------------------------

``tee()`` -- Clone stream
-------------------------

``enumerate()`` -- Count values
-------------------------------

``through()`` -- Forward through another topic
----------------------------------------------

``echo()`` -- Repeat to one or more topics
------------------------------------------

.. _stream-operations:

Reference
=========

.. note::

    Do not create ``Stream`` objects directly, instead use: ``app.stream``
    to instantiate new streams.

Methods
-------

General
^^^^^^^

.. class:: Stream
    :noindex:

    .. automethod:: get_active_stream
        :noindex:

    .. automethod:: add_processor



Joins
^^^^^

.. class:: Stream
    :noindex:

    .. automethod:: join
        :noindex:

    .. automethod:: left_join
        :noindex:

    .. automethod:: inner_join
        :noindex:

    .. automethod:: outer_join
        :noindex:

Iteration tools
^^^^^^^^^^^^^^^

.. class:: Stream
    :noindex:

    .. autocomethod:: items
        :async-for:
        :noindex:

    .. autocomethod:: take
        :async-for:
        :noindex:

    .. automethod:: tee
        :noindex:

    .. automethod:: enumerate
        :noindex:

    .. automethod:: through
        :noindex:

    .. automethod:: echo
        :noindex:

    .. automethod:: group_by
        :noindex:

Processing
^^^^^^^^^^

.. class:: Stream
    :noindex:

    .. autocomethod:: send
        :noindex:

Topics
^^^^^^

.. class:: Stream
    :noindex:

    .. automethod:: derive_topic
        :noindex:

Attributes
----------

.. class:: Stream
    :noindex:

    .. autoattribute:: app

    .. autoattribute:: channel

    .. autoattribute:: task_owner

    .. autoattribute:: current_event

    .. autoattribute:: concurrency_index

    .. autoattribute:: children

    .. autoattribute:: link


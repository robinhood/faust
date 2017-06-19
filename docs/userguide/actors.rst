.. _actors-guide:

=========================================
  Actors
=========================================

.. module:: faust

.. currentmodule:: faust

.. contents::
    :local:
    :depth: 1

.. _actor-basics:

Basics
======

What are actors?
----------------

The actor model provides a solution for safe concurrent computations
by using message passing and isolating state:

- they have an inbox that is processed in order
- the actor may have local state that is mutated as messages are processed.
- The actor may reply to a request with a return value.

Actors in Faust diverge from the traditional definition in that you do not
address them directly, instead multiple instances of the actor may share
the same inbox.

What about streaming?
---------------------

Stream processing frameworks do not usually have actors, but Faust
differentiates by fusing stream processing with Python async iterators
in a way that gives you the flexibility to embed stream processing directly
into your programs, or web servers.

Comparing streams and actors may seem like a strange idea, but while differing in
nomenclature, at the core they are very similar, and can easily share
implementation.

In Faust actors can be used to define both passive stream processing
workflows, and active network services, with zero overhead from the features

An actor in Faust is an async function that takes a stream as argument
and iterates over it.

Here's an example actor that adds numbers:

.. code-block:: python

    # examples/actor.py

    import faust

    # The model describes the data sent to our actor,
    # and in our case we will use a JSON serialized dictionary
    # with two integer fields: a, and b.
    class Add(faust.Record):
        a: int
        b: int

    # Next, we create the Faust application object that
    # configures our environment.
    app = faust.App('actor-example')

    # The Kafka topic used by our actor is named 'adding',
    # and we specify that the values in this topic are of the Add model.
    # (you can also specify the key_type if your topic uses keys).
    topic = app.topic('adding', value_type=Add)

    @app.actor(topic)
    async def adding(stream):
        async for value in stream:
            # here we receive Add objects, add a + b.
            yield value.a + value.b

Starting a worker will now start a single instance of this actor:

.. code-block:: console

    $ faust -A examples.actor worker -l info

To send values to it, you can open a second console to run this program:

.. code-block:: python

    # examples/send_to_actor.py
    import asyncio
    from .actor import Add, adding

    async def send_value() -> None:
        print(await adding.ask(Add(a=4, b=4)))

    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_value())

.. code-block:: console

    $ python examples/send_to_actor.py

.. admonition:: Static types

    Faust is typed using the type annotations available in Python 3.6,
    and can be checked using the `mypy`_ type checker.

    .. _`mypy`: http://mypy-lang.org

    The same function above can be annotated like this:

    .. code-block:: python

        from typing import AsyncIterable
        from faust import StreamT

        @app.actor(topic)
        async def adding(stream: StreamT[Add]) -> AsyncIterable[int]:
            async for value in stream:
                yield value.a + value.b


Under the Hood: The ``@actor`` decorator
----------------------------------------

You can easily start a stream processor in Faust without using actors.
Just start an :mod:`asyncio` task that iterates over a stream:

.. code-block:: python

    # examples/noactor.py
    import asyncio

    app = faust.App('noactor')
    topic = app.topic('noactor')

    async def mystream():
        async for event in topic.stream():
            print(f'Received: {event!r}')

    async def start_streams():
        await app.start()
        await mystream()

    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start_streams())

Essentially what the ``@actor`` decorator does, given a function like this:

.. code-block:: python

    @app.actor(topic)
    async def mystream(stream):
        async for event in stream:
            print(f'Received: {event!r}')
            yield event

Is that it wraps your function, that returns an async iterator (since it uses
``yield``) in code similar to this:

.. code-block:: python

    def actor(topic):

        def create_actor_from(fun):
            async def _start_actor():
                stream = topic.stream()
                async for result in fun(stream):
                    maybe_reply_to_caller(result)

Defining actors
===============

.. _actor-topic:

The topic argument to the actor decorator defines the main topic
that actor reads from (this implies it's not necessarily the only
topic, as is the case when using stream joins, for example).

Topics are defined using the :meth:`@topic` helper, and returns a
:class:`faust.Topic` description:

    topic = app.topic('topic_name1', 'topic_name2',
                      key_type=Model,
                      value_type=Model,
                      ...)

If the topic description provides multiple topic names, the main
topic of the actor will be the first topic in that list (``"topic_name1"``).

The ``key_type`` and ``value_type`` describes how messages in the topics
are serialized.  This can either be a model (such as :class:`faust.Record`,
), a :class:`faust.Codec`, or the name of a serializer.  If not specified
then the default serializer defined by the app will be used.

.. tip::

    If you don't specify a topic, the actor will use the actor name
    as topic: the name will be the fully qualified name of the actor function
    (e.g. ``examples.actor.adder``).

.. _actor-concurrency:

Concurrency
-----------

You can start multiple instances of an actor by specifying the ``concurrency``
argument.

.. warning::

    Since having concurrent instances of an actor means that events in
    the stream will be processed out of order, it's very important that
    you do not mutate :ref:`tables <guide-tables>` from witin the
    actor function:

    An actor with `concurrency > 1`, can only read from a table, never write.

Here's an actor example that can safely process the stream out of order:
whenever a new newsarticle is created something posts to the 'news' topic,
this actor retrieves that article and stores it in a database.

.. code-block:: python

    news_topic = app.topic('news')

    @app.actor()
    async def imports_news(articles):
        async for article in articles:
            response = await aiohttp.ClientSession().get(article.url)
            await store_article_in_db(response)

.. _actor-sinks:

Sinks
-----

Sinks can be used to perform additional actions after the actor has processed
an event in the stream, such as forwarding alerts to a monitoring system,
logging to Slack, etc. A sink can be callable, async callable, a topic or
another actor.

Function Callback
    Regular functions take a single argument (the value yielded by the actor):

    .. code-block:: python

        def mysink(value):
            print(f'ACTOR YIELD: {value!r}')

        @app.actor(sink=[mysink])
        async def myactor(stream):
            ...

Async Function Callback
    Async functions can also be used, in this case the async function will be
    awaiated by the actor:

    .. code-block:: python3

        async def mysink(value):
            print(f'ACTOR YIELD: {value!r}')
            # This will force the actor instance that yielded this value
            # to sleep for 1.0 second before continuing on the next event
            # in the stream.
            await asyncio.sleep(1)

        @app.actor(sink=[mysink])
        async def myactor(stream):
            ...

Topic
    Specifying a topic as sink will force the actor to forward yielded values
    to that topic:

    .. code-block:: python

        actor_log_topic = app.topic('actor_log')

        @app.actor(sink=[actor_log_topic])
        async def myactor(stream):
            ...

Another Actor
    Specyfing another actor as sink will force the actor to forward yielded
    values to that actor:

    .. code-block:: python

        @app.actor()
        async def actor_b(stream):
            async for event in stream:
                print(f'ACTOR B RECEIVED: {event!r}')

        @app.actor(sink=[actor_b])
        async def actor_a(stream):
            async for event in stream:
                print(f'ACTOR A RECEIVED: {event!r}')

Using actors
============

Cast or Ask?
------------

When communicating with an actor you can request the result of the
operation to be sent to a topic: this is the ``reply_to`` topic.
The reply topic may be the topic of another actor, a topic used by a different
system altogether, or it may be a local ephemeral topic that will collect
replies to the current process.

Performing a ``cast`` means no reply is expected, you are only sending the
actor a message, not expecting a reply back.

``cast(value, *, key=None, partition=None)``
    Casting a value to an actor is asynchronous:

    .. code-block:: python

        await adder.cast(Add(a=2, b=2))

    The actor will receive this value, but it will not send a reply.

``ask(value, *, key=None, partition=None, reply_to=None, correlation_id=None)``
    Asking an actor will send a reply back to the current process:

    .. code-block:: python

        value = await adder.ask(Add(a=2, b=2))
        assert value == 4

``send(key, value, partition, reply_to=None, correlation_id=None)``
    The ``Actor.send`` method is the underlying mechanism used by ``cast`` and
    ``ask``, and enables you to request that a reply is sent to another actor
    or a specific topic.

    Send to another actor:

    .. code-block:: python

        await adder.send(value=Add(a=2, b=2), reply_to=another_actor)

Streaming Map/Reduce
--------------------

The actor also provides operations for streaming values to the actors and
gathering the results: ``map`` streams results as they come in (unordered),
and ``join`` waits until the operations are complete and return the results
in order as a list.

``map(values: Union[AsyncIterable[V], Iterable[V]])``
    Map takes an async iterable, or a regular iterable, and returns an async
    iterator yielding results as they come in:

    .. code-block:: python

        async for reply in actor.map([1, 2, 3, 4, 5, 6, 7, 8]):
            print(f'RECEIVED REPLY: {reply!r}')

    The iterator will start before all the messages have been sent, and
    should be efficient even for infinite lists.  Note that order of replies
    is not preserved since the map is executed concurrently.

``kvmap(items: Union[AsyncIterable[Tuple[K, V], Iterable[Tuple[K, V]]]])``
    Same as ``map``, but takes an async iterable/iterable of ``(key, value)`` tuples,
    where the key in each pair is used as the Kafka message key.

``join(values: Union[AsyncIterable[V], Iterable[V]])``
    Join works like ``map`` but will wait until all of the values have been
    processed and returns them as a list in the original order (so
    cannot be used for infinite lists).

    .. code-block:: python

        results = await pow2.join([1, 2, 3, 4, 5, 6, 7, 8])
        assert results == [1, 4, 9, 16, 25, 36, 49, 64]

``kvjoin(items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]])``
    Same as join, but takes an async iterable/iterable of ``(key, value)`` tuples,
    where the key in each pair is used as the Kafka message key.

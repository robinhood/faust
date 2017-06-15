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

Actors add a convenient interface on top of the primitives in Faust,
you don't have to use them but it makes developing application easier.

An actor in Faust is an async function that takes a stream as argument
and iterates over it:

.. code-block:: python
    # examples/actor.py

    import faust

    # the record describes the data sent to our actor
    class Add(faust.Record):
        a: int
        b: int

    app = faust.App('actor-example')
    # TIP: You may also reuse topics used by other systems
    # Faust actors are compatible with any topic, as long as it
    # can deserialize what's in it (while json is the default
    # serialization is extensible).
    topic = app.topic('adding', value_type=Add)

    @app.actor(topic)
    async def adding(stream):
        async for value in stream:
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


Defining actors
===============

.. _actor-concurrency:

Concurrency
-----------

You can start multiple instances of an actor by specifying the ``concurrency``
argument.

Since having concurrent instances of an actor means that events in the
stream will be processed out of order, it's very important that you do not
populate tables:

.. warning::

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
an event in the stream.  A sink can be: callable, async callable, a topic or
another actor.

Function Callback
~~~~~~~~~~~~~~~~~

Regular functions take a single argument (the value yielded by the actor):

.. code-block:: python

    def mysink(value):
        print(f'ACTOR YIELD: {value!r}')

    @app.actor(sink=[mysink])
    async def myactor(stream):
        ...

Async Function Callback
~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~

Specifying a topic as sink will force the actor to forward yielded values
to that topic:

.. code-block:: python

    actor_log_topic = app.topic('actor_log')

    @app.actor(sink=[actor_log_topic])
    async def myactor(stream):
        ...

Another Actor
~~~~~~~~~~~~~

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


``cast``
~~~~~~~~

Casting a value to an actor is asynchronous:

.. code-block:: python

    await adder.cast(Add(a=2, b=2))

The actor will receive this value, but it will not send a reply.

``ask``
~~~~~~~

Asking an actor will send a reply back to the current process:

.. code-block:: python

    value = await adder.ask(Add(a=2, b=2))
    assert value == 4


``send``
~~~~~~~~

The ``Actor.send`` method is the underlying mechanism used by ``cast`` and
``ask``, and enables you to request that a reply is sent to another actor or a
specific topic.

Send to another actor:

.. code-block:: python

    await adder.send(value=Add(a=2, b=2), reply_to=another_actor)

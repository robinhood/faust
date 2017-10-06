.. _agents-guide:

=========================================
  Agents
=========================================

.. module:: faust

.. currentmodule:: faust

.. contents::
    :local:
    :depth: 1

.. _agent-basics:

Basics
======

Faust differentiates itself from other Stream processing frameworks by fusing
stream processing with Python async iterators in a way that gives you the
flexibility to embed stream processing directly
into your programs, or web servers; the agent portion means that you can
communicate with your stream processors, or create event processing handlers
that extend the scope of traditional stream processing systems.

With Faust agents can be used to define both passive stream processing
workflows, and active network services, with zero overhead from the features

An agent in Faust is simply an async function that takes a stream as argument
and iterates over it.

Here's an example agent that adds numbers:

.. sourcecode:: python

    # examples/agent.py
    import faust

    # The model describes the data sent to our agent,
    # and in our case we will use a JSON serialized dictionary
    # with two integer fields: a, and b.
    class Add(faust.Record):
        a: int
        b: int

    # Next, we create the Faust application object that
    # configures our environment.
    app = faust.App('agent-example')

    # The Kafka topic used by our agent is named 'adding',
    # and we specify that the values in this topic are of the Add model.
    # (you can also specify the key_type if your topic uses keys).
    topic = app.topic('adding', value_type=Add)

    @app.agent(topic)
    async def adding(stream):
        async for value in stream:
            # here we receive Add objects, add a + b.
            yield value.a + value.b

Starting a worker will now start a single instance of this agent:

.. sourcecode:: console

    $ faust -A examples.agent worker -l info

To send values to it, you can open a second console to run this program:

.. sourcecode:: python

    # examples/send_to_agent.py
    import asyncio
    from .agent import Add, adding

    async def send_value() -> None:
        print(await adding.ask(Add(a=4, b=4)))

    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_value())

.. sourcecode:: console

    $ python examples/send_to_agent.py

The :meth:`Agent.ask() <faust.Agent.ask>` method wraps the value sent in
a special structure that includes the return address (reply-to).  When the
agent sees this type of structure it will reply with the result yielded
as a result of processing the value.

.. admonition:: Static types

    Faust is typed using the type annotations available in Python 3.6,
    and can be checked using the `mypy`_ type checker.

    .. _`mypy`: http://mypy-lang.org

    The same function above can be annotated like this:

    .. sourcecode:: python

        from typing import AsyncIterable
        from faust import StreamT

        @app.agent(topic)
        async def adding(stream: StreamT[Add]) -> AsyncIterable[int]:
            async for value in stream:
                yield value.a + value.b

    The ``StreamT`` type used for the agents stream argument is a subclass
    of :class:`~typing.AsyncIterable` extended with the stream API.
    You could type this argument using
    ``AsyncIterable``, but then :pypi:`mypy` would stop you with a typing
    error should you use stream-specific methods such as ``.group_by()``,
    ``through()``, etc.


Under the Hood: The ``@agent`` decorator
----------------------------------------

You can easily start a stream processor in Faust without using agents,
by simply starting an :mod:`asyncio` task that iterates over a stream:

.. sourcecode:: python

    # examples/noagents.py
    import asyncio

    app = faust.App('noagents')
    topic = app.topic('noagents')

    async def mystream():
        async for event in topic.stream():
            print(f'Received: {event!r}')

    async def start_streams():
        await app.start()
        await mystream()

    if __name__ == '__main__':
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start_streams())

Essentially what the ``@agent`` decorator does, given a function like this:

.. sourcecode:: python

    @app.agent(topic)
    async def mystream(stream):
        async for event in stream:
            print(f'Received: {event!r}')
            yield event

Is that it wraps your function, that returns an async iterator (since it uses
``yield``) in code similar to this:

.. sourcecode:: python

    def agent(topic):

        def create_agent_from(fun):
            async def _start_agent():
                stream = topic.stream()
                async for result in fun(stream):
                    maybe_reply_to_caller(result)

Defining Agents
===============

.. _agent-topic:

The Topic
---------

The topic argument to the agent decorator defines the main topic
that agent reads from (this implies it's not necessarily the only
topic, as is the case when using stream joins, for example).

Topics are defined using the :meth:`@topic` helper, and returns a
:class:`faust.Topic` description::

    topic = app.topic('topic_name1', 'topic_name2',
                      key_type=Model,
                      value_type=Model,
                      ...)

If the topic description provides multiple topic names, the main
topic of the agent will be the first topic in that list (``"topic_name1"``).

The ``key_type`` and ``value_type`` describes how messages in the topics
are serialized.  This can either be a model (such as :class:`faust.Record`,
), a :class:`faust.Codec`, or the name of a serializer.  If not specified
then the default serializer defined by the app will be used.

.. tip::

    If you don't specify a topic, the agent will use the agent name
    as topic: the name will be the fully qualified name of the agent function
    (e.g. ``examples.agent.adder``).

.. seealso::

    The :ref:`guide-streams` guide for more information about topics.

The Stream
----------

The decorated function should be unary, acceping a single ``stream`` argument.
which is created from the agents topic.

This object is async iterable and an instance of the :class:`~faust.Stream`
class, created from the topic provided to the decorator.

Iterating over this stream, using the :keyword:`async for`, will iterate
over the messages in the topic.

You can also use the Stream API, for using :meth:`~faust.Stream.group_by`
to partition the stream differently:

.. sourcecode:: python

    # examples/groupby.py
    import faust

    class BankTransfer(faust.Record):
        account_id: str
        amount: float

    app = faust.App('groupby')
    topic = app.topic('groupby', value_type=BankTransfer)

    @app.agent(topic)
    async def stream(s):
        async for transfer in s.group_by(BankTransfer.account_id):
            # transfers will now be distributed such that transfers
            # with the same account_id always arrives to the same agent
            # instance
            ...

Using stream-to-stream joins with agents is a bit more tricky, considering
that the agent always needs to have one main topic.  You may use one topic
as the seed and combine that with more topics, but then it will be impossible
to communicate directly with the agent since you have to send a message to all
the topics, and that is more than challenging:

.. sourcecode:: python

    topic1 = app.topic('foo1')
    topic2 = app.topic('foo2')

    @app.agent(topic)
    async def mystream(stream):
        async for event in (stream & topic2.stream()).join(...):
            ...

What you could do is define a separate topic for communicating with the agent:

.. sourcecode:: python

    topic1 = app.topic('foo1')
    topic2 = app.topic('foo2')
    backchannel_topic = app.topic('foo-backchannel')

    @app.agent(backchannel_topic)
    async def mystream(backchannel):
        joined_streams = (topic1.stream() & topic2.stream()).join(...)
        async for event in (backchannel & joined_streams):
            if event.topic in backchannel.source.topic.topics:
                yield 'some_reply'
            else:
                # handle joined stream

But even when you want to remotely inquire about the state of this stream processor,
there are better ways to do so (like using one stream processor task, and one
agent), so agents are not the best way to process joined streams, instead you
should use a traditional asyncio Task:

.. sourcecode:: python

    @app.task()
    def mystream():
        async for event in (topic1.stream() & topic2.stream()).join(...):
            # process merged event

.. seealso::

    The :ref:`guide-streams` guide for more information about streams and topics.


.. _agent-concurrency:

Concurrency
-----------

You can start multiple instances of an agent by specifying the ``concurrency``
argument.

.. warning::

    Since having concurrent instances of an agent means that events in
    the stream will be processed out of order, it's very important that
    you do not mutate :ref:`tables <guide-tables>` from witin the
    agent function:

    An agent with `concurrency > 1`, can only read from a table, never write.

Here's an agent example that can safely process the stream out of order:
whenever a new newsarticle is created something posts to the 'news' topic,
this agent retrieves that article and stores it in a database.

.. sourcecode:: python

    news_topic = app.topic('news')

    @app.agent()
    async def imports_news(articles):
        async for article in articles:
            response = await aiohttp.ClientSession().get(article.url)
            await store_article_in_db(response)

.. _agent-sinks:

Sinks
-----

Sinks can be used to perform additional actions after the agent has processed
an event in the stream, such as forwarding alerts to a monitoring system,
logging to Slack, etc. A sink can be callable, async callable, a topic or
another agent.

Function Callback
    Regular functions take a single argument (the value yielded by the agent):

    .. sourcecode:: python

        def mysink(value):
            print(f'AGENT YIELD: {value!r}')

        @app.agent(sink=[mysink])
        async def myagent(stream):
            ...

Async Function Callback
    Async functions can also be used, in this case the async function will be
    awaiated by the agent:

    .. sourcecode:: python

        async def mysink(value):
            print(f'AGENT YIELD: {value!r}')
            # This will force the agent instance that yielded this value
            # to sleep for 1.0 second before continuing on the next event
            # in the stream.
            await asyncio.sleep(1)

        @app.agent(sink=[mysink])
        async def myagent(stream):
            ...

Topic
    Specifying a topic as sink will force the agent to forward yielded values
    to that topic:

    .. sourcecode:: python

        agent_log_topic = app.topic('agent_log')

        @app.agent(sink=[agent_log_topic])
        async def myagent(stream):
            ...

Another Agent
    Specyfing another agent as sink will force the agent to forward yielded
    values to that agent:

    .. sourcecode:: python

        @app.agent()
        async def agent_b(stream):
            async for event in stream:
                print(f'AGENT B RECEIVED: {event!r}')

        @app.agent(sink=[agent_b])
        async def agent_a(stream):
            async for event in stream:
                print(f'AGENT A RECEIVED: {event!r}')

Using Agents
============

Cast or Ask?
------------

When communicating with an agent you can request the result of the
operation to be sent to a topic: this is the ``reply_to`` topic.
The reply topic may be the topic of another agent, a topic used by a different
system altogether, or it may be a local ephemeral topic that will collect
replies to the current process.

Performing a ``cast`` means no reply is expected, you are only sending the
agent a message, not expecting a reply back.  This is the preferred mode
of operation for most agents, and any time you are about to use the RPC
facilities of agents you should take a step back to reconsider if there
is a way to solve your problem in a streaming manner.

``cast(value, *, key=None, partition=None)``
    Casting a value to an agent is asynchronous:

    .. sourcecode:: python

        await adder.cast(Add(a=2, b=2))

    The agent will receive this value, but it will not send a reply.

``ask(value, *, key=None, partition=None, reply_to=None, correlation_id=None)``
    Asking an agent will send a reply back to the current process:

    .. sourcecode:: python

        value = await adder.ask(Add(a=2, b=2))
        assert value == 4

``send(key, value, partition, reply_to=None, correlation_id=None)``
    The ``Agent.send`` method is the underlying mechanism used by ``cast`` and
    ``ask``, and enables you to request that a reply is sent to another agent
    or a specific topic.

    Send to another agent::

    .. sourcecode:: python

        await adder.send(value=Add(a=2, b=2), reply_to=another_agent)

Streaming Map/Reduce
--------------------

The agent also provides operations for streaming values to the agents and
gathering the results: ``map`` streams results as they come in (unordered),
and ``join`` waits until the operations are complete and return the results
in order as a list.

``map(values: Union[AsyncIterable[V], Iterable[V]])``
    Map takes an async iterable, or a regular iterable, and returns an async
    iterator yielding results as they come in:

    .. sourcecode:: python

        async for reply in agent.map([1, 2, 3, 4, 5, 6, 7, 8]):
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

    .. sourcecode:: python

        results = await pow2.join([1, 2, 3, 4, 5, 6, 7, 8])
        assert results == [1, 4, 9, 16, 25, 36, 49, 64]

``kvjoin(items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]])``
    Same as join, but takes an async iterable/iterable of ``(key, value)`` tuples,
    where the key in each pair is used as the Kafka message key.

.. _guide-agents:

=============================================
  Agents - Self-organizing Stream Processors
=============================================

.. module:: faust

.. currentmodule:: faust

.. contents::
    :local:
    :depth: 2

.. _agent-basics:

What is an Agent?
=================

An agent is a distributed system processing the events in a stream.

Every event is a message in the stream and is structured as a key/value pair
that can be described using :ref:`models <guide-models>` for type safety
and straightforward serialization support.

Streams can be sharded in a round-robin manner, or partitioned by
the message key; this decides how the stream divides
between available agent instances in the cluster.

**Create an agent**
    To create an agent, you need to use the ``@app.agent`` decorator
    on an async function taking a stream as the argument. Further,
    it must iterate over the stream using the :keyword:`async for` keyword
    to process the stream:

    .. sourcecode:: python

        # faustexample.py

        import faust

        app = faust.App('example', broker='kafka://localhost:9092')


        @app.agent()
        async def myagent(stream):
            async for event in stream:
                ...  # process event

**Start a worker for the agent**
    The :program:`faust worker` program can be used to start a worker from
    the same directory as the :file:`faustexample.py` file:

    .. sourcecode:: console

        $ faust -A faustexample worker -l info

Every new worker that you start will force the cluster to rebalance
partitions so that every agent receives a specific portion of the stream.

.. topic:: Partitioning

    When an agent reads from a topic, the stream is partitioned based on the
    key of the message. For example, the stream could have keys that are
    account ids, and values that are high scores, then partitioning decide
    that any message with the same account id as key, is delivered to the same
    agent instance.

    Sometimes you'll have to repartition the stream, to ensure you are
    receiving the right portion of the data.  See :ref:`guide-streams` for
    more information on the :meth:`Stream.group_by() <faust.Stream.group_by>`
    method.

.. topic:: Round-Robin

    If you don't set a key (i.e. ``key=None``), the messages will be delivered to
    available workers in round-robin order. This is useful to simply distribute work
    amongst a cluster of workers, and you can always repartition that stream later
    should you need to access data in a table or similar.

.. admonition:: Fault tolerancy

    If the worker for a partition fails, or is blocked from the network for
    some reason, there is no need to worry because Kafka solves this by moving
    the partition to a worker that works.

    Faust also takes advantage of "standby tables" and a custom partition
    manager that prefes to promote any node with a full copy of the data,
    saving startup time and ensuring availability.

Here's a complete example of an app, having an agent that adds numbers:

.. sourcecode:: python

    # examples/agent.py
    import faust

    # The model describes the data sent to our agent,
    # We will use a JSON serialized dictionary
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

To send values to it, open a second console to run this program:

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


.. topic:: Define commands with the ``@app.command`` decorator.

    You can also use :ref:`tasks-cli-commands` to add actions for your
    application on the command line.  Use the ``@app.command`` decorator to
    rewrite the example program above (:file:`examples/agent.py`), like this:

    .. sourcecode:: python

        @app.command()
        async def send_value() -> None:
            print(await adding.ask(Add(a=4, b=4)))

    After adding this to your :file:`examples/agent.py` module, run your
    new command using the :program:`faust` program:

    .. sourcecode:: console

        $ faust -A examples.agent send_value

    You may also specify command line arguments and options:

    .. sourcecode:: python

        from faust.cli import argument, option

        @app.command(
            argument('a', type=int, help='First number to add'),
            argument('b', type=int, help='Second number to add'),
            option('--print/--no-print', help='Enable debug output'),
        )
        async def send_value(a: int, b: int, print: bool) -> None:
            if print:
                print(f'Sending Add({x}, {y})...')
            print(await adding.ask(Add(a, b)))

    Then pass those arguments on the command line:

    .. sourcecode:: console

        $ faust -A examples.agent send_value 4 8 --print
        Sending Add(4, 8)...
        12

The :meth:`Agent.ask() <faust.Agent.ask>` method wraps the value sent in
a particular structure that includes the return address (reply-to).  When the
agent sees this type of arrangement, it will reply with the result yielded
by the agent as a result of processing the event.

.. admonition:: Static types

    Faust is typed using the type annotations available in Python 3.6,
    and can be checked using the `mypy`_ type checker.

    .. _`mypy`: http://mypy-lang.org

    Add type hints to your agent function like this:

    .. sourcecode:: python

        from typing import AsyncIterable
        from faust import StreamT

        @app.agent(topic)
        async def adding(stream: StreamT[Add]) -> AsyncIterable[int]:
            async for value in stream:
                yield value.a + value.b

    The ``StreamT`` type used for the agent's stream argument is a subclass
    of :class:`~typing.AsyncIterable` extended with the stream API.
    You could type this call using
    ``AsyncIterable``, but then :pypi:`mypy` would stop you with a typing
    error should you use stream-specific methods such as ``.group_by()``,
    ``through()``, etc.


Under the Hood: The ``@agent`` decorator
----------------------------------------

You can quickly start a stream processor in Faust without using agents.
Do so merely by launching an :mod:`asyncio` task that iterates over a stream:

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

It wraps your function returning async iterator (since it uses
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

Topics are defined using the :meth:`@topic` helper and return a
:class:`faust.Topic` description::

    topic = app.topic('topic_name1', 'topic_name2',
                      key_type=Model,
                      value_type=Model,
                      ...)

Should the topic description provide multiple topic names, the main
topic of the agent will be the first topic in that list (``"topic_name1"``).

The ``key_type`` and ``value_type`` describe how to serialize and deserialize
messages in the topic, and you provide it as a model (such
as :class:`faust.Record`), a :class:`faust.Codec`, or the name of a serializer.

If not specified it will use the default serializer defined by the app.

.. tip::

    If you don't specify a topic, the agent will use the agent name
    as the topic: the name will be the fully qualified name of the agent function
    (e.g., ``examples.agent.adder``).

.. seealso::

    - The :ref:`guide-channels` guide -- for more information about topics
      and channels.

The Stream
----------

The decorated function is unary, meaning it must accept a single argument.

The object passed in as the argument to the agent is an async iterable
:class:`~faust.Stream` instance, created from the topic/channel provided
to the decorator:

.. sourcecode:: python

    @app.agent(topic_or_channel)
    async def myagent(stream):
        async for item in stream:
            ...

Iterating over this stream, using the :keyword:`async for` keyword, will in turn
iterate over messages in the topic/channel.

You can also use the :meth:`~faust.Stream.group_by` method of the Stream API,
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

A two-way join works by waiting until it has a message from both topics,
so to synchronously wait for a reply from the agent you would
have to send messages to both topics.  A three-way join means
you have to send a message to each of the three topics and only
then can a reply be produced.

For this reason, you're discouraged from using joins in an agent,
unless you know what you're doing:

.. sourcecode:: python

    topic1 = app.topic('foo1')
    topic2 = app.topic('foo2')

    @app.agent(topic)
    async def mystream(stream):
        # XXX This is not proper use of an agent, as it performs a join.
        # It works fine as long as you don't expect to be able to use
        # ``agent.ask``, ``agent.map`` and similar
        # methods that wait for a reply.
        async for event in (stream & topic2.stream()).join(...):
            ...


For joins, the best practice is to use the ``@app.task`` decorator instead,
to launch an :class:`asyncio.Task` when the app starts,
that manually iterates over the joined stream:

.. sourcecode:: python

    @app.task()
    def mystream():
        async for event in (topic1.stream() & topic2.stream()).join(...):
            # process merged event
            ...


.. seealso::

    - The :ref:`guide-streams` guide -- for more information about streams.

    - The :ref:`guide-channels` guide -- for more information about topics
      and channels.

.. _agent-concurrency:

Concurrency
-----------

Use the ``concurrency`` argument to start multiple instances of an agent
on every worker instance.  Each agent instance (actor) will process
items in the stream concurrently (and in no particular order).

.. warning::

    Concurrent instances of an agent will process the stream out-of-order,
    so you aren't allowed to mutate :ref:`tables <guide-tables>`
    from within the agent function:

    An agent having `concurrency > 1`, can only read from a table, never write.

Here's an agent example that can safely process the stream out of order.

Our hypothetical backend system publishes a message to the Kafka "news" topic
every time a news article is published by an author.

We define an agent that consumes from this topic and
for every new article will retrieve the full article over HTTP,
then store that in a database somewhere (yeah, pretty contrived):

.. sourcecode:: python

    class Article(faust.Record, isodates=True):
        url: str
        date_published: datetime

    news_topic = app.topic('news', value_type=Article)

    @app.agent(news_topic, concurrency=10)
    async def imports_news(articles):
        async for article in articles:
            response = await aiohttp.ClientSession().get(article.url)
            await store_article_in_db(response)

.. _agent-sinks:

Sinks
-----

Sinks can be used to perform additional actions after the agent has processed
an event in the stream, such as forwarding alerts to a monitoring system,
logging to Slack, etc. A sink can be callable, async callable, a topic/channel or
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
    If you provide an async function, the agent will :keyword:`await` it:

    .. sourcecode:: python

        async def mysink(value):
            print(f'AGENT YIELD: {value!r}')
            # OBS This will force the agent instance that yielded this value
            # to sleep for 1.0 second before continuing on the next event
            # in the stream.
            await asyncio.sleep(1)

        @app.agent(sink=[mysink])
        async def myagent(stream):
            ...

Topic
    Specifying a topic as the sink will force the agent to forward yielded
    values it:

    .. sourcecode:: python

        agent_log_topic = app.topic('agent_log')

        @app.agent(sink=[agent_log_topic])
        async def myagent(stream):
            ...

Another Agent
    Specifying another agent as the sink will force the agent to forward yielded
    values to it:

    .. sourcecode:: python

        @app.agent()
        async def agent_b(stream):
            async for event in stream:
                print(f'AGENT B RECEIVED: {event!r}')

        @app.agent(sink=[agent_b])
        async def agent_a(stream):
            async for event in stream:
                print(f'AGENT A RECEIVED: {event!r}')


.. _agent-errors:

When agents raise an error
--------------------------

If an agent raises in the middle of processing an :term:`event`
what do we do with :term:`acking` it?

Currently the source message will be acked and not processed again,
simply because it violates ""exactly-once" semantics".

- What about retries?

    It'd be safe to retry processing the event if the agent
    processing is :term:`idempotent`, but we don't enforce idempotency
    in stream processors so it's not something we can enable by default.

    The retry would also have to stop processing of the topic
    so that order is maintained: the next offset in the topic can only
    be processed after the event is retried.

- How about crashing?

    Crashing the instance to require human intervention is certainly
    a choice, but far from ideal considering how common mistakes
    in code or unhandled exceptions are.  It may be better to log
    the error and have ops replay and reprocess the stream on
    notification.

Using Agents
============

Cast or Ask?
------------

When communicating with an agent, you can ask for the result of the
request to be forwarded to another topic: this is the :setting:`reply_to` topic.

The ``reply_to`` topic may be the topic of another agent, a source topic
populated by a different system, or it may be a local ephemeral topic
collecting replies to the current process.

If you perform a ``cast``, you're passively sending something to the agent,
and it will not reply back.

Systems perform better when no synchronization is required, so you should
try to solve your problems in a streaming manner.  If B needs to happen
after A, try to have A call B instead (which could be accomplished
using ``reply_to=B``).


``cast(value, *, key=None, partition=None)``
    A cast is non-blocking as it will not wait for a reply:

    .. sourcecode:: python

        await adder.cast(Add(a=2, b=2))

    The agent will receive the request, but it will not send a reply.

``ask(value, *, key=None, partition=None, reply_to=None, correlation_id=None)``
    Asking an agent will send a reply back to process that sent the request:

    .. sourcecode:: python

        value = await adder.ask(Add(a=2, b=2))
        assert value == 4

``send(key, value, partition, reply_to=None, correlation_id=None)``
    The ``Agent.send`` method is the underlying mechanism used by ``cast`` and
    ``ask``.

    Use it to send the reply to another agent:

    .. sourcecode:: python

        await adder.send(value=Add(a=2, b=2), reply_to=another_agent)

Streaming Map/Reduce
--------------------

These map/reduce operations are shortcuts used to stream lots of values
into agents while at the same time gathering the results.

``map`` streams results as they come in (out-of-order), and ``join`` waits
until all the steps are complete (back-to-order) and return the results
in a list with orering preserved:

``map(values: Union[AsyncIterable[V], Iterable[V]])``
    Map takes an async iterable, or a regular iterable, and returns an async
    iterator yielding results as they come in:

    .. sourcecode:: python

        async for reply in agent.map([1, 2, 3, 4, 5, 6, 7, 8]):
            print(f'RECEIVED REPLY: {reply!r}')

    The iterator will start before all the messages have been sent, and
    should be efficient even for infinite lists.

    As the map executes concurrently, the **replies will not appear in any
    particular order**.

``kvmap(items: Union[AsyncIterable[Tuple[K, V], Iterable[Tuple[K, V]]]])``
    Same as ``map``, but takes an async iterable/iterable of ``(key, value)`` tuples,
    where the key in each pair is used as the Kafka message key.

``join(values: Union[AsyncIterable[V], Iterable[V]])``
    Join works like ``map`` but will wait until all of the values have been
    processed and returns them as a list in the original order.

    The :keyword:`await` will continue only after the map sequence is over,
    and all results are accounted for, so do not attempt to use ``join``
    together with infinite data structures ;-)

    .. sourcecode:: python

        results = await pow2.join([1, 2, 3, 4, 5, 6, 7, 8])
        assert results == [1, 4, 9, 16, 25, 36, 49, 64]

``kvjoin(items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]])``
    Same as join, but takes an async iterable/iterable of ``(key, value)`` tuples,
    where the key in each pair is used as the message key.

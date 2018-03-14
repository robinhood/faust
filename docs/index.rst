=======================================================================
 Faust - Python Stream Processing
=======================================================================

.. sourcecode:: python

    # Python Streams ٩(◕‿◕)۶
    # Forever scalable event processing & in-memory
    # durable K/V store; as a library w/ asyncio & static typing.
    import faust

**Faust** is a Python library for *event processing* and *streaming applications*
that are decentralized and fault-tolerant.

Heavily inspired by `Kafka Streams`_, Faust takes
a radically much more straightforward approach to stream processing
and is very simple to learn and use.

It is similar to tools such as `Apache Spark`_/`Storm`_/`Samza`_,
and `Apache Flink`_, but the Faust API is used for both *stream processing*
and *event processing*, as provided by libraries such as :pypi:`Celery`.

Faust lets you write streaming pipelines using native Python code,
so instead of a DSL like ``stream().groupBy(x).filterNot(y).etc.``,
Faust is just Python so you can reuse your existing code:

.. sourcecode:: python

    app = faust.App('myapp', broker='kafka://localhost')

    @app.agent()
    async def process(stream):
        async for event in stream:
            if event > 1000:
                yield alert('WOW')

But wait, something is different? Yeah, that's the new :keyword:`async
def`/:keyword:`await` syntax added in Python 3.6.  Faust takes advantage
of :mod:`asyncio` to be asynchronous: many of these agents can run at
the same time along with other background tasks, periodic timers,
and network services.

The :keyword:`async for` expression means you can perform network
requests and do other I/O as a side effect of processing
a stream -- without blocking other agents from executing at the same time.

Faust depends on Apache Kafka as a message broker. Thus it expects the
ability to go forward and backward in time, treating the stream as a
compacted log of events. We didn't specify a topic in the example above,
but every agent is associated with a Kafka topic, and not specifying one
means it will be anonymous. If you have an existing source topic you want
to use you can also do so:

.. sourcecode:: python

    orders_topic = app.topic('orders')

    @app.agent(orders_topic)
    async def process(stream):
        ...

Faust optionally runs a web server so you can host your Web App on the
same system, allowing you to rapidly prototype traditionally complex
web app architectures that are easy to deploy and scale.

Also tables... `Kafka Streams`_ describes this as "turning the database
inside-out," and Faust supports it too! It means Faust doubles as a
distributed key/value store. Stored locally using `RocksDB`_,
an embedded database and using Kafka topics as a write-ahead log
for recovery.

Count page views per URL:

.. sourcecode:: python

    click_topic = app.topic(click, key_type=str, value_type=str)

    # default value for missing URL will be 0 with `default=int`
    counts = app.Table('click_counts', default=int)

    @app.agent(click_topic)
    async def count_click(clicks):
        async for url, count in clicks.items():  # key, value
            counts[url] += count

    async def click(url, n=1):
        await counts.send(key=url, value=n)


The data in streams can be diverse as everything from byte streams, text,
to manually deserialized data structures is easy to use. Additionally,
Faust supports using modern Python syntax to describe how keys and values
in topics are serialized and deserialized:

.. sourcecode:: python

    class Order(faust.Record):
        account_id: str
        instrument_id: str
        amount: float
        value: float

    orders_topic = app.topic('orders', key_type=str, value_type=Order)

Faust is statically typed, using the :pypi:`mypy` type checker,
so you can take advantage of static types when writing Faust applications.

.. tip::

    For agents that do not modify tables, you can specify the number
    of concurrent coroutines (``@app.agent(concurrency=1000)``) to process
    up to 1000 events in a topic concurrently. Using asynchronous I/O you
    can process thousands of external web requests every second, or some
    other I/O related task, and if that is not enough, you can start more
    machines to help process the workload.

**Learn more about Faust in the** :ref:`intro` **introduction page**
    to read more about Faust, system requirements, installation instructions,
    community resources, and more.

**or go directly to the** :ref:`quickstart` **tutorial**
    to see Faust in action by programming a streaming application.

**then explore the** :ref:`User Guide <guide>`
    for in-depth information organized by topic.

.. _`Kafka Streams`: https://kafka.apache.org/documentation/streams
.. _`Apache Spark`: http://spark.apache.org
.. _`Storm`: http://storm.apache.org
.. _`Samza`: http://samza.apache.org
.. _`Apache Flink`: http://flink.apache.org
.. _`RocksDB`: http://rocksdb.org

Contents
========

.. toctree::
    :maxdepth: 1

    copyright

.. toctree::
    :maxdepth: 2

    introduction/index

.. toctree::
    :maxdepth: 2

    playbooks/index
    userguide/index

.. toctree::
    :maxdepth: 1

    faq
    reference/index
    changelog
    contributing
    developerguide/index
    glossary

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. figure:: images/drawing.png
    :align: left
    :scale: 60%


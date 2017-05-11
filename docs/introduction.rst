.. _intro:

=============================
 Introduction to Faust
=============================

.. contents::
    :local:
    :depth: 1

Faust is a Python library for event processing and streaming applications
that are distributed and fault-tolerant.

It's inspired by tools such as `Kafka Streams`_, `Apache Spark`_,
`Apache Storm`_, `Apache Samza`_ and `Apache Flink`_; but takes
a radically much simpler approach to stream processing.

Modern web applications are increasingly being written as a collection
of microservices and even before this it has been difficult to write
data reporting operations at scale.  In a reactive stream based system,
you don't have to strain your database with costly queries, instead a streaming
data pipeline updates information as events happen in your system, in real-time.

Faust also enables you to take advantage of asyncio and asynchronous
processing, moving complicated and costly operations outside
of the webserver process: converting video, notifying third-party services,
etc. are common use cases for event processing.

You may not know it yet, but if you're writing a modern web application,
you probably already have a need for Faust.

.. _`Kafka Streams`: https://kafka.apache.org/documentation/streams
.. _`Apache Spark`: http://spark.apache.org
.. _`Apache Storm`: http://storm.apache.org
.. _`Apache Flink`: http://flink.apache.org
.. _`Apache Samza`: http://samza.apache.org

Faust is...
==========================

**Simple**
    Faust is extremely easy to use compared to other stream processing
    solutions.  There's no DSL to limit your creativity, no restricted
    set of operations to work from, and since Faust is a library it can
    integrate with just about anything.

    Here's one of the simplest applications you can make:

    .. code-block:: python

        import faust

        class Greeting(faust.Record):
            from_name: str
            to_name: str

        app = faust.App('hello-app', url='kafka://localhost')
        topic = app.topic('hello-topic', value_type=Greeting)

        @app.task
        async def hello(app):
            async for greeting in app.stream(topic):
                print(f'Hello from {greeting.from_name} to {greeting.to_name}')

        @app.timer(interval=1.0)
        async def example_sender(app):
            await app.send(
                topic,
                value=Greeting(from_name='Faust', to_name='you'),
            )

        if __name__ == '__main__':
            app.start()

    You're probably a bit intimidated by the `async` and `await` keywords,
    but you don't have to know how asyncio works to use
    Faust: just mimic the examples and you'll be fine.

    The example application starts two tasks: one is processing a stream,
    the other is a background thread sending events to that stream.
    In a real-live application your system will publish
    events to Kafka topics that your processors can consume from,
    and the background thread is only needed to feed data into our
    example.

**Highly Available**
    Faust is highly available and can survive network problems and server
    crashes.  In the case of node failure it can automatically recover,
    and tables have standby nodes that will take over.

**Distributed**
    Start more instances of your application as needed.

**Fast**
    Faust applications can hopefully handle millions of events per second
    in the future.

**Flexible**
    Faust is just Python, and a stream is just an infinite async iterator.
    If you know how to use Python, you already know how to use Faust,
    and it works with your favorite Python libraries like Django, Flask,
    SQLAlchemy, NTLK, NumPy, Scikit, TensorFlow, etc.

.. topic:: Faust can be used for...

    .. hlist::
        :columns: 2

        - **Event Processing**

        - **Distributed Joins & Aggregations**

        - **Machine Learning**

        - **Asynchronous Tasks**

        - **Distributed Computing**

        - **Data Denormalization**

        - **Intrusion Detection**

        - **Realtime Web & Web Sockets.**

        - **and much more...**

What do I need?
===============

.. sidebar:: Version Requirements
    :subtitle: Faust version 1.0 runs on

    **Core**

    - Python 3.6
    - Kafka 0.10 or later.

    **Extensions**

    - RocksDB 5.0 or later, python-rocksdb

Faust requires Python 3.6 or later, and a running Kafka broker.

There's currently no plan to port Faust to earlier Python versions,
please get in touch if this is something that you want to work on.

Extensions
----------

+------------+-------------+--------------------------------------------------+
| **Name**   | **Version** | **Bundle**                                       |
+------------+-------------+--------------------------------------------------+
| rocksdb    | 5.0         | ``pip install faust[rocksdb]``                   |
+------------+-------------+--------------------------------------------------+
| uvloop     | 0.8.0       | ``pip install faust[uvloop]``                    |
+------------+-------------+--------------------------------------------------+
| aiomonitor | 0.2.1       | ``pip install faust[debug]``                     |
+------------+-------------+--------------------------------------------------+
| aiodns     | 1.0         | ``pip install faust[fast]``                      |
+------------+-------------+--------------------------------------------------+
| fastavro   | 0.12        | ``pip install faust[fast]``                      |
+------------+-------------+--------------------------------------------------+

.. note::

    You can install multiple bundles at the same time:

    .. code-block:: console

        $ pip install -U faust[fast,rocksdb,uvloop]

    and also use them in requirement files:

    :file:`requirements.txt`:

    .. code-block:: text

        faust[fast,rocksdb,uvloop]

How do I use it?
================

.. topic:: Step 1: Add events to your system

    - Was an account created? Publish to Kafka.

    - Did someone change their password? Publish to Kafka.

    - Did someone make an order, create a comment, tag something, ...?
      Publish it all to Kafka!

.. topic:: Step 2: Use Faust to process those events

    Some ideas based around the events mentioned above:

    - Send email once an order is dispatched.

    - Find orders that were made, but no associated dispatch event
      after three days.

    - Find accounts that changed their password from a suspicious IP address.

    - Starting to get the idea?

Design considerations
=====================

Modern Python
    Faust uses modern Python 3 features such as ``async``/``await`` and type
    annotations.  You can take advantage of type annotations when writing
    Faust applications, but this is not mandatory.

Library
    Faust is designed to be used as a library, and embeds into
    any existing Python program, while also including helpers that
    make it easy to deploy applications without boilerplate.

Live happy, die hard
    Faust is programmed to crash on encountering an error such as losing
    the connection to Kafka.  This means error recovery is up to supervisor
    tools such as `supervisord`_, `Circus`_, or one provided by your Operating
    System.

Extensible
    Faust abstracts away storages, serializers and even message transports,
    to make it easy for developers to extend it with new capabilities,
    and integrate into your existing systems.

.. _`supervisord`: http://supervisord.org

.. _`circus`: http://circus.readthedocs.io/

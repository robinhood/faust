.. _devguide-overview:

================================
 Contributors Guide to the Code
================================

.. contents::
    :local:

Module Overview
===============

``faust.app``
    Defines the Faust application: configuration, sending messages, etc.

``faust.cli``
    Command-line interface.

``faust.exceptions``
    All custom exceptions are defined in this module.

``faust.models``
    Models describe how message keys and values are serialized/deserialized.

``faust.sensors``
    Sensors record statistics from a running Faust application.

``faust.serializers``
    Serialization using JSON, and codecs for encoding.

``faust.stores``
    Table storage: in-memory, RocksDB, etc.

``faust.streams``
    Stream and table implementation.

``faust.topics``
    Creating topic descriptions, and tools related to topics.

``faust.transport``
    Message transport implementations, e.g. aiokafka.

``faust.types``
    Public interface for static typing.

``faust.utils``
    Utilities.  Note: This package is not allowed to import from the
    top-level package.

``faust.web``
    Web abstractions and web apps served by the Faust web server.

``faust.windows``
    Windowing strategies.

``faust.worker``
    Deployment helper for faust applications: signal handling, graceful
    shutdown, etc.

Services
========

Everything in Faust that can be started/stopped and restarted, is a
:class:`~faust.utils.services.Service`.

Services can start other services, but they can also start asyncio.Tasks via
`self.add_future`.  These dependencies will be started/stopped/restarted with
the service.

``Worker``
----------

The worker can be used to start a Faust application, and performs tasks like
setting up logging, installs signal handlers and debugging tools etc.

``App``
-------

The app configures the Faust instance, and is the entrypoint for just about
everything that happens in a Faust instance.  Consuming/Producing messages,
starting streams and agents, etc.

The app is usually started by ``Worker``, but can also be started alone if
less operating system interaction is wanted, like if you want to embed Faust
in an application that already sets up signal handling and logging.

``Monitor``
-----------

The monitor is a feature-complete sensor that collects statistics about
the running instance.  The monitor data can be exposed by the web server.

``Producer``
------------

The producer is used to publish messages to Kafka topics, and is started
whenever necessary. The App will always starts this when a Faust instance is starting,
in anticipation of messages to be produced.

``Consumer``
------------

The Consumer is responsible for consuming messages from Kafka topics, to be
delivered to the streams.  It does not actually fetch messages (the
``Fetcher`` services does tha), but it handles everything to do with
consumption, like managing topic subscriptions etc.

``Agent``
---------

Agents are also services, and any async function decorated using ``@app.agent``
will start with the app.

``Conductor``
------------------

The topic conductor manages topic subscriptions, and forward messages
from the Kafka consumer to the streams.

``app.stream(topic)`` will iterate over the topic: ``aiter(topic)``.
The conductor feeds messages into that iteration, so the stream
receives messages in the topic::

    async for event in stream(event async for event in topic)

``TableManager``
----------------

Manages tables, including recovery from changelog and caching table contents.
The table manager also starts the tables themselves, and acts as a registry of
tables in the Faust instance.

``Table``
---------

Any user defined table.

``Store``
---------

Every table has a separate store, the store describes how the table is stored
in this instance.  It could be stored in-memory (default), or as a RocksDB
key/value database if the data set is too big to fit in  memory.

``Stream``
----------

These are individual streams, started after everything is set up.

``Fetcher``
-----------

The Fetcher is the service that actually retrieves messages from the kafka
topic.  The fetcher forwards these messages to the TopicManager, which in
turns forwards it to Topic's and streams.

``Web``
-------------

This is a local web server started by the app (see :setting:`web_enable`
setting).






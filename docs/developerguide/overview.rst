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

``faust.bin``
    Command-line interface.

``faust.exceptions``
    All custom exceptions are defined in this module.

``faust.models``
    Models describe how message keys and values are serialized/deserialized.

``faust.sensors``
    Sensors record statistics from a running Faust application.

``faust.serializers``
    Serialization using JSON/Avro, and codecs.

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

Since the Service class requires the asyncio loop at start, there's also
ServiceProxy.  This special subclass is used by App and Actor as they
are created at module time, for example the module ``t.py``::

    # t.py
    import faust

    app = faust.App('myid')

The ServiceProxy makes the initialization of the service part lazy, and
delegates all service methods to a composite class (App -> AppService, Actor
-> ActorService).

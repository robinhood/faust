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

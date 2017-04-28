.. _guide-application:

=======================================
 Application
=======================================

.. _application-basics:

Basics
======

To start using Faust you must define an application instance:

.. code-block:: pycon

    >>> import faust
    >>> app = faust.App('example')

The first argument is the name, and ID of the app.  The ID is used to generate
topics and consumer groups in Kafka.

.. _application-facts:

The application...
------------------

- Is used to encapsulate one or more tasks, forming a process that handles
  streams and maintains tables of information shared between those tasks.

- Can execute on many machines in parallel, forming a cluster of application
  instances.

- But application instances do not share state between them.

    `Instance-A` of 'example' can not access information in tables on
    `Instance-B`: their memory is after all separate, and all communication
    happens via message passing.

- Handles configuration (e.g. the Kafka broker URL, commit interval and so
  on).

- Manages sensors that record statistics and monitors running streams.

- Also defines how Faust works, so you can create an application subclass
  to override how streams and tables are created, or how messages
  are serialized/deserialized, etc.

It is safe to...
----------------

- Run multiple application instances in the same process:

    .. code-block:: pycon

        >>> app1 = faust.App('demo1')
        >>> app2 = faust.App('demo2')

- Share an app between multiple threads (the app is :term:`thread safe`).

.. _application-configuration:

Configuration
=============

A number of keyword arguments are avaialable when instantiating the app, that
enables you to configure it

.. _app-url:

``url``
-------
:type: ``str``
:default: ``"aiokafka://localhost:9092"``

The transport URL defines something like a broker that Faust will use to
send and receive messages.
Currently the only supported transport is ``aiokafka://``.

You can specify a list of hosts by separating them using semicomma:

.. code-block:: text

    aiokafka://kafka1.example.com:9092;kafka2.example.com:9092

.. _app-store:

``store``
---------
:type: ``str``
:default: ``memory://``

The backend used for table storage.
Tables are stored in-memory only by default.

.. _app-avro_registry_url:

``avro_registry_url``
---------------------
:type: ``str``
:default: :const:`None`

The URL of an Avro schema registry server.

See http://docs.confluent.io/1.0/schema-registry/docs/intro.html
.. _app-client_id:

``client_id``
-------------
:type: ``str``
:default: `faust-VERSION`

The client id is used to identify the software used, and is not usually
configured by the user.

.. _app-commit_interval:

``commit_interval``
-------------------
:type: `float`
:default: ``30.0``

How often we commit messages that have been fully processed (:term:`acked`).

.. _app-key_serializer:

``key_serializer``
------------------
:type: ``Union[str, Codec]``
:default: :const:`None`

Serializer used for keys by default when no serializer is specified, or a
model is not being used.

This can be the name of a serializer/codec, or an actual
:class:`faust.serializers.codecs.Codec` instance.

.. _app-value_serializer:

``value_serializer``
------------------
:type: ``Union[str, Codec]``
:default: ``"json"``

Serializer used for values by default when no serializer is specified, or a
model is not being used.

This can be the name of a serializer/codec, or an actual
:class:`faust.serializers.codecs.Codec` instance.

.. _app-num_standby_replicas:

``num_standy_replicas``
-----------------------

XXX NEED TO BE DOCUMENTED XXX

.. _app-replication_factor:

``replication_factor``
----------------------

XXX NEED TO BE DOCUMENTED XXX

.. _app-Stream:

``Stream``
----------
:type: ``Union[str, Type]``
:default: ``"faust.Stream"``

The :class:`faust.Stream` class to use for streams, or the fully-qualified path to one.

.. _app-Table:

``Table``
----------
:type: ``Union[str, Type]``
:default: ``"faust.Table"``

The :class:`faust.Table` class to use for tables, or the fully-qualified path to one.

.. _app-WebSite:

``WebSite``
----------
:type: ``Union[str, Type]``
:default: ``"faust.web.site:create_site"``

A class or callable that creates the :class:`~faust.web.base.Web` instance
that forms what a Faust instance serves over the web.  It can also be the
fully qualified path to one.

.. _guide-application:

=======================================
 Application
=======================================

.. module:: faust

.. currentmodule:: faust

.. contents::
    :local:
    :depth: 1

.. _application-basics:

Basics
======

To start using Faust you must define an application instance:

.. sourcecode:: pycon

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

- Handles configuration.

- Manages sensors that record statistics and monitors running streams.

- Defines how Faust works.

    You can create an application subclass to override how streams and
    tables are created, how messages are serialized and deserialized, and so
    on.


.. topic:: It is safe to...

    - Run multiple application instances in the same process:

        .. sourcecode:: pycon

            >>> app1 = faust.App('demo1')
            >>> app2 = faust.App('demo2')

    - Share an app between multiple threads (the app is :term:`thread safe`).

.. _application-configuration:

Configuration
=============

A number of keyword arguments are avaialable when instantiating the app, these
form the configuration of your Faust application.

The only required paramater is the application id, a string shared by
all instances of the app, that uniquely identifies it:

.. sourcecode:: pycon

    >>> app = faust.App('myid')

The rest of the configuration are passed as keyword-only arguments,
and all of the options described below are optional:

.. sourcecode:: python

    >>> app = faust.App(
    ...     'myid',
    ...     url='kafka://example.com',
    ...     store='rocksdb://',
    ... )

Parameters
----------

`id`
    :type: ``str``

    A string that uniquely identifies the app, to be shared between all
    instances of the app.  Two app instances with the same ID is considered
    to be in the same group.

    This parameter is required.

`url`
    :type: ``str``
    :default: ``"aiokafka://localhost:9092"``

    The transport URL defines something like a broker that Faust will use to
    send and receive messages.
    Currently the only supported transport is ``aiokafka://``.

    You can specify a list of hosts by separating them using semicomma:

    .. sourcecode:: text

        aiokafka://kafka1.example.com:9092;kafka2.example.com:9092

`store`
    :type: ``str``
    :default: ``memory://``

    The backend used for table storage.
    Tables are stored in-memory only by default.

`avro_registry_url`
    :type: ``str``
    :default: :const:`None`

    The URL of an Avro schema registry server.

    See http://docs.confluent.io/1.0/schema-registry/docs/intro.html

`client_id`
    :type: ``str``
    :default: `faust-VERSION`

    The client id is used to identify the software used, and is not usually
    configured by the user.

`commit_interval`
    :type: `float`, :class:`~datetime.timedelta`
    :default: ``30.0``

    How often we commit messages that have been fully processed (:term:`acked`).

`table_cleanup_interval`
    :type: `float`, :class:`~datetime.timedelta`
    :default: ``1.0``

    How often we cleanup tables to remove expired entries.

`key_serializer`
    :type: ``Union[str, Codec]``
    :default: :const:`None`

    Serializer used for keys by default when no serializer is specified, or a
    model is not being used.

    This can be the name of a serializer/codec, or an actual
    :class:`faust.serializers.codecs.Codec` instance.

`value_serializer`
    :type: ``Union[str, Codec]``
    :default: ``"json"``

    Serializer used for values by default when no serializer is specified, or a
    model is not being used.

    This can be the name of a serializer/codec, or an actual
    :class:`faust.serializers.codecs.Codec` instance.

`num_standby_replicas`
    :type: ``int``
    :default: ``0``

    The number of standby replicas for each task.

        replication_factor (int): The replication factor for changelog topics
            and repartition topics created by the application.  Default: ``1``.

`replication_factor`
    :type: ``int``
    :default: ``1``

    The replication factor for changlog topics and repartition topics created
    by the application.

`default_partitions`
    :type: ``int``
    :default: 8

    Default number of partitions for new topics.

`Stream`
    :type: ``Union[str, Type]``
    :default: ``"faust.Stream"``

    The :class:`~faust.Stream` class to use for streams, or the fully-qualified
    path to one.

`Table`
    :type: ``Union[str, Type[TableT]]``
    :default: ``"faust.Table"``

    The :class:`~faust.Table` class to use for tables, or the fully-qualified
    path to one.

`Set`
    :type: ``Union[str, Type[SetT]]``
    :default: ``"faust.Set"``

    The :class:`~faust.Set` class to use for sets, or the fully-qualified
    path to one.

Reference
=========

Methods
-------

Topics
^^^^^^

.. class:: App
    :noindex:

    .. automethod:: topic
        :noindex:

Decorators
^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: actor
        :noindex:

    .. automethod:: task
        :noindex:

    .. automethod:: timer
        :noindex:

Creating streams and tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: stream
        :noindex:

    .. automethod:: Table
        :noindex:

    .. automethod:: Set
        :noindex:

Sending messages
^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: send
        :noindex:

    .. automethod:: send_soon
        :noindex:

Committing topic offsets
^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: commit
        :noindex:

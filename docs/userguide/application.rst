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

The first thing you need to use Faust is an application.

To create one in Python you need to provide
a name (the id), message broker, and a table storage driver to use.

.. sourcecode:: pycon

    >>> import faust
    >>> app = faust.App('example', url='kafka://', store='rocksdb://')

.. _application-facts:

The application...
------------------

The application is an instance of the Faust library.

For very special needs it can be inherited from, and a subclass
will have the ability to change how almost everything works.

Comparing the application to the interface of frameworks like Django,
there are clear benefits.

In Django the global settings module means having multiple configurations is
impossible, and the API is organized by modules so you sometimes
end up with lots of import statements, and many modules to keep track of.
Further you often end up monkey patching to change how something works.

The application keeps the library flexible to changes, and allows
for many applications to coexist in the same process space.

.. topic:: It is safe to...

    - Run multiple application instances in the same process:

        .. sourcecode:: pycon

            >>> app1 = faust.App('demo1')
            >>> app2 = faust.App('demo2')

    - Share an app between multiple threads (the app is :term:`thread safe`).

.. _application-configuration:

Configuration
=============

The defaults are sensible so you can safely
use Faust without changing them.  You probably *will want* to
set the ``url`` and ``store`` options, to configure the broker and
storage driver.

Here we set the broker url to ``kafka://kafka.example.com``, and
the storage driver to ``rocksdb://``:

.. sourcecode:: python

    >>> app = faust.App(
    ...     'myid',
    ...     url='kafka://kafka.example.com',
    ...     store='rocksdb://',
    ... )

If a broker url is not set it will use "localhost".
The first part of the broker URL ("kafka://") is the driver. Only
:pypi:`aiokafka` is supported in version 1.0.

The store decides how distributed tables are stored locally, and version
1.0 only supports two options:

+----------------+-----------------------------------------------+
| ``memory://``  | In-memory only (development)                  |
+----------------+-----------------------------------------------+
| ``rocksdb://`` | `RocksDB`_ an embedded database (production)  |
+----------------+-----------------------------------------------+

Using the ``memory://`` store is OK when developing your project and testing
things out, but for large tables it can take hours to recover after
restart.

`RocksDB`_ recovers in seconds or less, is embedded so don't require a server or
additional infrastructure, and it's stored on the file system so tables can exceed
available memory.

.. _`RocksDB`: http://rocksdb.org/

Parameters
----------

`id`
    :type: ``str``

    A string that uniquely identifies the app, to be shared between all
    instances of the app.  Two app instances with the same id is considered
    to be in the same group.

    This parameter is required.

    .. admonition:: The id and Kafka

        When using Kafka, the id is used to generate app-local topics, and
        names for consumer groups, etc.

`url`
    :type: ``str``
    :default: ``"aiokafka://localhost:9092"``

    Faust needs the URL of a transport to send and receive messages.

    Currently the only supported transport is the ``aiokafka://`` Kafka client.

    You can specify a list of hosts by separating them using semi-comma:

    .. sourcecode:: text

        aiokafka://kafka1.example.com:9092;kafka2.example.com:9092

`store`
    :type: ``str``
    :default: ``memory://``

    The backend used for table storage.
    Tables are stored in-memory only by default, but this is only really
    suitable for testing and development purposes.

    In production a persistent store, such as ``rocksdb://`` should be used.

`avro_registry_url`
    :type: ``str``
    :default: :const:`None`

    The URL of an Avro schema registry server.

    See http://docs.confluent.io/1.0/schema-registry/docs/intro.html

    NOTE:: Currently unsupported.

`client_id`
    :type: ``str``
    :default: `faust-VERSION`

    The client id is used to identify the software used, and is not usually
    configured by the user.

`commit_interval`
    :type: `float`, :class:`~datetime.timedelta`
    :default: ``3.0``

    How often we commit messages that have been fully processed (:term:`acked`).

`table_cleanup_interval`
    :type: `float`, :class:`~datetime.timedelta`
    :default: ``30.0``

    How often we cleanup tables to remove expired entries.

`key_serializer`
    :type: ``Union[str, Codec]``
    :default: :const:`None`

    Serializer used for keys by default when no serializer is specified, or a
    model is not being used.

    This can be the name of a serializer/codec, or an actual
    :class:`faust.serializers.codecs.Codec` instance.

    .. seealso::

        :ref:`guide-codecs`

`value_serializer`
    :type: ``Union[str, Codec]``
    :default: ``"json"``

    Serializer used for values by default when no serializer is specified, or a
    model is not being used.

    This can be the name of a serializer/codec, or an actual
    :class:`faust.serializers.codecs.Codec` instance.

    .. seealso::

        :ref:`guide-codecs`

`num_standby_replicas`
    :type: ``int``
    :default: ``1``

    The number of standby replicas for each table.

`replication_factor`
    :type: ``int``
    :default: ``1``

    The replication factor for changlog topics and repartition topics created
    by the application.

    .. note::

        This would generally be configured to the replication factor for your
        Kafka cluster.

`default_partitions`
    :type: ``int``
    :default: ``8``

    Default number of partitions for new topics.

    .. note::

        This defines the maximum number of workers we could distribute the
        workload of the application (also sometimes referred as the sharding
        factor of the application).

`create_reply_topic`
    :type: ``bool``
    :default: :const:`False`

    Set this to :const:`True` if you plan on using the RPC with agents.

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

`TableManager`
    :type: ``Union[str, Type[TableManagerT]]``
    :default: ``"faust.tables.TableManager"``

    The :class:`~faust.tables.TableManager` used for managing tables,
    or the fully-qualified path to one.

`Serializers`
    :type: ``Union[str, Type[RegistryT]]``
    :default: ``"faust.serializers.Registry"``

    The :class:`~faust.serializers.Registry` class used for
    serializing/deserializing messages; or the fully-qualified path to one.

Reference
=========

Methods
-------

Topics & Channels
^^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: topic
        :noindex:

    .. automethod:: channel
        :noindex:

Decorators
^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: agent
        :noindex:

    .. automethod:: task
        :noindex:

    .. automethod:: timer
        :noindex:

    .. automethod:: page
        :noindex:

    .. automethod:: command
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

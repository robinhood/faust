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

The application is an instance of the Faust library.

To create one in Python you need to provide
a name for the application (the id), a message broker, and a driver to use for
table storage.

.. sourcecode:: pycon

    >>> import faust
    >>> app = faust.App('example', url='kafka://', store='rocksdb://')


For very special needs the :class:`faust.App` class can be inherited from, and a subclass
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
set the ``url`` and ``store`` options, though, to configure the broker and
storage driver.

Here we set the broker url to Kafka, and the storage driver to `RocksDB`_:

.. sourcecode:: python

    >>> app = faust.App(
    ...     'myid',
    ...     url='kafka://kafka.example.com',
    ...     store='rocksdb://',
    ... )

"kafka://localhost" is used if no broker url is set at all.
The first part of the broker URL ("kafka://") is the driver.

The store decides how distributed tables are stored locally, and version
1.0 supports two options:

+----------------+-----------------------------------------------+
| ``rocksdb://`` | `RocksDB`_ an embedded database (production)  |
+----------------+-----------------------------------------------+
| ``memory://``  | In-memory (development)                       |
+----------------+-----------------------------------------------+

Using the ``memory://`` store is OK when developing your project and testing
things out, but for large tables it can take hours to recover after
restart, so you should never use it in production.

`RocksDB`_ recovers in seconds or less, is embedded and don't require a server or
additional infrastructure, and stored on the file system so tables can exceed
the size of available memory.

.. _`RocksDB`: http://rocksdb.org/

Parameters
----------

`id`
    :type: ``str``

    A string uniquely identifiying the app, shared across all
    instances such that two app instances with the same `id` is
    considered to be in the same "group".

    This parameter is required.

    .. admonition:: The id and Kafka

        When using Kafka, the id is used to generate app-local topics, and
        names for consumer groups, etc.

`url`
    :type: ``str``
    :default: ``"aiokafka://localhost:9092"``

    Faust needs the URL of a "transport" used to send and receive messages.

    Currently the only supported transport is the ``aiokafka://`` Kafka client.

    You can specify multiple hosts at the same time by separating them using
    the semi-comma::

    .. sourcecode:: text

        aiokafka://kafka1.example.com:9092;kafka2.example.com:9092

`store`
    :type: ``str``
    :default: ``memory://``

    The backend used for table storage.
    Tables are stored in-memory only by default, but this is only really
    suitable for testing and development purposes.

    In production a persistent table store, such as ``rocksdb://`` is
    preferred.

`autodiscover`
    :type: ``Union[bool, Iterable[str], Callable[[], Iterable[str]]]``

    Enable autodiscovery of agent, page and command decorators.

    .. warning::

        The autodiscover functionality uses :pypi:`venusian` to
        scan wanted packages for ``@app.agent``, ``@app.page``, and
        ``@app.command`` decorators, but to do so it is required
        to traverse the package directory and import every package
        in them.

        Importing random modules like this can be dangerous if best
        practices are not followed in user modules: starting threads,
        network I/O, monkey-patching, or similar, should not happen as
        a side effect of importing a module.

    The value for this argument can be:

    ``bool``
        If ``App(autodiscover=True)`` is set, the autodiscovery will
        scan the package name described in the ``origin`` attribute.
        The ``origin`` attribute is automatically set when you start
        a worker using the :program:`faust` command and the
        :option:`-A examples.simple <faust -A>`, option set, or
        execute your main script using `python examples/simple.py`` when
        that script calls ``app.main()``.

    ``Sequence[str]``
        The argument can also be a list of packages to scan::

            app = App(..., autodiscover=['proj_orders', 'proj_accounts'])

    ``Callable[[], Sequence[str]]``
        The argument can also be a function returning a list of packages
        to scan::

            def get_all_packages_to_scan():
                return ['proj_orders', 'proj_accounts']

             app = App(..., autodiscover=get_all_packages_to_scan)

    .. admonition:: Django

        If you're using Django you could use this to scan for
        agents/pages/commands for all packages in ``INSTALLED_APPS``::

            from django.conf import settings

            app = App(..., autodiscover=lambda: settings.INSTALLED_APPS)

        If you're using recent versions of Django, apps may
        be defined outside of the setting and you should use the following
        instead::

            from django.apps import apps

            app = App(...,
                      autodiscover=(config.name
                                    for config in apps.get_app_configs())

        We use :keyword:`lambda` in the first example, and a generator
        expression in the latter example. This way you can safely import the
        module containing this app without forcing the Django settings machinery
        to be initialized (i.e. settings imported).

    .. tip::

        For manual control over autodiscovery, you can also call the
        :meth:`@discover` method, manually.

`avro_registry_url`
    :type: ``str``
    :default: :const:`None`

    The URL of an Avro schema registry server.

    See http://docs.confluent.io/1.0/schema-registry/docs/intro.html

    NOTE:: Currently unsupported.

`canonical_url`
    :type:  ``str``
    :default: ``socket.gethostname()``

    You shouldn't have to set this manually.

    The canonical URL defines how to reach the web server on a running
    worker node, and is usually set by combining the :option:`faust worker --web-host`
    and :option:`faust worker --web-port` command line arguments, not
    by passing it as a keyword argument to :class:`App`.


`client_id`
    :type: ``str``
    :default: ``faust-VERSION``

    You shouldn't have to set this manually.

    The client id is used to identify the software used, and is not usually
    configured by the user.

`datadir`
    :type: ``Union[str, pathlib.Path]``
    :default: ``{appid}-data``

    The directory in which this instance stores local table data, etc.

    .. seealso::

        Can also be set this using :option:`faust --datadir` option, but a default
        can be passed as a keyword argument to :class:`App`.

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
    :default: ``"json"``

    Serializer used for keys by default when no serializer is specified, or a
    model is not being used.

    This can be the name of a serializer/codec, or an actual
    :class:`faust.serializers.codecs.Codec` instance.

    .. seealso::

        :ref:`codecs`

`value_serializer`
    :type: ``Union[str, Codec]``
    :default: ``"json"``

    Serializer used for values by default when no serializer is specified, or a
    model is not being used.

    This can be string, the name of a serializer/codec, or an actual
    :class:`faust.serializers.codecs.Codec` instance.

    .. seealso::

        :ref:`codecs`

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

`reply_to`
    :type: ``str``
    :default: `<generated>`

    The name of the reply topic used by this instance.  If not set one will be
    automatically generated when the app is created.

`create_reply_topic`
    :type: ``bool``
    :default: :const:`False`

    Set this to :const:`True` if you plan on using the RPC with agents.

`reply_expires`
    :type: ``Union[float, datetime.timedelta]``
    :default: ``timedelta(days=1)``

    The expiry time (in seconds float, or timedelta), for how long replies
    will stay in the instances local reply topic before being removed.

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

Command line
^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: main
        :noindex:

Defining Tables
^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: Table
        :noindex:

    .. automethod:: Set
        :noindex:

Decorator Discovery
^^^^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: discover
        :noindex:

Creating streams
^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: stream
        :noindex:

Sending messages
^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: send
        :noindex:

Committing topic offsets
^^^^^^^^^^^^^^^^^^^^^^^^

.. class:: App
    :noindex:

    .. automethod:: commit
        :noindex:

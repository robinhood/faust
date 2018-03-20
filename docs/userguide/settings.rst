.. _guide-settings:

====================================
 Configuration Reference
====================================

Required Settings
=================

.. setting:: id

``id``
------

:type: ``str``

A string uniquely identifying the app, shared across all
instances such that two app instances with the same `id` are
considered to be in the same "group".

This parameter is required.

.. admonition:: The id and Kafka

    When using Kafka, the id is used to generate app-local topics, and
    names for consumer groups.

Commonly Used Settings
======================

.. setting:: broker

``broker``
----------

:type: ``str``
:default: ``"aiokafka://localhost:9092"``

Faust needs the URL of a "transport" to send and receive messages.

Currently, the only supported production transport is ``kafka://``.
This uses the :pypi:`aiokafka` client under the hood, for consuming and
producing messages.

You can specify multiple hosts at the same time by separating them using
the semi-comma:

.. sourcecode:: text

    aiokafka://kafka1.example.com:9092;kafka2.example.com:9092

.. setting:: store

``store``
---------

:type: ``str``
:default: ``memory://``

The backend used for table storage.

Tables are stored in-memory by default, but you should
not use the ``memory://`` store in production.

In production, a persistent table store, such as ``rocksdb://`` is
preferred.

.. setting:: autodiscover

``autodiscover``
----------------

:type: ``Union[bool, Iterable[str], Callable[[], Iterable[str]]]``

Enable autodiscovery of agent, task, timer, page and command decorators.

Faust has an API to add different :mod:`asyncio` services and other user
extensions, such as "Agents", HTTP web views, command-line commands, and
timers to your Faust workers.  These can be defined in any module, so to
discover them at startup, the worker needs to traverse packages looking
for them.

.. warning::

    The autodiscovery functionality uses the :pypi:`Venusian` library to
    scan wanted packages for ``@app.agent``, ``@app.page``,
    ``@app.command``, ``@app.task`` and ``@app.timer`` decorators,
    but to do so, it's required to traverse the package path and import
    every module in it.

    Importing random modules like this can be dangerous so make sure you
    follow Python programming best practices. Do not start
    threads; perform network I/O; do test monkey-patching for mocks or similar,
    as a side effect of importing a module.  If you encounter a case such as
    this then please find a way to perform your action in a lazy manner.

.. warning::

    If the above warning is something you cannot fix, or if it's out of your
    control, then please set ``autodiscover=False`` and make sure the worker
    imports all modules where your decorators are defined.

The value for this argument can be:

``bool``
    If ``App(autodiscover=True)`` is set, the autodiscovery will
    scan the package name described in the ``origin`` attribute.

    The ``origin`` attribute is automatically set when you start
    a worker using the :program:`faust` command line program, for example::

    .. sourcecode:: console

        faust -A example.simple worker

    The :option:`-A <faust -A>`, option specifies the app, but you can also
    create a shortcut entrypoint entrypoint by calling ``app.main()``:

    .. sourcecode:: python

        if __name__ == '__main__':
            app.main()

    Then you can start the :program:`faust` program by executing for example
    ``python myscript.py worker --loglevel=INFO``, and it will use the correct
    application.

``Sequence[str]``
    The argument can also be a list of packages to scan::

        app = App(..., autodiscover=['proj_orders', 'proj_accounts'])

``Callable[[], Sequence[str]]``
    The argument can also be a function returning a list of packages
    to scan::

        def get_all_packages_to_scan():
            return ['proj_orders', 'proj_accounts']

        app = App(..., autodiscover=get_all_packages_to_scan)

False)

    If everything you need is in a self-contained module, or you import the
    stuff you need manually, just set ``autodiscover`` to False and don't
    worry about it :-)

.. admonition:: Django

    When using :pypi:`Django` and the :envvar:`DJANGO_SETTINGS_MODULE`
    environment variable is set, the Faust app will scan all packages found
    in the ``INSTALLED_APPS`` setting.

    If you're using Django you can use this to scan for
    agents/pages/commands in all packages defined in ``INSTALLED_APPS``.

    Faust will automatically detect that you're using Django and do the
    right thing if you do::

        app = App(..., autodiscover=True)

    It will find agents and other decorators in all of the reusable Django
    apps. If you want to manually control what packages are traversed, then provide
    a list::

        app = App(..., autodiscover=['package1', 'package2'])

    or if you want exactly :const:`None` packages to be traversed, then
    provide a False:

        app = App(.., autodiscover=False)

    which is the default, so you can simply omit the argument.

.. tip::

    For manual control over autodiscovery, you can also call the
    :meth:`@discover` method manually.

.. setting:: version

``version``
-----------

:type: :class:`int`
:default: 1

Version of the app, that when changed will create a new isolated
instance of the application. The first version is 1, the second version is 2,
and so on.

.. admonition:: Source topics will not be affected by a version change.

    Faust applications will use two kinds of topics: source topics, and
    internally managed topics. The source topics are declared by the producer,
    and we do not have the opportunity to modify any configuration settings,
    like number of partitions for a source topic; we may only consume from
    them. To mark a topic as internal, use: ``app.topic(..., internal=True)``.

.. setting:: datadir

``datadir``
-----------

:type: ``Union[str, pathlib.Path]``
:default: ``"{appid}-data"``
:environment: :envvar:`FAUST_DATADIR`, :envvar:`F_DATADIR`

The directory in which this instance stores the data used by local tables, etc.

.. seealso::

    - The data directory can also be set using the :option:`faust --datadir`
      option, from the command-line, so there's usually no reason to provide
      a default value when creating the app.

.. setting:: tabledir

``tabledir``
------------

:type: ``Union[str, pathlib.Path]``
:default: ``"tables"``

The directory in which this instance stores local table data.
Usually you will want to configure the :setting:`datadir` setting, but if you
want to store tables separately you can configure this one.

If the path provided is relative (it has no leading slash), then the path will
be considered to be relative to the :setting:`datadir` setting.

.. setting:: id_format

``id_format``
-------------

:type: :class:`str`
:default: ``"{id}-v{self.version}"``

The format string used to generate the final :setting:`id` value by combining
it with the :setting:`version` parameter.

.. setting:: loghandlers

``loghandlers``
---------------

:type: ``List[logging.LogHandler]``
:default: :const:`None`

Specify a list of custom log handlers to use in worker instances.

.. setting:: origin

``origin``
----------

:type: :class:`str`
:default: :const:`None`

The reverse path used to find the app, for example if the app is located in::

    from myproj.app import app

Then the ``origin`` should be ``"myproj.app"``.

The :program:`faust worker` program will try to automatically set the origin,
but if you are having problems with autogenerated names then you can set
origin manually.

Serialization Settings
======================

.. setting:: key_serializer

``key_serializer``
------------------

:type: ``Union[str, Codec]``
:default: ``"json"``

Serializer used for keys by default when no serializer is specified, or a
model is not being used.

This can be the name of a serializer/codec, or an actual
:class:`faust.serializers.codecs.Codec` instance.

.. seealso::

    - The :ref:`codecs` section in the model guide -- for more information
      about codecs.

.. setting:: value_serializer

``value_serializer``
--------------------

:type: ``Union[str, Codec]``
:default: ``"json"``

Serializer used for values by default when no serializer is specified, or a
model is not being used.

This can be string, the name of a serializer/codec, or an actual
:class:`faust.serializers.codecs.Codec` instance.

.. seealso::

    - The :ref:`codecs` section in the model guide -- for more information
      about codecs.


Topic Settings
==============

.. setting:: topic_replication_factor

``topic_replication_factor``
----------------------------

:type: :class:`int`
:default: ``1``

The default replication factor for topics created by the application.

.. note::

    Generally this should be the same as the configured
    replication factor for your Kafka cluster.

.. setting:: topic_partitions

``topic_partitions``
--------------------

:type: :class:`int`
:default: ``8``

Default number of partitions for new topics.

.. note::

    This defines the maximum number of workers we could distribute the
    workload of the application (also sometimes referred as the sharding
    factor of the application).


Advanced Broker Settings
========================

.. setting:: broker_client_id

``broker_client_id``
--------------------

:type: ``str``
:default: ``faust-{VERSION}``

You shouldn't have to set this manually.

The client id is used to identify the software used, and is not usually
configured by the user.

.. setting:: broker_commit_interval

``broker_commit_interval``
--------------------------

:type: :class:`float`, :class:`~datetime.timedelta`
:default: ``2.8``

How often we commit messages that have been fully processed (:term:`acked`).

.. setting:: broker_commit_livelock_soft_timeout

``broker_commit_livelock_soft_timeout``
---------------------------------------

:type: class:`float`, :class:`~datetime.timedelta`
:default: ``300.0`` (five minutes).

How long time it takes before we warn that the Kafka commit offset has
not advanced (only when processing messages).

Advanced Table Settings
=======================

.. setting:: table_cleanup_interval

``table_cleanup_interval``
--------------------------

:type: :class:`float`, :class:`~datetime.timedelta`
:default: ``30.0``

How often we cleanup tables to remove expired entries.

.. setting:: table_standby_replicas

``table_standby_replicas``
--------------------------

:type: :class:`int`
:default: ``1``

The number of standby replicas for each table.

Advanced Stream Settings
========================

.. setting:: stream_buffer_maxsize

``stream_buffer_maxsize``
-------------------------

:type: :class:`int`
:default: 32768

This setting control backpressure to streams and agents reading from streams.

If set to 1000 (default) this means that an agent can only keep at most
1000 unprocessed items in the stream buffer.

Essentially this will a limit the number of messages a stream can "prefetch".

Advanced Web Server Settings
============================

.. setting:: canonical_url

``canonical_url``
-----------------

:type:  ``str``
:default: ``socket.gethostname()``

You shouldn't have to set this manually.

The canonical URL defines how to reach the web server on a running
worker node, and is usually set by combining the :option:`faust worker --web-host`
and :option:`faust worker --web-port` command line arguments, not
by passing it as a keyword argument to :class:`App`.

Agent RPC Settings
==================

.. setting:: reply_to

``reply_to``
------------

:type: ``str``
:default: `<generated>`

The name of the reply topic used by this instance.  If not set one will be
automatically generated when the app is created.

.. setting:: reply_create_topic

``reply_create_topic``
----------------------

:type: ``bool``
:default: :const:`False`

Set this to :const:`True` if you plan on using the RPC with agents.

This will create the internal topic used for RPC replies on that instance
at startup.

.. Setting:: reply_expires

``reply_expires``
-----------------

:type: ``Union[float, datetime.timedelta]``
:default: ``timedelta(days=1)``

The expiry time (in seconds float, or timedelta), for how long replies
will stay in the instances local reply topic before being removed.

.. setting:: reply_to_prefix

``reply_to_prefix``
-------------------

:type: ``str``
:default: ``"f-reply-"``

The prefix used when generating reply topic names.

Extension Settings
==================

.. setting:: Agent

``Agent``
---------

:type: ``Union[str, Type]``
:default: ``"faust.Agent"``

The :class:`~faust.Agent` class to use for agents, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyAgent(faust.Agent):
        ...

    app = App(..., Agent=MyAgent)

Example using the string path to a class::

    app = App(..., Agent='myproj.agents.Agent')

.. setting:: Stream

``Stream``
----------

:type: ``Union[str, Type]``
:default: ``"faust.Stream"``

The :class:`~faust.Stream` class to use for streams, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseStream(faust.Stream):
        ...

    app = App(..., Stream=MyBaseStream)

Example using the string path to a class::

    app = App(..., Stream='myproj.streams.Stream')

.. setting:: Table

``Table``
---------

:type: ``Union[str, Type[TableT]]``
:default: ``"faust.Table"``

The :class:`~faust.Table` class to use for tables, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseTable(faust.Table):
        ...

    app = App(..., Table=MyBaseTable)

Example using the string path to a class::

    app = App(..., Table='myproj.tables.Table')

.. setting:: Set

``Set``
-------

:type: ``Union[str, Type[SetT]]``
:default: ``"faust.Set"``

The :class:`~faust.Set` class to use for sets, or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseSetTable(faust.Set):
        ...

    app = App(..., Set=MyBaseSetTable)

Example using the string path to a class::

    app = App(..., Set='myproj.tables.Set')

.. setting:: TableManager

``TableManager``
----------------

:type: ``Union[str, Type[TableManagerT]]``
:default: ``"faust.tables.TableManager"``

The :class:`~faust.tables.TableManager` used for managing tables,
or the fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.tables import TableManager

    class MyTableManager(TableManager):
        ...

    app = App(..., TableManager=MyTableManager)

Example using the string path to a class::

    app = App(..., TableManager='myproj.tables.TableManager')

.. setting:: Serializers

``Serializers``
---------------

:type: ``Union[str, Type[RegistryT]]``
:default: ``"faust.serializers.Registry"``

The :class:`~faust.serializers.Registry` class used for
serializing/deserializing messages; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.serialiers import Registry

    class MyRegistry(Registry):
        ...

    app = App(..., Serializers=MyRegistry)

Example using the string path to a class::

    app = App(..., Serializers='myproj.serializers.Registry')

.. setting:: Worker

``Worker``
----------

:type: ``Union[str, Type[WorkerT]]``
:default: ``"faust.Worker"``

The :class:`~faust.Worker` class used for starting a worker
for this app; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust

    class MyWorker(faust.Worker):
        ...

    app = faust.App(..., Worker=Worker)

Example using the string path to a class::

    app = faust.App(..., Worker='myproj.workers.Worker')

.. setting:: PartitionAssignor

``PartitionAssignor``
---------------------

:type: ``Union[str, Type[PartitionAssignorT]]``
:default: ``"faust.assignor.PartitionAssignor"``

The :class:`~faust.assignor.PartitionAssignor` class used for assigning
topic partitions to worker instances; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.assignor import PartitionAssignor

    class MyPartitionAssignor(PartitionAssignor):
        ...

    app = App(..., PartitionAssignor=PartitionAssignor)

Example using the string path to a class::

    app = App(..., Worker='myproj.assignor.PartitionAssignor')

.. setting:: LeaderAssignor

``LeaderAssignor``
------------------

:type: ``Union[str, Type[LeaderAssignorT]]``
:default: ``"faust.assignor.LeaderAssignor"``

The :class:`~faust.assignor.LeaderAssignor` class used for assigning
a master Faust instance for the app; or the fully-qualified path
to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.assignor import LeaderAssignor

    class MyLeaderAssignor(LeaderAssignor):
        ...

    app = App(..., LeaderAssignor=LeaderAssignor)

Example using the string path to a class::

    app = App(..., Worker='myproj.assignor.LeaderAssignor')

.. setting:: Router

``Router``
----------

:type: ``Union[str, Type[RouterT]]``
:default: ``"faust.app.router.Router"``

The :class:`~faust.router.Router` class used for routing requests
to a worker instance having the partition for a specific key (e.g. table key);
or the fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.router import Router

    class MyRouter(Router):
        ...

    app = App(..., Router=Router)

Example using the string path to a class::

    app = App(..., Router='myproj.routers.Router')

.. setting:: TopicConductor

``TopicConductor``
------------------

:type: ``Union[str, Type[ConductorT]]``
:default: ``"faust.topics:TopicConductor"``

The :class:`~faust.topics.TopicConductor` class used for routing events
from the Kafka consumer to streams reading from topics; or the fully-qualified
path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    from faust.topics import TopicConductor

    class MyTopicConductor(TopicConductor):
        ...

    app = App(..., TopicConductor=TopicConductor)

Example using the string path to a class::

    app = App(..., TopicConductor='myproj.conductors.TopicConductor')

.. setting:: Topic

``Topic``
---------

:type: ``Union[str, Type[TopicT]]``
:default: ``"faust.Topic"``

The :class:`~faust.Topic` class used for defining new topics; or the
fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust

    class MyTopic(faust.Topic):
        ...

    app = faust.App(..., Topic=MyTopic)

Example using the string path to a class::

    app = faust.App(..., Topic='myproj.topics.Topic')

.. setting:: HttpClient

``HttpClient``
--------------

:type: ``Union[str, Type[HttpClientT]]``
:default: ``"aiohttp.client:ClientSession"``

The :class:`aiohttp.client.ClientSession` class used as a HTTP client; or the
fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust
    from aiohttp.client import ClientSession

    class HttpClient(ClientSession):
        ...

    app = faust.App(..., HttpClient=HttpClient)

Example using the string path to a class::

    app = faust.App(..., HttpClient='myproj.http.HttpClient')

.. setting:: Monitor

``Monitor``
-----------

:type: ``Union[str, Type[SensorT]]``
:default: ``"faust.sensors:Monitor"``

The :class:`~faust.sensors.Monitor` class as the main sensor
gathering statistics for the application; or the
fully-qualified path to one (supported by
:func:`~mode.utils.imports.symbol_by_name`).

Example using a class::

    import faust
    from faust.sensors import Monitor

    class MyMonitor(Monitor):
        ...

    app = faust.App(..., Monitor=MyMonitor)

Example using the string path to a class::

    app = faust.App(..., Monitor='myproj.monitors.Monitor')

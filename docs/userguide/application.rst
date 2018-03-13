.. _guide-application:

=======================================
 Application
=======================================

.. module:: faust

.. currentmodule:: faust

.. contents::
    :local:
    :depth: 2

.. _application-basics:

What is an Application?
=======================

An application is an *instance of the library*, and provides
the core API of Faust.

The application can define stream processors (agents), topics, channels,
web views, CLI commands and more.

To create one you need to provide
a name for the application (the id), a message broker, and a driver to use for
table storage:

.. sourcecode:: pycon

    >>> import faust
    >>> app = faust.App('example', broker='kafka://', store='rocksdb://')

.. topic:: It is safe to...

    - Run multiple application instances in the same process:

        .. sourcecode:: pycon

            >>> app1 = faust.App('demo1')
            >>> app2 = faust.App('demo2')

    - Share an app between multiple threads: the app is :term:`thread safe`.

.. _application-configuration:

Application Parameters
======================

You must provide a name for the app, and also you *will want* to
set the ``broker`` and ``store`` options to configure the broker URL and
a storage driver.

Other than that the rest have sensible defaults so you can safely use Faust
without changing them.

Here we set the broker URL to Kafka, and the storage driver to `RocksDB`_:

.. sourcecode:: python

    >>> app = faust.App(
    ...     'myid',
    ...     broker='kafka://kafka.example.com',
    ...     store='rocksdb://',
    ... )

"kafka://localhost" is used if you don't configure a broker URL.
The first part of the URL ("kafka://"), is called the scheme and specifies
the driver that you want to use (it can also be the fully qualified
path to a Python class).

The storage driver decides how to keep distributed tables locally, and
Faust version 1.0 supports two options:

+----------------+-----------------------------------------------+
| ``rocksdb://`` | `RocksDB`_ an embedded database (production)  |
+----------------+-----------------------------------------------+
| ``memory://``  | In-memory (development)                       |
+----------------+-----------------------------------------------+

Using the ``memory://`` store is OK when developing your project and testing
things out, but for large tables, it can take hours to recover after
a restart, so you should never use it in production.

`RocksDB`_ recovers tables in seconds or less, is embedded and don't require
a server or additional infrastructure. It also stores them in the file system
so tables can exceed the size of available memory.

.. _`RocksDB`: http://rocksdb.org/

Required Parameters
-------------------

.. setting:: id

``id``
~~~~~~

:type: ``str``

A string uniquely identifying the app, shared across all
instances such that two app instances with the same `id` are
considered to be in the same "group".

This parameter is required.

.. admonition:: The id and Kafka

    When using Kafka, the id is used to generate app-local topics, and
    names for consumer groups.

Common Parameters
-----------------

.. setting:: broker

``broker``
~~~~~~~~~~

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
~~~~~~~~~

:type: ``str``
:default: ``memory://``

The backend used for table storage.

Tables are stored in-memory by default, but you should
not use the ``memory://`` store in production.

In production, a persistent table store, such as ``rocksdb://`` is
preferred.

.. setting:: autodiscover

``autodiscover``
~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~

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
~~~~~~~~~~~

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
~~~~~~~~~~~~

:type: ``Union[str, pathlib.Path]``
:default: ``"tables"``

The directory in which this instance stores local table data.
Usually you will want to configure the :setting:`datadir` setting, but if you
want to store tables separately you can configure this one.

If the path provided is relative (it has no leading slash), then the path will
be considered to be relative to the :setting:`datadir` setting.

.. setting:: id_format

``id_format``
~~~~~~~~~~~~~

:type: :class:`str`
:default: ``"{id}-v{self.version}"``

The format string used to generate the final :setting:`id` value by combining
it with the :setting:`version` parameter.

.. setting:: loghandlers

``loghandlers``
~~~~~~~~~~~~~~~

:type: ``List[logging.LogHandler]``
:default: :const:`None`

Specify a list of custom log handlers to use in worker instances.

.. setting:: origin

``origin``
~~~~~~~~~~

:type: :class:`str`
:default: :const:`None`

The reverse path used to find the app, for example if the app is located in::

    from myproj.app import app

Then the ``origin`` should be ``"myproj.app"``.

The :program:`faust worker` program will try to automatically set the origin,
but if you are having problems with autogenerated names then you can set
origin manually.

Serialization Parameters
------------------------

.. setting:: key_serializer

``key_serializer``
~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~

:type: ``Union[str, Codec]``
:default: ``"json"``

Serializer used for values by default when no serializer is specified, or a
model is not being used.

This can be string, the name of a serializer/codec, or an actual
:class:`faust.serializers.codecs.Codec` instance.

.. seealso::

    - The :ref:`codecs` section in the model guide -- for more information
      about codecs.


Topic Options
-------------

.. setting:: topic_replication_factor

``topic_replication_factor``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:type: :class:`int`
:default: ``1``

The default replication factor for topics created by the application.

.. note::

    Generally this should be the same as the configured
    replication factor for your Kafka cluster.

.. setting:: topic_partitions

``topic_partitions``
~~~~~~~~~~~~~~~~~~~~

:type: :class:`int`
:default: ``8``

Default number of partitions for new topics.

.. note::

    This defines the maximum number of workers we could distribute the
    workload of the application (also sometimes referred as the sharding
    factor of the application).


Advanced Broker Options
-----------------------

.. setting:: broker_client_id

``broker_client_id``
~~~~~~~~~~~~~~~~~~~~

:type: ``str``
:default: ``faust-{VERSION}``

You shouldn't have to set this manually.

The client id is used to identify the software used, and is not usually
configured by the user.

.. setting:: broker_commit_interval

``broker_commit_interval``
~~~~~~~~~~~~~~~~~~~~~~~~~~

:type: :class:`float`, :class:`~datetime.timedelta`
:default: ``3.0``

How often we commit messages that have been fully processed (:term:`acked`).

.. setting:: broker_commit_livelock_soft_timeout

``broker_commit_livelock_soft_timeout``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

:type: class:`float`, :class:`~datetime.timedelta`
:default: ``300.0`` (five minutes).

How long time it takes before we warn that the Kafka commit offset has
not advanced (only when processing messages).

Advanced Table Options
----------------------

.. setting:: table_cleanup_interval

``table_cleanup_interval``
~~~~~~~~~~~~~~~~~~~~~~~~~~

:type: :class:`float`, :class:`~datetime.timedelta`
:default: ``30.0``

How often we cleanup tables to remove expired entries.

.. setting:: table_standby_replicas

``table_standby_replicas``
~~~~~~~~~~~~~~~~~~~~~~~~~~

:type: :class:`int`
:default: ``1``

The number of standby replicas for each table.

Advanced Stream Parameters
--------------------------

.. setting:: stream_buffer_maxsize

``stream_buffer_maxsize``
~~~~~~~~~~~~~~~~~~~~~~~~~
:type: :class:`int`
:default: 1000

This setting control backpressure to streams and agents reading from streams.

If set to 1000 (default) this means that an agent can only keep at most
1000 unprocessed items in the stream buffer.

Essentially this will a limit the number of messages a stream can "prefetch".

Advanced Web Parameters
-----------------------

.. setting:: canonical_url

``canonical_url``
~~~~~~~~~~~~~~~~~

:type:  ``str``
:default: ``socket.gethostname()``

You shouldn't have to set this manually.

The canonical URL defines how to reach the web server on a running
worker node, and is usually set by combining the :option:`faust worker --web-host`
and :option:`faust worker --web-port` command line arguments, not
by passing it as a keyword argument to :class:`App`.

Agent RPC Parameters
--------------------

.. setting:: reply_to

``reply_to``
~~~~~~~~~~~~

:type: ``str``
:default: `<generated>`

The name of the reply topic used by this instance.  If not set one will be
automatically generated when the app is created.

.. setting:: reply_create_topic

``reply_create_topic``
~~~~~~~~~~~~~~~~~~~~~~

:type: ``bool``
:default: :const:`False`

Set this to :const:`True` if you plan on using the RPC with agents.

This will create the internal topic used for RPC replies on that instance
at startup.

.. Setting:: reply_expires

``reply_expires``
~~~~~~~~~~~~~~~~~

:type: ``Union[float, datetime.timedelta]``
:default: ``timedelta(days=1)``

The expiry time (in seconds float, or timedelta), for how long replies
will stay in the instances local reply topic before being removed.

.. setting:: reply_to_prefix

``reply_to_prefix``
~~~~~~~~~~~~~~~~~~~

:type: ``str``
:default: ``"f-reply-"``

The prefix used when generating reply topic names.

Subclassing Parameters
----------------------

.. setting:: Agent

``Agent``
~~~~~~~~~

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
~~~~~~~~~~

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
~~~~~~~~~

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
~~~~~~~

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
~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~

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
~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~

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
~~~~~~~~~~~~~~

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
~~~~~~~~~~~

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

Actions
=======

``app.topic()`` -- Create a topic-description
---------------------------------------------

Use the :meth:`~@topic` method to create a topic description, used
for example to tell stream processors what Kafka topic to read from, and how the keys
and values in that topic are serialized:

.. sourcecode:: python

    topic = app.topic('name_of_topic')

    @app.agent(topic)
    async def process(stream):
        async for event in stream:
            ...

Use the ``key_type`` and ``value_type`` arguments to specify the models used for key
and value serialization:

.. sourcecode:: python

    class MyValueModel(faust.Record):
        name: str
        value: float

    topic = app.topic(
        'name_of_topic',
        key_type=bytes,
        value_type=MyValueModel,
    )

The default ``key_type`` is :class:`bytes`, treating the key as a binary
string, but the key can also be specified as a model type.

.. seealso::

    - The :ref:`guide-channels` guide -- for more about topics and channels.

    - The :ref:`guide-models` guide -- for more about models and serialization.

``app.channel()`` -- Create a local channel
-------------------------------------------

Use :meth:`~@channel` to create an in-memory communication channel:

.. sourcecode:: python

    import faust

    app = faust.App('channel')

    class MyModel(faust.Record):
        x: int

    channel = app.channel(value_type=MyModel)

    @app.agent(channel)
    async def process(stream):
        async for event in stream:
            print(f'Received: {event!r}')

    @app.timer(1.0)
    async def populate():
        await channel.send(MyModel(303))

.. seealso::

    - The :ref:`guide-channels` guide -- for more about topics and channels.

    - The :ref:`guide-models` guide -- for more about models and serialization.

``app.Table()`` -- Define a new table
-------------------------------------

Use :meth:`~@Table` to define a new distributed dictionary; the only
required argument is a unique and identifying name. Here we also
set a default value so the table acts as a :class:`~collections.defaultdict`:

.. sourcecode:: python

    transfer_counts = app.Table(
        'transfer_counts',
        default=int,
        key_type=str,
        value_type=int,
    )

The default argument is passed in as a callable, and in our example
calling ``int()`` returns the number zero, so whenever a key is missing
in the table, it's added with a value of zero:

.. sourcecode:: python

    >>> table['missing']
    0

    >>> table['also-missing'] += 1
    >>> table['also-missing']
    1


The table needs to relate every update to an associated source topic event,
so you must be iterating over a stream to modify a table. Like in this agent
where also ``.group_by()`` is used to repartition the stream by account id,
ensuring every unique account delivers to the same agent instance,
and the count-per-account records accurately:

.. sourcecode:: python

    @app.agent(transfers_topic)
    async def transfer(transfers):
        async for transfer in transfers.group_by(Transfer.account):
            transfer_counts[transfer.account] += 1


Moreover, the agent modifying the table cannot process the source topic out of
order, so only agents with ``concurrency=1`` are allowed to update tables.

.. seealso::

    - The :ref:`guide-tables` guide -- for more information about tables.

        Learn how to create a "windowed table" where aggregate values are placed
        into configurable time windows, providing you with answers to questions like
        "what was the value in the last five minutes", or "what was the value of
        this count like yesterday".

``app.Set()`` -- Define a new Set-based table
---------------------------------------------

Use :meth:`~@Set` to create a set table that only tracks membership and does not
associate each key with a particular value:

.. sourcecode:: python

    users_with_transfer = app.Set('users-with-transfers', key_type=str)

    @app.agent(transfers_topic)
    async def transfer(transfers):
        async for transfer in transfers:
            users_with_transfer.add(transfer.username)

.. seealso::

    - The :ref:`guide-tables` guide -- for more information about tables and
      sets.

``@app.agent()`` -- Define a new stream processor
-------------------------------------------------

Use the :meth:`~@agent` decorator to define an asynchronous stream processor:

.. sourcecode:: python

    # examples/agent.py
    import faust

    app = faust.App('stream-example')

    @app.agent()
    async def myagent(stream):
        """Example agent."""
        async for value in stream:
            print(f'MYAGENT RECEIVED -- {value!r}')
            yield value

    if __name__ == '__main__':
        app.main()


This agent does not have a specific topic set -- so an anonymous topic will be
created for it.  Use the :program:`faust agents` program to list the topics
used by each agent:

.. sourcecode:: console

    $ python examples/agent.py agents
    ┌Agents────┬───────────────────────────────────────┬────────────────┐
    │ name     │ topic                                 │ help           │
    ├──────────┼───────────────────────────────────────┼────────────────┤
    │ @myagent │ stream-example-examples.agent.myagent │ Example agent. │
    └──────────┴───────────────────────────────────────┴────────────────┘

The agent reads from the "stream-example-examples.agent.myagent" topic, whose
name is generated from the application :setting:`id` setting, the
application :setting:`version` setting, and the fully qualified path of the
agent (``examples.agent.myagent``).

**Start a worker to consume from the topic:**

.. sourcecode:: console

    $ python examples/agent.py worker -l info

Next, in a new console, send the agent a value using the :program:`faust send`
program.  The first argument to send is the name of the topic, and the second
argument is the value to send (use ``--key=k`` to specify key).  The name of
the topic can also start with the @ character to name an agent instead.

Use ``@agent`` to send a value of ``"hello"`` to the topic of our agent:

.. sourcecode:: console

    $ python examples/agent.py send @myagent hello

Finally, you should see in the worker console that it received our message:

.. sourcecode:: console

    MYAGENT RECEIVED -- b'hello'

.. seealso::

    - The :ref:`guide-agents` guide -- for more information about agents.

    - The :ref:`guide-channels` guide -- for more information about channels
      and topics.

``@app.task()`` -- Define a new support task.
---------------------------------------------

Use the :meth:`~@task` decorator to define an asynchronous task to be started
with the app:

.. sourcecode:: python

    @app.task()
    async def mytask():
        print('APP STARTED AND OPERATIONAL')

The task will be started when the app starts, by scheduling it as an
:class:`asyncio.Task` on the event loop. It will only be started once the app
is fully operational, meaning it has started consuming messages from Kafka.

.. seealso::

    - The :ref:`tasks-basics` section in the :ref:`guide-tasks` -- for more
      information about defining tasks.

``@app.timer()`` -- Define a new periodic task
----------------------------------------------

Use the :meth:`~@timer` decorator to define an asynchronous periodic task
that runs every 30.0 seconds:

.. sourcecode:: python

    @app.timer(30.0)
    async def my_periodic_task():
        print('THIRTY SECONDS PASSED')

The timer will start 30 seconds after the worker instance has started and is in an
operational state.

.. seealso::

    - The :ref:`tasks-timers` section in the :ref:`guide-tasks` guide -- for
      more information about creating timers.

``@app.page()`` -- Define a new Web View
----------------------------------------

Use the :meth:`~@page` decorator to define a new web view from an
async function:

.. sourcecode:: python

    # examples/view.py
    import faust

    app = faust.App('view-example')

    @app.page('/path/to/view/')
    async def myview(web, request):
        print(f'FOO PARAM: {request.query["foo"]}')

    if __name__ == '__main__':
        app.main()

Next run a worker instance to start the web server on port 6066 (default):

.. sourcecode:: console

    $ python examples/view.py worker -l info

Then visit your view in the browser by going to http://localhost:6066/path/to/view/:

.. sourcecode:: console

    $ open http://localhost:6066/path/to/view/


.. seealso::

    - The :ref:`tasks-web-views` section in the :ref:`guide-tasks` guide -- to
      learn more about defining views.

.. _application-main:

``app.main()`` -- Start the :program:`faust` command-line program.
------------------------------------------------------------------

To have your script extend the :program:`faust` program, you can call
``app.main()``:

.. sourcecode:: python

    # examples/command.py
    import faust

    app = faust.App('umbrella-command-example')

    if __name__ == '__main__':
        app.main()

This will use the arguments in ``sys.argv`` and will support the same
arguments as the :program:`faust` umbrella command.

To see a list of available commands, execute your program:

.. sourcecode:: console

    $ python examples/command.py

To get help for a particular subcommand run:

.. sourcecode:: console

    $ python examples/command.py worker --help

.. seealso::

   - The :meth:`~@main` method in the API reference.

   - The :ref:`guide-deployment` guide -- for more on deploying Faust
     applications.

``@app.command()`` -- Define a new command-line command
-------------------------------------------------------

Use the :meth:`~@command` decorator to define a new subcommand
for the :program:`faust` command-line program:

.. sourcecode:: python

    # examples/command.py
    impor faust

    app = faust.App('example-subcommand')

    @app.command()
    async def example():
        """This docstring is used as the command help in --help."""
        print('RUNNING EXAMPLE COMMAND')

    if __name__ == '__main__':
        app.main()

You can now run your subcommand:

.. sourcecode:: console

    $ python examples/command.py example
    RUNNING EXAMPLE COMMAND

.. seealso::

    - The :ref:`tasks-cli-commands` section in the :ref:`guide-tasks` guide --
      for more information about defining subcommands.

        Including how to specify command-line arguments and parameters to your
        command.

``@app.service()`` -- Define a new service
------------------------------------------

The :meth:`~@service` decorator adds a custom :class:`mode.Service` class
as a dependency of the app.

.. topic:: What is a Service?

    A service is something that can be started and stopped, and Faust
    is built out of many such services.

    The :pypi:`mode` library was extracted out of Faust for being generally
    useful, and Faust uses this library as a dependency.

    Examples of classes that are services in Faust include: the
    :class:`~faust.App`, a :class:`stream <faust.Stream>`, an :class:`agent <faust.Agent>`,
    a :class:`table <faust.Table>`, the :class:`~faust.TableManager`, the
    :class:`~faust.topics.TopicConductor`, and just about everything that is
    started and stopped is.

    Services can also have background tasks, or execute in an OS thread.

You can *decorate a service class* to have it start with the app:

.. sourcecode:: python

    # examples/service.py
    import faust
    from mode import Service

    app = faust.App('service-example')

    @app.service
    class MyService(Service):

        async def on_start(self):
            print('MYSERVICE IS STARTING')

        async def on_stop(self):
            print('MYSERVICE IS STOPPING')

        @Service.task
        async def _background_task(self):
            while not self.should_stop:
                print('BACKGROUND TASK WAKE UP')
                await self.sleep(1.0)

    if __name__ == '__main__':
        app.main()

To start the app and see it and action run a worker:

.. sourcecode:: console

    python examples/service.py worker -l info


You can also add services at runtime in application subclasses:

.. sourcecode:: python

    class MyApp(App):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.some_service = self.service(SomeService())

Application Signals
===================

You may have experience signals in other frameworks such as `Django`_
and `Celery`_.

The main difference between signals in Faust is that they accept
positional arguments, and that they also come with asynchronous versions
for use with :mod:`asyncio`.

Signals are an implementation of the `Observer`_  design pattern.

.. _`Django`: http://djangoproject.com
.. _`Celery`: http://celeryproject.org
.. _`Observer`: https://en.wikipedia.org/wiki/Observer_pattern

.. signal:: App.on_partitions_revoked

``App.on_partitions_revoked``
-----------------------------

:sender: :class:`faust.App`
:arguments: :class:`Set[TP] <faust.types.tuples.TP>`

The ``on_partitions_revoked`` signal is an asynchronous signal called after every
Kafka rebalance and provides a single argument which is the set of
newly revoked partitions.

Add a callback to be called when partitions are revoked:

.. sourcecode:: python

    from typing import Set
    from faust.types import AppT, TP

    @app.on_partitions_revoked.connect
    async def on_partitions_assigned(app: AppT,
                                     revoked: Set[TP], **kwargs) -> None:
        print(f'Partitions are being revoked: {revoked}')

Using ``app`` as an instance when connecting here means we will only be called
for that particular app instance.  If you want to be called for all app instances
then you must connect to the signal of the class (``App``):

.. sourcecode:: python

    @faust.App.on_partitions_revoked.connect
    async def on_partitions_revoked(app: AppT,
                                     revoked: Set[TP], **kwargs) -> None:
        ...


.. admonition:: Signal handlers must always accept ``**kwargs``.

    Signal handler must always accept ``**kwargs`` so that they
    are backwards compatible when new arguments are added.

    Similarly new arguments must be added as keyword arguments
    to be backwards compatible.

.. signal:: App.on_partitions_assigned

``App.on_partitions_assigned``
------------------------------

:sender: :class:`faust.App`
:arguments: :class:`Set[TP] <faust.types.tuples.TP>`

The ``on_partitions_assigned`` signal is an asynchronous signal called after every
Kafka rebalance and provides a single argument which is the set of
assigned partitions.

Add a callback to be called when partitions are assigned:

.. sourcecode:: python

    from typing import Set
    from faust.types import AppT, TP

    @app.on_partitions_assigned.connect
    async def on_partitions_assigned(app: AppT,
                                     assigned: Set[TP], **kwargs) -> None:
        print(f'Partitions are being assigned: {assigned}')


.. signal:: App.on_configured

``App.on_configured``
---------------------

:sender: :class:`faust.App`
:arguments: :class:`faust.Settings`
:synchronous: This is a synchronous signal (do not use :keyword:`async def`).

Called as the app reads configuration, just before the application
configuration is set, but after the configuration is read.

Takes arguments: ``(app, conf)``, where conf is the :class:`faust.Settings`
object being built and is the instance that ``app.conf`` will be set to
after this signal returns.

Use the ``on_configured`` signal to configure your app:

.. sourcecode:: python

    import os
    import faust

    app = faust.App('myapp')

    @app.on_configured.connect
    def configure(app, conf, **kwargs):
        conf.broker_url = os.environ.get('FAUST_BROKER')
        conf.store_url = os.environ.get('STORE_URL')

.. signal:: App.on_before_configured

``App.on_before_configured``
----------------------------

:sender: :class:`faust.App`
:arguments: *none*
:synchronous: This is a synchronous signal (do not use :keyword:`async def`).

Called before the app reads configuration, and before the
:signal:`App.on_configured` signal is dispatched.

Takes only sender as argument, which is the app being configured:

.. sourcecode:: python

    @app.on_before_configured
    def before_configuration(app, **kwargs):
        print(f'App {app} is being configured')

.. signal:: App.on_after_configured

``App.on_after_configured``
---------------------------

:sender: :class:`faust.App`
:arguments: *none*
:synchronous: This is a synchronous signal (do not use :keyword:`async def`).

Called after app is fully configured and ready for use.

Takes only sender as argument, which is the app that was configured:

.. sourcecode:: python

    @app.on_after_configured
    def after_configuration(app, **kwargs):
        print(f'App {app} has been configured.')

.. signal:: App.on_worker_init

``App.on_worker_init``
----------------------

:sender: :class:`faust.App`
:arguments: *none*
:synchronous: This is a synchronous signal (do not use :keyword:`async def`).

Called by the :program:`faust worker` program (or when using `app.main()`)
to apply worker specific customizations.

Takes only sender as argument, which is the app a worker is being started for:

.. sourcecode:: python

    @app.on_worker_init
    def on_worker_init(app, **kwargs):
        print(f'Working starting for app {app}')

Miscellaneous
=============

Why use applications?
---------------------

For special needs, you can inherit from the :class:`faust.App` class, and a subclass
will have the ability to change how almost everything works.

Comparing the application to the interface of frameworks like Django,
there are clear benefits.

In Django, the global settings module means having multiple configurations
are impossible, and with an API organized by modules, you sometimes end up
with lots of import statements and keeping track of many modules. Further,
you often end up monkey patching to change how something works.

The application keeps the library flexible to changes, and allows
for many applications to coexist in the same process space.


Reference
=========

See :class:`faust.App` in the API reference for a full list of methods
and attributes supported.

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

The application can define agents, streams, topics & channels, and more.

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

    - Share an app between multiple threads (the app is :term:`thread safe`).

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

.. _application-id:

``id``
~~~~~~

:type: ``str``

A string uniquely identifying the app, shared across all
instances such that two app instances with the same `id` are
considered to be in the same "group".

This parameter is required.

.. admonition:: The id and Kafka

    When using Kafka, the id is used to generate app-local topics, and
    names for consumer groups, etc.

Common Parameters
-----------------

``broker``
~~~~~~~~~~

:type: ``str``
:default: ``"aiokafka://localhost:9092"``

Faust needs the URL of a "transport" to send and receive messages.

Currently, the only supported transport is the ``aiokafka://`` Kafka client.

You can specify multiple hosts at the same time by separating them using
the semi-comma:

.. sourcecode:: text

    aiokafka://kafka1.example.com:9092;kafka2.example.com:9092

``store``
~~~~~~~~~

:type: ``str``
:default: ``memory://``

The backend used for table storage.
Tables are stored in-memory only by default, but you should
only used this for testing and development purposes.

In production, a persistent table store, such as ``rocksdb://`` is
preferred.

``autodiscover``
~~~~~~~~~~~~~~~~

:type: ``Union[bool, Iterable[str], Callable[[], Iterable[str]]]``

Enable autodiscovery of agent, page and command decorators.

.. warning::

    The autodiscovery functionality uses :pypi:`Venusian` to
    scan wanted packages for ``@app.agent``, ``@app.page``,
    ``@app.command``, ``@app.task`` and ``@app.timer`` decorators,
    but to do so, it's required to traverse the package directory and import
    every package in them.

    Importing random modules like this can be dangerous if you don't
    follow best practices for user modules:
    do not start threads, perform network I/O, do monkey-patching, or similar,
    as a side effect of importing a module.

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

    If you're using Django you can use this to scan for
    agents/pages/commands in all packages defined in ``INSTALLED_APPS``::

        from django.conf import settings

        app = App(..., autodiscover=lambda: settings.INSTALLED_APPS)

    If you're using a recent version of Django, where apps can
    be defined in app configs, use the following
    instead::

        from django.apps import apps

        app = App(...,
                  autodiscover=(config.name
                                for config in apps.get_app_configs())

    We use :keyword:`lambda` in the first example, and a generator
    expression in the latter example. This way you can safely import the
    module containing this app, without forcing the Django settings machinery
    to be initialized (i.e. settings imported).

.. tip::

    For manual control over autodiscovery, you can also call the
    :meth:`@discover` method, manually.

``datadir``
~~~~~~~~~~~

:type: ``Union[str, pathlib.Path]``
:default: ``{appid}-data``

The directory in which this instance stores local table data, etc.

.. seealso::

    - The data directory can also be set using the :option:`faust --datadir`
      option, from the command-line, so there's usually no reason to provide
      a default value when creating the app.

Serialization Parameters
------------------------

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

Advanced Broker Options
-----------------------

``client_id``
~~~~~~~~~~~~~

:type: ``str``
:default: ``faust-VERSION``

You shouldn't have to set this manually.

The client id is used to identify the software used, and is not usually
configured by the user.

``commit_interval``
~~~~~~~~~~~~~~~~~~~

:type: `float`, :class:`~datetime.timedelta`
:default: ``3.0``

How often we commit messages that have been fully processed (:term:`acked`).

``default_partitions``
~~~~~~~~~~~~~~~~~~~~~~

:type: ``int``
:default: ``8``

Default number of partitions for new topics.

.. note::

    This defines the maximum number of workers we could distribute the
    workload of the application (also sometimes referred as the sharding
    factor of the application).

Advanced Table Options
----------------------

``table_cleanup_interval``
~~~~~~~~~~~~~~~~~~~~~~~~~~

:type: `float`, :class:`~datetime.timedelta`
:default: ``30.0``

How often we cleanup tables to remove expired entries.

``num_standby_replicas``
~~~~~~~~~~~~~~~~~~~~~~~~

:type: ``int``
:default: ``1``

The number of standby replicas for each table.

``replication_factor``
~~~~~~~~~~~~~~~~~~~~~~

:type: ``int``
:default: ``1``

The replication factor for changelog topics and repartition topics created
by the application.

.. note::

    This would generally be configured to the replication factor for your
    Kafka cluster.

Web Parameters
--------------

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

``reply_to``
~~~~~~~~~~~~

:type: ``str``
:default: `<generated>`

The name of the reply topic used by this instance.  If not set one will be
automatically generated when the app is created.

``create_reply_topic``
~~~~~~~~~~~~~~~~~~~~~~

:type: ``bool``
:default: :const:`False`

Set this to :const:`True` if you plan on using the RPC with agents.

``reply_expires``
~~~~~~~~~~~~~~~~~

:type: ``Union[float, datetime.timedelta]``
:default: ``timedelta(days=1)``

The expiry time (in seconds float, or timedelta), for how long replies
will stay in the instances local reply topic before being removed.

Subclassing Parameters
----------------------

``Stream``
~~~~~~~~~~

:type: ``Union[str, Type]``
:default: ``"faust.Stream"``

The :class:`~faust.Stream` class to use for streams, or the fully-qualified
path to one (supported by :func:`~faust.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseStream(faust.Stream):
        ...

    app = App(..., Stream=MyBaseStream)

Example using the string path to a class::

    app = App(..., Stream='myproj.streams.Stream')

``Table``
~~~~~~~~~

:type: ``Union[str, Type[TableT]]``
:default: ``"faust.Table"``

The :class:`~faust.Table` class to use for tables, or the fully-qualified
path to one (supported by :func:`~faust.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseTable(faust.Table):
        ...

    app = App(..., Table=MyBaseTable)

Example using the string path to a class::

    app = App(..., Table='myproj.tables.Table')

``Set``
~~~~~~~

:type: ``Union[str, Type[SetT]]``
:default: ``"faust.Set"``

The :class:`~faust.Set` class to use for sets, or the fully-qualified
path to one (supported by :func:`~faust.utils.imports.symbol_by_name`).

Example using a class::

    class MyBaseSetTable(faust.Set):
        ...

    app = App(..., Set=MyBaseSetTable)

Example using the string path to a class::

    app = App(..., Set='myproj.tables.Set')

``TableManager``
~~~~~~~~~~~~~~~~

:type: ``Union[str, Type[TableManagerT]]``
:default: ``"faust.tables.TableManager"``

The :class:`~faust.tables.TableManager` used for managing tables,
or the fully-qualified path to one (supported by
:func:`~faust.utils.imports.symbol_by_name`).

Example using a class::

    from faust.tables import TableManager

    class MyTableManager(TableManager):
        ...

    app = App(..., TableManager=MyTableManager)

Example using the string path to a class::

    app = App(..., TableManager='myproj.tables.TableManager')

``Serializers``
~~~~~~~~~~~~~~~

:type: ``Union[str, Type[RegistryT]]``
:default: ``"faust.serializers.Registry"``

The :class:`~faust.serializers.Registry` class used for
serializing/deserializing messages; or the fully-qualified path
to one (supported by :func:`~faust.utils.imports.symbol_by_name`).

Example using a class::

    from faust.serialiers import Registry

    class MyRegistry(Registry):
        ...

    app = App(..., Serializers=MyRegistry)

Example using the string path to a class::

    app = App(..., Serializers='myproj.serializers.Registry')


Actions
=======

``app.topic()`` -- Create a topic-description
---------------------------------------------

Use the :meth:`~@topic` method to create a topic description, used
for example to tell agents what Kafka topic to read from:

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
name is generated from the :ref:`application id <application-id>`, the
application version, and the fully qualified path of the
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

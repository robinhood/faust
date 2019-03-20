.. _guide-application:

=======================================
 The App - Define your Faust project
=======================================

.. topic:: \

    *“I am not omniscient, but I know a lot.”*

    -- Goethe, *Faust: First part*

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
table storage (optional)

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

.. sourcecode:: pycon

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
a server or additional infrastructure. It also stores them on the file system
in such a way that tables can exceed the size of available memory.

.. _`RocksDB`: http://rocksdb.org/

.. seealso::

    :ref:`guide-settings`: for a full list of supported configuration
        settings -- these can be passed as keyword arguments
        when creating the :class:`faust.App`.

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

Topic Arguments
~~~~~~~~~~~~~~~

+ ``key_type``/``value_type``: :data:`~faust.types.models.ModelArg`

    Use the ``key_type`` and ``value_type`` arguments to specify the models
    used for key and value serialization:

    .. sourcecode:: python

        class MyValueModel(faust.Record):
            name: str
            value: float

        topic = app.topic(
            'name_of_topic',
            key_type=bytes,
            value_type=MyValueModel,
        )

    The default ``key_type`` is :class:`bytes` and treats the key as a binary
    string. The key can also be specified as a model type
    (``key_type=MyKeyModel``).

    .. seealso::

        - The :ref:`guide-channels` guide -- for more about topics
          and channels.

        - The :ref:`guide-models` guide -- for more about models
          and serialization.

+ ``key_serializer``/``value_serializer``: :data:`~faust.types.codecs.CodecArg`

    The codec/serializer type used for keys and values in this topic.

    If not specified the default will be taken from the
    :setting:`key_serializer` and :setting:`value_serializer` settings.

    .. seealso::

        - The :ref:`codecs` section in the :ref:`guide-models` guide -- for
          more information on available codecs, and also how to make your own
          custom encoders and decoders.

+ ``partitions``: :class:`int`

    The number of partitions this topic should have.
    If not specified the default in the :setting:`topic_partitions` setting
    is used.

    Note: if this is an automatically created topic, or an externally managed
    source topic, then please set this value to :const:`None`.

+ ``retention``: :class:`~mode.utils.times.Seconds`

    Number of seconds (as :class:`float`/:class:`~datetime.timedelta`) to keep
    messages in the topic before they can be expired by the server.

+ ``compacting``: :class:`bool`

    Set to :const:`True` if this should be a compacting topic.  The Kafka
    broker will then periodically compact the topic, only keeping the most
    recent value for a key.

+ ``acks``: :class:`bool`

    Enable automatic acknowledgement for this topic.  If you disable this
    then you are responsible for manually acknowleding each event.

+ ``internal``: :class:`bool`

    If set to :const:`True` this means we own and are responsible for this
    topic: we are allowed to create or delete the topic.

+ ``maxsize``: :class:`int`

    The maximum buffer size used for this channel, with default taken from
    the :setting:`stream_buffer_maxsize` setting. When this buffer is exceeded
    the worker will have to wait for agent/stream consumers to catch up, and
    if the buffer is frequently full this will negatively affect performance.

    Try tweaking the buffer sizes, but also the
    :setting:`broker_commit_interval` setting to make sure it commits more
    frequently with larger buffer sizes.

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

Channel Arguments
~~~~~~~~~~~~~~~~~

+ ``key_type``/``value_type``: :data:`~faust.types.models.ModelArg`

    Use the ``key_type`` and ``value_type`` arguments to specify the models
    used for key and value serialization:

    .. sourcecode:: python

        class MyValueModel(faust.Record):
            name: str
            value: float

        channel = app.channel(key_type=bytes, value_type=MyValueModel)

+ ``key_serializer``/``value_serializer``: :data:`~faust.types.codecs.CodecArg`

    The codec/serializer type used for keys and values in this channell

    If not specified the default will be taken from the
    :setting:`key_serializer` and :setting:`value_serializer` settings.

+ ``maxsize``: :class:`int`

    This is the maximum number of pending messages in the channel.
    If this number is exceeded any call to `channel.put(value)` will block
    until something consumes another message from the channel.

    Defaults to the :setting:`stream_buffer_maxsize` setting.

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

.. sourcecode:: pycon

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

.. _application-table-arguments:

Table Arguments
~~~~~~~~~~~~~~~

+ ``name``: :class:`str`

    The name of the table.  This must be *unique* as two tables with the same
    in the same application will share changelog topics.

+ ``help``: :class:`str`

    Short human readable description of table purpose.

+ ``default``: :class:`Callable[[], Any] <collections.Callable>`

    User provided function called to get default value for missing keys.

    Without any default this attempt to access a missing key
    will raise :exc:`KeyError`:

    .. sourcecode:: pycon

        >>> table = app.Table('nodefault', default=None)

        >>> table['missing']
        Traceback (most recent call last):
          File "<stdin>", line 1, in <module>
        KeyError: 'missing'

    With the default callback set to :class:`int`, the same missing
    key will now set the key to ``0`` and return ``0``:

    .. sourcecode:: pycon

        >>> table = app.Table('hasdefault', default=int)

        >>> table['missing']
        0

+ ``key_type``/``value_type``: :data:`~faust.types.models.ModelArg`

    Use the ``key_type`` and ``value_type`` arguments to specify the models
    used for serializing/deserializing keys and values in this table.

    .. sourcecode:: python

        class MyValueModel(faust.Record):
            name: str
            value: float

        table = app.Table(key_type=bytes, value_type=MyValueModel)

+ ``store``: :class:`str` or :class:`~yarl.URL`

    The name of a storage backend to use, or the URL to one.

    Default is taken from the :setting:`store` setting.

+ ``partitions``: :class:`int`

    The number of partitions for the changelog topic used by this table.

    Default is taken from the :setting:`topic_partitions` setting.

+ ``changelog_topic``: :class:`~faust.topics.Topic`

    The changelog topic description to use for this table.

    Only for advanced users who know what they're doing.

+ ``recovery_buffer_size``: :class:`int`

    How often we flush changelog records during recovery.  Default is every
    1000 changelog messages.

+ ``standby_buffer_size``: :class:`int`

    How often we flush changelog records during recovery.  Default
    is :const:`None` (always).

+ ``on_changelog_event``: :class:`Callable[[EventT], Awaitable[None]]`

    A callback called for every changelog event during recovery and while
    keeping table standbys in sync.

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

Agent Arguments
~~~~~~~~~~~~~~~

+ ``name``: :class:`str`

    The name of the agent is automatically taken from the decorated
    function and the module it is defined in.

    You can also specify the name manually, but note that this should
    include the module name, e.g.: ``name='proj.agents.add'``.

+ ``channel``: :class:`~faust.channels.Channel`

    The channel or topic this agent should consume from.

+ ``concurrency``: :class:`int`

    The number of concurrent actors to start for this agent.

    For example if you have an agent processing RSS feeds, a concurrency
    of ``100`` means you can process up to hundred RSS feeds at the same time.

    Adding concurrency to your agent also means it will process events
    in the topic *out of order*, and should you rewind the stream that order
    may differ when processing the events a second time.

    .. admonition:: Concurrency and tables

        Concurrent agents are **not allowed to modify tables**: an
        exception is raised if this is attempted.

        They are however allowed to read from tables.

+ ``sink``: :data:`Iterable[SinkT] <faust.types.agents.SinkT>`

    For agents that also yield a value: forward the value
    to be processed by one or more "sinks".

    A sink can be another agent, a topic, or a callback (async or non-async).

    .. seealso::

        :ref:`agent-sinks` -- for more information on using sinks.

+ ``on_error``: :class:`Callable[[Agent, BaseException], None]`

    Optional error callback to be called when this agent
    raises an unexpected exception.

+ ``supervisor_strategy``: :class:`mode.SupervisorStrategyT`

    A supervisor strategy that decides what happens when this agent
    raises an exception.

    The default supervisor strategy is
    :class:`mode.OneForOneSupervisor` -- restarting one and one agent instance
    as they crash.

    Other built-in supervisor strategies include:

        + :class:`mode.OneForAllSupervisor`

            If one agent instance of this type raises an exception we will
            restart all other agent instances of this type

        + :class:`mode.CrashingSupervisor`

            If one agent instance of this type raises an exception we will
            crash the worker instance.

+ ``**kwargs``

    If the ``channel`` argument is not specified the agent will use an
    automatically named topic.

    Any additional keyword arguments are considered to be configuration
    for this topic, with support for the same arguments as :meth:`@topic`.

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

Timer Arguments
~~~~~~~~~~~~~~~

+ ``on_leader``: :class:`bool`

    If enabled this timer will only execute on one of the worker instances
    at a time -- that is only on the leader of the cluster.

    This can be used as a distributed mutex to execute something
    on one machine at a time.

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

``@app.command()`` -- Define a new command-line command
-------------------------------------------------------

Use the :meth:`~@command` decorator to define a new subcommand
for the :program:`faust` command-line program:

.. sourcecode:: python

    # examples/command.py
    import faust

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
    :class:`~faust.transports.Conductor`, and just about everything that is
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
    async def on_partitions_revoked(app: AppT,
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
        conf.broker = os.environ.get('FAUST_BROKER')
        conf.store = os.environ.get('STORE_URL')

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

    @app.on_before_configured.connect
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

    @app.on_after_configured.connect
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

    @app.on_worker_init.connect
    def on_worker_init(app, **kwargs):
        print(f'Working starting for app {app}')

.. _app-starting:

Starting the App
================

You can start a worker instance for your app from the command-line, or you can
start it inline in your Python process.  To accomodate the many ways you may
want to embed a Faust application, starting the app have several possible entrypoints:


*App entrypoints*:

1) :program:`faust worker`

    The :program:`faust worker` program starts a worker instance for an app
    from the command-line.

    You may turn any self-contained module into the faust program by adding
    this to the end of the file::

        if __name__ == '__main__':
            app.main()

    For packages you can add a ``__main__.py`` module or setuptools
    entrypoints to ``setup.py``.

    If you have the module name where an app is defined, you can start a worker
    for it with the :option:`faust -A` option:

    .. sourcecode:: console

        $ faust -A myproj worker -l info

    The above will import the app from the ``myproj`` module using
    ``from myproj import app``. If you need to specify a different attribute
    you can use a fully qualified path:

    .. sourcecode:: console

        $ faust -A myproj:faust_app worker -l info

2) -> :class:`faust.cli.worker.worker` (CLI interface)

    This is the :program:`faust worker` program defined as a Python
    :pypi:`click` command.

    It is responsible for:

    - Parsing the command-line arguments supported by :program:`faust worker`.
    - Printing the banner box (you will not get that with entrypoint 3 or 4).
    - Starting the :class:`faust.Worker` (see next step).

3) -> :class:`faust.Worker`

    This is used for starting a worker from Python when you also want to
    install process signal handlers, etc.  It supports the same options
    as on the :program:`faust worker` command-line, but now they are passed
    in as keyword arguments to :class:`faust.Worker`.

    The Faust worker is a subclass of :class:`mode.Worker`, which makes
    sense given that Faust is built out of many different :pypi:`mode`
    services starting in a particular order.

    The :class:`faust.Worker` entrypoint is responsible for:

    - Changing the directory when the ``workdir`` argument is set.

    - Setting the process title (when :pypi:`setproctitle` is
      installed), for more helpful entry in ``ps`` listings.

    - Setting up :mod:`logging`: handlers, formatters and level.

    - If :option:`--debug <faust --debug>` is enabled:

      - Starting the :pypi:`aiomonitor` debugging backdoor.

      - Starting the blocking detector.

    - Setting up :sig:`TERM` and :sig:`INT` signal handlers.

    - Setting up the :sig:`USR1` cry handler that logs a traceback.

    - Starting the web server.

    - Autodiscovery (see :setting:`autodiscovery`).

    - Starting the :class:`faust.App` (see next step).

    - Properly shut down of the event loop on exit.

    To start a worker,

    1) from synchronous code, use ``Worker.execute_from_commandline``:

        .. sourcecode:: pycon

            >>> worker = Worker(app)
            >>> worker.execute_from_commandline()

    2) or from an :keyword:`async def` function call ``await worker.start()``:

        .. warning::

            You will be responsible for gracefully shutting down the event
            loop.

        .. sourcecode:: python

            async def start_worker(worker: Worker) -> None:
                await worker.start()

            def manage_loop():
                loop = asyncio.get_event_loop()
                worker = Worker(app, loop=loop)
                try:
                    loop.run_until_complete(start_worker(worker))
                finally:
                    worker.stop_and_shutdown_loop()

    .. admonition:: Multiple apps

        If you want your worker to start multiple apps, you would have
        to pass them in with the ``*services`` starargs::

            worker = Worker(app1, app2, app3, app4)

        This way the extra apps will be started together with the main app,
        and the main app of the worker (``worker.app``) will end up being
        the first positional argument (``app1``).

        Note that the web server will only concern itself with the
        main app, so if you want web access to the other apps you have to
        include web servers for them (also passed in as ``*services``
        starargs).

4) -> :class:`faust.App`

    The "worker" only concerns itself with the terminal, process
    signal handlers, logging, debugging mechanisms, etc., the rest
    is up to the app.

    You can call ``await app.start()`` directly to get a side-effect free
    instance that can be embedded in any environment. It won't even emit logs
    to the console unless you have configured :mod:`logging` manually,
    and it won't set up any :sig:`TERM`/:sig:`INT` signal handlers, which
    means :keyword:`finally` blocks won't execute at shutdown.

    Start app directly:

    .. sourcecode:: python

        async def start_app(app):
            await app.start()

    This will block until the worker shuts down, so if you want to
    start other parts of your program, you can start this in the background:

    .. sourcecode:: python

        def start_in_loop(app):
            loop = asyncio.get_event_loop()
            loop.ensure_future(app.start())

    If your program is written as a set of :pypi:`Mode` services, you can
    simply add the app as a depdendency to your service:

    .. sourcecode:: python

        class MyService(mode.Service):

            def on_init_dependencies(self):
                return [faust_app]

Client-Only Mode
================

The app can also be started in "client-only" mode, which means the app
can be used for sending agent RPC requests and retrieving replies, but not
start a full Faust worker:

.. sourcecode:: python

    await app.start_client()

.. _project-layout:

Projects and Directory Layout
=============================

Faust is a library; it does not mandate any specific directory layout
and integrates with any existing framework or project conventions.

That said, new projects written from scratch using Faust will want some
guidance on how to organize, so we include this as a suggestion in the
documentation.

.. _project-layout-standalone:

Small/Standalone Projects
-------------------------

You can create a small Faust service with no supporting directories at all,
we refer to this as a "standalone module": a module that contains everything
it needs to run a full service.

The Faust distribution comes with several standalone examples,
such as `examples/word_count.py`.

.. _project-layout-large:

Medium/Large Projects
---------------------

Projects need more organization as they grow larger,
so we convert the standalone module into a directory layout:

.. sourcecode:: text

    + proj/
        - setup.py
        - MANIFEST.in
        - README.rst
        - setup.cfg

        + proj/
            - __init__.py
            - __main__.py
            - app.py

            + users/
            -   __init__.py
            -   agents.py
            -   commands.py
            -   models.py
            -   views.py

            + orders/
                -   __init__.py
                -   agents.py
                -   models.py
                -   views.py

Problem: Autodiscovery
~~~~~~~~~~~~~~~~~~~~~~

Now we have many `@app.agent`/`@app.timer`'/`@app.command` decorators,
and models spread across a nested directory. These have to be imported by
the program to be registered and used.

Enter the :setting:`autodiscover` setting:

.. sourcecode:: python

    # proj/app.py
    import faust

    app = faust.App(
        'proj',
        version=1,
        autodiscover=True,
        origin='proj'   # imported name for this project (import proj -> "proj")
    )

    def main() -> None:
        app.main()

Using the :setting:`autodiscover`  and setting it to :const:`True` means
it will traverse the directory of the origin module to find agents, timers,
tasks, commands and web views, etc.

If you want more careful control you can specify a list of modules to traverse instead:

.. sourcecode:: python

    app = faust.App(
        'proj',
        version=1,
        autodiscover=['proj.users', 'proj.orders'],
        origin='proj'
    )

.. admonition:: Autodiscovery when using Django

    When using `autodiscover=True` in a Django project,
    only the apps listed in ``INSTALLED_APPS`` will be traversed.

    See also :ref:`project-layout-django`.

Problem: Entrypoint
~~~~~~~~~~~~~~~~~~~

The :file:`proj/__main__.py` module can act as the entrypoint for this
project:

.. sourcecode:: python

    # proj/__main__.py
    from proj.app import app
    app.main()

After creating this module you can now start a worker by doing:

.. sourcecode:: python

    python -m proj worker -l info

Now you're probably thinking, "I'm too lazy to type python dash em all the
time", but don't worry: take it one step further by using
setuptools to install a command-line program for your project.

1) Create a :file:`setup.py` for your project.

    This step is not needed if you already have one.

    You can read lots about creating your :file:`setup.py` in the
    :pypi:`setuptools` documentation here:
    https://setuptools.readthedocs.io/en/latest/setuptools.html#developer-s-guide

    A minimum example that will work well enough:

    .. sourcecode:: python

        #!/usr/bin/env python
        from setuptools import find_packages, setup

        setup(
            name='proj',
            version='1.0.0',
            description='Use Faust to blah blah blah',
            author='Ola Normann',
            author_email='ola.normann@example.com',
            url='http://proj.example.com',
            platforms=['any'],
            license='Proprietary',
            packages=find_packages(exclude=['tests', 'tests.*']),
            include_package_data=True,
            zip_safe=False,
            install_requires=['faust'],
            python_requires='~=3.6',
        )

    For inspiration you can also look to the `setup.py` files in the
    :pypi:`faust` and :pypi:`mode` source code distributions.

2) Add the command as a setuptools entrypoint.

    To your :file:`setup.py` add the following argument:

    .. sourcecode:: python

        setup(
            ...,
            entry_points={
                'console_scripts': [
                    'proj = proj.app:main',
                ],
            },
        )

    This essentially defines that the ``proj`` program runs `from proj.app
    import main`

3) Install your package using setup.py or :program:`pip`.

    When developing your project locally you should use ``setup.py develop``
    to use the source code directory as a Python package:

    .. sourcecode:: console

        $ python setup.py develop

    You can now run the `proj` command you added to :file:`setup.py` in step
    two:

    .. sourcecode:: console

        $ proj worker -l info

    Why use ``develop``? You can use ``python setup.py install``, but then
    you have to run that every time you make modifications to the source
    files.

Another upside to using ``setup.py`` is that you can distribute your projects
as ``pip install``-able packages.


.. _project-layout-django:

Django Projects
---------------

Django has their own conventions for directory layout, but your Django
reusable apps will want some way to import your Faust app.

We believe the best place to define the Faust app in a Django project, is in
a dedicated reusable app. See the ``faustapp`` app in the
:file:`examples/django` directory in the Faust source code distribution.

.. _app-misc:

Miscellaneous
=============

.. _app-misc-app_rationale:

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

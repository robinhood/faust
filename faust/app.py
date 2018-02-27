"""Faust Application.

An app is an instance of the Faust library.
Everything starts here.

"""
import asyncio
import importlib
import inspect
import re
import typing
import warnings
from collections import defaultdict
from functools import wraps
from heapq import heappop, heappush
from itertools import chain
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Iterable, Iterator, List, Mapping, MutableMapping, MutableSequence,
    NamedTuple, Optional, Pattern, Set as _Set, Type, Union, cast,
)

import venusian
from mode import Seconds, Service, ServiceT, SupervisorStrategyT, want_seconds
from mode.proxy import ServiceProxy
from mode.services import ServiceCallbacks
from mode.utils.aiter import aiter
from mode.utils.collections import force_mapping
from mode.utils.futures import FlowControlEvent, ThrowableQueue, stampede
from mode.utils.imports import import_from_cwd, symbol_by_name
from mode.utils.types.trees import NodeT

from . import transport
from .agents import (
    AgentFun, AgentManager, AgentT, ReplyConsumer, SinkT,
)
from .channels import Channel, ChannelT
from .exceptions import ImproperlyConfigured, SameNode
from .fixups import FixupT, fixups
from .sensors import Monitor, SensorDelegate
from .streams import current_event

from .types import (
    CodecArg, CollectionT, FutureMessage, K, Message, MessageSentCallback,
    ModelArg, RecordMetadata, RegistryT, TP, TopicT, V,
)
from .types.app import (
    AppT, HttpClientT, PageArg, RoutedViewGetHandler, TaskArg, ViewGetHandler,
)
from .types.assignor import LeaderAssignorT, PartitionAssignorT
from .types.router import RouterT
from .types.settings import Settings
from .types.streams import StreamT
from .types.tables import SetT, TableManagerT, TableT
from .types.topics import ConductorT
from .types.transports import ConsumerT, ProducerT, TPorTopicSet, TransportT
from .types.windows import WindowT
from .utils.objects import Unordered, cached_property
from .web.views import Request, Response, Site, View

if typing.TYPE_CHECKING:
    from .cli.base import AppCommand
    from .channels import Event
    from .worker import Worker as WorkerT
else:
    class AppCommand: ...  # noqa
    class Event: ...       # noqa
    class WorkerT: ...     # noqa

__all__ = ['App']

#: Format string for ``repr(app)``.
APP_REPR = """
<{name}({c.id}): {c.broker} {s.state} agents({agents}) topics({topics})>
""".strip()

# Venusian decorator scan categories.

SCAN_AGENT = 'faust.agent'
SCAN_COMMAND = 'faust.command'
SCAN_PAGE = 'faust.page'
SCAN_SERVICE = 'faust.service'
SCAN_TASK = 'faust.task'

#: Default decorator categories for :pypi`venusian` to scan for when
#: autodiscovering.
SCAN_CATEGORIES: Iterable[str] = [
    SCAN_AGENT,
    SCAN_COMMAND,
    SCAN_PAGE,
    SCAN_SERVICE,
    SCAN_TASK,
]

#: List of regular expressions for :pypi:`venusian` that acts as a filter
#: for modules that :pypi:`venusian` should ignore when autodiscovering
#: decorators.
SCAN_IGNORE: Iterable[str] = ['test_.*']

E_NEED_ORIGIN = """
`origin` argument to faust.App is mandatory when autodiscovery enabled.

This parameter sets the canonical path to the project package,
and describes how a user, or program can find it on the command-line when using
the `faust -A project` option.  It's also used as the default package
to scan when autodiscovery is enabled.

If your app is defined in a module: ``project/app.py``, then the
origin will be "project":

    # file: project/app.py
    import faust

    app = faust.App(
        id='myid',
        origin='project',
    )
"""

W_OPTION_DEPRECATED = """\
Argument {old!r} is deprecated and scheduled for removal in Faust 1.0.

Please use {new!r} instead.
"""


class _AttachedHeapEntry(NamedTuple):
    # Tuple used in heapq entry for app._pending_on_commit.
    # These are used to send messages when an offset is committed
    # (sending of the message is attached to an offset in a source topic).
    offset: int
    message: Unordered[FutureMessage]


class AppService(Service):
    """Service responsible for starting/stopping an application."""

    # The App() is created during module import and cannot subclass Service
    # directly as Service.__init__ creates the asyncio event loop, and
    # creating the event loop as a side effect of importing a module
    # is a dangerous practice (e.g., if you switch to uvloop after you can
    # end up in a situation where some services use the old loop).

    # To solve this we use ServiceProxy to split into App + AppService,
    # in a way such that the AppService is started lazily when first needed.

    _extra_service_instances: List[ServiceT] = None

    def __init__(self, app: 'App', **kwargs: Any) -> None:
        self.app: App = app
        super().__init__(loop=self.app.loop, **kwargs)

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # Client-Only: Boots up enough services to be able to
        # produce to topics and receive replies from topics.
        # XXX If we switch to socket RPC using routers we can remove this.
        if self.app.client_only:
            return self._components_client()
        # Server: Starts everything.
        return self._components_server()

    def _components_client(self) -> Iterable[ServiceT]:
        # Returns the components started when running in Client-Only mode.
        return cast(Iterable[ServiceT], chain(
            [self.app.producer],
            [self.app.consumer],
            [self.app._reply_consumer],
            [self.app.topics],
            [self.app._fetcher],
        ))

    def _components_server(self) -> Iterable[ServiceT]:
        # Returns the components started when running normally (Server mode).
        # Note: has side effects: adds the monitor to the app's list of
        # sensors.

        # Add the main Monitor sensor.
        # The beacon is also reattached in case the monitor
        # was created by the user.
        self.app.monitor.beacon.reattach(self.beacon)
        self.app.monitor.loop = self.loop
        self.app.sensors.add(self.app.monitor)

        # Then return the list of "subservices",
        # those that'll be started when the app starts,
        # stopped when the app stops,
        # etc...
        return cast(Iterable[ServiceT], chain(
            # Sensors (Sensor): always start first and stop last.
            self.app.sensors,
            # Producer (transport.Producer): always stop after Consumer.
            [self.app.producer],
            # Consumer (transport.Consumer): always stop after TopicConductor
            [self.app.consumer],
            # Leader Assignor (assignor.LeaderAssignor)
            [self.app._leader_assignor],
            # Reply Consumer (ReplyConsumer)
            [self.app._reply_consumer],
            # Agents (app.agents)
            self.app.agents.values(),
            # Topic Manager (app.TopicConductor))
            [self.app.topics],
            # Table Manager (app.TableManager)
            [self.app.tables],
            # Fetcher (transport.Fetcher)
            [self.app._fetcher],
        ))

    async def on_first_start(self) -> None:
        self.app._create_directories()
        if not self.app.agents:
            # XXX I can imagine use cases where an app is useful
            #     without agents, but use this as more of an assertion
            #     to make sure agents are registered correctly. [ask]
            raise ImproperlyConfigured(
                'Attempting to start app that has no agents')
        await self.app.on_first_start()

    async def on_start(self) -> None:
        self.app.finalize()
        await self.app.on_start()

    async def on_started(self) -> None:
        # Wait for table recovery to complete.
        if not await self.wait_for_stopped(self.app.tables.recovery_completed):
            # Add all asyncio.Tasks, like timers, etc.
            await self.on_started_init_extra_tasks()

            # Start user-provided services.
            await self.on_started_init_extra_services()

            # Call the app-is-fully-started callback used by Worker
            # to print the "ready" message that signals to the user that
            # the worker is ready to start processing.
            if self.app.on_startup_finished:
                await self.app.on_startup_finished()

            await self.app.on_started()

    async def on_started_init_extra_tasks(self) -> None:
        for task in self.app._tasks:
            # pass app if decorated function takes argument
            target: Any
            if inspect.signature(task).parameters:
                target = cast(Callable[[AppT], Awaitable], task)(self.app)
            else:
                target = cast(Callable[[], Awaitable], task)()
            self.add_future(target)

    async def on_started_init_extra_services(self) -> None:
        if self._extra_service_instances is None:
            # instantiate the services added using the @app.service decorator.
            self._extra_service_instances = [
                s(loop=self.loop,
                  beacon=self.beacon) if inspect.isclass(s) else s
                for s in self.app._extra_services
            ]
            for service in self._extra_service_instances:
                # start the services now, or when the app is started.
                await self.add_runtime_dependency(service)

    async def on_stop(self) -> None:
        await self.app.on_stop()

    async def on_shutdown(self) -> None:
        await self.app.on_shutdown()

    async def on_restart(self) -> None:
        await self.app.on_restart()

    @property
    def label(self) -> str:
        return self.app.label

    @property
    def shortlabel(self) -> str:
        return self.app.shortlabel


class App(AppT, ServiceProxy, ServiceCallbacks):
    """Faust Application.

    Arguments:
        id (str): Application ID.

        broker (str): Broker URL. Default is ``"aiokafka://localhost:9092"``.
        broker_client_id (str):  Client id used for producer/consumer.
        broker_commit_interval (Seconds): How often we commit messages that
            have been fully processed.  Default ``30.0``.
        key_serializer (CodecArg): Default serializer for Topics
            that don't have one set.  Default: :const:`None`.
        value_serializer (CodecArg): Default serializer for event types
            that don't have one set.  Default: ``"json"``.
        topic_replication_factor (int): The default replication factor for
            topics created by the application.  efault:
            ``1``. Generally, this would be the same as the configured
            replication factor for your Kafka cluster.
        table_standby_replicas (int): The number of standby replicas for each
            table.  Default: ``1``.
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.
    """

    #: Set this to True if app should only start the services required to
    #: operate as an RPC client (producer and reply consumer).
    client_only = False

    #: Source of configuration: ``app.conf`` (when configured)
    _conf: Settings = None

    #: Original configuration source object.
    _config_source: Any = None

    # Default producer instance.
    _producer: ProducerT = None

    # Default consumer instance.
    _consumer: ConsumerT = None

    # Transport is created on demand: use `.transport` property.
    _transport: Optional[TransportT] = None

    _assignor: PartitionAssignorT = None

    # Mapping used to attach messages to a source message such that
    # only when the source message is acked, only then do we publish
    # its attached messages.
    #
    # The mapping maintains one list for each TopicPartition,
    # where the lists are used as heap queues containing tuples
    # of ``(source_message_offset, FutureMessage)``.
    _pending_on_commit: MutableMapping[TP, List[_AttachedHeapEntry]]

    # Monitor is created on demand: use `.monitor` property.
    _monitor: Monitor = None

    # @app.task decorator adds asyncio tasks to be started
    # with the app here.
    _tasks: MutableSequence[TaskArg]

    _http_client: HttpClientT = None

    _extra_services: List[ServiceT] = None

    # we set this the first time on_partition_is_called
    # so that we only restart agents on rebalance, not the
    # first time we start.
    _partitions_revoked_count: int = 0

    def __init__(self, id: str, *,
                 monitor: Monitor = None,
                 config_source: Any = None,
                 **options: Any) -> None:
        self._default_options = (id, options)
        self.agents = AgentManager()
        self.sensors = SensorDelegate(self)
        self._monitor = monitor
        self._tasks = []
        self._pending_on_commit = defaultdict(list)
        self.on_startup_finished: Callable = None
        self.pages = []
        self._extra_services = []
        self._config_source = config_source

        self._init_signals()
        self.fixups = self._init_fixups()
        ServiceProxy.__init__(self)

    def _init_signals(self) -> None:
        self.on_before_configured = (
            self.on_before_configured.with_default_sender(self))
        self.on_configured = self.on_configured.with_default_sender(self)
        self.on_after_configured = (
            self.on_after_configured.with_default_sender(self))
        self.on_partitions_assigned = (
            self.on_partitions_assigned.with_default_sender(self))
        self.on_partitions_revoked = (
            self.on_partitions_revoked.with_default_sender(self))
        self.on_worker_init = self.on_worker_init.with_default_sender(self)

    def _init_fixups(self) -> MutableSequence[FixupT]:
        return list(fixups(self))

    def config_from_object(self, obj: Any,
                           *,
                           silent: bool = False,
                           force: bool = False) -> None:
        """Read configuration from object.

        Object is either an actual object or the name of a module to import.

        Examples:
            >>> app.config_from_object('myproj.faustconfig')

            >>> from myproj import faustconfig
            >>> app.config_from_object(faustconfig)

        Arguments:
            silent (bool): If true then import errors will be ignored.
            force (bool): Force reading configuration immediately.
                By default the configuration will be read only when required.
        """
        self._config_source = obj
        if force or self.configured:
            self._conf = None
            self._configure(silent=silent)

    def finalize(self) -> None:
        if not self.finalized:
            self.finalized = True
            id = self.conf.id
            if not id:
                raise ImproperlyConfigured('App requires an id!')

    async def on_stop(self) -> None:
        if self._http_client:
            self._http_client.close()

    def worker_init(self) -> None:
        for fixup in self.fixups:
            fixup.on_worker_init()
        self.on_worker_init.send()

    def discover(self,
                 *extra_modules: str,
                 categories: Iterable[str] = SCAN_CATEGORIES,
                 ignore: Iterable[str] = SCAN_IGNORE) -> None:
        modules = set(self._discovery_modules())
        modules |= set(extra_modules)
        for fixup in self.fixups:
            modules |= set(fixup.autodiscover_modules())
        if modules:
            scanner = self._new_scanner(*[re.compile(pat) for pat in ignore])
            for name in modules:
                try:
                    module = importlib.import_module(name)
                except ModuleNotFoundError:
                    raise ModuleNotFoundError(
                        f'Unknown module {name} in App.conf.autodiscover list')
                scanner.scan(
                    module,
                    categories=tuple(categories),
                )

    def _discovery_modules(self) -> List[str]:
        modules: List[str] = []
        autodiscover = self.conf.autodiscover
        if autodiscover:
            if isinstance(autodiscover, bool):
                if self.conf.origin is None:
                    raise ImproperlyConfigured(E_NEED_ORIGIN)
            elif callable(autodiscover):
                modules.extend(
                    cast(Callable[[], Iterator[str]], autodiscover)())
            else:
                modules.extend(autodiscover)
            modules.append(self.conf.origin)
        return modules

    def _new_scanner(
            self, *ignore: Pattern, **kwargs: Any) -> venusian.Scanner:
        return venusian.Scanner(ignore=[pat.search for pat in ignore])

    def main(self) -> None:
        """Execute the :program:`faust` umbrella command using this app."""
        from .cli.faust import cli
        self.finalize()
        self.worker_init()
        if self.conf.autodiscover:
            self.discover()
        cli(app=self)

    def topic(self, *topics: str,
              pattern: Union[str, Pattern] = None,
              key_type: ModelArg = None,
              value_type: ModelArg = None,
              key_serializer: CodecArg = None,
              value_serializer: CodecArg = None,
              partitions: int = None,
              retention: Seconds = None,
              compacting: bool = None,
              deleting: bool = None,
              replicas: int = None,
              acks: bool = True,
              internal: bool = False,
              config: Mapping[str, Any] = None) -> TopicT:
        """Create topic description.

        Topics are named channels (for example a Kafka topic),
        to communicate locally create a :meth:`channel`.

        See Also:
            :class:`faust.topics.Topic`
        """
        Topic = (self.conf.Topic if self.finalized
                 else symbol_by_name('faust:Topic'))
        return Topic(
            self,
            topics=topics,
            pattern=pattern,
            key_type=key_type,
            value_type=value_type,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            partitions=partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            replicas=replicas,
            acks=acks,
            internal=internal,
            config=config,
        )

    def channel(self, *,
                key_type: ModelArg = None,
                value_type: ModelArg = None,
                maxsize: int = 1,
                loop: asyncio.AbstractEventLoop = None) -> ChannelT:
        """Create new channel.

        By default this will create an in-memory channel
        used for intra-process communication, but in practice
        channels can be backed by any transport (network or even means
        of inter-process communication).

        See Also:
            :class:`faust.channels.Channel`.
        """
        return Channel(
            self,
            key_type=key_type,
            value_type=value_type,
            maxsize=maxsize,
            loop=loop,
        )

    def agent(self,
              channel: Union[str, ChannelT] = None,
              *,
              name: str = None,
              concurrency: int = 1,
              supervisor_strategy: Type[SupervisorStrategyT] = None,
              sink: Iterable[SinkT] = None,
              **kwargs: Any) -> Callable[[AgentFun], AgentT]:
        """Create Agent from async def function.

        The decorated function may be an async iterator, in this
        mode the value yielded in reaction to a request will be the reply::

            @app.agent()
            async def my_agent(requests):
                async for number in requests:
                    yield number * 2

        It can also be a regular async function, but then replies are not
        supported::

            @app.agent()
            async def my_agent(stream):
                async for number in stream:
                    print(f'Received: {number!r}')
        """
        def _inner(fun: AgentFun) -> AgentT:
            Agent = (self.conf.Agent if self.finalized
                     else symbol_by_name('faust:Agent'))
            agent = Agent(
                fun,
                name=name,
                app=self,
                channel=channel,
                concurrency=concurrency,
                supervisor_strategy=supervisor_strategy,
                sink=sink,
                on_error=self._on_agent_error,
                help=fun.__doc__,
                **kwargs)
            self.agents[agent.name] = agent
            venusian.attach(agent, agent.on_discovered, category=SCAN_AGENT)
            return agent
        return _inner
    actor = agent  # XXX Compatibility alias: REMOVE FOR 1.0

    async def _on_agent_error(
            self, agent: AgentT, exc: BaseException) -> None:
        # XXX If an agent raises in the middle of processing an event
        # what do we do with acking it?  Currently the source message will be
        # acked and not processed again, simply because it violates
        # ""exactly-once" semantics".
        #
        # - What about retries?
        # It'd be safe to retry processing the event if the agent
        # processing is idempotent, but we don't enforce idempotency
        # in stream processors so it's not something we can enable by default.
        #
        # The retry would also have to stop processing of the topic
        # so that order is maintained: the next offset in the topic can only
        # be processed after the event is retried.
        #
        # - How about crashing?
        # Crashing the instance to require human intervention is certainly
        # a choice, but far from ideal considering how common mistakes
        # in code or unhandled exceptions are.  It may be better to log
        # the error and have ops replay and reprocess the stream on
        # notification.
        if self._consumer:
            try:
                await self._consumer.on_task_error(exc)
            except MemoryError:
                raise
            except Exception as exc:
                self.log.exception('Consumer error callback raised: %r', exc)

    def task(self, fun: TaskArg) -> TaskArg:
        """Define an async def function to be started with the app.

        This is like :meth:`timer` but a one-shot task only
        executed at worker startup (after recovery and the worker is
        fully ready for operation).

        The function may take zero or one argument.
        If the target function takes an argument, the ``app`` argument
        is passed::

            >>> @app.task
            >>> async def on_startup(app):
            ...    print('STARTING UP: %r' % (app,))

        Nullary functions are also supported::

            >>> @app.task
            >>> async def on_startup():
            ...     print('STARTING UP')
        """
        venusian.attach(fun, self._on_task_discovered, category=SCAN_TASK)
        self._tasks.append(fun)
        return fun

    def _on_task_discovered(
            self, scanner: venusian.Scanner, name: str, obj: TaskArg) -> None:
        ...

    def timer(self, interval: Seconds,
              on_leader: bool = False) -> Callable:
        """Define an async def function to be run at periodic intervals.

        Like :meth:`task`, but executes periodically until the worker
        is shut down.

        This decorator takes an async function and adds it to a
        list of timers started with the app.

        Arguments:
            interval (Seconds): How often the timer executes in seconds.

            on_leader (bool) = False: Should the timer only run on the leader?

        Example:
            >>> @app.timer(interval=10.0)
            >>> async def every_10_seconds():
            ...     print('TEN SECONDS JUST PASSED')


            >>> app.timer(interval=5.0, on_leader=True)
            >>> async def every_5_seconds():
            ...     print('FIVE SECONDS JUST PASSED. ALSO, I AM THE LEADER!')
        """
        interval_s = want_seconds(interval)

        def _inner(fun: TaskArg) -> TaskArg:
            @self.task
            @wraps(fun)
            async def around_timer(*args: Any) -> None:
                while not self._service.should_stop:
                    await self._service.sleep(interval_s)
                    should_run = not on_leader or self.is_leader()
                    if should_run:
                        await fun(*args)  # type: ignore
            return around_timer
        return _inner

    def service(self, cls: Type[ServiceT]) -> Type[ServiceT]:
        """Decorate :class:`mode.Service` to be started with the app.

        Examples:
            .. sourcecode:: python

                from mode import Service

                @app.service
                class Foo(Service):
                    ...
        """
        venusian.attach(
            cls, self._on_service_discovered, category=SCAN_SERVICE)
        self._extra_services.append(cls)
        return cls

    def _on_service_discovered(
            self, scanner: venusian.Scanner,
            name: str, obj: Type[ServiceT]) -> None:
        ...

    def is_leader(self) -> bool:
        return self._leader_assignor.is_leader()

    def stream(self, channel: Union[AsyncIterable, Iterable],
               beacon: NodeT = None,
               **kwargs: Any) -> StreamT:
        """Create new stream from channel/topic/iterable/async iterable.

        Arguments:
            channel: Iterable to stream over (async or non-async).

            kwargs: See :class:`Stream`.

        Returns:
            faust.Stream:
                to iterate over events in the stream.
        """
        return self.conf.Stream(
            app=self,
            channel=aiter(channel) if channel is not None else None,
            beacon=beacon or self.beacon,
            **kwargs)

    def Table(self, name: str,
              *,
              default: Callable[[], Any] = None,
              window: WindowT = None,
              partitions: int = None,
              help: str = None,
              **kwargs: Any) -> TableT:
        """Define new table.

        Arguments:
            name: Name used for table, note that two tables living in
                the same application cannot have the same name.

            default: A callable, or type that will return a default value
               for keys missing in this table.
            window: A windowing strategy to wrap this window in.

        Examples:
            >>> table = app.Table('user_to_amount', default=int)
            >>> table['George']
            0
            >>> table['Elaine'] += 1
            >>> table['Elaine'] += 1
            >>> table['Elaine']
            2
        """
        Table = (self.conf.Table if self.finalized
                 else symbol_by_name('faust:Table'))
        table = self.tables.add(Table(
            self,
            name=name,
            default=default,
            beacon=self.beacon,
            partitions=partitions,
            help=help,
            **kwargs))
        return table.using_window(window) if window else table

    def Set(self, name: str,
            *,
            window: WindowT = None,
            partitions: int = None,
            help: str = None,
            **kwargs: Any) -> SetT:
        """Define new Set table.

        A set is a table with the :class:`typing.AbstractSet` interface,
        that internally stores the table as a dictionary of (key, True)
        values.

        Note:
            The set does *not have* the dict/Mapping interface.
        """
        return self.tables.add(self.conf.Set(
            self,
            name=name,
            beacon=self.beacon,
            partitions=partitions,
            window=window,
            help=help,
            **kwargs))

    def page(self, path: str,
             *,
             base: Type[View] = View) -> Callable[[PageArg], Type[Site]]:

        def _decorator(fun: PageArg) -> Type[Site]:
            view: Type[View] = None
            name: str
            if inspect.isclass(fun):
                typ = cast(Type[View], fun)
                if issubclass(typ, View):
                    name = fun.__name__
                    view = typ
                else:
                    raise TypeError(
                        'Class argument to @page must be subclass of View')
            if view is None:
                handler = cast(ViewGetHandler, fun)
                if callable(handler):
                    name = handler.__name__
                    view = type(name, (View,), {
                        'get': handler,
                        __doc__: handler.__doc__,
                        '__module__': handler.__module__,
                    })
                else:
                    raise TypeError(f'Not view, nor callable: {fun!r}')

            site: Type[Site] = type('Site', (Site,), {
                'views': {path: view},
                '__module__': fun.__module__,
            })
            self.pages.append(('', site))
            venusian.attach(site, site.on_discovered, category=SCAN_PAGE)
            return site
        return _decorator

    def table_route(self, table: CollectionT,
                    shard_param: str) -> RoutedViewGetHandler:
        def _decorator(fun: ViewGetHandler) -> ViewGetHandler:

            @wraps(fun)
            async def get(view: View, request: Request) -> Response:
                key = request.query[shard_param]
                try:
                    return await self.router.route_req(
                        table.name, key, view.web, request)
                except SameNode:
                    return await fun(view, request)
            return get

        return _decorator

    def command(self,
                *options: Any,
                base: Type[AppCommand] = None,
                **kwargs: Any) -> Callable[[Callable], Type[AppCommand]]:
        if options is None and base is None and kwargs is None:
            raise TypeError('Use parens in @app.command(), not @app.command.')
        if base is None:
            from .cli import base as cli_base
            base = cli_base.AppCommand

        def _inner(fun: Callable[..., Awaitable[Any]]) -> Type[AppCommand]:
            target: Any = fun
            if not inspect.signature(fun).parameters:
                # if it does not take self argument, use staticmethod
                target = staticmethod(fun)

            cmd = type(fun.__name__, (base,), {
                'run': target,
                '__doc__': fun.__doc__,
                '__name__': fun.__name__,
                '__qualname__': fun.__qualname__,
                '__module__': fun.__module__,
                '__wrapped__': fun,
                'options': options,
                **kwargs})

            def on_discovered(scanner: venusian.Scanner,
                              name: str,
                              obj: AppCommand) -> None:
                ...
            venusian.attach(cmd, on_discovered, category=SCAN_COMMAND)
            return cmd
        return _inner

    async def start_client(self) -> None:
        """Start the app in Client-Only mode necessary for RPC requests.

        Notes:
            Once started as a client the app cannot be restarted as Server.
        """
        self.client_only = True
        await self._service.maybe_start()

    async def maybe_start_client(self) -> None:
        """Start the app in Client-Only mode if not started as Server."""
        if not self._service.started:
            await self.start_client()

    async def _maybe_attach(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            force: bool = False) -> Awaitable[RecordMetadata]:
        # XXX The concept of attaching should be deprecated when we
        # have Kafka transaction support (:kip:`KIP-98`).
        # This is why the interface related to attaching is private.

        # attach message to current event if there is one.
        if not force:
            event = current_event()
            if event is not None:
                return cast(Event, event)._attach(
                    channel, key, value,
                    partition=partition,
                    key_serializer=key_serializer,
                    value_serializer=value_serializer,
                    callback=callback,
                )
        return await self.send(
            channel, key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    async def send(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        """Send event to channel/topic.

        Arguments:
            channel: Channel/topic or the name of a
                topic to send event to.
            key: Message key.
            value: Message value.
            partition: Specific partition to send to.
                If not set the partition will be chosen by the partitioner.
            key_serializer: Serializer to use
                only when key is not a model.
            value_serializer: Serializer to use
                only when value is not a model.
            callback: Callable to be called after
                the message is published.  Signature must be unary as the
                :class:`~faust.types.tuples.FutureMessage` future is passed
                to it.

                The resulting :class:`faust.types.tuples.RecordMetadata`
                object is then available as ``fut.result()``.
        """
        if isinstance(channel, str):
            channel = self.topic(channel)
        return await channel.send(
            key, value, partition,
            key_serializer, value_serializer, callback,
        )

    def _send_attached(
            self,
            message: Message,
            channel: Union[str, ChannelT],
            key: K,
            value: V,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        # This attaches message to be published when source message' is
        # acknowledged.  To be replaced by transactions in :kip:`KIP-98`.

        # get heap queue for this TopicPartition
        # items in this list are ``(source_offset, Unordered[FutureMessage])``
        # tuples.
        buf = self._pending_on_commit[message.tp]
        chan = self.topic(channel) if isinstance(channel, str) else channel
        fut = chan.as_future_message(
            key, value, partition,
            key_serializer, value_serializer, callback)
        # Note: Since FutureMessage have members that are unhashable
        # we wrap it in an Unordered object to stop heappush from crashing.
        # Unordered simply orders by random order, which is fine
        # since offsets are always unique.
        heappush(buf, _AttachedHeapEntry(message.offset, Unordered(fut)))
        return fut

    async def _commit_attached(self, tp: TP, offset: int) -> None:
        # publish pending messages attached to this TP+offset

        # make shallow copy to allow concurrent modifications (append)
        attached = list(self._get_attached(tp, offset))
        if attached:
            await asyncio.wait(
                [await fut.message.channel.publish_message(fut, wait=False)
                 for fut in attached],
                return_when=asyncio.ALL_COMPLETED,
                loop=self.loop,
            )

    def _get_attached(self,
                      tp: TP, commit_offset: int) -> Iterator[FutureMessage]:
        # Return attached messages for TopicPartition within committed offset.
        attached = self._pending_on_commit.get(tp)
        while attached:
            # get the entry with the smallest offset in this TP
            entry = heappop(attached)

            # if the entry offset is smaller or equal to the offset
            # being committed
            if entry[0] <= commit_offset:
                # we use it by extracting the FutureMessage
                # from _AttachedHeapEntry, where entry.message is
                # Unordered[FutureMessage].
                yield entry.message.value
            else:
                # else we put it back and exit (this was the smallest offset).
                heappush(attached, entry)
                break

    @stampede
    async def maybe_start_producer(self) -> ProducerT:
        """Start producer if it has not already been started."""
        producer = self.producer
        # producer may also have been started by app.start()
        await producer.maybe_start()
        return producer

    async def commit(self, topics: TPorTopicSet) -> bool:
        """Commit acked messages in topics'.

        Warning:
            This will commit acked messages in **all topics**
            if the topics argument is passed in as :const:`None`.
        """
        return await self.topics.commit(topics)

    async def _on_partitions_assigned(self, assigned: _Set[TP]) -> None:
        """Handle new topic partition assignment.

        This is called during a rebalance after :meth:`on_partitions_revoked`.

        The new assignment provided overrides the previous
        assignment, so any tp no longer in the assigned' list will have
        been revoked.
        """
        try:
            self.flow_control.resume()
            # Wait for TopicConductor to finish any new subscriptions
            await self.topics.wait_for_subscriptions()
            await self.consumer.pause_partitions(assigned)
            await self._fetcher.restart()
            self.log.info(f'Restarted fetcher')
            await self.topics.on_partitions_assigned(assigned)
            await self.tables.on_partitions_assigned(assigned)
            await self.on_partitions_assigned.send(assigned)
        except Exception as exc:
            await self.crash(exc)

    async def _on_partitions_revoked(self, revoked: _Set[TP]) -> None:
        """Handle revocation of topic partitions.

        This is called during a rebalance and is followed by
        :meth:`on_partitions_assigned`.

        Revoked means the partitions no longer exist, or they
        have been reassigned to a different node.
        """
        try:
            self._partitions_revoked_count += 1
            self.log.dev('ON PARTITIONS REVOKED')
            await self.topics.on_partitions_revoked(revoked)
            await self.tables.on_partitions_revoked(revoked)
            await self._fetcher.stop()
            assignment = self.consumer.assignment()
            if assignment:
                self.flow_control.suspend()
                await self.consumer.pause_partitions(assignment)
                await self.consumer.wait_empty()
            else:
                self.log.dev('ON P. REVOKED NOT COMMITTING: ASSIGNMENT EMPTY')
            if self._partitions_revoked_count > 1:
                await self.agents.restart()
            await self.on_partitions_revoked.send(revoked)
        except Exception as exc:
            await self.crash(exc)

    def _new_producer(self, beacon: NodeT = None) -> ProducerT:
        return self.transport.create_producer(beacon=beacon or self.beacon)

    def _new_consumer(self) -> ConsumerT:
        return self.transport.create_consumer(
            callback=self.topics.on_message,
            on_partitions_revoked=self._on_partitions_revoked,
            on_partitions_assigned=self._on_partitions_assigned,
            beacon=self.beacon,
        )

    def _new_transport(self) -> TransportT:
        return transport.by_url(self.conf.broker)(
            self.conf.broker, self, loop=self.loop)

    def FlowControlQueue(
            self,
            maxsize: int = None,
            *,
            clear_on_resume: bool = False,
            loop: asyncio.AbstractEventLoop = None) -> ThrowableQueue:
        """Like :class:`asyncio.Queue`, but can be suspended/resumed."""
        return ThrowableQueue(
            maxsize=maxsize,
            flow_control=self.flow_control,
            clear_on_resume=clear_on_resume,
            loop=loop or self.loop,
        )

    def Worker(self, **kwargs: Any) -> WorkerT:
        return self.conf.Worker(self, **kwargs)

    def _create_directories(self) -> None:
        self.conf.datadir.mkdir(exist_ok=True)
        self.conf.tabledir.mkdir(exist_ok=True)

    def __repr__(self) -> str:
        return APP_REPR.format(
            name=type(self).__name__,
            s=self,
            c=self.conf,
            agents=self.agents,
            topics=len(self.topics),
        )

    def _configure(self, *, silent: bool = False) -> None:
        self.on_before_configured.send()
        conf = self._load_settings(silent=silent)
        self.on_configured.send(conf)
        self._conf, self.configured = conf, True
        self.on_after_configured.send()

    def _load_settings(self, *, silent: bool = False) -> Settings:
        changes: Mapping[str, Any] = {}
        appid, defaults = self._default_options
        if self._config_source:
            changes = self._load_settings_from_source(
                self._config_source, silent=silent)
        conf = {**defaults, **changes}
        return Settings(appid, **self._prepare_compat_settings(conf))

    def _prepare_compat_settings(self, options: MutableMapping) -> Mapping:
        COMPAT_OPTIONS = {
            'client_id': 'broker_client_id',
            'commit_interval': 'broker_commit_interval',
            'create_reply_topic': 'reply_create_topic',
            'num_standby_replicas': 'table_standby_replicas',
            'default_partitions': 'topic_partitions',
            'replication_factor': 'topic_replication_factor',
        }
        for old, new in COMPAT_OPTIONS.items():
            val = options.get(new)
            try:
                options[new] = options[old]
            except KeyError:
                pass
            else:
                if val is not None:
                    raise ImproperlyConfigured(
                        f'Cannot use both compat option {old!r} and {new!r}')
                warnings.warn(FutureWarning(
                    W_OPTION_DEPRECATED.format(old=old, new=new)))
        return options

    def _load_settings_from_source(self, source: Any,
                                   *,
                                   silent: bool = False) -> Mapping:
        if isinstance(source, str):
            try:
                source = self._smart_import(source, imp=import_from_cwd)
            except (AttributeError, ImportError):
                if not silent:
                    raise
                return {}
        return force_mapping(source)

    def _smart_import(self, path: str, imp: Any = None) -> Any:
        imp = importlib.import_module if imp is None else imp
        if ':' in path:
            # Path includes attribute so can just jump
            # here (e.g., ``os.path:abspath``).
            return symbol_by_name(path, imp=imp)

        # Not sure if path is just a module name or if it includes an
        # attribute name (e.g., ``os.path``, vs, ``os.path.abspath``).
        try:
            return imp(path)
        except ImportError:
            # Not a module name, so try module + attribute.
            return symbol_by_name(path, imp=imp)

    @property
    def conf(self) -> Settings:
        if not self.finalized:
            raise ImproperlyConfigured(
                'App configuration accessed before app.finalize()')
        if self._conf is None:
            self._configure()
        return self._conf

    @conf.setter
    def conf(self, settings: Settings) -> None:
        self._conf = settings

    @property
    def producer(self) -> ProducerT:
        if self._producer is None:
            self._producer = self._new_producer()
        return self._producer

    @producer.setter
    def producer(self, producer: ProducerT) -> None:
        self._producer = producer

    @property
    def consumer(self) -> ConsumerT:
        if self._consumer is None:
            self._consumer = self._new_consumer()
        return self._consumer

    @consumer.setter
    def consumer(self, consumer: ConsumerT) -> None:
        self._consumer = consumer

    @property
    def transport(self) -> TransportT:
        """Message transport."""
        if self._transport is None:
            self._transport = self._new_transport()
        return self._transport

    @transport.setter
    def transport(self, transport: TransportT) -> None:
        self._transport = transport

    @cached_property
    def _service(self) -> ServiceT:
        # We subclass from ServiceProxy and this delegates any ServiceT
        # feature to this service (e.g. ``app.start()`` calls
        # ``app._service.start()``.  See comment in ServiceProxy.
        return AppService(self)

    @cached_property
    def tables(self) -> TableManagerT:
        """Map of available tables, and the table manager service."""
        TableManager = (self.conf.TableManager if self.finalized
                        else symbol_by_name('faust.tables:TableManager'))
        return TableManager(
            app=self,
            loop=self.loop,
            beacon=self.beacon,
        )

    @cached_property
    def topics(self) -> ConductorT:
        """Topic manager.

        This is the mediator that moves messages fetched by the Consumer
        into the correct Topic instances.

        It's also a set of registered topics by string topic name, so you
        can check if a topic is being consumed from by doing
        ``topic in app.topics``.
        """
        return self.conf.TopicConductor(
            app=self, loop=self.loop, beacon=self.beacon)

    @property
    def monitor(self) -> Monitor:
        if self._monitor is None:
            self._monitor = cast(Monitor, self.app.conf.Monitor(
                loop=self.loop, beacon=self.beacon))
        return self._monitor

    @monitor.setter
    def monitor(self, monitor: Monitor) -> None:
        self._monitor = monitor

    @cached_property
    def _fetcher(self) -> ServiceT:
        return self.transport.Fetcher(self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def _reply_consumer(self) -> ReplyConsumer:
        return ReplyConsumer(self, loop=self.loop, beacon=self.beacon)

    @property
    def label(self) -> str:
        return f'{self.shortlabel}: {self.conf.id}@{self.conf.broker}'

    @property
    def shortlabel(self) -> str:
        return type(self).__name__

    @cached_property
    def flow_control(self) -> FlowControlEvent:
        return FlowControlEvent(loop=self.loop)

    @property
    def http_client(self) -> HttpClientT:
        if self._http_client is None:
            self._http_client = self.conf.HttpClient()
        return self._http_client

    @cached_property
    def assignor(self) -> PartitionAssignorT:
        return self.conf.PartitionAssignor(
            self, replicas=self.conf.table_standby_replicas)

    @cached_property
    def _leader_assignor(self) -> LeaderAssignorT:
        return self.conf.LeaderAssignor(
            self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def router(self) -> RouterT:
        return self.conf.Router(self)

    @cached_property
    def serializers(self) -> RegistryT:
        self.finalize()  # easiest way to autofinalize for topic.send
        return self.conf.Serializers(
            key_serializer=self.conf.key_serializer,
            value_serializer=self.conf.value_serializer,
        )

"""Faust Application.

Everything starts here.
An app is an instance of the Faust library.

For very special needs it can be inherited from, and a subclass
has the ability to change how almost everything works.

"""
import asyncio
import importlib
import inspect
import re
import typing

from collections import defaultdict
from datetime import timedelta
from functools import wraps
from heapq import heappop, heappush
from itertools import chain
from pathlib import Path
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Iterable, Iterator, List, Mapping, MutableMapping, MutableSequence,
    NamedTuple, Optional, Pattern, Type, Union, cast,
)
from uuid import uuid4

from mode import Seconds, Service, ServiceT, want_seconds
from mode.proxy import ServiceProxy
from mode.utils.types.trees import NodeT
import venusian
from yarl import URL

from . import __version__ as faust_version
from . import transport
from .agents import Agent, AgentFun, AgentT, ReplyConsumer, SinkT
from .assignor import LeaderAssignor, PartitionAssignor
from .channels import Channel, ChannelT
from .cli._env import DATADIR
from .exceptions import ImproperlyConfigured
from .router import Router
from .sensors import Monitor, SensorDelegate
from .streams import current_event
from .topics import ConductorT, Topic, TopicConductor

from .types import (
    CodecArg, FutureMessage, K, Message, MessageSentCallback,
    ModelArg, RecordMetadata, TP, TopicT, V,
)
from .types.app import AppT, AutodiscoverArg, PageArg, TaskArg, ViewGetHandler
from .types.assignor import LeaderAssignorT
from .types.serializers import RegistryT
from .types.streams import StreamT
from .types.tables import SetT, TableManagerT, TableT
from .types.transports import ConsumerT, ProducerT, TPorTopicSet, TransportT
from .types.windows import WindowT
from .utils.aiter import aiter
from .utils.compat import OrderedDict
from .utils.futures import FlowControlEvent, FlowControlQueue, stampede
from .utils.imports import SymbolArg, symbol_by_name
from .utils.objects import Unordered, cached_property
from .web.views import Site, View

if typing.TYPE_CHECKING:
    from .cli.base import AppCommand
    from .channels import Event
    from .worker import Worker as WorkerT
else:
    class AppCommand: ...  # noqa
    class Event: ...       # noqa
    class WorkerT: ...     # noqa


__all__ = ['App']

#: Default transport URL.
TRANSPORT_URL = 'kafka://localhost:9092'

#: Default table state directory path (unless absolute, relative to datadir).
TABLEDIR = 'tables'  # {appid}-data/tables/

#: Default path to stream class used by ``app.stream``.
STREAM_TYPE = 'faust.Stream'

#: Default path to table manager class used by ``app.tables``.
TABLE_MANAGER_TYPE = 'faust.tables.TableManager'

#: Default path to table class used by ``app.Table``.
TABLE_TYPE = 'faust.Table'

#: Default path to set class used by ``app.Set``.
SET_TYPE = 'faust.Set'

#: Default path to serializer registry class used by ``app.serializers``.
REGISTRY_TYPE = 'faust.serializers.Registry'

#: Default path to Worker class used by ``app.Worker``.
WORKER_TYPE = 'faust.worker.Worker'

#: Default Kafka Client ID.
CLIENT_ID = f'faust-{faust_version}'

#: Default value for the :attr:`App.commit_interval` keyword argument
#: that decides how often we commit acknowledged messages.
COMMIT_INTERVAL = 3.0

#: Default value for the :attr:`App.table_cleanup_interval` keyword argument
#: that decides how often we clean up expired items in windowed tables.
TABLE_CLEANUP_INTERVAL = 30.0

#: Prefix used for reply topics.
REPLY_TOPIC_PREFIX = 'f-reply-'

#: Default expiry time for replies in seconds (float/timedelta).
REPLY_EXPIRES = timedelta(days=1)

#: Max number of messages channels/streams/topics can "prefetch".
STREAM_BUFFER_MAXSIZE = 1000

#: Format string for ``repr(app)``.
APP_REPR = """
<{name}({s.id}): {s.url} {s.state} agents({agents}) topics({topics})>
""".strip()

#: Default decorator categories for :pypi`venusian` to scan for when
#: autodiscovering.
SCAN_CATEGORIES: Iterable[str] = [
    'faust.agent',
    'faust.command',
    'faust.page',
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


class _AttachedHeapEntry(NamedTuple):
    offset: int
    message: Unordered[FutureMessage]


class AppService(Service):
    """Service responsible for starting/stopping an application."""

    # The App() is created during module import and cannot subclass Service
    # directly as Service.__init__ creates the asyncio event loop, and
    # creating the event loop as a side effect of importing a module
    # is a bad practice (e.g. should you switch to uvloop you may end up
    # in a situation where some services use the old event loop).

    # To solve this we use ServiceProxy to split into App + AppService,
    # in a way so that the AppService is started lazily only when first
    # needed.

    def __init__(self, app: 'App', **kwargs: Any) -> None:
        self.app: App = app
        super().__init__(loop=self.app.loop, **kwargs)

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # Client-Only: Boots up enough services to be able to
        # produce to topics and receive replies from topics.
        # XXX If we switch to socket RPC using routers we can remove this.
        if self.app.client_only:
            return self._components_client()
        return self._components_server()

    def _components_client(self) -> Iterable[ServiceT]:
        # The components started when running in Client-Only mode.
        return cast(Iterable[ServiceT], chain(
            [self.app.producer],
            [self.app.consumer],
            [self.app._reply_consumer],
            [self.app.topics],
            [self.app._fetcher],
        ))

    def _components_server(self) -> Iterable[ServiceT]:
        # The components started when running normally (Server mode).

        # Add the main Monitor sensor.
        # beacon reattached after initialized in case a custom monitor added
        self.app.monitor.beacon.reattach(self.beacon)
        self.app.monitor.loop = self.loop
        self.app.sensors.add(self.app.monitor)

        # Then return the list of "subservices",
        # those that'll be started when the app starts,
        # stopped when the app stops,
        # etc...
        return cast(Iterable[ServiceT], chain(
            # Sensors (Sensor): always start first, stop last.
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

    async def on_started(self) -> None:
        await self.wait(self.app.tables.recovery_completed.wait())
        # Add all asyncio.Tasks, like timers, etc.
        for task in self.app._tasks:
            # pass app if decorated function takes argument
            target: Any
            if inspect.signature(task).parameters:
                target = cast(Callable[[AppT], Awaitable], task)(self.app)
            else:
                target = cast(Callable[[], Awaitable], task)()
            self.add_future(target)

        # Call the app-is-fully-started callback used by Worker
        # to print the "ready" message when Faust is ready to
        # start processing.
        if self.app.on_startup_finished:
            await self.app.on_startup_finished()

    @property
    def label(self) -> str:
        return self.app.label

    @property
    def shortlabel(self) -> str:
        return self.app.shortlabel


class App(AppT, ServiceProxy):
    """Faust Application.

    Arguments:
        id (str): Application ID.

    Keyword Arguments:
        url (str):
            Transport URL.  Default: ``"aiokafka://localhost:9092"``.
        client_id (str):  Client id used for producer/consumer.
        commit_interval (Seconds): How often we commit messages that
            have been fully processed.  Default ``30.0``.
        key_serializer (CodecArg): Default serializer for Topics
            that do not have an explicit serializer set.
            Default: :const:`None`.
        value_serializer (CodecArg): Default serializer for event types
            that do not have an explicit serializer set.  Default: ``"json"``.
        num_standby_replicas (int): The number of standby replicas for each
            table.  Default: ``1``.
        replication_factor (int): The replication factor for changelog topics
            and repartition topics created by the application.  Default:
            ``1``. Generally, this would be the same as the configured
            replication factor for your kafka cluster.
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.
    """

    #: Set if app should only start the services required to operate
    #: as an RPC client (producer and reply consumer).
    client_only = False

    # Default producer instance.
    _producer: ProducerT = None

    # Default consumer instance.
    _consumer: ConsumerT = None

    # Set when consumer is started.
    _consumer_started: bool = False

    # Transport is created on demand: use `.transport` property.
    _transport: Optional[TransportT] = None

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

    def __init__(
            self, id: str,
            *,
            version: int = 1,
            url: Union[str, URL] = TRANSPORT_URL,
            store: Union[str, URL] = 'memory://',
            autodiscover: AutodiscoverArg = False,
            origin: str = None,
            avro_registry_url: Union[str, URL] = None,
            canonical_url: Union[str, URL] = None,
            client_id: str = CLIENT_ID,
            datadir: Union[Path, str] = DATADIR,
            tabledir: Union[Path, str] = TABLEDIR,
            commit_interval: Seconds = COMMIT_INTERVAL,
            table_cleanup_interval: Seconds = TABLE_CLEANUP_INTERVAL,
            key_serializer: CodecArg = 'json',
            value_serializer: CodecArg = 'json',
            num_standby_replicas: int = 1,
            replication_factor: int = 1,
            default_partitions: int = 8,
            reply_to: str = None,
            create_reply_topic: bool = False,
            reply_expires: Seconds = REPLY_EXPIRES,
            Stream: SymbolArg[Type[StreamT]] = STREAM_TYPE,
            Table: SymbolArg[Type[TableT]] = TABLE_TYPE,
            TableManager: SymbolArg[Type[TableManagerT]] = TABLE_MANAGER_TYPE,
            Set: SymbolArg[Type[SetT]] = SET_TYPE,
            Serializers: SymbolArg[Type[RegistryT]] = REGISTRY_TYPE,
            Worker: SymbolArg[Type[WorkerT]] = WORKER_TYPE,
            monitor: Monitor = None,
            on_startup_finished: Callable = None,
            stream_buffer_maxsize: int = STREAM_BUFFER_MAXSIZE,
            loop: asyncio.AbstractEventLoop = None) -> None:
        self.loop = loop
        self.id = f'{id}-{version}'
        self.url = URL(url)
        self.client_id = client_id
        self.canonical_url = URL(canonical_url or '')
        # datadir is a format string that can contain {appid}
        self.datadir = Path(str(datadir).format(appid=self.id)).expanduser()
        self.tabledir = self._datadir_path(Path(tabledir)).expanduser()
        self.commit_interval = want_seconds(commit_interval)
        self.table_cleanup_interval = want_seconds(table_cleanup_interval)
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.num_standby_replicas = num_standby_replicas
        self.replication_factor = replication_factor
        self.default_partitions = default_partitions
        self.reply_to = reply_to or REPLY_TOPIC_PREFIX + str(uuid4())
        self.create_reply_topic = create_reply_topic
        self.reply_expires = want_seconds(reply_expires or REPLY_EXPIRES)
        self.avro_registry_url = avro_registry_url
        self.Stream = symbol_by_name(Stream)
        self.TableType = symbol_by_name(Table)
        self.SetType = symbol_by_name(Set)
        self.TableManager = symbol_by_name(TableManager)
        self.Serializers = symbol_by_name(Serializers)
        self.serializers = self.Serializers(
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
        )
        self._worker_type = Worker
        self.assignor = PartitionAssignor(self,
                                          replicas=self.num_standby_replicas)
        self.router = Router(self)
        self.table_route = self.router.router
        self.agents = OrderedDict()
        self.sensors = SensorDelegate(self)
        self.store = URL(store)
        self._monitor = monitor
        self._tasks = []
        self._pending_on_commit = defaultdict(list)
        self.on_startup_finished: Callable = on_startup_finished
        self.origin = origin
        self.autodiscover = autodiscover
        self.pages = []
        self.stream_buffer_maxsize = stream_buffer_maxsize
        ServiceProxy.__init__(self)

    def discover(self,
                 *extra_modules: str,
                 categories: Iterable[str] = SCAN_CATEGORIES,
                 ignore: Iterable[str] = SCAN_IGNORE) -> None:
        modules = set(self._discovery_modules()) | set(extra_modules)
        if modules:
            scanner = self._new_scanner(*[re.compile(pat) for pat in ignore])
            for name in modules:
                try:
                    module = importlib.import_module(name)
                except ModuleNotFoundError:
                    raise ModuleNotFoundError(
                        f'Unknown module {name} in App.autodiscover list')
                scanner.scan(
                    module,
                    categories=tuple(categories),
                )

    def _discovery_modules(self) -> List[str]:
        modules: List[str] = []
        if self.autodiscover:
            if isinstance(self.autodiscover, bool):
                if self.origin is None:
                    raise ImproperlyConfigured(E_NEED_ORIGIN)
            elif callable(self.autodiscover):
                modules.extend(
                    cast(Callable[[], Iterator[str]], self.autodiscover)())
            else:
                modules.extend(self.autodiscover)
            modules.append(self.origin)
        return modules

    def _new_scanner(
            self, *ignore: Pattern, **kwargs: Any) -> venusian.Scanner:
        return venusian.Scanner(ignore=[pat.search for pat in ignore])

    def _datadir_path(self, path: Path) -> Path:
        return path if path.is_absolute() else self.datadir / path

    def main(self) -> None:
        """Execute the :program:`faust` umbrella command using this app."""
        from .cli.faust import cli
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
              sink: Iterable[SinkT] = None) -> Callable[[AgentFun], AgentT]:
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
            agent = Agent(
                fun,
                name=name,
                app=self,
                channel=channel,
                concurrency=concurrency,
                sink=sink,
                on_error=self._on_agent_error,
                help=fun.__doc__,
            )

            self.agents[agent.name] = agent

            def on_discovered(scanner: venusian.Scanner,
                              name: str,
                              obj: AgentT) -> None:
                ...
            venusian.attach(agent, on_discovered, category='faust.agent')
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

        If the target function is unary the ``app`` argument is passed::

            >>> @app.task
            >>> async def on_startup(app):
            ...    print('STARTING UP: %r' % (app,))

        Nullary functions are also supported::

            >>> @app.task
            >>> async def on_startup():
            ...     print('STARTING UP')
        """
        self._tasks.append(fun)
        return fun

    def timer(self, interval: Seconds,
              on_leader: bool = False) -> Callable:
        """Define an async def function to be run at periodic intervals.

        Like :meth:`task`, but executes periodically until the worker
        is shut down.

        This decorator takes an async function and adds it to a
        list of timers started with the app.

        Arguments:
            interval (Seconds): How often the timer executes in seconds.

        Keyword Arguments:
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

    def is_leader(self) -> bool:
        return self._leader_assignor.is_leader()

    def stream(self, channel: Union[AsyncIterable, Iterable],
               beacon: NodeT = None,
               **kwargs: Any) -> StreamT:
        """Create new stream from channel/topic/iterable/async iterable.

        Arguments:
            channel: Iterable to stream over (async or non-async).

        Keyword Arguments:
            kwargs: See :class:`Stream`.

        Returns:
            faust.Stream:
                to iterate over events in the stream.
        """
        return self.Stream(
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

        Keyword Arguments:
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
        table = self.tables.add(self.TableType(
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
        return self.tables.add(self.SetType(
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
                        'get': staticmethod(handler),
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

            def on_discovered(scanner: venusian.Scanner,
                              name: str,
                              obj: Site) -> None:
                ...
            venusian.attach(site, on_discovered, category='faust.page')
            return site
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
            venusian.attach(cmd, on_discovered, category='faust.command')
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

    async def on_partitions_assigned(self, assigned: Iterable[TP]) -> None:
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
            await self.topics.on_partitions_assigned(assigned)
            await self.tables.on_partitions_assigned(assigned)
        except Exception as exc:
            await self.crash(exc)

    async def on_partitions_revoked(self, revoked: Iterable[TP]) -> None:
        """Handle revocation of topic partitions.

        This is called during a rebalance and is followed by
        :meth:`on_partitions_assigned`.

        Revoked means the partitions no longer exist, or they
        have been reassigned to a different node.
        """
        try:
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
        except Exception as exc:
            await self.crash(exc)

    def _new_producer(self, beacon: NodeT = None) -> ProducerT:
        return self.transport.create_producer(
            beacon=beacon or self.beacon,
        )

    def _new_consumer(self) -> ConsumerT:
        return self.transport.create_consumer(
            callback=self.topics.on_message,
            on_partitions_revoked=self.on_partitions_revoked,
            on_partitions_assigned=self.on_partitions_assigned,
            beacon=self.beacon,
        )

    def _new_transport(self) -> TransportT:
        return transport.by_url(self.url)(self.url, self, loop=self.loop)

    def FlowControlQueue(
            self,
            maxsize: int = None,
            *,
            clear_on_resume: bool = False,
            loop: asyncio.AbstractEventLoop = None) -> asyncio.Queue:
        """Like :class:`asyncio.Queue`, but can be suspended/resumed."""
        return FlowControlQueue(
            maxsize=maxsize,
            flow_control=self.flow_control,
            clear_on_resume=clear_on_resume,
            loop=loop or self.loop,
        )

    def Worker(self, **kwargs: Any) -> WorkerT:
        return symbol_by_name(self._worker_type)(self, **kwargs)

    def _create_directories(self) -> None:
        self.datadir.mkdir(exist_ok=True)
        self.tabledir.mkdir(exist_ok=True)

    def __repr__(self) -> str:
        return APP_REPR.format(
            name=type(self).__name__,
            s=self,
            agents=self.agents,
            topics=len(self.topics),
        )

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
        return self.TableManager(
            app=self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def topics(self) -> ConductorT:
        """Topic manager.

        This is the mediator that moves messages fetched by the Consumer
        into the correct Topic instances.

        It's also a set of registered topics, so you can check
        if a topic is being consumed from by doing ``topic in app.topics``.
        """
        return TopicConductor(app=self, loop=self.loop, beacon=self.beacon)

    @property
    def monitor(self) -> Monitor:
        if self._monitor is None:
            self._monitor = Monitor()
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

    @cached_property
    def _leader_assignor(self) -> LeaderAssignorT:
        return LeaderAssignor(self, loop=self.loop, beacon=self.beacon)

    @property
    def label(self) -> str:
        return f'{self.shortlabel}: {self.id}@{self.url}'

    @property
    def shortlabel(self) -> str:
        return type(self).__name__

    @cached_property
    def flow_control(self) -> FlowControlEvent:
        return FlowControlEvent(loop=self.loop)


_flake8_RecordMetadata_is_used: RecordMetadata  # XXX flake8 bug

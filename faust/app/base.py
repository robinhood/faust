"""Faust Application.

An app is an instance of the Faust library.
Everything starts here.

"""
import asyncio
import importlib
import re
import typing
import warnings
from functools import wraps
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Pattern,
    Set as _Set,
    Type,
    Union,
    cast,
)

import venusian
from mode import Seconds, ServiceT, SupervisorStrategyT, want_seconds
from mode.proxy import ServiceProxy
from mode.services import ServiceCallbacks
from mode.utils.aiter import aiter
from mode.utils.collections import force_mapping
from mode.utils.futures import FlowControlEvent, ThrowableQueue, stampede
from mode.utils.imports import import_from_cwd, smart_import, symbol_by_name
from mode.utils.types.trees import NodeT

from faust import transport
from faust.agents import (
    AgentFun,
    AgentManager,
    AgentT,
    ReplyConsumer,
    SinkT,
)
from faust.channels import Channel, ChannelT
from faust.exceptions import ImproperlyConfigured, SameNode
from faust.fixups import FixupT, fixups
from faust.sensors import Monitor, SensorDelegate
from faust.utils.objects import cached_property
from faust.web.views import Request, Response, Site, View

from faust.types.app import AppT, TaskArg
from faust.types.assignor import LeaderAssignorT, PartitionAssignorT
from faust.types.codecs import CodecArg
from faust.types.core import K, V
from faust.types.models import ModelArg
from faust.types.router import RouterT
from faust.types.serializers import RegistryT
from faust.types.settings import Settings
from faust.types.streams import StreamT
from faust.types.tables import CollectionT, SetT, TableManagerT, TableT
from faust.types.topics import TopicT
from faust.types.transports import (
    ConductorT,
    ConsumerT,
    ProducerT,
    TPorTopicSet,
    TransportT,
)
from faust.types.tuples import MessageSentCallback, RecordMetadata, TP
from faust.types.web import (
    HttpClientT,
    PageArg,
    RoutedViewGetHandler,
    ViewGetHandler,
)
from faust.types.windows import WindowT

from ._attached import Attachments
from .service import AppService

if typing.TYPE_CHECKING:
    from faust.cli.base import AppCommand
    from faust.events import Event
    from faust.worker import Worker as WorkerT
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


class App(AppT, ServiceProxy, ServiceCallbacks):
    """Faust Application.

    Arguments:
        id (str): Application ID.

    Keyword Arguments:
        loop (asyncio.AbstractEventLoop):
            Provide specific asyncio event loop instance.

    See Also:
        :ref:`application-configuration` -- for supported keyword arguments.

    """

    #: Set this to True if app should only start the services required to
    #: operate as an RPC client (producer and reply consumer).
    client_only = False

    _attachments: Attachments = None

    #: Source of configuration: ``app.conf`` (when configured)
    _conf: Settings = None

    #: Original configuration source object.
    _config_source: Any = None

    # Default consumer instance.
    _consumer: ConsumerT = None

    # Transport is created on demand: use `.transport` property.
    _transport: Optional[TransportT] = None

    _assignor: PartitionAssignorT = None

    # Monitor is created on demand: use `.monitor` property.
    _monitor: Monitor = None

    # @app.task decorator adds asyncio tasks to be started
    # with the app here.
    _tasks: MutableSequence[TaskArg]

    _http_client: HttpClientT = None

    _extra_services: List[ServiceT] = None

    def __init__(self,
                 id: str,
                 *,
                 monitor: Monitor = None,
                 config_source: Any = None,
                 **options: Any) -> None:
        self._default_options = (id, options)
        self.agents = AgentManager()
        self.sensors = SensorDelegate(self)
        self._attachments = Attachments(self)
        self._monitor = monitor
        self._tasks = []
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

    def config_from_object(self,
                           obj: Any,
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
            await self._http_client.close()

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

    def _new_scanner(self, *ignore: Pattern,
                     **kwargs: Any) -> venusian.Scanner:
        return venusian.Scanner(ignore=[pat.search for pat in ignore])

    def main(self) -> None:
        """Execute the :program:`faust` umbrella command using this app."""
        from faust.cli.faust import cli
        self.finalize()
        self.worker_init()
        if self.conf.autodiscover:
            self.discover()
        cli(app=self)

    def topic(self,
              *topics: str,
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
              config: Mapping[str, Any] = None,
              maxsize: int = None,
              loop: asyncio.AbstractEventLoop = None) -> TopicT:
        """Create topic description.

        Topics are named channels (for example a Kafka topic),
        to communicate locally create a :meth:`channel`.

        See Also:
            :class:`faust.topics.Topic`
        """
        Topic = (self.conf.Topic
                 if self.finalized else symbol_by_name('faust:Topic'))
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
            loop=loop,
        )

    def channel(self,
                *,
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
            Agent = (self.conf.Agent
                     if self.finalized else symbol_by_name('faust:Agent'))
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

    async def _on_agent_error(self, agent: AgentT, exc: BaseException) -> None:
        # See agent-errors in docs/userguide/agents.rst
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

    def _on_task_discovered(self, scanner: venusian.Scanner, name: str,
                            obj: TaskArg) -> None:
        ...

    def timer(self, interval: Seconds, on_leader: bool = False) -> Callable:
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

    def _on_service_discovered(self, scanner: venusian.Scanner, name: str,
                               obj: Type[ServiceT]) -> None:
        ...

    def is_leader(self) -> bool:
        return self._leader_assignor.is_leader()

    def stream(self,
               channel: Union[AsyncIterable, Iterable],
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

    def Table(self,
              name: str,
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
        Table = (self.conf.Table
                 if self.finalized else symbol_by_name('faust:Table'))
        table = self.tables.add(
            Table(
                self,
                name=name,
                default=default,
                beacon=self.beacon,
                partitions=partitions,
                help=help,
                **kwargs))
        return table.using_window(window) if window else table

    def Set(self,
            name: str,
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
        Set = self.conf.Set if self.finalized else symbol_by_name('faust:Set')
        return self.tables.add(
            Set(self,
                name=name,
                beacon=self.beacon,
                partitions=partitions,
                window=window,
                help=help,
                **kwargs))

    def page(self, path: str, *,
             base: Type[View] = View) -> Callable[[PageArg], Type[Site]]:
        def _decorator(fun: PageArg) -> Type[Site]:
            site = Site.from_handler(path, base=base)(fun)
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
                    return await self.router.route_req(table.name, key,
                                                       view.web, request)
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
        _base: Type[AppCommand] = base
        if _base is None:
            from faust.cli import base as cli_base
            _base = cli_base.AppCommand

        def _inner(fun: Callable[..., Awaitable[Any]]) -> Type[AppCommand]:
            cmd = _base.from_handler(*options, **kwargs)(fun)
            venusian.attach(cmd, cmd.on_discovered, category=SCAN_COMMAND)
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
        chan: ChannelT
        if isinstance(channel, str):
            chan = self.topic(channel)
        else:
            chan = channel
        return await chan.send(
            key,
            value,
            partition,
            key_serializer,
            value_serializer,
            callback,
        )

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

    async def _on_partitions_revoked(self, revoked: _Set[TP]) -> None:
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
                # Allow big buffers: clear the queues so we can
                # wait for currently processing event in each stream
                # to finish (note: this means _on_partitions_revoked
                # cannot execute in a thread).
                self.flow_control.clear()
                # wait for currently processing event in each stream.
                await self.consumer.wait_empty()
            else:
                self.log.dev('ON P. REVOKED NOT COMMITTING: ASSIGNMENT EMPTY')
            await self.on_partitions_revoked.send(revoked)
        except Exception as exc:
            await self.crash(exc)

    async def _on_partitions_assigned(self, assigned: _Set[TP]) -> None:
        """Handle new topic partition assignment.

        This is called during a rebalance after :meth:`on_partitions_revoked`.

        The new assignment provided overrides the previous
        assignment, so any tp no longer in the assigned' list will have
        been revoked.
        """
        try:
            await self.consumer.verify_subscription(assigned)
            # Wait for transport.Conductor to finish any new subscriptions
            await self.topics.wait_for_subscriptions()
            await self.consumer.pause_partitions(assigned)
            await self._fetcher.restart()
            self.log.info(f'Restarted fetcher')
            await self.topics.on_partitions_assigned(assigned)
            await self.tables.on_partitions_assigned(assigned)
            await self.on_partitions_assigned.send(assigned)
            self.flow_control.resume()
        except Exception as exc:
            await self.crash(exc)

    def _new_producer(self) -> ProducerT:
        return self.transport.create_producer(beacon=self.beacon)

    def _new_consumer(self) -> ConsumerT:
        return self.transport.create_consumer(
            callback=self.topics.on_message,
            on_partitions_revoked=self._on_partitions_revoked,
            on_partitions_assigned=self._on_partitions_assigned,
            beacon=self.beacon,
        )

    def _new_conductor(self) -> ConductorT:
        return self.transport.create_conductor(beacon=self.beacon)

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
        self.conf.appdir.mkdir(exist_ok=True)
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
                warnings.warn(
                    FutureWarning(
                        W_OPTION_DEPRECATED.format(old=old, new=new)))
        return options

    def _load_settings_from_source(self, source: Any, *,
                                   silent: bool = False) -> Mapping:
        if isinstance(source, str):
            try:
                source = smart_import(source, imp=import_from_cwd)
            except (AttributeError, ImportError):
                if not silent:
                    raise
                return {}
        return force_mapping(source)

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

    @cached_property
    def producer(self) -> ProducerT:
        return self._new_producer()

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
        TableManager = (self.conf.TableManager if self.finalized else
                        symbol_by_name('faust.tables:TableManager'))
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
        return self._new_conductor()

    @property
    def monitor(self) -> Monitor:
        if self._monitor is None:
            self._monitor = cast(Monitor,
                                 self.conf.Monitor(
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

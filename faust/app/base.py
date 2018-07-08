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
    Callable, Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Pattern,
    Set,
    Type,
    Union,
    cast,
)

from mode import Seconds, ServiceT, SupervisorStrategyT, want_seconds
from mode.proxy import ServiceProxy
from mode.services import ServiceCallbacks
from mode.utils.aiter import aiter
from mode.utils.collections import force_mapping
from mode.utils.futures import stampede
from mode.utils.imports import import_from_cwd, smart_import, symbol_by_name
from mode.utils.logging import flight_recorder
from mode.utils.objects import cached_property
from mode.utils.queues import FlowControlEvent, ThrowableQueue
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
from faust.utils import venusian
from faust.web.views import Request, Response, Site, View, Web

from faust.types.app import AppT, TaskArg
from faust.types.assignor import LeaderAssignorT, PartitionAssignorT
from faust.types.codecs import CodecArg
from faust.types.core import K, V
from faust.types.models import ModelArg
from faust.types.router import RouterT
from faust.types.serializers import RegistryT
from faust.types.settings import Settings
from faust.types.streams import StreamT
from faust.types.tables import CollectionT, TableManagerT, TableT
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

if typing.TYPE_CHECKING:  # pragma: no cover
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

# Venusian (pypi): This is used for "autodiscovery" of user code,
# CLI commands, and much more.
# Named after same concept from Django: the Django Admin autodiscover function
# that finds custom admin configuration in ``app.admin.py`` modules.

SCAN_AGENT = 'faust.agent'
SCAN_COMMAND = 'faust.command'
SCAN_PAGE = 'faust.page'
SCAN_SERVICE = 'faust.service'
SCAN_TASK = 'faust.task'

#: Default decorator categories for :pypi`venusian` to scan for when
#: autodiscovering things like @app.agent decorators.
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
SCAN_IGNORE: Iterable[str] = ['test_.*', '.*__main__.*']

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
        loop (asyncio.AbstractEventLoop): optional event loop to use.

    See Also:
        :ref:`application-configuration` -- for supported keyword arguments.

    """

    #: Set this to True if app should only start the services required to
    #: operate as an RPC client (producer and simple reply consumer).
    client_only = False

    #: Source of configuration: ``app.conf`` (when configured)
    _conf: Optional[Settings] = None

    #: Original configuration source object.
    _config_source: Any = None

    # Default consumer instance.
    _consumer: Optional[ConsumerT] = None

    # Transport is created on demand: use `.transport` property.
    _transport: Optional[TransportT] = None

    _assignor: Optional[PartitionAssignorT] = None

    # Monitor is created on demand: use `.monitor` property.
    _monitor: Optional[Monitor] = None

    # @app.task decorator adds asyncio tasks to be started
    # with the app here.
    _tasks: MutableSequence[TaskArg]

    _http_client: Optional[HttpClientT] = None

    _extra_services: List[ServiceT]

    # See faust/app/_attached.py
    _attachments: Attachments

    _on_revoked_timeout = None

    def __init__(self,
                 id: str,
                 *,
                 monitor: Monitor = None,
                 config_source: Any = None,
                 loop: asyncio.AbstractEventLoop = None,
                 **options: Any) -> None:
        # This is passed to the configuration in self.conf
        self._default_options = (id, options)

        # The agent manager manages all agents.
        self.agents = AgentManager(self)

        # Sensors monitor Faust using a standard sensor API.
        self.sensors = SensorDelegate(self)

        # this is a local hack we use until we have proper
        # transactions support in the Python Kafka Client
        # and can have "exactly once" semantics.
        self._attachments = Attachments(self)

        # "The Monitor" is a special sensor that provides detailed stats
        # for the web server.
        self._monitor = monitor

        # Any additional asyncio.Task's specified using @app.task decorator.
        self._tasks = []

        # Called as soon as the a worker is fully operational.
        self.on_startup_finished: Optional[Callable] = None

        # Any additional web server views added using @app.page decorator.
        self.pages = []

        # Any additional services added using the @app.service decorator.
        self._extra_services = []

        # The configuration source object/module passed to ``config_by_object``
        # for introspectio purposes.
        self._config_source = config_source

        # create default sender for signals such as self.on_configured
        self._init_signals()

        # initialize fixups (automatically applied extensions,
        # such as Django integration).
        self.fixups = self._init_fixups()

        self.loop = loop

        # init the service proxy required to ensure of lazy loading
        # of the service class (see faust/app/service.py).
        ServiceProxy.__init__(self)

    def _init_signals(self) -> None:
        # Signals in Faust are the same as in Django, but asynchronous by
        # default (:class:`mode.SyncSignal` is the normal ``def`` version)).
        #
        # Signals in Faust are usually local to the app instance::
        #
        #  @app.on_before_configured.connect  # <-- only sent by this app
        #  def on_before_configured(self):
        #    ...
        #
        # In Django signals are usually global, and an easter-egg
        # provides this in Faust also::
        #
        #    V---- NOTE upper case A in App
        #   @App.on_before_configured.connect  # <-- sent by ALL apps
        #   def on_before_configured(app):
        #
        # Note: Signals are local-only, and cannot do anything to other
        # processes or machines.
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
        # Returns list of "fixups"
        # Fixups are small additional patches we apply when certain
        # platforms or frameworks are present.
        #
        # One example is the Django fixup, responsible for Django integration
        # whenever the DJANGO_SETTINGS_MODULE environment variable is
        # set. See faust/fixups/django.py, it's not complicated - using
        # setuptools entrypoints you can very easily create extensions that
        # are automatically enabled by installing a PyPI package with
        # `pip install myname`.
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
        # Finalization signals that the application have been configured
        # and is ready to use.

        # If you access configuration before an explicit call to
        # ``app.finalize()`` you will get an error.
        # The ``app.main`` entrypoint and the ``faust -A app`` command
        # both will automatically finalize the app for you.
        if not self.finalized:
            self.finalized = True
            id = self.conf.id
            if not id:
                raise ImproperlyConfigured('App requires an id!')

    async def on_stop(self) -> None:
        await self._maybe_close_http_client()

    async def _maybe_close_http_client(self) -> None:
        if self._http_client:
            await self._http_client.close()

    def worker_init(self) -> None:
        # This init is called by the `faust worker` command.
        for fixup in self.fixups:
            fixup.on_worker_init()
        self.on_worker_init.send()

    def discover(self,
                 *extra_modules: str,
                 categories: Iterable[str] = SCAN_CATEGORIES,
                 ignore: Iterable[str] = SCAN_IGNORE) -> None:
        # based on autodiscovery in Django,
        # but finds @app.agent decorators, and so on.
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
            if self.conf.origin:
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
        that exist on a server.  To make an ephemeral local communication
        channel use: :meth:`channel`.

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
              isolated_partitions: bool = False,
              **kwargs: Any) -> Callable[[AgentFun], AgentT]:
        """Create Agent from async def function.

        It can be a regular async function::

            @app.agent()
            async def my_agent(stream):
                async for number in stream:
                    print(f'Received: {number!r}')

        Or it can be an async iterator that yields values.
        These values can be used as the reply in an RPC-style call,
        or for sinks: callbacks that forward events to
        other agents/topics/statsd, and so on::

            @app.agent(sink=[log_topic])
            async def my_agent(requests):
                async for number in requests:
                    yield number * 2

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
                isolated_partitions=isolated_partitions,
                on_error=self._on_agent_error,
                help=fun.__doc__,
                **kwargs)
            self.agents[agent.name] = agent
            venusian.attach(agent, category=SCAN_AGENT)
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

        The function may take zero, or one argument.
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
        venusian.attach(fun, category=SCAN_TASK)
        self._tasks.append(fun)
        return fun

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
        venusian.attach(cls, category=SCAN_SERVICE)
        self._extra_services.append(cls)
        return cls

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

    def page(self, path: str, *,
             base: Type[View] = View) -> Callable[[PageArg], Type[Site]]:
        def _decorator(fun: PageArg) -> Type[Site]:
            site = Site.from_handler(path, base=base)(fun)
            self.pages.append(('', site))
            venusian.attach(site, category=SCAN_PAGE)
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
                base: Optional[Type[AppCommand]] = None,
                **kwargs: Any) -> Callable[[Callable], Type[AppCommand]]:
        if options is None and base is None and kwargs is None:
            raise TypeError('Use parens in @app.command(), not @app.command.')
        _base: Type[AppCommand]
        if base is None:
            from faust.cli import base as cli_base
            _base = cli_base.AppCommand
        else:
            _base = base

        def _inner(fun: Callable[..., Awaitable[Any]]) -> Type[AppCommand]:
            cmd = _base.from_handler(*options, **kwargs)(fun)
            venusian.attach(cmd, category=SCAN_COMMAND)
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
            channel: Channel/topic or the name of a topic to send event to.
            key: Message key.
            value: Message value.
            partition: Specific partition to send to.
                If not set the partition will be chosen by the partitioner.
            key_serializer: Serializer to use (if value is not model).
            value_serializer: Serializer to use (if value is not model).
            callback: Called after the message is fully delivered to the
                channel, but not to the consumer.
                Signature must be unary as the
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
        """Ensure producer is started."""
        producer = self.producer
        # producer may also have been started by app.start()
        await producer.maybe_start()
        return producer

    async def commit(self, topics: TPorTopicSet) -> bool:
        """Commit offset for acked messages in specified topics'.

        Warning:
            This will commit acked messages in **all topics**
            if the topics argument is passed in as :const:`None`.
        """
        return await self.topics.commit(topics)

    async def _on_partitions_revoked(self, revoked: Set[TP]) -> None:
        """Handle revocation of topic partitions.

        This is called during a rebalance and is followed by
        :meth:`on_partitions_assigned`.

        Revoked means the partitions no longer exist, or they
        have been reassigned to a different node.
        """
        if self.should_stop:
            return self._on_rebalance_when_stopped()
        session_timeout = self.conf.broker_session_timeout
        with flight_recorder(self.log, timeout=session_timeout) as on_timeout:
            self._on_revoked_timeout = on_timeout
            try:
                self.log.dev('ON PARTITIONS REVOKED')
                on_timeout.info('fetcher.stop()')
                await self._stop_fetcher()
                on_timeout.info('tables.stop_standbys()')
                await self.tables._stop_standbys()
                assignment = self.consumer.assignment()
                if assignment:
                    on_timeout.info('flow_control.suspend()')
                    self.flow_control.suspend()
                    on_timeout.info('consumer.pause_partitions')
                    await self.consumer.pause_partitions(assignment)
                    # Every agent instance has an incoming buffer of messages
                    # (a asyncio.Queue) -- we clear those to make sure
                    # agents will not start processing them.
                    #
                    # This allows for large buffer sizes
                    # (stream_buffer_maxsize).
                    on_timeout.info('flow_control.clear()')
                    self.flow_control.clear()

                    # even if we clear, some of the agent instances may have
                    # already started working on an event.
                    #
                    # we need to wait for them.
                    if self.conf.stream_wait_empty:
                        on_timeout.info('consumer.wait_empty()')
                        await self.consumer.wait_empty()
                    on_timeout.info('agents.on_partitions_revoked')
                    await self.agents.on_partitions_revoked(revoked)
                else:
                    self.log.dev('ON P. REVOKED NOT COMMITTING: NO ASSIGNMENT')
                on_timeout.info('topics.on_partitions_revoked()')
                await self.topics.on_partitions_revoked(revoked)
                on_timeout.info('tables.on_partitions_revoked()')
                await self.tables.on_partitions_revoked(revoked)
                on_timeout.info('+send signal: on_partitions_revoked')
                await self.on_partitions_revoked.send(revoked)
                on_timeout.info('-send signal: on_partitions_revoked')
            except Exception as exc:
                on_timeout.info('on partitions assigned crashed: %r', exc)
                await self.crash(exc)
            finally:
                self._on_revoked_timeout = None

    async def _stop_fetcher(self) -> None:
        await self._fetcher.stop()
        # Reset fetcher service state so that we can restart it
        # in TableManager table recovery.
        self._fetcher.service_reset()

    def _on_rebalance_when_stopped(self) -> None:
        self.consumer.close()

    async def _on_partitions_assigned(self, assigned: Set[TP]) -> None:
        """Handle new topic partition assignment.

        This is called during a rebalance after :meth:`on_partitions_revoked`.

        The new assignment overrides the previous
        assignment, so any tp no longer in the assigned' list will have
        been revoked.
        """
        if self.should_stop:
            return self._on_rebalance_when_stopped()
        session_timeout = self.conf.broker_session_timeout
        self.unassigned = not assigned
        with flight_recorder(self.log, timeout=session_timeout) as on_timeout:
            try:
                on_timeout.info('fetcher.stop()')
                await self._stop_fetcher()
                on_timeout.info('agents.on_partitions_assigned()')
                await self.agents.on_partitions_assigned(assigned)
                # Wait for transport.Conductor to finish
                # calling Consumer.subscribe
                on_timeout.info('topics.wait_for_subscriptions()')
                await self.topics.wait_for_subscriptions()
                on_timeout.info('consumer.pause_partitions()')
                await self.consumer.pause_partitions(assigned)
                on_timeout.info('topics.on_partitions_assigned()')
                await self.topics.on_partitions_assigned(assigned)
                on_timeout.info('tables.on_partitions_assigned()')
                await self.tables.on_partitions_assigned(assigned)
                on_timeout.info('+send signal: on_partitions_assigned')
                await self.on_partitions_assigned.send(assigned)
                on_timeout.info('-send signal: on_partitions_assigned')
                on_timeout.info('flow_control.resume()')
                self.flow_control.resume()
            except Exception as exc:
                on_timeout.info('on partitions assigned crashed: %r', exc)
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

    def on_webserver_init(self, web: Web) -> None:
        ...

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

    @cached_property
    def _service(self) -> ServiceT:
        # We subclass from ServiceProxy and this delegates any ServiceT
        # feature to this service (e.g. ``app.start()`` calls
        # ``app._service.start()``.  See comment in ServiceProxy.
        return AppService(self)

    @property
    def conf(self) -> Settings:
        if not self.finalized:
            raise ImproperlyConfigured(
                'App configuration accessed before app.finalize()')
        if self._conf is None:
            self._configure()
        return cast(Settings, self._conf)

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
        """Topic Conductor.

        This is the mediator that moves messages fetched by the Consumer
        into the streams.

        It's also a set of registered topics by string topic name, so you
        can check if a topic is being consumed from by doing
        ``topic in app.topics``.
        """
        return self._new_conductor()

    @property
    def monitor(self) -> Monitor:
        """Monitor keeps stats about what's going on inside the worker."""
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
        """Fetcher helps Kafka Consumer retrieve records in topics."""
        return self.transport.Fetcher(self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def _reply_consumer(self) -> ReplyConsumer:
        """Kafka Consumer that consumes agent replies."""
        return ReplyConsumer(self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def flow_control(self) -> FlowControlEvent:
        """Internal flow control.

        This object controls flow into stream queues,
        and can also clear all buffers.
        """
        return FlowControlEvent(loop=self.loop)

    @property
    def http_client(self) -> HttpClientT:
        """HTTP Client Session."""
        if self._http_client is None:
            self._http_client = self.conf.HttpClient()
        return self._http_client

    @cached_property
    def assignor(self) -> PartitionAssignorT:
        """Partition Assignor.

        Responsible for partition assignment.
        """
        return self.conf.PartitionAssignor(
            self, replicas=self.conf.table_standby_replicas)

    @cached_property
    def _leader_assignor(self) -> LeaderAssignorT:
        """The leader assignor is a special Kafka partition assignor.

        It's used to find the leader in a cluster of Faust worker nodes,
        and enables the ``@app.timer(on_leader=True)`` feature that executes
        exclusively on one node at a time. Excellent for things that would
        traditionally require a lock/mutex."""
        return self.conf.LeaderAssignor(
            self, loop=self.loop, beacon=self.beacon)

    @cached_property
    def router(self) -> RouterT:
        """Find the node partitioned data belongs to.

        The router helps us route web requests to the wanted Faust node.
        If a topic is sharded by account_id, the router can send us to the
        Faust worker responsible for any account.  Used by the
        ``@app.table_route`` decorator.
        """
        return self.conf.Router(self)

    @cached_property
    def serializers(self) -> RegistryT:
        # Serializer registry.
        # Many things such as key_serializer/value_serializer configures
        # the serializer by name (e.g. "json"). The serializer registry
        # lets you extend Faust with support for additional
        # serialization formats.
        self.finalize()  # easiest way to autofinalize for topic.send
        return self.conf.Serializers(
            key_serializer=self.conf.key_serializer,
            value_serializer=self.conf.value_serializer,
        )

    @property
    def label(self) -> str:
        return f'{self.shortlabel}: {self.conf.id}@{self.conf.broker}'

    @property
    def shortlabel(self) -> str:
        return type(self).__name__

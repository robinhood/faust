"""Faust Application.

An app is an instance of the Faust library.
Everything starts here.

"""
import asyncio
import importlib
import inspect
import typing
import warnings
from datetime import tzinfo
from functools import wraps
from itertools import chain
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    ContextManager,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Pattern,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import opentracing
from mode import Seconds, Service, ServiceT, SupervisorStrategyT, want_seconds
from mode.utils.aiter import aiter
from mode.utils.collections import force_mapping
from mode.utils.contexts import nullcontext
from mode.utils.futures import stampede
from mode.utils.imports import import_from_cwd, smart_import
from mode.utils.logging import flight_recorder
from mode.utils.objects import cached_property, shortlabel
from mode.utils.typing import NoReturn
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
from faust.exceptions import ConsumerNotStarted, ImproperlyConfigured, SameNode
from faust.fixups import FixupT, fixups
from faust.sensors import Monitor, SensorDelegate
from faust.utils import cron, venusian
from faust.utils.tracing import (
    call_with_trace,
    noop_span,
    operation_name_from_fun,
    set_current_span,
    traced_from_parent_span,
)
from faust.web import drivers as web_drivers
from faust.web.cache import backends as cache_backends
from faust.web.views import View

from faust.types._env import STRICT
from faust.types.app import AppT, BootStrategyT, TaskArg
from faust.types.assignor import LeaderAssignorT, PartitionAssignorT
from faust.types.codecs import CodecArg
from faust.types.core import HeadersArg, K, V
from faust.types.enums import ProcessingGuarantee
from faust.types.models import ModelArg
from faust.types.router import RouterT
from faust.types.serializers import RegistryT
from faust.types.settings import Settings as _Settings
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
    CacheBackendT,
    HttpClientT,
    PageArg,
    Request,
    ResourceOptions,
    Response,
    ViewDecorator,
    ViewHandlerFun,
    Web,
)
from faust.types.windows import WindowT

from ._attached import Attachments

if typing.TYPE_CHECKING:  # pragma: no cover
    from faust.cli.base import AppCommand as _AppCommand
    from faust.worker import Worker as _Worker
else:
    class _AppCommand: ...  # noqa
    class _Worker: ...     # noqa

__all__ = ['App', 'BootStrategy']

#: Format string for ``repr(app)``.
APP_REPR_FINALIZED = '''
<{name}({c.id}): {c.broker} {s.state} agents({agents}) {id:#x}>
'''.strip()

APP_REPR_UNFINALIZED = '''
<{name}: <non-finalized> {id:#x}>
'''.strip()

# Venusian (pypi): This is used for "autodiscovery" of user code,
# CLI commands, and much more.
# Named after same concept from Django: the Django Admin autodiscover function
# that finds custom admin configuration in ``{app}/admin.py`` modules.

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

E_NEED_ORIGIN = '''
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
'''

W_OPTION_DEPRECATED = '''\
Argument {old!r} is deprecated and scheduled for removal in Faust 1.0.

Please use {new!r} instead.
'''

W_DEPRECATED_SHARD_PARAM = '''\
The second argument to `@table_route` is deprecated,
please use the `query_param` keyword argument instead.
'''

# @app.task decorator may be called in several ways:
#
# 1) Without parens:
#    @app.task
#    async def foo():
#
# 2) With parens:
#   @app.task()
#
# 3) With parens and arguments
#   @app.task(on_leader=True)
#
# This means @app.task attempts to do the right thing depending
# on how it's used. All the frameworks do this, but we have to also type it.
TaskDecoratorRet = Union[
    Callable[[TaskArg], TaskArg],
    TaskArg,
]


class BootStrategy(BootStrategyT):
    """App startup strategy.

    The startup strategy defines the graph of services
    to start when the Faust worker for an app starts.
    """

    enable_kafka: bool = True
    # We want these to take default from `enable_kafka`
    # attribute, but still want to allow subclasses to define
    # them like this:
    #   class MyBoot(BootStrategy):
    #       enable_kafka_consumer = False
    enable_kafka_consumer: Optional[bool] = None
    enable_kafka_producer: Optional[bool] = None

    enable_web: Optional[bool] = None
    enable_sensors: bool = True

    def __init__(self, app: AppT, *,
                 enable_web: bool = None,
                 enable_kafka: bool = None,
                 enable_kafka_producer: bool = None,
                 enable_kafka_consumer: bool = None,
                 enable_sensors: bool = None) -> None:
        self.app = app

        if enable_kafka is not None:
            self.enable_kafka = enable_kafka
        if enable_kafka_producer is not None:
            self.enable_kafka_producer = enable_kafka_producer
        if enable_kafka_consumer is not None:
            self.enable_kafka_consumer = enable_kafka_consumer
        if enable_web is not None:
            self.enable_web = enable_web
        if enable_sensors is not None:
            self.enable_sensors = enable_sensors

    def server(self) -> Iterable[ServiceT]:
        return self._chain(
            # Sensors (Sensor): always start first and stop last.
            self.sensors(),
            # Producer: always stop after Consumer.
            self.kafka_producer(),
            # Web
            self.web_server(),
            # Consumer: always stop after Conductor
            self.kafka_consumer(),
            # AgentManager starts agents (app.agents)
            self.agents(),
            # Conductor (transport.Conductor))
            self.kafka_conductor(),
            # Table Manager (app.TableManager)
            self.tables(),
        )

    def client_only(self) -> Iterable[ServiceT]:
        return self._chain(
            self.kafka_producer(),
            self.kafka_client_consumer(),
            self.kafka_conductor(),
            [self.app._fetcher],
        )

    def producer_only(self) -> Iterable[ServiceT]:
        return self._chain(
            self.web_server(),
            self.kafka_producer(),
        )

    def _chain(self, *arguments: Iterable[ServiceT]) -> Iterable[ServiceT]:
        return cast(Iterable[ServiceT], chain(*arguments))

    def sensors(self) -> Iterable[ServiceT]:
        if self.enable_sensors:
            return self.app.sensors
        return []

    def kafka_producer(self) -> Iterable[ServiceT]:
        if self._should_enable_kafka_producer():
            return [self.app.producer]
        return []

    def _should_enable_kafka_producer(self) -> bool:
        if self.enable_kafka_producer is None:
            return self.enable_kafka
        return self.enable_kafka_producer

    def kafka_consumer(self) -> Iterable[ServiceT]:
        if self._should_enable_kafka_consumer():
            return [
                self.app.consumer,
                # Leader Assignor (assignor.LeaderAssignor)
                self.app._leader_assignor,
                # Reply Consumer (ReplyConsumer)
                self.app._reply_consumer,
            ]
        return []

    def _should_enable_kafka_consumer(self) -> bool:
        if self.enable_kafka_consumer is None:
            return self.enable_kafka
        return self.enable_kafka_consumer

    def kafka_client_consumer(self) -> Iterable[ServiceT]:
        return [
            self.app.consumer,
            self.app._reply_consumer,
        ]

    def agents(self) -> Iterable[ServiceT]:
        return [self.app.agents]

    def kafka_conductor(self) -> Iterable[ServiceT]:
        if self._should_enable_kafka_consumer():
            return [self.app.topics]
        return []

    def web_server(self) -> Iterable[ServiceT]:
        if self._should_enable_web():
            return list(self.web_components()) + [self.app.web]
        return []

    def _should_enable_web(self) -> bool:
        if self.enable_web is None:
            return self.app.conf.web_enabled
        return self.enable_web

    def web_components(self) -> Iterable[ServiceT]:
        return [self.app.cache]

    def tables(self) -> Iterable[ServiceT]:
        return [self.app.tables]


class App(AppT, Service):
    """Faust Application.

    Arguments:
        id (str): Application ID.

    Keyword Arguments:
        loop (asyncio.AbstractEventLoop): optional event loop to use.

    See Also:
        :ref:`application-configuration` -- for supported keyword arguments.

    """

    BootStrategy = BootStrategy
    Settings = _Settings

    #: Set this to True if app should only start the services required to
    #: operate as an RPC client (producer and simple reply consumer).
    client_only = False

    #: Set this to True if app should run without consumer/tables.
    producer_only = False

    #: Source of configuration: ``app.conf`` (when configured)
    _conf: Optional[_Settings] = None

    #: Original configuration source object.
    _config_source: Any = None

    # Default consumer instance.
    _consumer: Optional[ConsumerT] = None

    # Default producer instance.
    _producer: Optional[ProducerT] = None

    # Transport is created on demand: use `.transport` property.
    _transport: Optional[TransportT] = None

    # Cache is created on demand: use `.cache` property.
    _cache: Optional[CacheBackendT] = None

    # Monitor is created on demand: use `.monitor` property.
    _monitor: Optional[Monitor] = None

    # @app.task decorator adds asyncio tasks to be started
    # with the app here.
    _tasks: MutableSequence[Callable[[], Awaitable]]

    _http_client: Optional[HttpClientT] = None

    _extra_services: List[ServiceT]
    _extra_service_instances: Optional[List[ServiceT]] = None

    # See faust/app/_attached.py
    _attachments: Attachments

    _on_revoked_timeout = None

    #: Current assignment
    _assignment: Optional[Set[TP]] = None

    #: Optional tracing support.
    tracer: Optional[Any] = None
    _rebalancing_span: Optional[opentracing.Span] = None

    def __init__(self,
                 id: str,
                 *,
                 monitor: Monitor = None,
                 config_source: Any = None,
                 loop: asyncio.AbstractEventLoop = None,
                 beacon: NodeT = None,
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

        self.boot_strategy = self.BootStrategy(self)

        Service.__init__(self, loop=loop, beacon=beacon)

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
        self.on_rebalance_complete = (
            self.on_rebalance_complete.with_default_sender(self))
        self.on_before_shutdown = (
            self.on_before_shutdown.with_default_sender(self))

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

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # Add the main Monitor sensor.
        # The beacon is also reattached in case the monitor
        # was created by the user.
        self.monitor.beacon.reattach(self.beacon)
        self.monitor.loop = self.loop
        self.sensors.add(self.monitor)

        if self.producer_only:
            return self.boot_strategy.producer_only()
        elif self.client_only:
            return self.boot_strategy.client_only()
        else:
            return self.boot_strategy.server()

    async def on_first_start(self) -> None:
        self._create_directories()

    async def on_start(self) -> None:
        self.finalize()

    async def on_started(self) -> None:
        # Wait for table recovery to complete (returns True if app stopped)
        if not await self._wait_for_table_recovery_completed():
            # Add all asyncio.Tasks, like timers, etc.
            await self.on_started_init_extra_tasks()

            # Start user-provided services.
            await self.on_started_init_extra_services()

            # Call the app-is-fully-started callback used by Worker
            # to print the "ready" message that signals to the user that
            # the worker is ready to start processing.
            if self.on_startup_finished:
                await self.on_startup_finished()

    async def _wait_for_table_recovery_completed(self) -> bool:
        if not self.producer_only and not self.client_only:
            return await self.wait_for_stopped(self.tables.recovery.completed)
        return False

    async def on_started_init_extra_tasks(self) -> None:
        for task in self._tasks:
            self.add_future(task())

    async def on_started_init_extra_services(self) -> None:
        if self._extra_service_instances is None:
            # instantiate the services added using the @app.service decorator.
            self._extra_service_instances = [
                await self.on_init_extra_service(service)
                for service in self._extra_services
            ]

    async def on_init_extra_service(
            self, service: Union[ServiceT, Type[ServiceT]]) -> ServiceT:
        s: ServiceT = self._prepare_subservice(service)
        # start the service now, or when the app is started.
        await self.add_runtime_dependency(s)
        return s

    def _prepare_subservice(
            self, service: Union[ServiceT, Type[ServiceT]]) -> ServiceT:
        if inspect.isclass(service):
            return service(loop=self.loop, beacon=self.beacon)
        return service

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
        if self.finalized or self.configured:
            self.Settings._warn_already_configured()
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
            scanner = venusian.Scanner()
            for name in modules:
                try:
                    module = importlib.import_module(name)
                except ModuleNotFoundError:
                    raise ModuleNotFoundError(
                        f'Unknown module {name} in App.conf.autodiscover list')
                scanner.scan(
                    module,
                    ignore=ignore,
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

    def main(self) -> NoReturn:
        """Execute the :program:`faust` umbrella command using this app."""
        from faust.cli.faust import cli
        self.finalize()
        self.worker_init()
        cli(app=self)
        raise SystemExit(3451)  # for mypy: NoReturn

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
              allow_empty: bool = False,
              loop: asyncio.AbstractEventLoop = None) -> TopicT:
        """Create topic description.

        Topics are named channels (for example a Kafka topic),
        that exist on a server.  To make an ephemeral local communication
        channel use: :meth:`channel`.

        See Also:
            :class:`faust.topics.Topic`
        """
        return self.conf.Topic(
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
            allow_empty=allow_empty,
            loop=loop,
        )

    def channel(self,
                *,
                key_type: ModelArg = None,
                value_type: ModelArg = None,
                maxsize: int = None,
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
              use_reply_headers: bool = False,
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
            agent = self.conf.Agent(
                fun,
                name=name,
                app=self,
                channel=channel,
                concurrency=concurrency,
                supervisor_strategy=supervisor_strategy,
                sink=sink,
                isolated_partitions=isolated_partitions,
                on_error=self._on_agent_error,
                use_reply_headers=use_reply_headers,
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

    def task(self,
             fun: TaskArg = None,
             *,
             on_leader: bool = False,
             traced: bool = True) -> TaskDecoratorRet:
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
        def _inner(fun: TaskArg) -> TaskArg:
            return self._task(fun, on_leader=on_leader, traced=traced)
        return _inner(fun) if fun is not None else _inner

    def _task(self, fun: TaskArg,
              on_leader: bool = False,
              traced: bool = False,
              ) -> TaskArg:
        app = self

        @wraps(fun)
        async def _wrapped() -> None:
            should_run = app.is_leader() if on_leader else True
            if should_run:
                with self.trace(shortlabel(fun), trace_enabled=traced):
                    # pass app only if decorated function takes an argument
                    if inspect.signature(fun).parameters:
                        task_takes_app = cast(Callable[[AppT], Awaitable], fun)
                        return await task_takes_app(app)
                    else:
                        task = cast(Callable[[], Awaitable], fun)
                        return await task()

        venusian.attach(_wrapped, category=SCAN_TASK)
        self._tasks.append(_wrapped)
        return _wrapped

    def timer(self, interval: Seconds,
              on_leader: bool = False,
              traced: bool = True) -> Callable:
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
            @wraps(fun)
            async def around_timer(*args: Any) -> None:
                while not self.should_stop:
                    await self.sleep(interval_s)
                    if not self.should_stop:
                        should_run = not on_leader or self.is_leader()
                        if should_run:
                            with self.trace(shortlabel(fun),
                                            trace_enabled=traced):
                                await fun(*args)  # type: ignore

            # If you call @app.task without parents the return value is:
            #    Callable[[TaskArg], TaskArg]
            # but we always call @app.task() - with parens, so return value is
            # always TaskArg.
            return cast(TaskArg, self.task(around_timer, traced=False))

        return _inner

    def crontab(self, cron_format: str, *,
                timezone: tzinfo = None,
                on_leader: bool = False,
                traced: bool = True) -> Callable:
        """Define periodic task using crontab description.

        This is an ``async def`` function to be run at the fixed times,
        defined by the cron format.

        Like :meth:`timer`, but executes at fixed times instead of executing
        at certain intervals.

        This decorator takes an async function and adds it to a
        list of cronjobs started with the app.

        Arguments:
            cron_format: The cron spec defining fixed times to run the
                decorated function.

        Keyword Arguments:
            timezone: The timezone to be taken into account for the cron jobs.
                If not set value from :setting:`timezone` will be taken.

            on_leader: Should the cron job only run on the leader?

        Example:
            >>> @app.crontab(cron_format='30 18 * * *',
                             timezone=pytz.timezone('US/Pacific'))
            >>> async def every_6_30_pm_pacific():
            ...     print('IT IS 6:30pm')


            >>> app.crontab(cron_format='30 18 * * *', on_leader=True)
            >>> async def every_6_30_pm():
            ...     print('6:30pm UTC; ALSO, I AM THE LEADER!')
        """
        def _inner(fun: TaskArg) -> TaskArg:
            @wraps(fun)
            async def cron_starter(*args: Any) -> None:
                _tz = self.conf.timezone if timezone is None else timezone
                while not self.should_stop:
                    await self.sleep(cron.secs_for_next(cron_format, _tz))
                    if not self.should_stop:
                        should_run = not on_leader or self.is_leader()
                        if should_run:
                            with self.trace(shortlabel(fun),
                                            trace_enabled=traced):
                                await fun(*args)  # type: ignore

            return cast(TaskArg, self.task(cron_starter, traced=False))

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
        table = self.tables.add(
            self.conf.Table(
                self,
                name=name,
                default=default,
                beacon=self.beacon,
                partitions=partitions,
                help=help,
                **kwargs))
        return table.using_window(window) if window else table

    def SetTable(self,
                 name: str,
                 *,
                 window: WindowT = None,
                 partitions: int = None,
                 help: str = None,
                 **kwargs: Any) -> TableT:
        table = self.tables.add(
            self.conf.SetTable(
                self,
                name=name,
                beacon=self.beacon,
                partitions=partitions,
                help=help,
                **kwargs))
        return table.using_window(window) if window else table

    def page(self, path: str, *,
             base: Type[View] = View,
             cors_options: Mapping[str, ResourceOptions] = None,
             name: str = None) -> Callable[[PageArg], Type[View]]:
        view_base: Type[View] = base if base is not None else View

        def _decorator(fun: PageArg) -> Type[View]:
            view: Optional[Type[View]] = None
            if inspect.isclass(fun):
                view = cast(Type[View], fun)
                if not issubclass(view, View):
                    raise TypeError(
                        'When decorating class, it must be subclass of View')
            if view is None:
                view = view_base.from_handler(cast(ViewHandlerFun, fun))
            view.view_name = name or view.__name__
            view.view_path = path
            self.web.add_view(view, cors_options=cors_options)
            venusian.attach(view, category=SCAN_PAGE)
            return view

        return _decorator

    def table_route(self, table: CollectionT,
                    shard_param: str = None,
                    *,
                    query_param: str = None,
                    match_info: str = None) -> ViewDecorator:
        def _decorator(fun: ViewHandlerFun) -> ViewHandlerFun:
            _query_param = query_param
            if shard_param is not None:
                warnings.warn(DeprecationWarning(W_DEPRECATED_SHARD_PARAM))
                if query_param:
                    raise TypeError(
                        'Cannot specify shard_param and query_param')
                _query_param = shard_param
            if _query_param is None and match_info is None:
                raise TypeError('Need one of query_param or shard_param')

            @wraps(fun)
            async def get(view: View, request: Request,
                          *args: Any, **kwargs: Any) -> Response:
                if match_info:
                    key = request.match_info[match_info]
                elif _query_param:
                    key = request.query[_query_param]
                else:  # pragma: no cover
                    raise Exception('cannot get here')
                try:
                    return await self.router.route_req(table.name, key,
                                                       view.web, request)
                except SameNode:
                    return await fun(  # type: ignore
                        view, request, *args, **kwargs)

            return get

        return _decorator

    def command(self,
                *options: Any,
                base: Optional[Type[_AppCommand]] = None,
                **kwargs: Any) -> Callable[[Callable], Type[_AppCommand]]:
        _base: Type[_AppCommand]
        if base is None:
            from faust.cli import base as cli_base
            _base = cli_base.AppCommand
        else:
            _base = base

        def _inner(fun: Callable[..., Awaitable[Any]]) -> Type[_AppCommand]:
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
        await self.maybe_start()

    async def maybe_start_client(self) -> None:
        """Start the app in Client-Only mode if not started as Server."""
        if not self.started:
            await self.start_client()

    def trace(self,
              name: str,
              trace_enabled: bool = True,
              **extra_context: Any) -> ContextManager:
        if self.tracer is None or not trace_enabled:
            return nullcontext()
        else:
            return self.tracer.trace(
                name=name,
                **extra_context)

    def traced(self, fun: Callable,
               name: str = None,
               sample_rate: float = 1.0,
               **context: Any) -> Callable:
        assert fun
        operation: str = name or operation_name_from_fun(fun)

        @wraps(fun)
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            span = self.trace(operation,
                              sample_rate=sample_rate,
                              **context)
            return call_with_trace(span, fun, None, *args, **kwargs)
        return wrapped

    def _start_span_from_rebalancing(self, name: str) -> opentracing.Span:
        rebalancing_span = self._rebalancing_span
        if rebalancing_span is not None and self.tracer is not None:
            span = self.tracer.get_tracer('_faust').start_span(
                operation_name=name,
                child_of=rebalancing_span,
            )
            set_current_span(span)
            return span
        else:
            return noop_span()

    async def send(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            timestamp: float = None,
            headers: HeadersArg = None,
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
            timestamp: Epoch seconds (from Jan 1 1970
                UTC) to use as the message timestamp. Defaults to current time.
            headers: Mapping of key/value pairs, or iterable of key value
                pairs to use as headers for the message.
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
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    @cached_property
    def in_transaction(self) -> bool:
        return (
            self.in_worker and
            self.conf.processing_guarantee == ProcessingGuarantee.EXACTLY_ONCE
        )

    @stampede
    async def maybe_start_producer(self) -> ProducerT:
        """Ensure producer is started."""
        if self.in_transaction:
            # return TransactionManager when
            # processing_guarantee="exactly_once" enabled.
            return self.consumer.transactions
        else:
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

    async def on_stop(self) -> None:
        await self._stop_consumer()
        # send shutdown signal
        await self.on_before_shutdown.send()
        await self._producer_flush(self.log)
        await self._maybe_close_http_client()

    async def _producer_flush(self, logger: Any) -> None:
        if self._producer is not None:
            logger.info('Flush producer buffer...')
            await self._producer.flush()

    async def _stop_consumer(self) -> None:
        if self._consumer is not None:
            consumer = self._consumer
            try:
                assignment = consumer.assignment()
            except ConsumerNotStarted:
                pass
            else:
                if assignment:
                    self.tables.on_partitions_revoked(assignment)
                    consumer.stop_flow()
                    self.flow_control.suspend()
                    consumer.pause_partitions(assignment)
                    self.flow_control.clear()
                    await self._stop_fetcher()
                    await self._consumer_wait_empty(consumer, self.log)

    async def _consumer_wait_empty(
            self, consumer: ConsumerT, logger: Any) -> None:
        if self.conf.stream_wait_empty:
            logger.info('Wait for streams...')
            await consumer.wait_empty()

    def on_rebalance_start(self) -> None:
        self.rebalancing = True
        self.rebalancing_count += 1
        if self.tracer:
            tracer = self.tracer.get_tracer('_faust')
            self._rebalancing_span = tracer.start_span(
                operation_name='rebalance',
                tags={'rebalancing_count': self.rebalancing_count},
            )
        self.tables.on_rebalance_start()

    def on_rebalance_end(self) -> None:
        self.rebalancing = False
        if self._rebalancing_span:
            self._rebalancing_span.finish()
        self._rebalancing_span = None

    async def _on_partitions_revoked(self, revoked: Set[TP]) -> None:
        """Handle revocation of topic partitions.

        This is called during a rebalance and is followed by
        :meth:`on_partitions_assigned`.

        Revoked means the partitions no longer exist, or they
        have been reassigned to a different node.
        """
        if self.should_stop:
            return self._on_rebalance_when_stopped()
        session_timeout = self.conf.broker_session_timeout * 0.95
        T = traced_from_parent_span()
        with flight_recorder(self.log, timeout=session_timeout) as on_timeout:
            self._on_revoked_timeout = on_timeout
            consumer = self.consumer
            try:
                self.log.dev('ON PARTITIONS REVOKED')
                T(self.tables.on_partitions_revoked)(revoked)
                assignment = consumer.assignment()
                if assignment:
                    on_timeout.info('flow_control.suspend()')
                    T(consumer.stop_flow)()
                    T(self.flow_control.suspend)()
                    on_timeout.info('consumer.pause_partitions')
                    T(consumer.pause_partitions)(assignment)
                    # Every agent instance has an incoming buffer of messages
                    # (a asyncio.Queue) -- we clear those to make sure
                    # agents will not start processing them.
                    #
                    # This allows for large buffer sizes
                    # (stream_buffer_maxsize).
                    on_timeout.info('flow_control.clear()')
                    T(self.flow_control.clear)()

                    # even if we clear, some of the agent instances may have
                    # already started working on an event.
                    #
                    # we need to wait for them.
                    await T(self._consumer_wait_empty)(consumer, on_timeout)
                    await T(self._producer_flush)(on_timeout)
                    if self.in_transaction:
                        await T(consumer.transactions.on_partitions_revoked)(
                            revoked)
                else:
                    self.log.dev('ON P. REVOKED NOT COMMITTING: NO ASSIGNMENT')
                on_timeout.info('+send signal: on_partitions_revoked')
                await T(self.on_partitions_revoked.send)(revoked)
                on_timeout.info('-send signal: on_partitions_revoked')
            except Exception as exc:
                on_timeout.info('on partitions revoked crashed: %r', exc)
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
        T = traced_from_parent_span()
        # shave some time off so we timeout before the broker
        # (Kafka does not send error, it just logs)
        session_timeout = self.conf.broker_session_timeout * 0.95
        self.unassigned = not assigned

        revoked, newly_assigned = self._update_assignment(assigned)

        with flight_recorder(self.log, timeout=session_timeout) as on_timeout:
            self._on_revoked_timeout = on_timeout
            consumer = self.consumer
            try:
                on_timeout.info('agents.on_rebalance()')
                await T(self.agents.on_rebalance,
                        revoked=revoked,
                        newly_assigned=newly_assigned)(revoked, newly_assigned)
                # Wait for transport.Conductor to finish
                # calling Consumer.subscribe
                on_timeout.info('topics.wait_for_subscriptions()')
                await T(self.topics.wait_for_subscriptions)()
                on_timeout.info('consumer.pause_partitions()')
                T(consumer.pause_partitions)(assigned)
                on_timeout.info('topics.on_partitions_assigned()')
                await T(self.topics.on_partitions_assigned)(assigned)
                on_timeout.info('transactions.on_rebalance()')
                if self.in_transaction:
                    await T(consumer.transactions.on_rebalance)(
                        assigned, revoked, newly_assigned)
                on_timeout.info('tables.on_rebalance()')
                await T(self.tables.on_rebalance)(
                    assigned, revoked, newly_assigned)
                on_timeout.info('+send signal: on_partitions_assigned')
                await T(self.on_partitions_assigned.send)(assigned)
                on_timeout.info('-send signal: on_partitions_assigned')
            except Exception as exc:
                on_timeout.info('on partitions assigned crashed: %r', exc)
                await self.crash(exc)

    def _update_assignment(
            self, assigned: Set[TP]) -> Tuple[Set[TP], Set[TP]]:
        revoked: Set[TP]
        newly_assigned: Set[TP]
        if self._assignment is not None:
            revoked = self._assignment - assigned
            newly_assigned = assigned - self._assignment
        else:
            revoked = set()
            newly_assigned = assigned
        self._assignment = assigned
        return revoked, newly_assigned

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
        return transport.by_url(self.conf.broker[0])(
            self.conf.broker, self, loop=self.loop)

    def _new_cache_backend(self) -> CacheBackendT:
        return cache_backends.by_url(self.conf.cache)(
            self, self.conf.cache, loop=self.loop)

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

    def Worker(self, **kwargs: Any) -> _Worker:
        return self.conf.Worker(self, **kwargs)

    def on_webserver_init(self, web: Web) -> None:
        ...

    def _create_directories(self) -> None:
        self.conf.datadir.mkdir(exist_ok=True)
        self.conf.appdir.mkdir(exist_ok=True)
        self.conf.tabledir.mkdir(exist_ok=True)

    def __repr__(self) -> str:
        if self._conf:
            return APP_REPR_FINALIZED.format(
                name=type(self).__name__,
                s=self,
                c=self.conf,
                agents=self.agents,
                id=id(self),
            )
        else:
            return APP_REPR_UNFINALIZED.format(
                name=type(self).__name__,
                id=id(self),
            )

    def _configure(self, *, silent: bool = False) -> None:
        self.on_before_configured.send()
        conf = self._load_settings(silent=silent)
        self.on_configured.send(conf)
        self._conf, self.configured = conf, True
        self.on_after_configured.send()

    def _load_settings(self, *, silent: bool = False) -> _Settings:
        changes: Mapping[str, Any] = {}
        appid, defaults = self._default_options
        if self._config_source:
            changes = self._load_settings_from_source(
                self._config_source, silent=silent)
        conf = {**defaults, **changes}
        return self.Settings(appid, **self._prepare_compat_settings(conf))

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
    def conf(self) -> _Settings:
        if not self.finalized and STRICT:
            raise ImproperlyConfigured(
                'App configuration accessed before app.finalize()')
        if self._conf is None:
            self._configure()
        return cast(_Settings, self._conf)

    @conf.setter
    def conf(self, settings: _Settings) -> None:
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

    @property
    def cache(self) -> CacheBackendT:
        if self._cache is None:
            self._cache = self._new_cache_backend()
        return self._cache

    @cache.setter
    def cache(self, cache: CacheBackendT) -> None:
        self._cache = cache

    @cached_property
    def tables(self) -> TableManagerT:
        """Map of available tables, and the table manager service."""
        return self.conf.TableManager(
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
        """Flow control of streams.

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

    @http_client.setter
    def http_client(self, client: HttpClientT) -> None:
        self._http_client = client

    @cached_property
    def assignor(self) -> PartitionAssignorT:
        """Partition Assignor.

        Responsible for partition assignment.
        """
        return self.conf.PartitionAssignor(
            self, replicas=self.conf.table_standby_replicas)

    @cached_property
    def _leader_assignor(self) -> LeaderAssignorT:
        """Leader assignor.

        The leader assignor is a special Kafka partition assignor,
        used to find the leader in a cluster of Faust worker nodes,
        and enables the ``@app.timer(on_leader=True)`` feature that executes
        exclusively on one node at a time. Excellent for things that would
        traditionally require a lock/mutex.
        """
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
    def web(self) -> Web:
        return self._new_web()

    def _new_web(self) -> Web:
        return web_drivers.by_url(self.conf.web)(self)

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

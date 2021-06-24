import logging
import os
import socket
import ssl
import typing
from datetime import timedelta, timezone, tzinfo
from pathlib import Path
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    List,
    Mapping,
    Optional,
    Type,
    Union,
)
from uuid import uuid4

from mode import SupervisorStrategyT
from mode.utils.imports import SymbolArg, symbol_by_name
from mode.utils.logging import Severity
from mode.utils.times import Seconds, want_seconds
from yarl import URL

from faust.types._env import DATADIR, WEB_BIND, WEB_PORT, WEB_TRANSPORT
from faust.types.agents import AgentT
from faust.types.assignor import LeaderAssignorT, PartitionAssignorT
from faust.types.auth import CredentialsArg, CredentialsT
from faust.types.codecs import CodecArg
from faust.types.events import EventT
from faust.types.enums import ProcessingGuarantee
from faust.types.router import RouterT
from faust.types.sensors import SensorT
from faust.types.serializers import RegistryT, SchemaT
from faust.types.streams import StreamT
from faust.types.transports import PartitionerT, SchedulingStrategyT
from faust.types.tables import GlobalTableT, TableManagerT, TableT
from faust.types.topics import TopicT
from faust.types.web import HttpClientT, ResourceOptions

from . import base
from . import params
from . import sections
from .params import BrokerArg, URLArg

if typing.TYPE_CHECKING:
    from faust.types.worker import Worker as _WorkerT
else:
    class _WorkerT: ...      # noqa

# XXX mypy borks if we do `from faust import __version__`
faust_version: str = symbol_by_name('faust:__version__')

AutodiscoverArg = Union[
    bool,
    Iterable[str],
    Callable[[], Iterable[str]],
]


class Settings(base.SettingsRegistry):
    NODE_HOSTNAME: ClassVar[str] = socket.gethostname()
    DEFAULT_BROKER_URL: ClassVar[str] = 'kafka://localhost:9092'

    _id: str
    _name: str
    _version: int

    #: Environment.
    #: Defaults to :data:`os.environ`.
    env: Mapping[str, str]

    def __init__(
            self,
            id: str, *,
            # Common settings:
            autodiscover: AutodiscoverArg = None,
            datadir: typing.Union[str, Path] = None,
            tabledir: typing.Union[str, Path] = None,
            debug: bool = None,
            env_prefix: str = None,
            id_format: str = None,
            origin: str = None,
            timezone: typing.Union[str, tzinfo] = None,
            version: int = None,
            # Agent settings:
            agent_supervisor: SymbolArg[Type[SupervisorStrategyT]] = None,
            # Broker settings:
            broker: BrokerArg = None,
            broker_consumer: BrokerArg = None,
            broker_producer: BrokerArg = None,
            broker_api_version: str = None,
            broker_check_crcs: bool = None,
            broker_client_id: str = None,
            broker_commit_every: int = None,
            broker_commit_interval: Seconds = None,
            broker_commit_livelock_soft_timeout: Seconds = None,
            broker_credentials: CredentialsArg = None,
            broker_heartbeat_interval: Seconds = None,
            broker_max_poll_interval: Seconds = None,
            broker_max_poll_records: int = None,
            broker_rebalance_timeout: Seconds = None,
            broker_request_timeout: Seconds = None,
            broker_session_timeout: Seconds = None,
            ssl_context: ssl.SSLContext = None,
            # Consumer settings:
            consumer_api_version: str = None,
            consumer_max_fetch_size: int = None,
            consumer_auto_offset_reset: str = None,
            consumer_group_instance_id: str = None,
            # Topic serialization settings:
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            # Logging settings:
            logging_config: Mapping = None,
            loghandlers: List[logging.Handler] = None,
            # Producer settings:
            producer_acks: int = None,
            producer_api_version: str = None,
            producer_compression_type: str = None,
            producer_linger_ms: int = None,
            producer_max_batch_size: int = None,
            producer_max_request_size: int = None,
            producer_partitioner: SymbolArg[PartitionerT] = None,
            producer_request_timeout: Seconds = None,
            # RPC settings:
            reply_create_topic: bool = None,
            reply_expires: Seconds = None,
            reply_to: str = None,
            reply_to_prefix: str = None,
            # Stream settings:
            processing_guarantee: Union[str, ProcessingGuarantee] = None,
            stream_buffer_maxsize: int = None,
            stream_processing_timeout: Seconds = None,
            stream_publish_on_commit: bool = None,
            stream_recovery_delay: Seconds = None,
            stream_wait_empty: bool = None,
            # Table settings:
            store: URLArg = None,
            table_cleanup_interval: Seconds = None,
            table_key_index_size: int = None,
            table_standby_replicas: int = None,
            # Topic settings:
            topic_allow_declare: bool = None,
            topic_disable_leader: bool = None,
            topic_partitions: int = None,
            topic_replication_factor: int = None,
            # Web server settings:
            cache: URLArg = None,
            canonical_url: URLArg = None,
            web: URLArg = None,
            web_bind: str = None,
            web_cors_options: typing.Mapping[str, ResourceOptions] = None,
            web_enabled: bool = None,
            web_host: str = None,
            web_in_thread: bool = None,
            web_port: int = None,
            web_transport: URLArg = None,
            # Worker settings:
            worker_redirect_stdouts: bool = None,
            worker_redirect_stdouts_level: Severity = None,
            # Extension settings:
            Agent: SymbolArg[Type[AgentT]] = None,
            ConsumerScheduler: SymbolArg[Type[SchedulingStrategyT]] = None,
            Event: SymbolArg[Type[EventT]] = None,
            Schema: SymbolArg[Type[SchemaT]] = None,
            Stream: SymbolArg[Type[StreamT]] = None,
            Table: SymbolArg[Type[TableT]] = None,
            SetTable: SymbolArg[Type[TableT]] = None,
            GlobalTable: SymbolArg[Type[GlobalTableT]] = None,
            SetGlobalTable: SymbolArg[Type[GlobalTableT]] = None,
            TableManager: SymbolArg[Type[TableManagerT]] = None,
            Serializers: SymbolArg[Type[RegistryT]] = None,
            Worker: SymbolArg[Type[_WorkerT]] = None,
            PartitionAssignor: SymbolArg[Type[PartitionAssignorT]] = None,
            LeaderAssignor: SymbolArg[Type[LeaderAssignorT]] = None,
            Router: SymbolArg[Type[RouterT]] = None,
            Topic: SymbolArg[Type[TopicT]] = None,
            HttpClient: SymbolArg[Type[HttpClientT]] = None,
            Monitor: SymbolArg[Type[SensorT]] = None,
            # Deprecated settings:
            stream_ack_cancelled_tasks: bool = None,
            stream_ack_exceptions: bool = None,
            url: URLArg = None,
            **kwargs: Any) -> None:
        ...  # replaced by __init_subclass__ in BaseSettings

    def on_init(self, id: str, **kwargs: Any) -> None:
        # version is required for the id
        # and the id is required as a component in several default
        # setting values so we hack this in here to make sure
        # it's set.
        self._init_env_prefix(**kwargs)
        self._version = kwargs.get('version', 1)
        self.id = id

    def _init_env_prefix(self,
                         env: Mapping[str, str] = None,
                         env_prefix: str = None,
                         **kwargs: Any) -> None:
        if env is None:
            env = os.environ
        self.env = env
        env_name = self.SETTINGS['env_prefix'].env_name
        if env_name is not None:
            prefix_from_env = self.env.get(env_name)
            # prioritize environment
            if prefix_from_env is not None:
                self._env_prefix = prefix_from_env
            else:
                # then use provided argument
                if env_prefix is not None:
                    self._env_prefix = env_prefix

    def getenv(self, env_name: str) -> Any:
        if self._env_prefix:
            env_name = self._env_prefix.rstrip('_') + '_' + env_name
        return self.env.get(env_name)

    def relative_to_appdir(self, path: Path) -> Path:
        """Prepare app directory path.

        If path is absolute the path is returned as-is,
        but if path is relative it will be assumed to belong
        under the app directory.
        """
        return path if path.is_absolute() else self.appdir / path

    def data_directory_for_version(self, version: int) -> Path:
        """Return the directory path for data belonging to specific version."""
        return self.datadir / f'v{version}'

    def find_old_versiondirs(self) -> Iterable[Path]:
        for version in reversed(range(0, self.version)):
            path = self.data_directory_for_version(version)
            if path.is_dir():
                yield path

    @property
    def name(self) -> str:
        # name is a read-only property
        return self._name

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, name: str) -> None:
        self._name = name
        self._id = self._prepare_id(name)  # id is name+version

    def _prepare_id(self, id: str) -> str:
        if self.version > 1:
            return self.id_format.format(id=id, self=self)
        return id

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.id}>'

    @property
    def appdir(self) -> Path:
        return self.data_directory_for_version(self.version)

    # This is an example new setting having type ``str``
    @sections.Common.setting(
        params.Str,
        env_name='ENVIRONMENT_VARIABLE_NAME',
        version_removed='1.0',  # this disables the setting
    )
    def MY_SETTING(self) -> str:
        """My custom setting.

        To contribute new settings you only have to define a new
        setting decorated attribute here.

        Look at the other settings for examples.

        Remember that once you've added the setting you must
        also render the configuration reference:

        .. sourcecode:: console

            $ make configref
        """

    @sections.Common.setting(
        params.Param[AutodiscoverArg, AutodiscoverArg],
        default=False,
    )
    def autodiscover(self) -> AutodiscoverArg:
        """Automatic discovery of agents, tasks, timers, views and commands.

        Faust has an API to add different :mod:`asyncio` services and other
        user extensions, such as "Agents", HTTP web views,
        command-line commands, and timers to your Faust workers.
        These can be defined in any module, so to discover them at startup,
        the worker needs to traverse packages looking for them.

        .. warning::

            The autodiscovery functionality uses the :pypi:`Venusian` library
            to scan wanted packages for ``@app.agent``, ``@app.page``,
            ``@app.command``, ``@app.task`` and ``@app.timer`` decorators,
            but to do so, it's required to traverse the package path and import
            every module in it.

            Importing random modules like this can be dangerous so make sure
            you follow Python programming best practices. Do not start
            threads; perform network I/O; do test monkey-patching for mocks or
            similar, as a side effect of importing a module.  If you encounter
            a case such as this then please find a way to perform your
            action in a lazy manner.

        .. warning::

            If the above warning is something you cannot fix, or if it's out
            of your control, then please set ``autodiscover=False`` and make
            sure the worker imports all modules where your
            decorators are defined.

        The value for this argument can be:

        ``bool``
            If ``App(autodiscover=True)`` is set, the autodiscovery will
            scan the package name described in the ``origin`` attribute.

            The ``origin`` attribute is automatically set when you start
            a worker using the :program:`faust` command line program,
            for example:

            .. sourcecode:: console

                faust -A example.simple worker

            The :option:`-A <faust -A>`, option specifies the app, but you
            can also create a shortcut entry point by calling ``app.main()``:

            .. sourcecode:: python

                if __name__ == '__main__':
                    app.main()

            Then you can start the :program:`faust` program by executing for
            example ``python myscript.py worker --loglevel=INFO``, and it
            will use the correct application.

        ``Sequence[str]``
            The argument can also be a list of packages to scan::

                app = App(..., autodiscover=['proj_orders', 'proj_accounts'])

        ``Callable[[], Sequence[str]]``
            The argument can also be a function returning a list of packages
            to scan::

                def get_all_packages_to_scan():
                    return ['proj_orders', 'proj_accounts']

                app = App(..., autodiscover=get_all_packages_to_scan)

        ``False``
            If everything you need is in a self-contained module, or you
            import the stuff you need manually, just set ``autodiscover``
            to False and don't worry about it :-)

        .. admonition:: Django

            When using :pypi:`Django` and the :envvar:`DJANGO_SETTINGS_MODULE`
            environment variable is set, the Faust app will scan all packages
            found in the ``INSTALLED_APPS`` setting.

            If you're using Django you can use this to scan for
            agents/pages/commands in all packages
            defined in ``INSTALLED_APPS``.

            Faust will automatically detect that you're using Django
            and do the right thing if you do::

                app = App(..., autodiscover=True)

            It will find agents and other decorators in all of the
            reusable Django applications. If you want to manually control
            what packages are traversed, then provide a list::

                app = App(..., autodiscover=['package1', 'package2'])

            or if you want exactly :const:`None` packages to be traversed,
            then provide a False:

                app = App(.., autodiscover=False)

            which is the default, so you can simply omit the argument.

        .. tip::

            For manual control over autodiscovery, you can also call the
            :meth:`@discover` method manually.
        """

    @sections.Common.setting(
        params.Path,
        env_name='APP_DATADIR',
        default=DATADIR,
        related_cli_options={'faust': '--datadir'},
    )
    def datadir(self, path: Path) -> Path:
        """Application data directory.

        The directory in which this instance stores the data used by
        local tables, etc.

        .. seealso::

            - The data directory can also be set using the
              :option:`faust --datadir` option, from the command-line,
              so there is usually no reason to provide a default value
              when creating the app.
        """

    @datadir.on_get_value  # type: ignore
    def _prepare_datadir(self, path: Path) -> Path:
        # allow expanding variables in path
        return Path(str(path).format(conf=self))

    @sections.Common.setting(
        params.Path,
        #: This path will be treated as relative to datadir,
        #: unless the provided poth is absolute.
        default='tables',
        env_name='APP_TABLEDIR',
    )
    def tabledir(self) -> Path:
        """Application table data directory.

        The directory in which this instance stores local table data.
        Usually you will want to configure the :setting:`datadir` setting,
        but if you want to store tables separately you can configure this one.

        If the path provided is relative (it has no leading slash), then the
        path will be considered to be relative to the :setting:`datadir`
        setting.
        """

    @tabledir.on_get_value  # type: ignore
    def _prepare_tabledir(self, path: Path) -> Path:
        return self.relative_to_appdir(path)

    @sections.Common.setting(
        params.Bool,
        env_name='APP_DEBUG',
        default=False,
        related_cli_options={'faust': '--debug'},
    )
    def debug(self) -> bool:
        """Use in development to expose sensor information endpoint.


        .. tip::

            If you want to enable the sensor statistics endpoint in production,
            without enabling the :setting:`debug` setting, you can do so
            by adding the following code:

            .. sourcecode:: python

                app.web.blueprints.add(
                    '/stats/', 'faust.web.apps.stats:blueprint')
        """

    @sections.Common.setting(
        params.Str,
        env_name='APP_ENV_PREFIX',
        version_introduced='1.11',
        default=None,
        ignore_default=True,
    )
    def env_prefix(self) -> str:
        """Environment variable prefix.

        When configuring Faust by environent variables,
        this adds a common prefix to all Faust environment value names.
        """

    @sections.Common.setting(
        params.Str,
        env_name='APP_ID_FORMAT',
        default='{id}-v{self.version}',
    )
    def id_format(self) -> str:
        """Application ID format template.

        The format string used to generate the final :setting:`id` value
        by combining it with the :setting:`version` parameter.
        """

    @sections.Common.setting(
        params.Str,
        default=None,
    )
    def origin(self) -> str:
        """The reverse path used to find the app.

        For example if the app is located in::

            from myproj.app import app

        Then the ``origin`` should be ``"myproj.app"``.

        The :program:`faust worker` program will try to automatically set
        the origin, but if you are having problems with auto generated names
        then you can set origin manually.
        """

    @sections.Common.setting(
        params.Timezone,
        version_introduced='1.4',
        env_name='TIMEZONE',
        default=timezone.utc,
    )
    def timezone(self) -> tzinfo:
        """Project timezone.

        The timezone used for date-related functionality such as cronjobs.
        """

    @sections.Common.setting(
        params.Int,
        env_name='APP_VERSION',
        default=1,
        min_value=1,
    )
    def version(self) -> int:
        """App version.

        Version of the app, that when changed will create a new isolated
        instance of the application. The first version is 1,
        the second version is 2, and so on.

        .. admonition:: Source topics will not be affected by a version change.

            Faust applications will use two kinds of topics: source topics, and
            internally managed topics. The source topics are declared by the
            producer, and we do not have the opportunity to modify any
            configuration settings, like number of partitions for a source
            topic; we may only consume from them. To mark a topic as internal,
            use: ``app.topic(..., internal=True)``.
        """

    @sections.Agent.setting(
        params.Symbol(Type[SupervisorStrategyT]),
        env_name='AGENT_SUPERVISOR',
        default='mode.OneForOneSupervisor',
    )
    def agent_supervisor(self) -> Type[SupervisorStrategyT]:
        """Default agent supervisor type.

        An agent may start multiple instances (actors) when
        the concurrency setting is higher than one (e.g.
        ``@app.agent(concurrency=2)``).

        Multiple instances of the same agent are considered to be in the same
        supervisor group.

        The default supervisor is the :class:`mode.OneForOneSupervisor`:
        if an instance in the group crashes, we restart that instance only.

        These are the supervisors supported:

        + :class:`mode.OneForOneSupervisor`

            If an instance in the group crashes we restart only that instance.

        + :class:`mode.OneForAllSupervisor`

            If an instance in the group crashes we restart the whole group.

        + :class:`mode.CrashingSupervisor`

            If an instance in the group crashes we stop the whole application,
            and exit so that the Operating System supervisor can restart us.

        + :class:`mode.ForfeitOneForOneSupervisor`

            If an instance in the group crashes we give up on that instance
            and never restart it again (until the program is restarted).

        + :class:`mode.ForfeitOneForAllSupervisor`

            If an instance in the group crashes we stop all instances
            in the group and never restarted them again (until the program is
            restarted).
        """

    @sections.Common.setting(
        params.Seconds,
        env_name='BLOCKING_TIMEOUT',
        default=None,
        related_cli_options={'faust': '--blocking-timeout'},
    )
    def blocking_timeout(self) -> Optional[float]:
        """Blocking timeout (in seconds).

        When specified the worker will start a periodic signal based
        timer that only triggers when the loop has been blocked
        for a time exceeding this timeout.

        This is the most safe way to detect blocking, but could have
        adverse effects on libraries that do not automatically
        retry interrupted system calls.

        Python itself does retry all interrupted system calls
        since version 3.5 (see :pep:`475`), but this might not
        be the case with C extensions added to the worker by the user.

        The blocking detector is a background thread
        that periodically wakes up to either arm a timer, or cancel
        an already armed timer. In pseudocode:

        .. sourcecode:: python

            while True:
                # cancel previous alarm and arm new alarm
                signal.signal(signal.SIGALRM, on_alarm)
                signal.setitimer(signal.ITIMER_REAL, blocking_timeout)
                # sleep to wakeup just before the timeout
                await asyncio.sleep(blocking_timeout * 0.96)

            def on_alarm(signum, frame):
                logger.warning('Blocking detected: ...')

        If the sleep does not wake up in time the alarm signal
        will be sent to the process and a traceback will be logged.
        """

    @sections.Common.setting(
        params.BrokerList,
        env_name='BROKER_URL',
    )
    def broker(self) -> List[URL]:
        """Broker URL, or a list of alternative broker URLs.

        Faust needs the URL of a "transport" to send and receive messages.

        Currently, the only supported production transport is ``kafka://``.
        This uses the :pypi:`aiokafka` client under the hood, for consuming and
        producing messages.

        You can specify multiple hosts at the same time by separating them
        using the semi-comma:

        .. sourcecode:: text

            kafka://kafka1.example.com:9092;kafka2.example.com:9092

        Which in actual code looks like this:

        .. sourcecode:: python

            BROKERS = 'kafka://kafka1.example.com:9092;kafka2.example.com:9092'
            app = faust.App(
                'id',
                broker=BROKERS,
            )

        You can also pass a list of URLs:

        .. sourcecode:: python

            app = faust.App(
                'id',
                broker=['kafka://kafka1.example.com:9092',
                        'kafka://kafka2.example.com:9092'],
            )

        .. seealso::

            You can configure the transport used for consuming and producing
            separately, by setting the :setting:`broker_consumer` and
            :setting:`broker_producer` settings.

            This setting is used as the default.

        **Available Transports**

        - ``kafka://``

            Alias to ``aiokafka://``

        - ``aiokafka://``

            The recommended transport using the :pypi:`aiokafka` client.

            Limitations: None

        - ``confluent://``

            Experimental transport using the :pypi:`confluent-kafka` client.

            Limitations: Does not do sticky partition assignment (not
                suitable for tables), and do not create any necessary internal
                topics (you have to create them manually).
        """

    @broker.on_set_default  # type: ignore
    def _prepare_broker(self) -> BrokerArg:
        return self._url or self.DEFAULT_BROKER_URL

    @sections.Broker.setting(
        params.BrokerList,
        version_introduced='1.7',
        env_name='BROKER_CONSUMER_URL',
        default_alias='broker',
    )
    def broker_consumer(self) -> List[URL]:
        """Consumer broker URL.

        You can use this setting to configure the transport used for
        producing and consuming separately.

        If not set the value found in :setting:`broker` will be used.
        """

    @sections.Broker.setting(
        params.BrokerList,
        version_introduced='1.7',
        env_name='BROKER_PRODUCER_URL',
        default_alias='broker',
    )
    def broker_producer(self) -> List[URL]:
        """Producer broker URL.

        You can use this setting to configure the transport used for
        producing and consuming separately.

        If not set the value found in :setting:`broker` will be used.
        """

    @sections.Broker.setting(
        params.Str,
        version_introduced='1.10',
        env_name='BROKER_API_VERSION',
        #: Default broker API version.
        #: Used as default for
        #:     + :setting:`broker_api_version`,
        #:     + :setting:`consumer_api_version`,
        #:     + :setting:`producer_api_version',
        default='auto',
    )
    def broker_api_version(self) -> str:
        """Broker API version,.

        This setting is also the default for :setting:`consumer_api_version`,
        and :setting:`producer_api_version`.

        Negotiate producer protocol version.

        The default value - "auto" means use the latest version supported
        by both client and server.

        Any other version set means you are requesting a specific version of
        the protocol.

        Example Kafka uses:

        **Disable sending headers for all messages produced**

        Kafka headers support was added in Kafka 0.11, so you can specify
        ``broker_api_version="0.10"`` to remove the headers from messages.
        """

    @sections.Broker.setting(
        params.Bool,
        env_name='BROKER_CHECK_CRCS',
        default=True,
    )
    def broker_check_crcs(self) -> bool:
        """Broker CRC check.

        Automatically check the CRC32 of the records consumed.
        """

    @sections.Broker.setting(
        params.Str,
        env_name='BROKER_CLIENT_ID',
        default=f'faust-{faust_version}',
    )
    def broker_client_id(self) -> str:
        """Broker client ID.

        There is rarely any reason to configure this setting.

        The client id is used to identify the software used, and is not usually
        configured by the user.
        """

    @sections.Broker.setting(
        params.UnsignedInt,
        env_name='BROKER_COMMIT_EVERY',
        default=10_000,
    )
    def broker_commit_every(self) -> int:
        """Broker commit message frequency.

        Commit offset every n messages.

        See also :setting:`broker_commit_interval`, which is how frequently
        we commit on a timer when there are few messages being received.
        """

    @sections.Broker.setting(
        params.Seconds,
        env_name='BROKER_COMMIT_INTERVAL',
        default=2.8,
    )
    def broker_commit_interval(self) -> float:
        """Broker commit time frequency.

        How often we commit messages that have been
        fully processed (:term:`acked`).
        """

    @sections.Broker.setting(
        params.Seconds,
        env_name='BROKER_COMMIT_LIVELOCK_SOFT_TIMEOUT',
        default=want_seconds(timedelta(minutes=5)),
    )
    def broker_commit_livelock_soft_timeout(self) -> float:
        """Commit livelock timeout.

        How long time it takes before we warn that the Kafka commit offset has
        not advanced (only when processing messages).
        """

    @sections.Common.setting(
        params.Credentials,
        version_introduced='1.5',
        env_name='BROKER_CREDENTIALS',
        default=None,
    )
    def broker_credentials(self) -> CredentialsT:
        """Broker authentication mechanism.

        Specify the authentication mechanism to use when connecting to
        the broker.

        The default is to not use any authentication.

        SASL Authentication
            You can enable SASL authentication via plain text:

            .. sourcecode:: python

                app = faust.App(
                    broker_credentials=faust.SASLCredentials(
                        username='x',
                        password='y',
                    ))

            .. warning::

                Do not use literal strings when specifying passwords in
                production, as they can remain visible in stack traces.

                Instead the best practice is to get the password from
                a configuration file, or from the environment:

                .. sourcecode:: python

                    BROKER_USERNAME = os.environ.get('BROKER_USERNAME')
                    BROKER_PASSWORD = os.environ.get('BROKER_PASSWORD')

                    app = faust.App(
                        broker_credentials=faust.SASLCredentials(
                            username=BROKER_USERNAME,
                            password=BROKER_PASSWORD,
                        ))

        GSSAPI Authentication
            GSSAPI authentication over plain text:

            .. sourcecode:: python

                app = faust.App(
                    broker_credentials=faust.GSSAPICredentials(
                        kerberos_service_name='faust',
                        kerberos_domain_name='example.com',
                    ),
                )

            GSSAPI authentication over SSL:

            .. sourcecode:: python

                import ssl
                ssl_context = ssl.create_default_context(
                    purpose=ssl.Purpose.SERVER_AUTH, cafile='ca.pem')
                ssl_context.load_cert_chain(
                    'client.cert', keyfile='client.key')

                app = faust.App(
                    broker_credentials=faust.GSSAPICredentials(
                        kerberos_service_name='faust',
                        kerberos_domain_name='example.com',
                        ssl_context=ssl_context,
                    ),
                )

        SSL Authentication
            Provide an SSL context for the Kafka broker connections.

            This allows Faust to use a secure SSL/TLS connection for the
            Kafka connections and enabling certificate-based authentication.

            .. sourcecode:: python

                import ssl

                ssl_context = ssl.create_default_context(
                    purpose=ssl.Purpose.SERVER_AUTH, cafile='ca.pem')
                ssl_context.load_cert_chain(
                    'client.cert', keyfile='client.key')
                app = faust.App(..., broker_credentials=ssl_context)
        """

    @sections.Broker.setting(
        params.Seconds,
        version_introduced='1.0.11',
        env_name='BROKER_HEARTBEAT_INTERVAL',
        default=3.0,
    )
    def broker_heartbeat_interval(self) -> float:
        """Broker heartbeat interval.

        How often we send heartbeats to the broker, and also how often
        we expect to receive heartbeats from the broker.

        If any of these time out, you should increase this setting.
        """

    @sections.Broker.setting(
        params.Seconds,
        version_introduced='1.7',
        env_name='BROKER_MAX_POLL_INTERVAL',
        default=1000.0,
    )
    def broker_max_poll_interval(self) -> float:
        """Broker max poll interval.

        The maximum allowed time (in seconds) between calls to consume messages
        If this interval is exceeded the consumer
        is considered failed and the group will rebalance in order to reassign
        the partitions to another consumer group member. If API methods block
        waiting for messages, that time does not count against this timeout.

        See `KIP-62`_ for technical details.

        .. _`KIP-62`:
            https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread
        """

    @sections.Broker.setting(
        params.UnsignedInt,
        version_introduced='1.4',
        env_name='BROKER_MAX_POLL_RECORDS',
        default=None,
        allow_none=True,
    )
    def broker_max_poll_records(self) -> Optional[int]:
        """Broker max poll records.

        The maximum number of records returned in a single call to ``poll()``.
        If you find that your application needs more time to process
        messages you may want to adjust :setting:`broker_max_poll_records`
        to tune the number of records that must be handled on every
        loop iteration.
        """

    @sections.Broker.setting(
        params.Seconds,
        version_introduced='1.10',
        env_name='BROKER_REBALANCE_TIMEOUT',
        default=60.0,
    )
    def broker_rebalance_timeout(self) -> float:
        """Broker rebalance timeout.

        How long to wait for a node to finish rebalancing before the broker
        will consider it dysfunctional and remove it from the cluster.

        Increase this if you experience the cluster being in a state of
        constantly rebalancing, but make sure you also increase the
        :setting:`broker_heartbeat_interval` at the same time.

        .. note::

            The session timeout must not be greater than the
            :setting:`broker_request_timeout`.
        """

    @sections.Broker.setting(
        params.Seconds,
        version_introduced='1.4',
        env_name='BROKER_REQUEST_TIMEOUT',
        default=90.0,
    )
    def broker_request_timeout(self) -> float:
        """Kafka client request timeout.

        .. note::

            The request timeout must not be less than the
            :setting:`broker_session_timeout`.
        """

    @sections.Broker.setting(
        params.Seconds,
        version_introduced='1.0.11',
        env_name='BROKER_SESSION_TIMEOUT',
        default=60.0,
    )
    def broker_session_timeout(self) -> float:
        """Broker session timeout.

        How long to wait for a node to finish rebalancing before the broker
        will consider it dysfunctional and remove it from the cluster.

        Increase this if you experience the cluster being in a state of
        constantly rebalancing, but make sure you also increase the
        :setting:`broker_heartbeat_interval` at the same time.

        .. note::

            The session timeout must not be greater than the
            :setting:`broker_request_timeout`.
        """

    @sections.Common.setting(
        params.SSLContext,
        default=None,
    )
    def ssl_context(self) -> ssl.SSLContext:
        """SSL configuration.

        See :setting:`credentials`.
        """

    @sections.Consumer.setting(
        params.Str,
        version_introduced='1.10',
        env_name='CONSUMER_API_VERSION',
        default_alias='broker_api_version',
    )
    def consumer_api_version(self) -> str:
        """Consumer API version.

        Configures the broker API version to use for consumers.
        See :setting:`broker_api_version` for more information.
        """

    @sections.Consumer.setting(
        params.UnsignedInt,
        version_introduced='1.4',
        env_name='CONSUMER_MAX_FETCH_SIZE',
        default=1024 ** 2,
    )
    def consumer_max_fetch_size(self) -> int:
        """Consumer max fetch size.

        The maximum amount of data per-partition the server will return.
        This size must be at least as large as the maximum message size.

        Note: This is PER PARTITION, so a limit of 1Mb when your
        workers consume from 10 topics having 100 partitions each,
        means a fetch request can be up to a gigabyte (10 * 100 * 1Mb),
        This limit being too generous may cause
        rebalancing issues: if the amount of time required
        to flush pending data stuck in socket buffers exceed
        the rebalancing timeout.

        You must keep this limit low enough to account
        for many partitions being assigned to a single node.
        """

    @sections.Consumer.setting(
        params.Str,
        version_introduced='1.5',
        env_name='CONSUMER_AUTO_OFFSET_RESET',
        default='earliest',
    )
    def consumer_auto_offset_reset(self) -> str:
        """Consumer auto offset reset.

        Where the consumer should start reading messages from when
        there is no initial offset, or the stored offset no longer
        exists, e.g. when starting a new consumer for the first time.

        Options include 'earliest', 'latest', 'none'.
        """

    @sections.Consumer.setting(
        params.Str,
        version_introduced='2.1',
        env_name='CONSUMER_GROUP_INSTANCE_ID',
        default=None,
    )
    def consumer_group_instance_id(self) -> str:
        """Consumer group instance id.

        The group_instance_id for static partition assignment.

        If not set, default assignment strategy is used. Otherwise,
        each consumer instance has to have a unique id.
        """

    @sections.Serialization.setting(
        params.Codec,
        env_name='APP_KEY_SERIALIZER',
        default='raw',
    )
    def key_serializer(self) -> CodecArg:
        """Default key serializer.

        Serializer used for keys by default when no serializer
        is specified, or a model is not being used.

        This can be the name of a serializer/codec, or an actual
        :class:`faust.serializers.codecs.Codec` instance.

        .. seealso::

            - The :ref:`codecs` section in the model guide -- for
              more information about codecs.
        """

    @sections.Serialization.setting(
        params.Codec,
        env_name='APP_VALUE_SERIALIZER',
        default='json',
    )
    def value_serializer(self) -> CodecArg:
        """Default value serializer.

        Serializer used for values by default when no serializer
        is specified, or a model is not being used.

        This can be string, the name of a serializer/codec, or an actual
        :class:`faust.serializers.codecs.Codec` instance.

        .. seealso::

            - The :ref:`codecs` section in the model guide -- for
              more information about codecs.
        """

    @sections.Common.setting(
        params.Dict[Any],
        version_introduced='1.5',
    )
    def logging_config(self) -> Mapping[str, Any]:
        """Logging dictionary configuration.

        Optional dictionary for logging configuration, as supported
        by :func:`logging.config.dictConfig`.
        """

    @sections.Common.setting(
        params.LogHandlers,
    )
    def loghandlers(self) -> List[logging.Handler]:
        """List of custom logging handlers.

        Specify a list of custom log handlers to use in worker instances.
        """

    @sections.Producer.setting(
        params.Int,
        env_name='PRODUCER_ACKS',
        default=-1,
        number_aliases={'all': -1},
    )
    def producer_acks(self) -> int:
        """Producer Acks.

        The number of acknowledgments the producer requires the leader to have
        received before considering a request complete. This controls the
        durability of records that are sent. The following settings are common:

        * ``0``: Producer will not wait for any acknowledgment from
                 the server at all. The message will immediately be
                 considered sent (Not recommended).
        * ``1``: The broker leader will write the record to its local
                 log but will respond without awaiting full acknowledgment
                 from all followers. In this case should the leader fail
                 immediately after acknowledging the record but before the
                 followers have replicated it then the record will be lost.
        * ``-1``: The broker leader will wait for the full set of in-sync
                  replicas to acknowledge the record. This guarantees that
                  the record will not be lost as long as at least one in-sync
                  replica remains alive. This is the strongest
                  available guarantee.
        """

    @sections.Producer.setting(
        params.Str,
        version_introduced='1.5.3',
        env_name='PRODUCER_API_VERSION',
        default_alias='broker_api_version',
    )
    def producer_api_version(self) -> str:
        """Producer API version.

        Configures the broker API version to use for producers.
        See :setting:`broker_api_version` for more information.
        """

    @sections.Producer.setting(
        params.Str,
        env_name='PRODUCER_COMPRESSION_TYPE',
        default=None,
    )
    def producer_compression_type(self) -> str:
        """Producer compression type.

        The compression type for all data generated by the producer.
        Valid values are `gzip`, `snappy`, `lz4`, or :const:`None`.
        """

    @sections.Producer.setting(
        params.Seconds,
        env_name='PRODUCER_LINGER',
    )
    def producer_linger(self) -> Optional[float]:
        """Producer batch linger configuration.

        Minimum time to batch before sending out messages from the producer.

        Should rarely have to change this.
        """

    @producer_linger.on_set_default  # type: ignore
    def _prepare_producer_linger(self) -> float:
        return float(self._producer_linger_ms) / 1000.0

    @sections.Producer.setting(
        params.UnsignedInt,
        env_name='PRODUCER_MAX_BATCH_SIZE',
        default=16384,
    )
    def producer_max_batch_size(self) -> int:
        """Producer max batch size.

        Max size of each producer batch, in bytes.
        """

    @sections.Producer.setting(
        params.UnsignedInt,
        env_name='PRODUCER_MAX_REQUEST_SIZE',
        default=1_000_000,
    )
    def producer_max_request_size(self) -> int:
        """Producer maximum request size.

        Maximum size of a request in bytes in the producer.

        Should rarely have to change this.
        """

    @sections.Producer.setting(
        params._Symbol[PartitionerT, Optional[PartitionerT]],
        version_introduced='1.2',
        default=None,
    )
    def producer_partitioner(self) -> Optional[PartitionerT]:
        """Producer partitioning strategy.

        The Kafka producer can be configured with a custom partitioner
        to change how keys are partitioned when producing to topics.

        The default partitioner for Kafka is implemented as follows,
        and can be used as a template for your own partitioner:

        .. sourcecode:: python

            import random
            from typing import List
            from kafka.partitioner.hashed import murmur2

            def partition(key: bytes,
                        all_partitions: List[int],
                        available: List[int]) -> int:
                '''Default partitioner.

                Hashes key to partition using murmur2 hashing
                (from java client) If key is None, selects partition
                randomly from available, or from all partitions if none
                are currently available

                Arguments:
                    key: partitioning key
                    all_partitions: list of all partitions sorted by
                                    partition ID.
                    available: list of available partitions
                               in no particular order
                Returns:
                    int: one of the values from ``all_partitions``
                         or ``available``.
                '''
                if key is None:
                    source = available if available else all_paritions
                    return random.choice(source)
                index: int = murmur2(key)
                index &= 0x7fffffff
                index %= len(all_partitions)
                return all_partitions[index]
        """

    @sections.Producer.setting(
        params.Seconds,
        version_introduced='1.4',
        env_name='PRODUCER_REQUEST_TIMEOUT',
        default=1200.0,  # 20 minutes
    )
    def producer_request_timeout(self) -> float:
        """Producer request timeout.

        Timeout for producer operations.
        This is set high by default, as this is also the time when
        producer batches expire and will no longer be retried.
        """

    @sections.RPC.setting(
        params.Bool,
        env_name='APP_REPLY_CREATE_TOPIC',
        default=False,
    )
    def reply_create_topic(self) -> bool:
        """Automatically create reply topics.

        Set this to :const:`True` if you plan on using the RPC with agents.

        This will create the internal topic used for RPC replies on that
        instance at startup.
        """

    @sections.RPC.setting(
        params.Seconds,
        env_name='APP_REPLY_EXPIRES',
        default=want_seconds(timedelta(days=1)),
    )
    def reply_expires(self) -> float:
        """RPC reply expiry time in seconds.

        The expiry time (in seconds :class:`float`,
        or :class:`~datetime.timedelta`), for how long replies will stay
        in the instances local reply topic before being removed.
        """

    @sections.RPC.setting(
        params.Str,
    )
    def reply_to(self) -> str:
        """Reply to address.

        The name of the reply topic used by this instance.
        If not set one will be automatically generated when the app
        is created.
        """

    @reply_to.on_set_default  # type: ignore
    def _prepare_reply_to_default(self) -> str:
        return f'{self.reply_to_prefix}{uuid4()}'

    @sections.RPC.setting(
        params.Str,
        env_name='APP_REPLY_TO_PREFIX',
        default='f-reply-',
    )
    def reply_to_prefix(self) -> str:
        """Reply address topic name prefix.

        The prefix used when generating reply topic names.
        """

    @sections.Common.setting(
        params.Enum(ProcessingGuarantee),
        version_introduced='1.5',
        env_name='PROCESSING_GUARANTEE',
        default=ProcessingGuarantee.AT_LEAST_ONCE,
    )
    def processing_guarantee(self) -> ProcessingGuarantee:
        """The processing guarantee that should be used.

        Possible values are "at_least_once" (default) and "exactly_once".

        Note that if exactly-once processing is enabled consumers are
        configured with ``isolation.level="read_committed"`` and producers
        are configured with ``retries=Integer.MAX_VALUE`` and
        ``enable.idempotence=true`` per default.

        Note that by default exactly-once processing requires a cluster of
        at least three brokers what is the recommended setting for production.
        For development you can change this, by adjusting broker setting
        ``transaction.state.log.replication.factor`` to the number of brokers
        you want to use.
        """

    @sections.Stream.setting(
        params.UnsignedInt,
        env_name='STREAM_BUFFER_MAXSIZE',
        default=4096,
    )
    def stream_buffer_maxsize(self) -> int:
        """Stream buffer maximum size.

        This setting control back pressure to streams and agents reading
        from streams.

        If set to 4096 (default) this means that an agent can only keep
        at most 4096 unprocessed items in the stream buffer.

        Essentially this will limit the number of messages a stream
        can "prefetch".

        Higher numbers gives better throughput, but do note that if
        your agent sends messages or update tables (which
        sends changelog messages).

        This means that if the buffer size is large, the
        :setting:`broker_commit_interval` or :setting:`broker_commit_every`
        settings must be set to commit frequently, avoiding back pressure
        from building up.

        A buffer size of 131_072 may let you process over 30,000 events
        a second as a baseline, but be careful with a buffer size that large
        when you also send messages or update tables.
        """

    @sections.Stream.setting(
        params.Seconds,
        version_introduced='1.10',
        env_name='STREAM_PROCESSING_TIMEOUT',
        default=5 * 60.0,
    )
    def stream_processing_timeout(self) -> float:
        """Stream processing timeout.

        Timeout (in seconds) for processing events in the stream.
        If processing of a single event exceeds this time we log an error,
        but do not stop processing.

        If you are seeing a warning like this you should either

        1) increase this timeout to allow agents to spend more time
            on a single event, or
        2) add a timeout to the operation in the agent, so stream processing
            always completes before the timeout.

        The latter is preferred for network operations such as web requests.
        If a network service you depend on is temporarily offline you should
        consider doing retries (send to separate topic):

        .. sourcecode:: python

            main_topic = app.topic('main')
            deadletter_topic = app.topic('main_deadletter')

            async def send_request(value, timeout: float = None) -> None:
                await app.http_client.get('http://foo.com', timeout=timeout)

            @app.agent(main_topic)
            async def main(stream):
                async for value in stream:
                try:
                    await send_request(value, timeout=5)
                except asyncio.TimeoutError:
                    await deadletter_topic.send(value)

            @app.agent(deadletter_topic)
                async def main_deadletter(stream):
                    async for value in stream:
                    # wait for 30 seconds before retrying.
                    await stream.sleep(30)
                    await send_request(value)
        """

    @sections.Stream.setting(
        params.Bool,
        default=False,
    )
    def stream_publish_on_commit(self) -> bool:
        """Stream delay producing until commit time.

        If enabled we buffer up sending messages until the
        source topic offset related to that processing is committed.
        This means when we do commit, we may have buffered up a LOT of messages
        so commit needs to happen frequently (make sure to decrease
        :setting:`broker_commit_every`).
        """

    @sections.Stream.setting(
        params.Seconds,
        version_introduced='1.3',
        version_changed={
            '1.5.3': 'Disabled by default.',
        },
        env_name='STREAM_RECOVERY_DELAY',
        default=0.0,
    )
    def stream_recovery_delay(self) -> float:
        """Stream recovery delayl

        Number of seconds to sleep before continuing after rebalance.
        We wait for a bit to allow for more nodes to join/leave before
        starting recovery tables and then processing streams. This to minimize
        the chance of errors rebalancing loops.
        """

    @sections.Stream.setting(
        params.Bool,
        env_name='STREAM_WAIT_EMPTY',
        default=True,
    )
    def stream_wait_empty(self) -> bool:
        """Stream wait empty.

        This setting controls whether the worker should wait for the
        currently processing task in an agent to complete before
        rebalancing or shutting down.

        On rebalance/shut down we clear the stream buffers. Those events
        will be reprocessed after the rebalance anyway, but we may have
        already started processing one event in every agent, and if we
        rebalance we will process that event again.

        By default we will wait for the currently active tasks, but if your
        streams are idempotent you can disable it using this setting.
        """

    @sections.Common.setting(
        params.URL,
        env_name='APP_STORE',
        default='memory://',
    )
    def store(self) -> URL:
        """Table storage backend URL.

        The backend used for table storage.

        Tables are stored in-memory by default, but you should
        not use the ``memory://`` store in production.

        In production, a persistent table store, such as ``rocksdb://`` is
        preferred.
        """

    @sections.Table.setting(
        params.Seconds,
        env_name='TABLE_CLEANUP_INTERVAL',
        default=30.0,
    )
    def table_cleanup_interval(self) -> float:
        """Table cleanup interval.

        How often we cleanup tables to remove expired entries.
        """

    @sections.Table.setting(
        params.UnsignedInt,
        version_introduced='1.7',
        env_name='TABLE_KEY_INDEX_SIZE',
        default=1000,
    )
    def table_key_index_size(self) -> int:
        """Table key index size.

        Tables keep a cache of key to partition number to speed up
        table lookups.

        This setting configures the maximum size of that cache.
        """

    @sections.Table.setting(
        params.UnsignedInt,
        env_name='TABLE_STANDBY_REPLICAS',
        default=1,
    )
    def table_standby_replicas(self) -> int:
        """Table standby replicas.

        The number of standby replicas for each table.
        """

    @sections.Topic.setting(
        params.Bool,
        version_introduced='1.5',
        env_name='TOPIC_ALLOW_DECLARE',
        default=True,
    )
    def topic_allow_declare(self) -> bool:
        """Allow creating new topics.

        This setting disables the creation of internal topics.

        Faust will only create topics that it considers to be fully
        owned and managed, such as intermediate repartition topics,
        table changelog topics etc.

        Some Kafka managers does not allow services to create topics,
        in that case you should set this to :const:`False`.
        """

    @sections.Topic.setting(
        params.Bool,
        version_introduced='1.7',
        env_name='TOPIC_DISABLE_LEADER',
        default=False,
    )
    def topic_disable_leader(self) -> bool:
        """Disable leader election topic.

        This setting disables the creation of the leader election topic.

        If you're not using the ``on_leader=True`` argument to task/timer/etc.,
        decorators then use this setting to disable creation of the topic.
        """

    @sections.Topic.setting(
        params.UnsignedInt,
        env_name='TOPIC_PARTITIONS',
        default=8,
    )
    def topic_partitions(self) -> int:
        """Topic partitions.

        Default number of partitions for new topics.

        .. note::

            This defines the maximum number of workers we could distribute the
            workload of the application (also sometimes referred as the
            sharding factor of the application).
        """

    @sections.Topic.setting(
        params.UnsignedInt,
        env_name='TOPIC_REPLICATION_FACTOR',
        default=1,
    )
    def topic_replication_factor(self) -> int:
        """Topic replication factor.

        The default replication factor for topics created by the application.

        .. note::

            Generally this should be the same as the configured
            replication factor for your Kafka cluster.
        """

    @sections.Common.setting(
        params.URL,
        version_introduced='1.2',
        env_name='CACHE_URL',
        default='memory://',
    )
    def cache(self) -> URL:
        """Cache backend URL.

        Optional backend used for Memcached-style caching.
        URL can be:

        + ``redis://host``
        + ``rediscluster://host``, or
        + ``memory://``.
        """

    @sections.WebServer.setting(
        params.URL,
        version_introduced='1.2',
        default='aiohttp://',
    )
    def web(self) -> URL:
        """Web server driver to use."""

    @sections.WebServer.setting(
        params.Str,
        version_introduced='1.2',
        env_name='WEB_BIND',
        default=WEB_BIND,
        related_cli_options={'faust worker': ['--web-bind']},
    )
    def web_bind(self) -> str:
        """Web network interface binding mask.

        The IP network address mask that decides what interfaces
        the web server will bind to.

        By default this will bind to all interfaces.

        This option is usually set by :option:`faust worker --web-bind`,
        not by passing it as a keyword argument to :class:`app`.
        """

    @sections.WebServer.setting(
        params.Dict[ResourceOptions],
        version_introduced='1.5',
    )
    def web_cors_options(self) -> Mapping[str, ResourceOptions]:
        """Cross Origin Resource Sharing options.

        Enable `Cross-Origin Resource Sharing`_ options for all web views
        in the internal web server.

        This should be specified as a dictionary of
        URLs to :class:`~faust.web.ResourceOptions`:

        .. sourcecode:: python

            app = App(..., web_cors_options={
                'http://foo.example.com': ResourceOptions(
                    allow_credentials=True,
                    allow_methods='*'k,
                )
            })

        Individual views may override the CORS options used as
        arguments to to ``@app.page`` and ``blueprint.route``.

        .. seealso::

            :pypi:`aiohttp_cors`: https://github.com/aio-libs/aiohttp-cors

        .. _`Cross-Origin Resource Sharing`:
            https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS
        """

    @sections.WebServer.setting(
        params.Bool,
        version_introduced='1.2',
        env_name='APP_WEB_ENABLED',
        default=True,
        related_cli_options={'faust worker': ['--with-web']},
    )
    def web_enabled(self) -> bool:
        """Enable/disable internal web server.

        Enable web server and other web components.

        This option can also be set using :option:`faust worker --without-web`.
        """

    @sections.WebServer.setting(
        params.Str,
        version_introduced='1.2',
        env_name='WEB_HOST',
        default_template='{conf.NODE_HOSTNAME}',
        related_cli_options={'faust worker': ['--web-host']},
    )
    def web_host(self) -> str:
        """Web server host name.

        Hostname used to access this web server, used for generating
        the :setting:`canonical_url` setting.

        This option is usually set by :option:`faust worker --web-host`,
        not by passing it as a keyword argument to :class:`app`.
        """

    @sections.WebServer.setting(
        params.Bool,
        version_introduced='1.5',
        default=False,
    )
    def web_in_thread(self) -> bool:
        """Run the web server in a separate thread.

        Use this if you have a large value for
        :setting:`stream_buffer_maxsize` and want the web server
        to be responsive when the worker is otherwise busy processing streams.

        .. note::

            Running the web server in a separate thread means web views
            and agents will not share the same event loop.
        """

    @sections.WebServer.setting(
        params.Port,
        version_introduced='1.2',
        env_name='WEB_PORT',
        default=WEB_PORT,
        related_cli_options={'faust worker': ['--web-port']},
    )
    def web_port(self) -> int:
        """Web server port.

        A port number between 1024 and 65535 to use for the web server.

        This option is usually set by :option:`faust worker --web-port`,
        not by passing it as a keyword argument to :class:`app`.
        """

    @sections.WebServer.setting(
        params.URL,
        version_introduced='1.2',
        default=WEB_TRANSPORT,
        related_cli_options={'faust worker': ['--web-transport']},
    )
    def web_transport(self) -> URL:
        """Network transport used for the web server.

        Default is to use TCP, but this setting also enables you to use
        Unix domainN sockets.  To use domain sockets specify an URL including
        the path to the file you want to create like this:

        .. sourcecode:: text

            unix:///tmp/server.sock

        This will create a new domain socket available
        in :file:`/tmp/server.sock`.
        """

    @sections.WebServer.setting(
        params.URL,
        default_template='http://{conf.web_host}:{conf.web_port}',
        env_name='NODE_CANONICAL_URL',
        related_cli_options={
            'faust worker': ['--web-host', '--web-port'],
        },
        related_settings=[web_host, web_port],
    )
    def canonical_url(self) -> URL:
        """Node specific canonical URL.

        You shouldn't have to set this manually.

        The canonical URL defines how to reach the web server on a running
        worker node, and is usually set by combining the
        :setting:`web_host` and :setting:`web_port` settings.
        """

    @sections.Worker.setting(
        params.Bool,
        env_name='WORKER_REDIRECT_STDOUTS',
        default=True,
    )
    def worker_redirect_stdouts(self) -> bool:
        """Redirecting standard outputs.

        Enable to have the worker redirect output to :data:`sys.stdout` and
        :data:`sys.stderr` to the Python logging system.

        Enabled by default.
        """

    @sections.Worker.setting(
        params.Severity,
        env_name='WORKER_REDIRECT_STDOUTS_LEVEL',
        default='WARN',
    )
    def worker_redirect_stdouts_level(self) -> Severity:
        """Level used when redirecting standard outputs.

        The logging level to use when redirect STDOUT/STDERR to logging.
        """

    @sections.Extension.setting(
        params.Symbol(Type[AgentT]),
        default='faust:Agent',
    )
    def Agent(self) -> Type[AgentT]:
        """Agent class type.

        The :class:`~faust.Agent` class to use for agents, or the
        fully-qualified path to one (supported by
        :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MyAgent(faust.Agent):
                ...

            app = App(..., Agent=MyAgent)

        Example using the string path to a class::

            app = App(..., Agent='myproj.agents.Agent')
        """

    @sections.Consumer.setting(
        params.Symbol(Type[SchedulingStrategyT]),
        version_introduced='1.5',
        default='faust.transport.utils:DefaultSchedulingStrategy',
    )
    def ConsumerScheduler(self) -> Type[SchedulingStrategyT]:
        """Consumer scheduler class.

        A strategy which dictates the priority of topics and
        partitions for incoming records.
        The default strategy does first round-robin over topics and then
        round-robin over partitions.

        Example using a class::

            class MySchedulingStrategy(DefaultSchedulingStrategy):
                ...

            app = App(..., ConsumerScheduler=MySchedulingStrategy)

        Example using the string path to a class::

            app = App(..., ConsumerScheduler='myproj.MySchedulingStrategy')
        """

    @sections.Extension.setting(
        params.Symbol(Type[EventT]),
        default='faust:Event',
    )
    def Event(self) -> Type[EventT]:
        """Event class type.

        The :class:`~faust.Event` class to use for creating new event objects,
        or the fully-qualified path to one (supported by
        :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MyBaseEvent(faust.Event):
                ...

            app = App(..., Event=MyBaseEvent)

        Example using the string path to a class::

            app = App(..., Event='myproj.events.Event')
        """

    @sections.Extension.setting(
        params.Symbol(Type[SchemaT]),
        default='faust:Schema',
    )
    def Schema(self) -> Type[SchemaT]:
        """Schema class type.

        The :class:`~faust.Schema` class to use as the default
        schema type when no schema specified. or the fully-qualified
        path to one (supported by :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MyBaseSchema(faust.Schema):
                ...

            app = App(..., Schema=MyBaseSchema)

        Example using the string path to a class::

            app = App(..., Schema='myproj.schemas.Schema')
        """

    @sections.Extension.setting(
        params.Symbol(Type[StreamT]),
        default='faust:Stream',
    )
    def Stream(self) -> Type[StreamT]:
        """Stream class type.

        The :class:`~faust.Stream` class to use for streams, or the
        fully-qualified path to one (supported by
        :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MyBaseStream(faust.Stream):
                ...

            app = App(..., Stream=MyBaseStream)

        Example using the string path to a class::

            app = App(..., Stream='myproj.streams.Stream')
        """

    @sections.Extension.setting(
        params.Symbol(Type[TableT]),
        default='faust:Table',
    )
    def Table(self) -> Type[TableT]:
        """Table class type.

        The :class:`~faust.Table` class to use for tables, or the
        fully-qualified path to one (supported by
        :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MyBaseTable(faust.Table):
                ...

            app = App(..., Table=MyBaseTable)

        Example using the string path to a class::

            app = App(..., Table='myproj.tables.Table')
        """

    @sections.Extension.setting(
        params.Symbol(Type[TableT]),
        default='faust:SetTable',
    )
    def SetTable(self) -> Type[TableT]:
        """SetTable extension table.

        The :class:`~faust.SetTable` class to use for table-of-set tables,
        or the fully-qualified path to one (supported
        by :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MySetTable(faust.SetTable):
                ...

            app = App(..., Table=MySetTable)

        Example using the string path to a class::

            app = App(..., Table='myproj.tables.MySetTable')
        """

    @sections.Extension.setting(
        params.Symbol(Type[GlobalTableT]),
        default='faust:GlobalTable',
    )
    def GlobalTable(self) -> Type[GlobalTableT]:
        """GlobalTable class type.

        The :class:`~faust.GlobalTable` class to use for tables,
        or the fully-qualified path to one (supported by
        :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MyBaseGlobalTable(faust.GlobalTable):
                ...

            app = App(..., GlobalTable=MyBaseGlobalTable)

        Example using the string path to a class::

            app = App(..., GlobalTable='myproj.tables.GlobalTable')
        """

    @sections.Extension.setting(
        params.Symbol(Type[GlobalTableT]),
        default='faust:SetGlobalTable',
    )
    def SetGlobalTable(self) -> Type[GlobalTableT]:
        """SetGlobalTable class type.

        The :class:`~faust.SetGlobalTable` class to use for tables, or the
        fully-qualified path to one (supported by
        :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            class MyBaseSetGlobalTable(faust.SetGlobalTable):
                ...

            app = App(..., SetGlobalTable=MyBaseGlobalSetTable)

        Example using the string path to a class::

            app = App(..., SetGlobalTable='myproj.tables.SetGlobalTable')
        """

    @sections.Extension.setting(
        params.Symbol(Type[TableManagerT]),
        default='faust.tables:TableManager',
    )
    def TableManager(self) -> Type[TableManagerT]:
        """Table manager class type.

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
        """

    @sections.Extension.setting(
        params.Symbol(Type[RegistryT]),
        default='faust.serializers:Registry',
    )
    def Serializers(self) -> Type[RegistryT]:
        """Serializer registry class type.

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
        """

    @sections.Extension.setting(
        params.Symbol(Type[_WorkerT]),
        default='faust.worker:Worker',
    )
    def Worker(self) -> Type[_WorkerT]:
        """Worker class type.

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
        """

    @sections.Extension.setting(
        params.Symbol(Type[PartitionAssignorT]),
        default='faust.assignor:PartitionAssignor',
    )
    def PartitionAssignor(self) -> Type[PartitionAssignorT]:
        """Partition assignor class type.

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
        """

    @sections.Extension.setting(
        params.Symbol(Type[LeaderAssignorT]),
        default='faust.assignor:LeaderAssignor',
    )
    def LeaderAssignor(self) -> Type[LeaderAssignorT]:
        """Leader assignor class type.

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
        """

    @sections.Extension.setting(
        params.Symbol(Type[RouterT]),
        default='faust.app.router:Router',
    )
    def Router(self) -> Type[RouterT]:
        """Router class type.

        The :class:`~faust.router.Router` class used for routing requests
        to a worker instance having the partition for a specific
        key (e.g. table key); or the fully-qualified path to one
        (supported by :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            from faust.router import Router

            class MyRouter(Router):
                ...

            app = App(..., Router=Router)

        Example using the string path to a class::

            app = App(..., Router='myproj.routers.Router')
        """

    @sections.Extension.setting(
        params.Symbol(Type[TopicT]),
        default='faust:Topic',
    )
    def Topic(self) -> Type[TopicT]:
        """Topic class type.

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
        """

    @sections.Extension.setting(
        params.Symbol(Type[HttpClientT]),
        default='aiohttp.client:ClientSession',
    )
    def HttpClient(self) -> Type[HttpClientT]:
        """Http client class type

        The :class:`aiohttp.client.ClientSession` class used as
        a HTTP client; or the fully-qualified path to one (supported by
        :func:`~mode.utils.imports.symbol_by_name`).

        Example using a class::

            import faust
            from aiohttp.client import ClientSession

            class HttpClient(ClientSession):
                ...

            app = faust.App(..., HttpClient=HttpClient)

        Example using the string path to a class::

            app = faust.App(..., HttpClient='myproj.http.HttpClient')
        """

    @sections.Extension.setting(
        params.Symbol(Type[SensorT]),
        default='faust.sensors:Monitor',
    )
    def Monitor(self) -> Type[SensorT]:
        """Monitor sensor class type.

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
        """

    @sections.Stream.setting(
        params.Bool,
        default=True,
        version_deprecated='1.0',
        deprecation_reason='no longer has any effect',
    )
    def stream_ack_cancelled_tasks(self) -> bool:
        """Deprecated setting has no effect."""

    @sections.Stream.setting(
        params.Bool,
        default=True,
        version_deprecated='1.0',
        deprecation_reason='no longer has any effect',
    )
    def stream_ack_exceptions(self) -> bool:
        """Deprecated setting has no effect."""

    @sections.Producer.setting(
        params.UnsignedInt,
        env_name='PRODUCER_LINGER_MS',
        version_deprecated='1.11',
        deprecation_reason='use producer_linger in seconds instead.',
        default=0,
    )
    def producer_linger_ms(self) -> int:
        """Deprecated setting, please use :setting:`producer_linger` instead.

        This used to be provided as milliseconds, the new setting
        uses seconds.
        """

    @sections.Common.setting(
        params.URL,
        default=None,
        version_deprecated=1.0,
        deprecation_reason='Please use "broker" setting instead',
    )
    def url(self) -> URL:
        """Backward compatibility alias to :setting:`broker`."""

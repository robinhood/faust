import inspect
import typing
from itertools import chain
from typing import Any, Iterable, List, Optional, Type, Union, cast

from mode import Service, ServiceT
from faust.exceptions import ImproperlyConfigured

if typing.TYPE_CHECKING:  # pragma: no cover
    from .base import App
else:
    class App: ...  # noqa


class AppService(Service):
    """Service responsible for starting/stopping an application."""

    # Service.__init__ needs the event loop to create asyncio.Events.
    # This means it creates the event loop if it does not exist:
    #    asyncio.get_event_loop()
    # This is usually fine, but the app is also defined at module-scope:
    #
    #   # myproj/app.py
    #   import faust
    #   app = faust.App('myapp')
    #
    #  This means the event loop will be created too early, and that makes
    #  it difficult to install a different event loop policy
    #  (asyncio.set_event_loop_policy).

    # To solve this we use ServiceProxy to split into App + AppService,
    # in a way such that Service.__init__ is called lazily when first needed.

    _extra_service_instances: Optional[List[ServiceT]]

    def __init__(self, app: App, **kwargs: Any) -> None:
        self.app: App = app
        self._extra_service_instances = None
        super().__init__(loop=self.app.loop, **kwargs)

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # Client-Only: Boots up enough services to be able to
        # produce to topics and receive replies from topics.
        # XXX Need better way to do RPC
        if self.app.producer_only:
            return self._components_producer_only()
        if self.app.client_only:
            return self._components_client()
        # Server: Starts everything.
        return self._components_server()

    def _components_producer_only(self) -> Iterable[ServiceT]:
        return cast(Iterable[ServiceT], (
            self.app.producer,
        ))

    def _components_client(self) -> Iterable[ServiceT]:
        # Returns the components started when running in Client-Only mode.
        return cast(Iterable[ServiceT], (
            self.app.producer,
            self.app.consumer,
            self.app._reply_consumer,
            self.app.topics,
            self.app._fetcher,
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
        return cast(
            Iterable[ServiceT],
            chain(
                # Sensors (Sensor): always start first and stop last.
                self.app.sensors,
                # Producer: always stop after Consumer.
                [self.app.producer],
                # Consumer: always stop after Conductor
                [self.app.consumer],
                # Leader Assignor (assignor.LeaderAssignor)
                [self.app._leader_assignor],
                # Reply Consumer (ReplyConsumer)
                [self.app._reply_consumer],
                # AgentManager starts agents (app.agents)
                [self.app.agents],
                # Conductor (transport.Conductor))
                [self.app.topics],
                # Table Manager (app.TableManager)
                [self.app.tables],
            ),
        )

    async def on_first_start(self) -> None:
        if not self.app.agents and not self.app.producer_only:
            # XXX I can imagine use cases where an app is useful
            #     without agents, but use this as more of an assertion
            #     to make sure agents are registered correctly. [ask]
            raise ImproperlyConfigured(
                'Attempting to start app that has no agents')
        self.app._create_directories()
        await self.app.on_first_start()

    async def on_start(self) -> None:
        self.app.finalize()
        await self.app.on_start()

    async def on_started(self) -> None:
        # Wait for table recovery to complete.
        if not await self.wait_for_table_recovery_completed():
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

    async def wait_for_table_recovery_completed(self) -> None:
        if not self.app.producer_only and not self.app.client_only:
            return await self.wait_for_stopped(
                self.app.tables.recovery_completed)

    async def on_started_init_extra_tasks(self) -> None:
        for task in self.app._tasks:
            self.add_future(task())

    async def on_started_init_extra_services(self) -> None:
        if self._extra_service_instances is None:
            # instantiate the services added using the @app.service decorator.
            self._extra_service_instances = [
                await self.on_init_extra_service(service)
                for service in self.app._extra_services
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

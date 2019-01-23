from mode import Service, label
from mode.utils.mocks import AsyncMock, Mock, call
import pytest


class OtherService(Service):
    ...


class test_AppService:

    def test_on_init_dependencies(self, *, app):
        app.boot_strategy = Mock(name='boot_strategy')
        app.client_only = True
        assert app.on_init_dependencies() == app.boot_strategy.client_only()

        app.client_only = False
        assert app.on_init_dependencies() == app.boot_strategy.server()

        app.producer_only = True
        assert app.on_init_dependencies() == app.boot_strategy.producer_only()

    def test_components_client(self, *, app):
        assert list(app.boot_strategy.client_only()) == [
            app.producer,
            app.consumer,
            app._reply_consumer,
            app.topics,
            app._fetcher,
        ]

    def test_components_producer_only(self, *, app):
        assert list(app.boot_strategy.producer_only()) == [
            app.cache,
            app.web,
            app.producer,
        ]

    def test_components_server(self, *, app):
        components = list(app.boot_strategy.server())
        expected_components = list(app.sensors)
        expected_components.extend([
            app.producer,
            app.cache,
            app.web,
            app.consumer,
            app._leader_assignor,
            app._reply_consumer,
        ])
        expected_components.extend(list(app.agents.values()))
        expected_components.extend([
            app.agents,
            app.topics,
            app.tables,
        ])
        assert components == expected_components

    @pytest.mark.asyncio
    async def test_on_first_start(self, *, app):
        app.agents = Mock(name='agents')
        app._create_directories = Mock(name='app._create_directories')
        await app.on_first_start()

        app._create_directories.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_start(self, *, app):
        app.finalize = Mock(name='app.finalize')
        await app.on_start()

        app.finalize.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_started(self, *, app):
        app._wait_for_table_recovery_completed = AsyncMock(return_value=True)
        app.on_started_init_extra_tasks = AsyncMock(name='osiet')
        app.on_started_init_extra_services = AsyncMock(name='osies')
        app.on_startup_finished = None
        app._wait_for_table_recovery_completed.coro.return_value = True
        await app.on_started()

        app._wait_for_table_recovery_completed.coro.return_value = False
        await app.on_started()

        app.on_started_init_extra_tasks.assert_called_once_with()
        app.on_started_init_extra_services.assert_called_once_with()

        app.on_startup_finished = AsyncMock(name='on_startup_finished')
        await app.on_started()

        app.on_startup_finished.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_wait_for_table_recovery_completed(self, *, app):
        app.wait_for_stopped = AsyncMock(name='wait_for_stopped')
        await app._wait_for_table_recovery_completed()
        app.wait_for_stopped.assert_called_once_with(
            app.tables.recovery.completed)

    @pytest.mark.asyncio
    async def test_on_started_init_extra_tasks(self, *, app):
        app.add_future = Mock(name='add_future')

        t1_mock = Mock(name='t1_mock')
        t2_mock = Mock(name='t2_mock')

        def t1():
            return t1_mock()

        def t2():
            return t2_mock(self)

        app._tasks = [t1, t2]
        await app.on_started_init_extra_tasks()

        app.add_future.assert_has_calls([
            call(t1()),
            call(t2()),
        ])

        t1_mock.assert_called_with()
        t2_mock.assert_called_with(self)

    @pytest.mark.asyncio
    async def test_on_started_init_extra_services(self, *, app):
        app.add_runtime_dependency = AsyncMock(name='add_runtime_dependency')
        service1 = Mock(name='service1', autospec=Service)
        app._extra_services = [service1]
        app._extra_service_instances = None
        await app.on_started_init_extra_services()

        app.add_runtime_dependency.assert_called_once_with(service1)
        assert app._extra_service_instances == [service1]
        await app.on_started_init_extra_services()  # noop

    def test_prepare_subservice(self, *, app):
        service = OtherService()
        assert app._prepare_subservice(service) is service

    def test_prepare_subservice__class(self, *, app):
        service = app._prepare_subservice(OtherService)
        assert service.loop is app.loop
        assert service.beacon.parent is app.beacon

    def test_label(self, *, app):
        assert label(app)

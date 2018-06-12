from mode import Service, label
from mode.utils.mocks import AsyncMock, Mock, call
from faust import App
from faust.exceptions import ImproperlyConfigured
import pytest


class OtherService(Service):
    ...


class test_AppService:

    @pytest.fixture
    def s(self, *, app):
        return app._service

    def test_on_init_dependencies(self, *, s):
        s._components_client = Mock(name='components_client')
        s._components_server = Mock(name='components_server')
        s.app.client_only = True
        assert s.on_init_dependencies() == s._components_client()

        s.app.client_only = False
        assert s.on_init_dependencies() == s._components_server()

    def test_components_client(self, *, s, app):
        assert list(s._components_client()) == [
            app.producer,
            app.consumer,
            app._reply_consumer,
            app.topics,
            app._fetcher,
        ]

    def test_components_server(self, *, s, app):
        components = list(s._components_server())
        expected_components = list(app.sensors)
        expected_components.extend([
            app.producer,
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
    async def test_on_first_start(self, *, s):
        s.app = Mock(
            name='app',
            autospec=App,
            on_first_start=AsyncMock(),
        )
        await s.on_first_start()

        s.app._create_directories.assert_called_once_with()
        s.app.on_first_start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_first_start__no_agents_raises_error(self, *, s):
        s.app = Mock(name='app', autospec=App)
        s.app.agents = {}
        with pytest.raises(ImproperlyConfigured):
            await s.on_first_start()

    @pytest.mark.asyncio
    async def test_on_start(self, *, s):
        s.app = Mock(
            name='app',
            autospec=App,
            on_start=AsyncMock(),
        )
        await s.on_start()

        s.app.finalize.assert_called_once_with()
        s.app.on_start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_started(self, *, s):
        s.wait_for_table_recovery_completed = AsyncMock(return_value=True)
        s.on_started_init_extra_tasks = AsyncMock(name='osiet')
        s.on_started_init_extra_services = AsyncMock(name='osies')
        s.app.on_started = AsyncMock(name='on_started')
        s.app.on_startup_finished = None
        s.wait_for_table_recovery_completed.coro.return_value = True
        await s.on_started()

        s.wait_for_table_recovery_completed.coro.return_value = False
        await s.on_started()

        s.on_started_init_extra_tasks.assert_called_once_with()
        s.on_started_init_extra_services.assert_called_once_with()
        s.app.on_started.assert_called_once_with()

        s.app.on_startup_finished = AsyncMock(name='on_startup_finished')
        await s.on_started()

        s.app.on_startup_finished.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_wait_for_table_recovery_completed(self, *, s):
        s.wait_for_stopped = AsyncMock(name='wait_for_stopped')
        await s.wait_for_table_recovery_completed()
        s.wait_for_stopped.assert_called_once_with(
            s.app.tables.recovery_completed)

    @pytest.mark.asyncio
    async def test_on_started_init_extra_tasks(self, *, s, app):
        s.add_future = Mock(name='add_future')

        t1_mock = Mock(name='t1_mock')
        t2_mock = Mock(name='t2_mock')

        def t1():
            return t1_mock()

        def t2(self):
            return t2_mock(self)

        s.app._tasks = [t1, t2]
        await s.on_started_init_extra_tasks()

        s.add_future.assert_has_calls([
            call(t1()),
            call(t2(app)),
        ])

    @pytest.mark.asyncio
    async def test_on_started_init_extra_services(self, *, s, app):
        s.add_runtime_dependency = AsyncMock(name='add_runtime_dependency')
        service1 = Mock(name='service1', autospec=Service)
        app._extra_services = [service1]
        s._extra_service_instances = None
        await s.on_started_init_extra_services()

        s.add_runtime_dependency.assert_called_once_with(service1)
        assert s._extra_service_instances == [service1]
        await s.on_started_init_extra_services()  # noop

    def test_prepare_subservice(self, *, s):
        service = OtherService()
        assert s._prepare_subservice(service) is service

    def test_prepare_subservice__class(self, *, s):
        service = s._prepare_subservice(OtherService)
        assert service.loop is s.loop
        assert service.beacon.parent is s.beacon

    @pytest.mark.asyncio
    async def test_on_restart(self, *, s, app):
        app.on_restart = AsyncMock(name='on_restart')
        await s.on_restart()
        app.on_restart.assert_called_once_with()

    def test_label(self, *, s):
        assert label(s)

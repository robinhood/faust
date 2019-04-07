import asyncio
import pytest
from faust.livecheck.exceptions import TestTimeout
from faust.livecheck.locals import current_execution_stack, current_test_stack
from faust.livecheck.models import SignalEvent
from faust.livecheck.signals import BaseSignal, Signal
from mode.utils.mocks import AsyncMock, Mock, patch


class test_BaseSignal:

    @pytest.fixture()
    def signal(self, *, case):
        return BaseSignal('foo', case, 1)

    @pytest.mark.asyncio
    async def test_send(self, *, signal):
        with pytest.raises(NotImplementedError):
            await signal.send('v', key='k')

    @pytest.mark.asyncio
    async def test_wait(self, *, signal):
        with pytest.raises(NotImplementedError):
            await signal.wait(key='k', timeout=10.0)

    @pytest.mark.asyncio
    async def test_resolve(self, *, signal):
        signal._set_current_value = Mock()
        signal._wakeup_resolvers = Mock()

        event = Mock()
        await signal.resolve('k', event)
        signal._set_current_value.assert_called_once_with('k', event)
        signal._wakeup_resolvers.assert_called_once_with()

    def test__set_name__(self, *, signal):
        signal.name = ''
        signal.__set_name__(type(signal), 'foo')
        assert signal.name == 'foo'
        signal.__set_name__(type(signal), 'bar')
        assert signal.name == 'foo'

    def test__wakeup_resolvers(self, *, signal):
        signal.case = Mock()
        signal._wakeup_resolvers()
        signal.case.app._can_resolve.set.assert_called_once_with()

    @pytest.mark.asyncio
    async def test__wait_for_resolved(self, *, signal):
        signal.case = Mock(app=Mock(wait=AsyncMock()))
        await signal._wait_for_resolved(timeout=3.01)
        signal.case.app.wait.assert_called_once_with(
            signal.case.app._can_resolve, timeout=3.01)
        signal.case.app._can_resolve.clear.assert_called_once_with()

    def test__get_current_value(self, *, signal):
        signal._set_current_value('k', 'foo')
        assert signal._get_current_value('k') == 'foo'

    def test__index_key(self, *, signal):
        assert signal._index_key('k') == (signal.name, signal.case.name, 'k')

    def test_repr(self, *, signal):
        assert repr(signal)


class test_Signal:

    @pytest.fixture()
    def signal(self, *, case):
        return Signal('foo', case, 1)

    @pytest.mark.asyncio
    async def test_send__no_test(self, *, signal):
        assert current_test_stack.top is None
        signal.case = Mock(app=Mock(bus=Mock(send=AsyncMock())))
        await signal.send('value', key='k', force=False)
        signal.case.app.bus.send.assert_not_called()

    @pytest.mark.asyncio
    async def test_send__no_test_force(self, *, signal):
        signal.case = Mock(app=Mock(bus=Mock(send=AsyncMock())))
        await signal.send('value', key='k', force=True)
        signal.case.app.bus.send.coro.assert_called_once_with(
            key='k',
            value=SignalEvent(
                signal_name=signal.name,
                case_name=signal.case.name,
                key='k',
                value='value',
            ),
        )

    @pytest.mark.asyncio
    async def test_send(self, *, signal, execution):
        with current_test_stack.push(execution):
            signal.case = Mock(app=Mock(bus=Mock(send=AsyncMock())))
            await signal.send('value', key=None, force=True)
            signal.case.app.bus.send.coro.assert_called_once_with(
                key=execution.id,
                value=SignalEvent(
                    signal_name=signal.name,
                    case_name=signal.case.name,
                    key=execution.id,
                    value='value',
                ),
            )

    @pytest.mark.asyncio
    async def test_wait__no_current_execution(self, *, signal):
        assert current_execution_stack.top is None
        with pytest.raises(RuntimeError):
            await signal.wait(timeout=1.01)

    @pytest.mark.asyncio
    async def test_wait(self, *, signal, runner):
        runner.on_signal_wait = AsyncMock()
        signal._wait_for_message_by_key = AsyncMock()
        runner.on_signal_received = AsyncMock()
        signal._verify_event = Mock()
        with current_execution_stack.push(runner):
            await signal.wait(timeout=2.02)
        signal._verify_event.assert_called_once()

    def test__verify_event(self, *, signal):
        ev = Mock()
        ev.key = 'key'
        ev.signal_name = 'name'
        ev.case_name = 'case'
        signal._verify_event(ev, 'key', 'name', 'case')

    @pytest.mark.asyncio
    async def test__wait_for_message_by_key__already_there(self, *, signal):
        signal._get_current_value = Mock(return_value='foo')
        ret = await signal._wait_for_message_by_key('k', timeout=1.0)
        assert ret == 'foo'

    @pytest.mark.asyncio
    async def test__wait_for_message_by_key__should_stop(self, *, signal):
        signal._get_current_value = Mock(side_effect=KeyError())
        app = signal.case.app
        app._stopped.set()
        with pytest.raises(asyncio.CancelledError):
            await signal._wait_for_message_by_key('k', timeout=1.0)

    @pytest.mark.asyncio
    async def test__wait_for_message_by_key(self, *, signal):
        signal._get_current_value = Mock(side_effect=KeyError())
        signal._wait_for_resolved = AsyncMock()

        async def on_resolved(**kwargs):
            if signal._wait_for_resolved.call_count >= 3:
                signal._get_current_value.side_effect = None
                signal._get_current_value.return_value = 30
        signal._wait_for_resolved.side_effect = on_resolved
        res = await signal._wait_for_message_by_key('k', timeout=1.0)
        assert res == 30

    @pytest.mark.asyncio
    async def test__wait_for_message_by_key__no_rem(self, *, signal):
        can_return = [False]
        signal._get_current_value = Mock()

        def on_get_value(k):
            if can_return[0]:
                signal._get_current_value.side_effect = None
                signal._get_current_value.return_value = 30
            raise KeyError()
        signal._get_current_value.side_effect = on_get_value

        with patch('faust.livecheck.signals.monotonic') as monotonic:
            monotonic.return_value = 111.1
            signal._wait_for_resolved = AsyncMock()

            async def on_resolved(**kwargs):
                if signal._wait_for_resolved.call_count >= 10:
                    monotonic.return_value = 333.3
                    can_return[0] = True

            signal._wait_for_resolved.side_effect = on_resolved
            res = await signal._wait_for_message_by_key('k', timeout=1.0)
            assert res == 30

    @pytest.mark.asyncio
    async def test__wait_for_message_by_key__no_timeout(self, *, signal):
        can_return = [False]
        signal._get_current_value = Mock()

        def on_get_value(k):
            if can_return[0]:
                signal._get_current_value.side_effect = None
                signal._get_current_value.return_value = 30
            raise KeyError()
        signal._get_current_value.side_effect = on_get_value

        with patch('faust.livecheck.signals.monotonic') as monotonic:
            monotonic.return_value = 111.1
            signal._wait_for_resolved = AsyncMock()

            async def on_resolved(**kwargs):
                if signal._wait_for_resolved.call_count >= 10:
                    monotonic.return_value = 333.3
                    can_return[0] = True

            signal._wait_for_resolved.side_effect = on_resolved
            res = await signal._wait_for_message_by_key('k', timeout=None)
            assert res == 30

    @pytest.mark.asyncio
    async def test__wait_for_message_by_key__timeout(self, *, signal):
        can_return = [False]
        signal._get_current_value = Mock(side_effect=KeyError())

        with patch('faust.livecheck.signals.monotonic') as monotonic:
            monotonic.return_value = 111.1
            signal._wait_for_resolved = AsyncMock()

            async def on_resolved(**kwargs):
                if signal._wait_for_resolved.call_count >= 10:
                    monotonic.return_value = 333.3
                    can_return[0] = True

            signal._wait_for_resolved.side_effect = on_resolved
            with pytest.raises(TestTimeout):
                await signal._wait_for_message_by_key('k', timeout=1.0)

    @pytest.mark.asyncio
    async def test__wait_for_message_by_key__app_stopped(self, *, signal):
        signal._get_current_value = Mock(side_effect=KeyError())

        with patch('faust.livecheck.signals.monotonic') as monotonic:
            monotonic.return_value = 111.1
            app = signal.case.app
            signal._wait_for_resolved = AsyncMock()

            async def on_resolved(**kwargs):
                app._stopped.set()

            signal._wait_for_resolved.side_effect = on_resolved
            with pytest.raises(asyncio.CancelledError):
                await signal._wait_for_message_by_key('k', timeout=1.0)

import asyncio
import pytest
from mode.utils.mocks import ANY, AsyncMock, Mock, patch
from faust.livecheck.exceptions import (
    LiveCheckError,
    TestFailed,
    TestRaised,
    TestSkipped,
    TestTimeout,
)
from faust.livecheck.models import State


class test_TestRunner:

    @pytest.mark.asyncio
    async def test_execute__case_inactive(self, *, runner, execution):
        with pytest.raises(TestSkipped):
            await self._do_execute(runner, execution, active=False)

    @pytest.mark.asyncio
    async def test_execute__test_expired(self, *, runner, execution):
        with pytest.raises(TestSkipped):
            await self._do_execute(runner, execution, expired=True)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('exc,raises,callback', [
        (TestSkipped('foo'), None, 'on_skipped'),
        (TestTimeout('bar'), None, 'on_timeout'),
        (AssertionError('baz'), TestFailed, 'on_failed'),
        (LiveCheckError('xuz'), None, 'on_error'),
        (KeyError('muz'), TestRaised, 'on_error'),
    ])
    async def test_execute__error_callbacks(self, exc, raises, callback, *,
                                            runner, execution):
        if raises is None:
            raises = type(exc)

        cb = AsyncMock()
        setattr(runner, callback, cb)
        runner.on_pass = AsyncMock()

        with pytest.raises(raises):
            await self._do_execute(
                runner, execution,
                active=True,
                expired=False,
                side_effect=exc,
            )
        cb.assert_called_once_with(exc)
        runner.on_pass.assert_not_called()

    @pytest.mark.asyncio
    async def test_execute__pass(self, *, runner, execution):
        runner.on_pass = AsyncMock()
        await self._do_execute(runner, execution)
        runner.on_pass.coro.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_execute__CancelledError(self, *, runner, execution):
        await self._do_execute(runner, execution,
                               side_effect=asyncio.CancelledError())

    async def _do_execute(self, runner, execution,
                          active=True, expired=False, side_effect=None):
        runner.case.active = active
        runner.test = execution
        runner.test.is_expired = expired
        runner.case.run = AsyncMock()
        if side_effect:
            runner.case.run.side_effect = side_effect
        await runner.execute()

    @pytest.mark.asyncio
    async def test_skip(self, runner):
        with pytest.raises(TestSkipped):
            runner.on_skipped = AsyncMock()
            await runner.skip('broken')
            runner.on_skipped.coro.assert_called_once_with(ANY)

    def test__prepare_args(self, *, runner):
        assert runner._prepare_args((1, 2, 3, object()))

    def test__prepare_kwargs(self, *, runner):
        assert runner._prepare_kwargs({
            'foo': object(),
            'bar': 1,
            'baz': 1.03,
        })

    @pytest.mark.asyncio
    async def test_on_skipped(self, *, runner):
        runner.case.on_test_skipped = AsyncMock()
        runner.state = State.PASS
        exc = TestSkipped()
        await runner.on_skipped(exc)
        assert runner.state == State.SKIP
        runner.case.on_test_skipped.assert_called_once_with(runner)

    @pytest.mark.asyncio
    async def test_on_start(self, *, runner):
        runner.case.on_test_start = AsyncMock()
        await runner.on_start()
        runner.case.on_test_start.assert_called_once_with(runner)

    @pytest.mark.asyncio
    async def test_on_signal_wait(self, *, runner):
        signal = Mock()
        signal.name = 'foo'
        signal.index = 2
        await runner.on_signal_wait(signal, 30.0)

    @pytest.mark.asyncio
    async def test_on_signal_received(self, *, runner):
        signal = Mock()
        signal.name = 'order_in_db'
        await runner.on_signal_received(signal, 100.0, 200.0)
        assert runner.signal_latency[signal.name] == 100.0

    @pytest.mark.asyncio
    async def test_on_failed(self, *, runner):
        runner.error = KeyError()
        runner.case.on_test_failed = AsyncMock()
        runner._finalize_report = AsyncMock()

        exc = TestFailed('foo the bar')
        await runner.on_failed(exc)

        assert runner.error is exc
        assert runner.state == State.FAIL
        runner.case.on_test_failed.coro.assert_called_once_with(runner, exc)
        runner._finalize_report.coro.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_error(self, *, runner):
        runner.error = KeyError()
        runner.case.on_test_error = AsyncMock()
        runner._finalize_report = AsyncMock()

        exc = TestRaised('foo the bar')
        await runner.on_error(exc)

        assert runner.error is exc
        assert runner.state == State.ERROR
        runner.case.on_test_error.coro.assert_called_once_with(runner, exc)
        runner._finalize_report.coro.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_timeout(self, *, runner):
        runner.error = KeyError()
        runner.case.on_test_timeout = AsyncMock()
        runner._finalize_report = AsyncMock()

        exc = TimeoutError('foo the bar')
        await runner.on_timeout(exc)

        assert runner.error is exc
        assert runner.state == State.TIMEOUT
        runner.case.on_test_timeout.coro.assert_called_once_with(runner, exc)
        runner._finalize_report.coro.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_pass(self, *, runner):
        runner.error = KeyError()
        runner.case.on_test_pass = AsyncMock()
        runner._finalize_report = AsyncMock()
        await runner.on_pass()

        assert runner.state == State.PASS
        assert runner.error is None

        runner.case.on_test_pass.coro.assert_called_once_with(runner)
        runner._finalize_report.coro.assert_called_once_with()

    @pytest.mark.asyncio
    async def test__finalize_report(self, *, runner):
        exc = None
        try:
            raise KeyError('foo')
        except KeyError as e:
            exc = e
        runner.error = exc
        runner.case.post_report = AsyncMock()
        runner.state = State.ERROR
        runner.runtime = 3.011
        await runner._finalize_report()

        report = runner.report
        assert report.case_name == runner.case.name
        assert report.state == State.ERROR
        assert report.test == runner.test
        assert report.runtime == runner.runtime
        assert report.signal_latency == {}
        assert report.error == str(exc)
        assert report.traceback
        runner.case.post_report.coro.assert_called_once_with(report)

    @pytest.mark.asyncio
    async def test__finalize_report__no_error(self, *, runner):
        runner.error = None
        runner.case.post_report = AsyncMock()
        runner.state = State.ERROR
        runner.runtime = 3.011
        await runner._finalize_report()

        report = runner.report
        assert report.case_name == runner.case.name
        assert report.state == State.ERROR
        assert report.test == runner.test
        assert report.runtime == runner.runtime
        assert report.signal_latency == {}
        assert report.error is None
        assert report.traceback is None
        runner.case.post_report.coro.assert_called_once_with(report)

    def test_log_info(self, *, runner):
        runner.case.realtime_logs = False
        runner.log_info('msg %r', 1)
        assert runner.logs == [('msg %r', (1,))]

    def test_log_info__realtime(self, *, runner):
        runner.case.realtime_logs = True
        runner.log = Mock()
        runner.log_info('msg %r', 1)
        runner.log.info.assert_called_once_with('msg %r', 1)

    def test_end(self, *, runner):
        with patch('faust.livecheck.runners.monotonic') as monotonic:
            monotonic.return_value = 100.0
            runner.started = 50.0
            runner.end()

            assert runner.ended == 100.0
            assert runner.runtime == 50.0

    def test__flush_logs(self, *, runner):
        runner.logs = [
            ('foo %r bar', (10,)),
            ('The test %r raised %r', ('foo', 'error')),
        ]
        runner.log = Mock()
        runner._flush_logs()
        assert runner.log.logger.log.call_count == 1
        assert 'The test' in runner.log.logger.log.call_args[0][1]
        assert 'foo 10 bar' in runner.log.logger.log.call_args[0][1]
        assert not runner.logs

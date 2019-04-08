from datetime import datetime, timedelta, timezone
import pytest
from mode.utils.mocks import ContextMock, patch
from faust.livecheck.models import TestExecution
from faust.livecheck.runners import TestRunner


@pytest.fixture()
def livecheck(*, app):
    return app.LiveCheck()


@pytest.fixture()
def execution():
    now = datetime.now().astimezone(timezone.utc)
    expires = now + timedelta(hours=3)
    return TestExecution(
        id='id',
        case_name='t.examples.test_foo',
        timestamp=now,
        test_args=('foo',),
        test_kwargs={'kw1': 1.03},
        expires=expires,
    )


@pytest.fixture()
def case(*, livecheck):
    @livecheck.case()
    class test_foo(livecheck.Case):

        async def run(self, arg1, kw1=None):
            assert arg1 == 'foo'
            assert kw1 == 1.03
            assert True
    return test_foo


@pytest.fixture()
def runner(*, execution, case):
    return TestRunner(case, execution, started=100.0)


@pytest.yield_fixture()
def current_test_stack():
    with patch('faust.livecheck.case.current_test_stack') as cts:
        cts.push = ContextMock()
        yield cts


@pytest.yield_fixture()
def current_execution_stack():
    with patch('faust.livecheck.case.current_execution_stack') as ces:
        ces.push = ContextMock()
        yield ces

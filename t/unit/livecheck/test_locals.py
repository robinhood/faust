from mode.utils.mocks import Mock
from faust.livecheck.locals import current_execution, current_execution_stack


def test_current_execution():
    m1 = Mock(name='m1')
    assert current_execution() is None
    with current_execution_stack.push(m1):
        assert current_execution() is m1
        m2 = Mock(name='m2')
        with current_execution_stack.push(m2):
            assert current_execution() is m2
            m3 = Mock(name='m3')
            with current_execution_stack.push(m3):
                assert current_execution() is m3
            assert current_execution() is m2
        assert current_execution() is m1
    assert current_execution() is None

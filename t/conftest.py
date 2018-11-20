import asyncio
import time
from typing import NamedTuple
import pytest
from mode.utils.mocks import MagicMock, patch

sentinel = object()


@pytest.fixture()
def patching(monkeypatch, request):
    """Monkeypath.setattr shortcut.

    Example:
        .. sourcecode:: python

        def test_foo(patching):
            # execv value here will be mock.MagicMock by default.
            execv = patching('os.execv')

            patching('sys.platform', 'darwin')  # set concrete value
            patching.setenv('DJANGO_SETTINGS_MODULE', 'x.settings')

            # val will be of type mock.MagicMock by default
            val = patching.setitem('path.to.dict', 'KEY')
    """
    return _patching(monkeypatch, request)


@pytest.fixture()
def loop():
    return asyncio.get_event_loop()


class _patching(object):

    def __init__(self, monkeypatch, request):
        self.monkeypatch = monkeypatch
        self.request = request

    def __getattr__(self, name):
        return getattr(self.monkeypatch, name)

    def __call__(self, path, value=sentinel, name=None,
                 new=MagicMock, **kwargs):
        value = self._value_or_mock(value, new, name, path, **kwargs)
        self.monkeypatch.setattr(path, value)
        return value

    def _value_or_mock(self, value, new, name, path, **kwargs):
        if value is sentinel:
            value = new(name=name or path.rpartition('.')[2])
        for k, v in kwargs.items():
            setattr(value, k, v)
        return value

    def setattr(self, target, name=sentinel, value=sentinel, **kwargs):
        # alias to __call__ with the interface of pytest.monkeypatch.setattr
        if value is sentinel:
            value, name = name, None
        return self(target, value, name=name)

    def setitem(self, dic, name, value=sentinel, new=MagicMock, **kwargs):
        # same as pytest.monkeypatch.setattr but default value is MagicMock
        value = self._value_or_mock(value, new, name, dic, **kwargs)
        self.monkeypatch.setitem(dic, name, value)
        return value


class TimeMarks(NamedTuple):
    time: float = None
    monotonic: float = None


@pytest.yield_fixture()
def freeze_time(event_loop, request):
    marks = request.node.get_closest_marker('time')
    timestamp = time.time()
    monotimestamp = time.monotonic()

    with patch('time.time') as time_:
        with patch('time.monotonic') as monotonic_:
            options = TimeMarks(**{
                **{'time': timestamp,
                   'monotonic': monotimestamp},
                **((marks.kwargs or {}) if marks else {}),
            })
            time_.return_value = options.time
            monotonic_.return_value = options.monotonic
            yield options

import logging as _logging
import os
import pytest
import faust
from copy import copy
from typing import Dict, IO, NamedTuple, Union
from faust.web.cache.backends.memory import CacheStorage
from faust.utils.tracing import set_current_span
from mode.utils.logging import setup_logging
from mode.utils.mocks import AsyncMock, Mock


class AppMarks(NamedTuple):
    name: str = 'funtest'
    store: str = 'memory://'
    cache: str = 'memory://'


def create_appmarks(name='funtest',
                    store='memory://',
                    cache='memory://',
                    **rest):
    options = AppMarks(
        name=name,
        store=store,
        cache=cache,
    )
    return options, rest


@pytest.yield_fixture()
def app(event_loop, request):
    os.environ.pop('F_DATADIR', None)
    os.environ.pop('FAUST_DATADIR', None)
    os.environ.pop('F_WORKDIR', None)
    os.environ.pop('FAUST_WORKDIR', None)
    marks = request.node.get_closest_marker('app')
    options, rest = create_appmarks(
        **((marks.kwargs or {}) if marks else {}))
    app = faust.App(
        options.name,
        store=options.store,
        cache=options.cache,
        **rest,
    )
    app.finalize()
    set_current_span(None)
    try:
        yield app
    finally:
        assert app.tracer is None


@pytest.fixture()
def web(app):
    app.web.init_server()
    return app.web


class LoggingMarks(NamedTuple):
    logfile: Union[str, IO] = None
    loglevel: Union[str, int] = 'info'
    logging_config: Dict = None


@pytest.yield_fixture()
def logging(request):
    marks = request.node.get_closest_marker('logging')
    options = LoggingMarks(**{
        **{'logfile': None,
           'loglevel': 'info',
           'logging_config': None},
        **((marks.kwargs or {}) if marks else {}),
    })
    _logging._acquireLock()
    try:
        prev_state = copy(_logging.Logger.manager.loggerDict)
        prev_handlers = copy(_logging.root.handlers)
    finally:
        _logging._releaseLock()
    try:
        setup_logging(
            logfile=options.logfile,
            loglevel=options.loglevel,
            logging_config=options.logging_config,
        )
        yield
    finally:
        _logging._acquireLock()
        try:
            _logging.Logger.manager.loggerDict = prev_state
            _logging.root.handlers = prev_handlers
        finally:
            _logging._releaseLock()


@pytest.fixture()
def mocked_redis(*, event_loop, monkeypatch):
    import aredis

    storage = CacheStorage()

    client_cls = Mock(
        name='StrictRedis',
        return_value=Mock(
            autospec=aredis.StrictRedis,
            ping=AsyncMock(),
            get=AsyncMock(side_effect=storage.get),
            set=AsyncMock(side_effect=storage.set),
            setex=AsyncMock(side_effect=storage.setex),
            delete=AsyncMock(side_effect=storage.delete),
            ttl=AsyncMock(side_effect=storage.ttl),
        ),
    )
    client_cls.storage = storage
    monkeypatch.setattr('aredis.StrictRedis', client_cls)
    return client_cls


def pytest_configure(config):
    config.addinivalue_line(
        'markers', 'app: App instance to use for tests',
    )
    config.addinivalue_line(
        'markers', 'logging: Configure logging setup to use for tests',
    )

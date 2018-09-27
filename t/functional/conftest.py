import faust
import logging as _logging
import pytest
from copy import copy
from typing import Dict, IO, NamedTuple, Union
from mode.utils.logging import setup_logging


class AppMarks(NamedTuple):
    name: str = 'funtest'
    store: str = 'memory://'
    cache: str = 'memory://'


@pytest.fixture()
def app(event_loop, request):
    marks = request.node.get_marker('app')
    options = AppMarks(**{
        **{'name': 'funtest',
           'store': 'memory://',
           'cache': 'memory://'},
        **((marks.kwargs or {}) if marks else {}),
    })
    app = faust.App(
        options.name,
        store=options.store,
        cache=options.cache,
    )
    app.finalize()
    return app


class LoggingMarks(NamedTuple):
    logfile: Union[str, IO] = None
    loglevel: Union[str, int] = 'info'
    logging_config: Dict = None


@pytest.yield_fixture()
def logging(request):
    marks = request.node.get_marker('logging')
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

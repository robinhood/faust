import faust
import logging as _logging
import pytest
from copy import copy
from typing import Dict, IO, NamedTuple, Union
from mode.utils.logging import setup_logging


@pytest.fixture()
def app():
    app = faust.App('funtest', store='memory://')
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

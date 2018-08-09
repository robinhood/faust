import asyncio
import logging
import socket
from typing import Any
import faust
from mode import Service
from .app import get_error_topic
from .models import Error


class LogPusher(Service):

    app: faust.App
    queue: asyncio.Queue

    def __init__(self, app: faust.App, **kwargs: Any) -> None:
        self.app = app
        self.queue = asyncio.Queue()
        super().__init__(**kwargs)

    def put(self, error: Error) -> None:
        self.queue.put_nowait(error)

    @Service.task
    async def _flush(self) -> None:
        app = self.app
        queue = self.queue
        error_topic = get_error_topic(app)
        while not self.should_stop:
            error = await queue.get()
            error.app_id = app.conf.id
            await error_topic.send(value=error)


class LogHandler(logging.Handler):

    app: faust.App

    def __init__(self, app: faust.App, *args: Any, **kwargs: Any) -> None:
        self.app = app
        super().__init__(*args, **kwargs)

    def emit(self, record: logging.LogRecord) -> None:
        self.format(record)
        return self._emit(record)

    def _emit(self, record: logging.LogRecord) -> None:
        no_alert = getattr(record, 'no_alert', False)
        if not no_alert and record.levelno >= logging.ERROR:
            stack = getattr(record, 'stack', None)
            traceback = None
            if stack is True:
                traceback = '\n'.join(traceback.format_stack())
            error = Error(
                message=record.getMessage(),
                format=str(record.msg),
                traceback=traceback,
                logger=record.name,
                hostname=socket.gethostname(),
                filename=record.filename,
                module=record.module,
                lineno=record.lineno,
                severity=record.levelname,
                timestamp=record.created,
            )
            self.app.logpusher.put(error)

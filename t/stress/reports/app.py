import socket
import faust
from . import states
from .models import Error, Status

REPORT_TOPIC_NAME = 'f-systemcheck'
ERRORS_TOPIC_NAME = 'f-errors'


def get_reporting_topic(app):
    return app.topic(REPORT_TOPIC_NAME, value_type=Status)


def get_error_topic(app):
    return app.topic(ERRORS_TOPIC_NAME, value_type=Error)


async def send_update(app, status: Status):
    return await get_reporting_topic(app).send(value=status)


async def send_simple_good_status(app, category: str,
                                  state: str = states.OK,
                                  color: str = 'green',
                                  count: int = 1,
                                  severity: str = 'INFO') -> None:
    status = Status(
        app_id=app.conf.id,
        hostname=socket.gethostname(),
        category=category,
        state=state,
        color=color,
        count=count,
        severity=severity,

    )
    await send_update(app, status)


async def send_simple_bad_status(app, category: str,
                                 state: str = states.STALL,
                                 color: str = 'red',
                                 count: int = 1,
                                 severity: str = 'CRIT') -> None:
    status = Status(
        app_id=app.conf.id,
        hostname=socket.gethostname(),
        category=category,
        state=state,
        color=color,
        count=count,
        severity=severity,

    )
    await send_update(app, status)


class SimpleCheck:

    def __init__(self, category: str) -> None:
        self.category: str = category
        self.failed: int = 0

    async def send_ok(self, app) -> None:
        self.failed = 0
        await send_simple_good_status(app, self.category)

    async def send_fail(self, app) -> None:
        self.failed += 1
        await send_simple_bad_status(app, self.category, count=self.failed)


class DashboardApp(faust.App):

    def on_webserver_init(self, web):
        from . import assets
        web.add_static('/dashboard/assets/', assets.get_path())


def get_reporting_app() -> DashboardApp:
    from ..app import create_app
    return create_app(
        'f-stress-systemcheck',
        base=DashboardApp,
        origin='t.stress.reports',
    )

import faust
from .models import Status

TOPIC_NAME = 'f-systemcheck'


def get_reporting_topic(app):
    return app.topic(TOPIC_NAME, value_type=Status)


async def send_update(app, status: Status):
    return await get_reporting_topic(app).send(value=status)


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

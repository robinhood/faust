from .models import Status

TOPIC_NAME = 'f-systemcheck'


def get_reporting_topic(app):
    return app.topic(TOPIC_NAME, value_type=Status)


async def send_update(app, status: Status):
    return await get_reporting_topic(app).send(value=status)


def get_reporting_app():
    from ..app import create_app
    return create_app(
        'f-stress-systemcheck',
        origin='t.stress.reports',
    )

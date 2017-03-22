import asyncio
import pytest
from case import Mock
from faust import App, Event, topic

test_topic = topic('test')


class Record(Event, serializer='json'):
    amount: float


@pytest.fixture
def app():
    instance = App('testid')
    instance.producer = Mock(name='producer')
    return instance


def done_future(result=True):
    f = asyncio.Future()
    f.set_result(result)
    return f


def setup_producer(app):
    app.producer.start.return_value = done_future()
    app.producer.send.return_value = done_future()
    app.producer.send_and_wait.return_value = done_future()


@pytest.mark.asyncio
async def test_send(app):
    event = Record(amount=0.0)
    setup_producer(app)
    await app.send(test_topic, 'key', event, wait=True)
    app.producer.send_and_wait.assert_called_with(
        test_topic.topics[0], b'key', event.dumps(),
    )
    app.producer.send.assert_not_called()
    await app.send(test_topic, 'key', event, wait=False)
    app.producer.send.assert_called_with(
        test_topic.topics[0], b'key', event.dumps(),
    )


import pytest
from faust import Event, Stream, Table, Topic
from faust.sensors import Sensor
from faust.transport.consumer import Consumer
from faust.transport.producer import Producer
from faust.types import Message, TP
from mode.utils.mocks import Mock

TP1 = TP('foo', 0)


@pytest.fixture
def message():
    return Mock(name='message', autospec=Message)


@pytest.fixture
def stream():
    return Mock(name='stream', autospec=Stream)


@pytest.fixture
def event():
    return Mock(name='event', autospec=Event)


@pytest.fixture
def topic():
    return Mock(name='topic', autospec=Topic)


@pytest.fixture
def table():
    return Mock(name='table', autospec=Table)


@pytest.fixture
def consumer():
    return Mock(name='consumer', autospec=Consumer)


@pytest.fixture
def producer():
    return Mock(name='producer', autospec=Producer)


class test_Sensor:

    @pytest.fixture
    def sensor(self, *, app):
        return Sensor()

    def test_on_message_in(self, *, sensor, message):
        sensor.on_message_in(TP1, 3, message)

    def test_on_stream_event_in(self, *, sensor, stream, event):
        sensor.on_stream_event_in(TP1, 3, stream, event)

    def test_on_stream_event_out(self, *, sensor, stream, event):
        sensor.on_stream_event_out(TP1, 3, stream, event)

    def test_on_message_out(self, *, sensor, message):
        sensor.on_message_out(TP1, 3, message)

    def test_on_topic_buffer_full(self, *, sensor, topic):
        sensor.on_topic_buffer_full(topic)

    def test_on_table_get(self, *, sensor, table):
        sensor.on_table_get(table, 'key')

    def test_on_table_set(self, *, sensor, table):
        sensor.on_table_set(table, 'key', 'value')

    def test_on_table_del(self, *, sensor, table):
        sensor.on_table_del(table, 'key')

    def test_on_commit_initiated(self, *, sensor, consumer):
        sensor.on_commit_initiated(consumer)

    def test_on_commit_completed(self, *, sensor, consumer):
        sensor.on_commit_completed(consumer, Mock(name='state'))

    def test_on_send_initiated(self, *, sensor, producer):
        sensor.on_send_initiated(producer, 'topic', 'message', 30, 40)

    def test_on_send_completed(self, *, sensor, producer):
        sensor.on_send_completed(
            producer, Mock(name='state'), Mock(name='metadata'))

    def test_on_send_error(self, *, sensor, producer):
        sensor.on_send_error(
            producer, KeyError('foo'), Mock(name='state'))

    def test_asdict(self, *, sensor):
        assert sensor.asdict() == {}


class test_SensorDelegate:

    @pytest.fixture
    def sensor(self):
        return Mock(name='sensor', autospec=Sensor)

    @pytest.fixture
    def sensors(self, *, app, sensor):
        sensors = app.sensors
        sensors.add(sensor)
        return sensors

    def test_remove(self, *, sensors, sensor):
        assert list(iter(sensors))
        sensors.remove(sensor)
        assert not list(iter(sensors))

    def test_on_message_in(self, *, sensors, sensor, message):
        sensors.on_message_in(TP1, 303, message)
        sensor.on_message_in.assert_called_once_with(TP1, 303, message)

    def test_on_stream_event_in(self, *, sensors, sensor, stream, event):
        sensors.on_stream_event_in(TP1, 303, stream, event)
        sensor.on_stream_event_in.assert_called_once_with(
            TP1, 303, stream, event)

    def test_on_stream_event_out(self, *, sensors, sensor, stream, event):
        sensors.on_stream_event_out(TP1, 303, stream, event)
        sensor.on_stream_event_out.assert_called_once_with(
            TP1, 303, stream, event)

    def test_on_topic_buffer_full(self, *, sensors, sensor, topic):
        sensors.on_topic_buffer_full(topic)
        sensor.on_topic_buffer_full.assert_called_once_with(topic)

    def test_on_message_out(self, *, sensors, sensor, message):
        sensors.on_message_out(TP1, 303, message)
        sensor.on_message_out.assert_called_once_with(TP1, 303, message)

    def test_on_table_get(self, *, sensors, sensor, table):
        sensors.on_table_get(table, 'key')
        sensor.on_table_get.assert_called_once_with(table, 'key')

    def test_on_table_set(self, *, sensors, sensor, table):
        sensors.on_table_set(table, 'key', 'value')
        sensor.on_table_set.assert_called_once_with(table, 'key', 'value')

    def test_on_table_del(self, *, sensors, sensor, table):
        sensors.on_table_del(table, 'key')
        sensor.on_table_del.assert_called_once_with(table, 'key')

    def test_on_commit(self, *, sensors, sensor, consumer):
        state = sensors.on_commit_initiated(consumer)
        sensor.on_commit_initiated.assert_called_once_with(consumer)

        sensors.on_commit_completed(consumer, state)
        sensor.on_commit_completed.assert_called_once_with(
            consumer, state[sensor])

    def test_on_send(self, *, sensors, sensor, producer):
        metadata = Mock(name='metadata')
        state = sensors.on_send_initiated(
            producer, 'topic', 'message', 303, 606)
        sensor.on_send_initiated.assert_called_once_with(
            producer, 'topic', 'message', 303, 606)

        sensors.on_send_completed(producer, state, metadata)
        sensor.on_send_completed.assert_called_once_with(
            producer, state[sensor], metadata)

        exc = KeyError('foo')
        sensors.on_send_error(producer, exc, state)
        sensor.on_send_error.assert_called_once_with(
            producer, exc, state[sensor])

    def test_repr(self, *, sensors):
        assert repr(sensors)

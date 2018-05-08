import pytest
from faust.sensors import Sensor
from faust.types import TP
from mode.utils.mocks import Mock

TP1 = TP('foo', 0)


class test_Sensor:

    @pytest.fixture
    def sensor(self, *, app):
        return Sensor()

    def test_on_message_in(self, *, sensor):
        sensor.on_message_in(TP1, 3, Mock(name='message'))

    def test_on_stream_event_in(self, *, sensor):
        sensor.on_stream_event_in(
            TP1, 3, Mock(name='stream'), Mock(name='event'))

    def test_on_stream_event_out(self, *, sensor):
        sensor.on_stream_event_out(
            TP1, 3, Mock(name='stream'), Mock(name='event'))

    def test_on_message_out(self, *, sensor):
        sensor.on_message_out(TP1, 3, Mock(name='message'))

    def test_on_topic_buffer_full(self, *, sensor):
        sensor.on_topic_buffer_full(Mock(name='topic'))

    def test_on_table_get(self, *, sensor):
        sensor.on_table_get(Mock(name='table'), 'key')

    def test_on_table_set(self, *, sensor):
        sensor.on_table_set(Mock(name='table'), 'key', 'value')

    def test_on_table_del(self, *, sensor):
        sensor.on_table_del(Mock(name='table'), 'key')

    def test_on_commit_initiated(self, *, sensor):
        sensor.on_commit_initiated(Mock(name='consumer'))

    def test_on_commit_completed(self, *, sensor):
        sensor.on_commit_completed(Mock(name='consumer'), Mock(name='state'))

    def test_on_send_initiated(self, *, sensor):
        sensor.on_send_initiated(Mock(name='producer'), 'topic', 30, 40)

    def test_on_send_completed(self, *, sensor):
        sensor.on_send_completed(Mock(name='producer'), Mock(name='state'))


class test_SensorDelegate:

    @pytest.fixture
    def sensor(self):
        return Mock(name='sensor')

    @pytest.fixture
    def sensors(self, *, app, sensor):
        sensors = app.sensors
        sensors.add(sensor)
        return sensors

    def test_remove(self, *, sensors, sensor):
        assert list(iter(sensors))
        sensors.remove(sensor)
        assert not list(iter(sensors))

    def test_on_message_in(self, *, sensors, sensor):
        message = Mock(name='message')
        sensors.on_message_in(TP1, 303, message)
        sensor.on_message_in.assert_called_once_with(TP1, 303, message)

    def test_on_stream_event_in(self, *, sensors, sensor):
        stream = Mock(name='stream')
        event = Mock(name='event')
        sensors.on_stream_event_in(TP1, 303, stream, event)
        sensor.on_stream_event_in.assert_called_once_with(
            TP1, 303, stream, event)

    def test_on_stream_event_out(self, *, sensors, sensor):
        stream = Mock(name='stream')
        event = Mock(name='event')
        sensors.on_stream_event_out(TP1, 303, stream, event)
        sensor.on_stream_event_out.assert_called_once_with(
            TP1, 303, stream, event)

    def test_on_topic_buffer_full(self, *, sensors, sensor):
        topic = Mock(name='topic')
        sensors.on_topic_buffer_full(topic)
        sensor.on_topic_buffer_full.assert_called_once_with(topic)

    def test_on_message_out(self, *, sensors, sensor):
        message = Mock(name='message')
        sensors.on_message_out(TP1, 303, message)
        sensor.on_message_out.assert_called_once_with(TP1, 303, message)

    def test_on_table_get(self, *, sensors, sensor):
        table = Mock(name='table')
        sensors.on_table_get(table, 'key')
        sensor.on_table_get.assert_called_once_with(table, 'key')

    def test_on_table_set(self, *, sensors, sensor):
        table = Mock(name='table')
        sensors.on_table_set(table, 'key', 'value')
        sensor.on_table_set.assert_called_once_with(table, 'key', 'value')

    def test_on_table_del(self, *, sensors, sensor):
        table = Mock(name='table')
        sensors.on_table_del(table, 'key')
        sensor.on_table_del.assert_called_once_with(table, 'key')

    def test_on_commit(self, *, sensors, sensor):
        consumer = Mock(name='consumer')
        state = sensors.on_commit_initiated(consumer)
        sensor.on_commit_initiated.assert_called_once_with(consumer)

        sensors.on_commit_completed(consumer, state)
        sensor.on_commit_completed.assert_called_once_with(
            consumer, state[sensor])

    def test_on_send(self, *, sensors, sensor):
        producer = Mock(name='producer')
        state = sensors.on_send_initiated(producer, 'topic', 303, 606)
        sensor.on_send_initiated.assert_called_once_with(
            producer, 'topic', 303, 606)

        sensors.on_send_completed(producer, state)
        sensor.on_send_completed.assert_called_once_with(
            producer, state[sensor])

    def test_repr(self, *, sensors):
        assert repr(sensors)

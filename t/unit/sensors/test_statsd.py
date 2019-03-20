import pytest
from faust.exceptions import ImproperlyConfigured
from faust.sensors.statsd import StatsdMonitor
from faust.types import TP
from mode.utils.mocks import ANY, Mock, call

TP1 = TP('foo', 3)


class test_StatsdMonitor:

    @pytest.fixture()
    def statsd(self, *, monkeypatch):
        statsd = Mock(name='statsd')
        monkeypatch.setattr('faust.sensors.statsd.statsd', statsd)
        return statsd

    @pytest.fixture()
    def stream(self):
        stream = Mock(name='stream')
        stream.shortlabel = 'Stream: Topic: foo'
        return stream

    @pytest.fixture()
    def event(self):
        event = Mock(name='event')
        event.message.stream_meta = {}
        return event

    @pytest.fixture()
    def table(self):
        table = Mock(name='table')
        table.name = 'table1'
        return table

    @pytest.fixture()
    def mon(self, *, statsd):
        return StatsdMonitor()

    def test_statsd(self, *, mon):
        assert mon.client

    def test_raises_if_statsd_not_installed(self, *, monkeypatch):
        monkeypatch.setattr('faust.sensors.statsd.statsd', None)
        with pytest.raises(ImproperlyConfigured):
            StatsdMonitor()

    def test_on_message_in_out(self, *, mon):
        message = Mock(name='message')
        mon.on_message_in(TP1, 400, message)

        mon.client.incr.assert_has_calls([
            call('messages_received', rate=mon.rate),
            call('messages_active', rate=mon.rate),
            call('topic.foo.messages_received', rate=mon.rate),
        ])
        mon.client.gauge.assert_called_once_with('read_offset.foo.3', 400)

        mon.on_message_out(TP1, 400, message)
        mon.client.decr.assert_called_once_with(
            'messages_active', rate=mon.rate)

    def test_on_stream_event_in_out(self, *, mon, stream, event):
        mon.on_stream_event_in(TP1, 401, stream, event)
        mon.client.incr.assert_has_calls([
            call('events', rate=mon.rate),
            call('stream.topic_foo.events', rate=mon.rate),
            call('events_active', rate=mon.rate),
        ])
        mon.on_stream_event_out(TP1, 401, stream, event)
        mon.client.decr.assert_called_once_with('events_active', rate=mon.rate)
        mon.client.timing.assert_called_once_with(
            'events_runtime',
            mon._time(mon.events_runtime[-1]),
            rate=mon.rate,
        )

    def test_on_table_get(self, *, mon, table):
        mon.on_table_get(table, 'key')
        mon.client.incr.assert_called_once_with(
            'table.table1.keys_retrieved', rate=mon.rate,
        )

    def test_on_table_set(self, *, mon, table):
        mon.on_table_set(table, 'key', 'value')
        mon.client.incr.assert_called_once_with(
            'table.table1.keys_updated', rate=mon.rate,
        )

    def test_on_table_del(self, *, mon, table):
        mon.on_table_del(table, 'key')
        mon.client.incr.assert_called_once_with(
            'table.table1.keys_deleted', rate=mon.rate,
        )

    def test_on_commit_completed(self, *, mon):
        consumer = Mock(name='consumer')
        state = mon.on_commit_initiated(consumer)
        mon.on_commit_completed(consumer, state)
        mon.client.timing.assert_called_once_with(
            'commit_latency', ANY, rate=mon.rate,
        )

    def test_on_send_initiated_completed(self, *, mon):
        producer = Mock(name='producer')
        state = mon.on_send_initiated(
            producer, 'topic1', 'message', 321, 123)
        mon.on_send_completed(producer, state, Mock(name='metadata'))

        mon.client.incr.assert_has_calls([
            call('topic.topic1.messages_sent', rate=mon.rate),
            call('messages_sent', rate=mon.rate),
        ])
        mon.client.timing.assert_called_once_with(
            'send_latency', ANY, rate=mon.rate,
        )

        mon.on_send_error(producer, KeyError('foo'), state)
        mon.client.incr.assert_has_calls([
            call('messages_sent_error', rate=mon.rate),
        ])
        mon.client.timing.assert_has_calls([
            call('send_latency_for_error', ANY, rate=mon.rate),
        ])

    def test_count(self, *, mon):
        mon.count('metric_name', count=3)
        mon.client.incr.assert_called_once_with(
            'metric_name', count=3, rate=mon.rate)

    def test_on_tp_commit(self, *, mon):
        offsets = {
            TP('foo', 0): 1001,
            TP('foo', 1): 2002,
            TP('bar', 3): 3003,
        }
        mon.on_tp_commit(offsets)
        mon.client.gauge.assert_has_calls([
            call('committed_offset.foo.0', 1001),
            call('committed_offset.foo.1', 2002),
            call('committed_offset.bar.3', 3003),
        ])

    def test_track_tp_end_offsets(self, *, mon):
        mon.track_tp_end_offset(TP('foo', 0), 4004)
        mon.client.gauge.assert_called_once_with(
            'end_offset.foo.0', 4004,
        )

    def test__time(self, *, mon):
        assert mon._time(1.03) == 1030.0

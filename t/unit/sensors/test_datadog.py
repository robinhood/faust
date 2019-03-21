import sys
import pytest
from faust.exceptions import ImproperlyConfigured
from faust.types import TP
from mode.utils.contexts import nullcontext
from mode.utils.mocks import ANY, Mock, call

if sys.version_info >= (3, 7):
    _catch_warnings = pytest.warns(DeprecationWarning)
else:
    _catch_warnings = nullcontext()
with _catch_warnings:
    # XXX hopefully datadog fixes this DeprecationWarning soon:
    # "Using or importing the ABCs from 'collections' instead
    # of from 'collections.abc'"

    # Using pytest.warns on Python < 3.7 means we will be notified
    # when this no longer warns and datadog has fixed the issue [ask]
    from faust.sensors.datadog import (  # noqa
        DatadogMonitor,
        DatadogStatsClient,
    )

TP1 = TP('foo', 3)


@pytest.fixture()
def dogstatsd(*, monkeypatch):
    dogstatsd = Mock(name='dogstatsd')
    monkeypatch.setattr('faust.sensors.datadog.DogStatsd', dogstatsd)
    return dogstatsd


@pytest.fixture()
def statsd(*, monkeypatch):
    statsd = Mock(name='datadog')
    monkeypatch.setattr('faust.sensors.datadog.datadog', statsd)
    return statsd


class test_DatadogStatsClient:

    @pytest.fixture()
    def client(self, *, dogstatsd, statsd):
        return DatadogStatsClient()

    def test_incr(self, *, client):
        client.incr('metric', count=3)
        client.client.increment.assert_called_once_with(
            'metric', value=3, sample_rate=1.0, tags=None)

    def test_decr(self, *, client):
        client.decr('metric', count=3)
        client.client.decrement.assert_called_once_with(
            'metric', value=3, sample_rate=1.0, tags=None)

    def test_timed(self, *, client):
        t = client.timed('metric', {'l': 'v'}, True)
        assert t is client.client.timed.return_value
        client.client.timed.assert_called_once_with(
            metric='metric',
            tags=['l:v'],
            sample_rate=client.rate,
            use_ms=True,
        )

    def test_histogram(self, *, client):
        client.histogram('metric', 'value', {'l': 'v'})
        client.client.histogram.assert_called_once_with(
            'metric',
            tags=['l:v'],
            value='value',
            sample_rate=client.rate,
        )


class test_DatadogMonitor:

    @pytest.fixture()
    def mon(self, *, statsd, dogstatsd):
        mon = DatadogMonitor()
        return mon

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

    def increment_call(self, name, *args, **kwargs):
        return call(name, *args, **kwargs)

    def test_raises_if_datadog_not_installed(self, *, monkeypatch):
        monkeypatch.setattr('faust.sensors.datadog.datadog', None)
        with pytest.raises(ImproperlyConfigured):
            DatadogMonitor()

    def test_statsd(self, *, mon):
        assert mon.client.client

    def test_on_message_in_out(self, *, mon):
        message = Mock(name='message')
        mon.on_message_in(TP1, 400, message)

        client = mon.client.client
        client.increment.assert_has_calls([
            call('messages_received',
                 sample_rate=mon.rate,
                 tags=['topic:foo', 'partition:3'],
                 value=1.0),
            call('messages_active',
                 sample_rate=mon.rate,
                 tags=['topic:foo', 'partition:3'],
                 value=1.0),
        ])
        client.gauge.assert_called_once_with(
            'read_offset',
            sample_rate=1.0,
            tags=['topic:foo', 'partition:3'],
            value=400,
        )

        mon.on_message_out(TP1, 400, message)
        client.decrement.assert_called_once_with(
            'messages_active',
            sample_rate=mon.rate,
            tags=['topic:foo', 'partition:3'],
            value=1.0)

    def test_on_stream_event_in_out(self, *, mon, stream, event):
        mon.on_stream_event_in(TP1, 401, stream, event)
        client = mon.client.client
        client.increment.assert_has_calls([
            call('events',
                 sample_rate=mon.rate,
                 tags=['topic:foo', 'partition:3', 'stream:topic_foo'],
                 value=1.0),
            call('events_active',
                 sample_rate=mon.rate,
                 tags=['topic:foo', 'partition:3', 'stream:topic_foo'],
                 value=1.0),
        ])
        mon.on_stream_event_out(TP1, 401, stream, event)
        client.decrement.assert_called_once_with(
            'events_active',
            sample_rate=mon.rate,
            tags=['topic:foo', 'partition:3', 'stream:topic_foo'],
            value=1.0,
        )
        client.timing.assert_called_once_with(
            'events_runtime',
            value=mon._time(mon.events_runtime[-1]),
            sample_rate=mon.rate,
            tags=['topic:foo', 'partition:3', 'stream:topic_foo'],
        )

    def test_on_table_get(self, *, mon, table):
        mon.on_table_get(table, 'key')
        mon.client.client.increment.assert_called_once_with(
            'table_keys_retrieved',
            sample_rate=mon.rate,
            tags=['table:table1'],
            value=1.0,
        )

    def test_on_table_set(self, *, mon, table):
        mon.on_table_set(table, 'key', 'value')
        mon.client.client.increment.assert_called_once_with(
            'table_keys_updated',
            sample_rate=mon.rate,
            tags=['table:table1'],
            value=1.0,
        )

    def test_on_table_del(self, *, mon, table):
        mon.on_table_del(table, 'key')
        mon.client.client.increment.assert_called_once_with(
            'table_keys_deleted',
            sample_rate=mon.rate,
            tags=['table:table1'],
            value=1.0,
        )

    def test_on_commit_completed(self, *, mon):
        consumer = Mock(name='consumer')
        state = mon.on_commit_initiated(consumer)
        mon.on_commit_completed(consumer, state)
        client = mon.client.client
        client.timing.assert_called_once_with(
            'commit_latency', value=ANY, sample_rate=mon.rate, tags=None,
        )

    def test_on_send_initiated_completed(self, *, mon):
        producer = Mock(name='producer')
        state = mon.on_send_initiated(
            producer, 'topic1', 'message', 321, 123)
        mon.on_send_completed(producer, state, Mock(name='metadata'))

        client = mon.client.client
        client.increment.assert_has_calls([
            call('topic_messages_sent',
                 sample_rate=mon.rate,
                 tags=['topic:topic1'],
                 value=1.0),
            call('messages_sent', sample_rate=mon.rate, tags=None, value=1.0),
        ])
        client.timing.assert_called_once_with(
            'send_latency', value=ANY, sample_rate=mon.rate, tags=None,
        )

        mon.on_send_error(producer, KeyError('foo'), state)
        client.increment.assert_has_calls([
            call('messages_send_failed',
                 sample_rate=mon.rate,
                 tags=None,
                 value=1.0),
        ])
        client.timing.assert_has_calls([
            call('send_latency_for_error',
                 value=ANY, sample_rate=mon.rate, tags=None),
        ])

    def test_count(self, *, mon):
        mon.count('metric_name', count=3)
        mon.client.client.increment.assert_called_once_with(
            'metric_name', value=3, sample_rate=mon.rate, tags=None)

    def test_on_tp_commit(self, *, mon):
        offsets = {
            TP('foo', 0): 1001,
            TP('foo', 1): 2002,
            TP('bar', 3): 3003,
        }
        mon.on_tp_commit(offsets)
        client = mon.client.client
        client.gauge.assert_has_calls([
            call('committed_offset',
                 value=1001,
                 tags=['topic:foo', 'partition:0'],
                 sample_rate=mon.rate),
            call('committed_offset',
                 value=2002,
                 tags=['topic:foo', 'partition:1'],
                 sample_rate=mon.rate),
            call('committed_offset',
                 value=3003,
                 tags=['topic:bar', 'partition:3'],
                 sample_rate=mon.rate),
        ])

    def test_track_tp_end_offsets(self, *, mon):
        mon.track_tp_end_offset(TP('foo', 0), 4004)
        client = mon.client.client
        client.gauge.assert_called_once_with(
            'end_offset',
            value=4004,
            tags=['topic:foo', 'partition:0'],
            sample_rate=mon.rate,
        )

    def test__time(self, *, mon):
        assert mon._time(1.03) == 1030.0

import pytest
from faust.exceptions import ImproperlyConfigured
from faust.types import TP
from mode.utils.mocks import Mock, call

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

    @pytest.fixture
    def time(self):
        timefun = Mock(name='time()')
        timefun.return_value = 101.1
        return timefun

    @pytest.fixture()
    def mon(self, *, statsd, dogstatsd, time):
        mon = DatadogMonitor(time=time)
        return mon

    @pytest.fixture()
    def stream(self):
        stream = Mock(name='stream')
        stream.shortlabel = 'Stream: Topic: foo'
        return stream

    @pytest.fixture()
    def event(self):
        return Mock(name='event')

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
            call('topic_messages_received',
                 sample_rate=mon.rate,
                 tags=['topic:foo'],
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
        state = mon.on_stream_event_in(TP1, 401, stream, event)
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
        mon.on_stream_event_out(TP1, 401, stream, event, state)
        client.decrement.assert_called_once_with(
            'events_active',
            sample_rate=mon.rate,
            tags=['topic:foo', 'partition:3', 'stream:topic_foo'],
            value=1.0,
        )
        client.timing.assert_called_once_with(
            'events_runtime',
            value=mon.secs_to_ms(mon.events_runtime[-1]),
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
            'commit_latency',
            value=mon.ms_since(float(state)),
            sample_rate=mon.rate, tags=None,
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
            'send_latency',
            value=mon.ms_since(float(state)),
            sample_rate=mon.rate, tags=None,
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
                 value=mon.ms_since(float(state)),
                 sample_rate=mon.rate, tags=None),
        ])

    def test_on_assignment_start_completed(self, *, mon):
        assignor = Mock(name='assignor')
        state = mon.on_assignment_start(assignor)
        mon.on_assignment_completed(assignor, state)

        client = mon.client.client
        client.increment.assert_has_calls([
            call('assignments_complete',
                 sample_rate=mon.rate,
                 tags=None,
                 value=1.0),
        ])
        client.timing.assert_called_once_with(
            'assignment_latency',
            value=mon.ms_since(state['time_start']),
            sample_rate=mon.rate, tags=None,
        )

    def test_on_assignment_start_error(self, *, mon):
        assignor = Mock(name='assignor')
        state = mon.on_assignment_start(assignor)
        mon.on_assignment_error(assignor, state, KeyError())

        client = mon.client.client
        client.increment.assert_has_calls([
            call('assignments_error',
                 sample_rate=mon.rate,
                 tags=None,
                 value=1.0),
        ])
        client.timing.assert_called_once_with(
            'assignment_latency',
            value=mon.ms_since(state['time_start']),
            sample_rate=mon.rate,
            tags=None,
        )

    def test_on_web_request(self, *, mon):
        response = Mock(name='response')
        response.status = 404
        self.assert_on_web_request(mon, response, expected_status=404)

    def test_on_web_request__None_status(self, *, mon):
        self.assert_on_web_request(mon, None, expected_status=500)

    def assert_on_web_request(self, mon, response,
                              expected_status):
        app = Mock(name='app')
        request = Mock(name='request')
        view = Mock(name='view')
        client = mon.client.client

        state = mon.on_web_request_start(app, request, view=view)
        mon.on_web_request_end(app, request, response, state, view=view)

        client.increment.assert_called_once_with(
            f'http_status_code.{expected_status}',
            sample_rate=mon.rate,
            tags=None,
            value=1.0,
        )

        client.timing.assert_called_once_with(
            'http_response_latency',
            value=mon.ms_since(state['time_end']),
            sample_rate=mon.rate,
            tags=None,
        )

    def test_on_rebalance(self, *, mon):
        app = Mock(name='app')
        client = mon.client.client

        state = mon.on_rebalance_start(app)

        client.increment.assert_called_once_with(
            'rebalances',
            sample_rate=mon.rate,
            tags=None,
            value=1.0,
        )

        mon.on_rebalance_return(app, state)

        client.increment.assert_has_calls([
            call('rebalances',
                 sample_rate=mon.rate,
                 tags=None,
                 value=1.0),
            call('rebalances_recovering',
                 sample_rate=mon.rate,
                 tags=None,
                 value=1.0),
        ])
        client.decrement.assert_called_once_with(
            'rebalances',
            sample_rate=mon.rate,
            tags=None,
            value=1.0,
        )
        client.timing.assert_called_once_with(
            'rebalance_return_latency',
            value=mon.ms_since(state['time_return']),
            sample_rate=mon.rate,
            tags=None,
        )

        mon.on_rebalance_end(app, state)

        client.decrement.assert_has_calls([
            call('rebalances',
                 sample_rate=mon.rate,
                 tags=None,
                 value=1.0),
            call('rebalances_recovering',
                 sample_rate=mon.rate,
                 tags=None,
                 value=1.0),
        ])

        client.timing.assert_has_calls([
            call('rebalance_return_latency',
                 value=mon.ms_since(state['time_return']),
                 sample_rate=mon.rate,
                 tags=None),
            call('rebalance_end_latency',
                 value=mon.ms_since(state['time_end']),
                 sample_rate=mon.rate,
                 tags=None),
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

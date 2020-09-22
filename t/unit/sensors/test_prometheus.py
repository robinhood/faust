from unittest.mock import patch

import pytest
from faust import web
from faust.exceptions import ImproperlyConfigured
from faust.sensors.prometheus import PrometheusMonitor
from faust.types import TP
from mode.utils.mocks import Mock, call

TP1 = TP('foo', 3)


class test_PrometheusMonitor:

    def time(self):
        timefun = Mock(name='time()')
        timefun.return_value = 101.1
        return timefun

    @patch('faust.sensors.prometheus.Histogram')
    @patch('faust.sensors.prometheus.Gauge')
    @patch('faust.sensors.prometheus.Counter')
    @patch.object(PrometheusMonitor, 'expose_metrics')
    def prometheus_client(self, app, counter, gauge, histogram, time=None):
        time = time or self.time()

        return PrometheusMonitor(app, time=time)

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

    @pytest.fixture()
    def response(self):
        return Mock(name='response', autospec=web.Response)

    @pytest.fixture()
    def view(self):
        return Mock(name='view', autospec=web.View)

    def test_prometheus_client_not_installed(self, app, monkeypatch):
        monkeypatch.setattr('faust.sensors.prometheus.prometheus_client', None)
        with pytest.raises(ImproperlyConfigured):
            PrometheusMonitor(app)

    def test_on_message_in_out(self):
        message = Mock(name='message')
        client = self.prometheus_client()
        client.on_message_in(TP1, 400, message)

        client.messages_received.inc.assert_called_once()
        client.active_messages.inc.assert_called_once()
        client.messages_received_per_topics.labels.assert_called_once_with(
            topic='foo')

        labels = client.messages_received_per_topics_partition.labels
        labels.assert_called_once_with(topic='foo', partition=3)
        labels(topic='foo', partition=3).set.assert_called_once_with(400)

        client.on_message_out(TP1, 400, message)
        client.active_messages.dec.assert_called_once()

    def test_on_stream_event_in_out(self, *, stream, event):
        client = self.prometheus_client()
        state = client.on_stream_event_in(TP1, 401, stream, event)

        client.total_events.inc.assert_called_once()
        client.total_active_events.inc.assert_called_once()
        client.total_events_per_stream.labels.assert_called_once_with(
            stream='stream.topic_foo.events',
        )

        client.on_stream_event_out(TP1, 401, stream, event, state)
        client.total_active_events.dec.assert_called_once()
        client.events_runtime_latency.observe.assert_called_once_with(
            client.secs_to_ms(client.events_runtime[-1]),
        )

    def test_on_table_get(self, table):
        client = self.prometheus_client()
        client.on_table_get(table, 'key')

        client.table_operations.labels.assert_called_once_with(
            table='table.table1', operation='keys_retrieved',
        )
        client.table_operations.labels(
            table='table.table1', operation='keys_retrieved',
        ).inc.assert_called_once()

    def test_on_table_set(self, table):
        client = self.prometheus_client()
        client.on_table_set(table, 'key', 'value')

        client.table_operations.labels.assert_called_once_with(
            table='table.table1', operation='keys_updated',
        )
        client.table_operations.labels(
            table='table.table1', operation='keys_updated',
        ).inc.assert_called_once()

    def test_on_table_del(self, table):
        client = self.prometheus_client()
        client.on_table_del(table, 'key')

        client.table_operations.labels.assert_called_once_with(
            table='table.table1', operation='keys_deleted',
        )
        client.table_operations.labels(
            table='table.table1', operation='keys_deleted',
        ).inc.assert_called_once()

    def test_on_commit_completed(self):
        consumer = Mock(name='consumer')
        client = self.prometheus_client()
        state = client.on_commit_initiated(consumer)
        client.on_commit_completed(consumer, state)

        client.consumer_commit_latency.observe.assert_called_once_with(
            client.ms_since(float(state)))

    def test_on_send_initiated_completed(self):
        producer = Mock(name='producer')
        client = self.prometheus_client()
        state = client.on_send_initiated(
            producer, 'topic1', 'message', 321, 123)

        client.on_send_completed(producer, state, Mock(name='metadata'))

        client.total_sent_messages.inc.assert_called_once()
        client.topic_messages_sent.labels.assert_called_once_with(
            topic='topic.topic1')
        client.topic_messages_sent.labels(
            topic='topic.topic1').inc.assert_called_once()

        client.producer_send_latency.observe.assert_called_once_with(
            client.ms_since(float(state)))

        client.on_send_error(producer, KeyError('foo'), state)

        client.total_error_messages_sent.inc.assert_called()
        client.producer_error_send_latency.observe.assert_called_with(
            client.ms_since(float(state)))

    def test_on_assignment_start_completed(self):
        assignor = Mock(name='assignor')
        client = self.prometheus_client()
        state = client.on_assignment_start(assignor)
        client.on_assignment_completed(assignor, state)

        client.assignment_operations.labels.assert_called_once_with(
            operation=client.COMPLETED)
        client.assignment_operations.labels(
            operation=client.COMPLETED).inc.assert_called_once()
        client.assign_latency.observe.assert_called_once_with(
            client.ms_since(state['time_start']))

    def test_on_assignment_start_failed(self):
        assignor = Mock(name='assignor')
        client = self.prometheus_client()
        state = client.on_assignment_start(assignor)
        client.on_assignment_error(assignor, state, KeyError())

        client.assignment_operations.labels.assert_called_once_with(
            operation=client.ERROR)
        client.assignment_operations.labels(
            operation=client.ERROR).inc.assert_called_once()
        client.assign_latency.observe.assert_called_once_with(
            client.ms_since(state['time_start']))

    def test_on_rebalance(self):
        app = Mock(name='app')
        client = self.prometheus_client()

        state = client.on_rebalance_start(app)
        client.total_rebalances.inc.assert_called_once()

        client.on_rebalance_return(app, state)
        client.total_rebalances.dec.assert_called_once()
        client.total_rebalances_recovering.inc.assert_called()
        client.revalance_done_consumer_latency.observe.assert_called_once_with(
            client.ms_since(state['time_return']))

        client.on_rebalance_end(app, state)
        client.total_rebalances_recovering.dec.assert_called()
        client.revalance_done_latency.observe(
            client.ms_since(state['time_end']))

    def test_on_web_request(self, request, response, view):
        response.status = 404
        self.assert_on_web_request(
            request, response, view, expected_status=404)

    def test_on_web_request_none_response(self, request, view):
        self.assert_on_web_request(request, None, view, expected_status=500)

    def assert_on_web_request(self, request, response, view,
                              expected_status):
        app = Mock(name='app')
        client = self.prometheus_client()
        state = client.on_web_request_start(app, request, view=view)
        client.on_web_request_end(app, request, response, state, view=view)

        client.http_status_codes.labels.assert_called_with(
            status_code=expected_status)
        client.http_status_codes.labels(
            status_code=expected_status).inc.assert_called()
        client.http_latency.observe.assert_called_with(
            client.ms_since(state['time_end']))

    def test_count(self):
        client = self.prometheus_client()
        client.count('metric_name', count=3)

        client.count_metrics_by_name.labels.assert_called_once_with(
            metric='metric_name')
        client.count_metrics_by_name.labels(
            metric='metric_name').inc.assert_called_once_with(3)

    def test_on_tp_commit(self):
        offsets = {
            TP('foo', 0): 1001,
            TP('foo', 1): 2002,
            TP('bar', 3): 3003,
        }
        client = self.prometheus_client()

        client.on_tp_commit(offsets)
        client.topic_partition_offset_commited.labels.assert_has_calls([
            call(topic='foo', partition=0),
            call().set(1001),
            call(topic='foo', partition=1),
            call().set(2002),
            call(topic='bar', partition=3),
            call().set(3003),
        ])

    def test_track_tp_end_offsets(self):
        client = self.prometheus_client()
        client.track_tp_end_offset(TP('foo', 0), 4004)

        client.topic_partition_end_offset.labels.assert_called_once_with(
            topic='foo', partition=0)
        client.topic_partition_end_offset.labels(
            topic='foo', partition=0).set.assert_called_once_with(4004)

from collections import deque
from http import HTTPStatus
from statistics import median
from typing import Any
import pytest
from faust import Event, Stream, Table, Topic
from faust.transport.consumer import Consumer
from faust.transport.producer import Producer
from faust.types import Message, TP
from faust.sensors.monitor import Monitor, TableState
from mode.utils.mocks import AsyncMock, Mock

TP1 = TP('foo', 0)


class test_Monitor:

    @pytest.fixture
    def time(self):
        timefun = Mock(name='time()')
        timefun.return_value = 101.1
        return timefun

    @pytest.fixture
    def message(self):
        return Mock(name='message', autospec=Message)

    @pytest.fixture
    def stream(self):
        return Mock(name='stream', autospec=Stream)

    @pytest.fixture
    def topic(self):
        return Mock(name='topic', autospec=Topic)

    @pytest.fixture
    def event(self):
        return Mock(name='event', autospec=Event)

    @pytest.fixture
    def table(self):
        return Mock(name='table', autospec=Table)

    @pytest.fixture
    def mon(self, *, time):
        mon = self.create_monitor()
        mon.time = time
        return mon

    def create_monitor(self, **kwargs: Any) -> Monitor:
        return Monitor(**kwargs)

    def create_populated_monitor(self,
            messages_active=101,
            messages_received_total=1001,
            messages_sent=303,
            messages_s=1000,
            messages_received_by_topic={'foo': 103},  # noqa
            events_active=202,
            events_total=2002,
            events_s=3000,
            events_runtime_avg=0.03,
            events_by_task={'mytask': 105},  # noqa
            events_by_stream={'stream': 105},  # noqa
            commit_latency=[1.03, 2.33, 16.33],  # noqa
            send_latency=[0.01, 0.04, 0.06, 0.010],  # noqa
            topic_buffer_full={'topic': 808},  # noqa
            **kwargs):
        return self.create_monitor(
            messages_active=messages_active,
            messages_received_total=messages_received_total,
            messages_sent=messages_sent,
            messages_s=messages_s,
            messages_received_by_topic=messages_received_by_topic,
            events_active=events_active,
            events_total=events_total,
            events_s=events_s,
            events_runtime_avg=events_runtime_avg,
            events_by_task=events_by_task,
            events_by_stream=events_by_stream,
            commit_latency=commit_latency,
            send_latency=send_latency,
            topic_buffer_full=topic_buffer_full,
            **kwargs)

    def test_init_max_avg_history(self):
        assert Monitor().max_avg_history == Monitor.max_avg_history

    def test_init_max_avg_history__default(self):
        assert Monitor(max_avg_history=33).max_avg_history == 33

    def test_init_max_commit_latency_history(self):
        assert (Monitor().max_commit_latency_history ==
                Monitor.max_commit_latency_history)

    def test_init_max_commit_latency_history__default(self):
        assert Monitor(
            max_commit_latency_history=33,
        ).max_commit_latency_history == 33

    def test_init_max_send_latency_history(self):
        assert (Monitor().max_send_latency_history ==
                Monitor.max_send_latency_history)

    def test_init_max_send_latency_history__default(self):
        assert Monitor(
            max_send_latency_history=33,
        ).max_send_latency_history == 33

    def test_init_max_assignment_latency_history(self):
        assert (Monitor().max_assignment_latency_history ==
                Monitor.max_assignment_latency_history)

    def test_init_max_assignment_latency_history__default(self):
        assert Monitor(
            max_assignment_latency_history=33,
        ).max_assignment_latency_history == 33

    def test_init_rebalances(self):
        assert Monitor(rebalances=99).rebalances == 99

    def test_asdict(self):
        mon = self.create_populated_monitor()
        assert mon.asdict() == {
            'messages_active': mon.messages_active,
            'messages_received_total': mon.messages_received_total,
            'messages_sent': mon.messages_sent,
            'messages_sent_by_topic': mon.messages_sent_by_topic,
            'messages_s': mon.messages_s,
            'messages_received_by_topic': mon.messages_received_by_topic,
            'events_active': mon.events_active,
            'events_total': mon.events_total,
            'events_s': mon.events_s,
            'events_runtime_avg': mon.events_runtime_avg,
            'events_by_task': mon._events_by_task_dict(),
            'events_by_stream': mon._events_by_stream_dict(),
            'commit_latency': mon.commit_latency,
            'send_latency': mon.send_latency,
            'assignment_latency': mon.assignment_latency,
            'assignments_completed': mon.assignments_completed,
            'assignments_failed': mon.assignments_failed,
            'send_errors': mon.send_errors,
            'topic_buffer_full': mon._topic_buffer_full_dict(),
            'metric_counts': mon._metric_counts_dict(),
            'tables': {
                name: table.asdict() for name, table in mon.tables.items()
            },
            'topic_committed_offsets': {},
            'topic_read_offsets': {},
            'topic_end_offsets': {},
            'rebalance_end_avg': mon.rebalance_end_avg,
            'rebalance_end_latency': mon.rebalance_end_latency,
            'rebalance_return_avg': mon.rebalance_return_avg,
            'rebalance_return_latency': mon.rebalance_return_latency,
            'rebalances': mon.rebalances,
            'http_response_codes': mon._http_response_codes_dict(),
            'http_response_latency': mon.http_response_latency,
            'http_response_latency_avg': mon.http_response_latency_avg,
        }

    def test_on_message_in(self, *, message, mon, time):
        for i in range(1, 11):
            offset = 3 + i
            mon.on_message_in(TP1, offset, message)

            assert mon.messages_received_total == i
            assert mon.messages_active == i
            assert mon.messages_received_by_topic[TP1.topic] == i
            assert message.time_in is time()
            assert mon.tp_read_offsets[TP1] == offset

    def test_on_stream_event_in(self, *, event, mon, stream, time):
        for i in range(1, 11):
            state = mon.on_stream_event_in(TP1, 3 + i, stream, event)

            assert mon.events_total == i
            assert mon.events_by_stream[str(stream)] == i
            assert mon.events_by_task[str(stream.task_owner)] == i
            assert mon.events_active == i
            assert state == {
                'time_in': time(),
                'time_out': None,
                'time_total': None,
            }

    def test_on_stream_event_out(self, *, event, mon, stream, time):
        other_time = 303.3
        mon.events_active = 10
        for i in range(1, 11):
            state = {
                'time_in': other_time,
                'time_out': None,
                'time_total': None,
            }
            mon.on_stream_event_out(TP1, 3 + i, stream, event, state)

            assert mon.events_active == 10 - i
            assert state == {
                'time_in': other_time,
                'time_out': time(),
                'time_total': time() - other_time,
            }
            assert mon.events_runtime[-1] == time() - other_time

    def test_on_stream_event_out__missing_state(
            self, *, event, mon, stream, time):
        # should not be an error
        mon.on_stream_event_out(TP1, 3, stream, event, None)

    def test_on_topic_buffer_full(self, *, mon, topic):
        for i in range(1, 11):
            mon.on_topic_buffer_full(topic)

            assert mon.topic_buffer_full[topic] == i

    def test_on_message_out(self, *, message, mon, time):
        mon.messages_active = 10
        message.time_in = 10.7

        for i in range(1, 11):
            mon.on_message_out(TP1, 3 + i, message)

            assert mon.messages_active == 10 - i
            assert message.time_out == time()
            assert message.time_total == time() - message.time_in
        message.time_in = None
        mon.on_message_out(TP1, 3 + 11, message)

    def test_on_table_get(self, *, mon, table):
        for i in range(1, 11):
            mon.on_table_get(table, 'k')
            assert mon._table_or_create(table).keys_retrieved == i

    def test_on_table_set(self, *, mon, table):
        for i in range(1, 11):
            mon.on_table_set(table, 'k', 'v')
            assert mon._table_or_create(table).keys_updated == i

    def test_on_table_del(self, *, mon, table):
        for i in range(1, 11):
            mon.on_table_del(table, 'k')
            assert mon._table_or_create(table).keys_deleted == i

    def test_on_commit_initiated(self, *, mon, time):
        assert mon.on_commit_initiated(
            Mock(name='consumer', autospec=Consumer)) == time()

    def test_on_commit_completed(self, *, mon, time):
        other_time = 56.7
        mon.on_commit_completed(
            Mock(name='consumer', autospec=Consumer), other_time)
        assert mon.commit_latency[-1] == time() - other_time

    def test_on_send_initiated(self, *, mon, time):
        for i in range(1, 11):
            state = mon.on_send_initiated(
                Mock(name='producer', autospec=Producer),
                'topic', 'message', 2, 4)
            assert mon.messages_sent == i
            assert mon.messages_sent_by_topic['topic'] == i
            assert state == time()

    def test_on_send_completed(self, *, mon, time):
        other_time = 56.7
        mon.on_send_completed(
            Mock(name='producer', autospec=Producer),
            other_time,
            Mock(name='metadata'),
        )
        assert mon.send_latency[-1] == time() - other_time

    def test_on_send_error(self, *, mon, time):
        mon.on_send_error(
            Mock(name='producer', autospec=Producer),
            Mock(name='state'),
            KeyError('foo'),
        )
        assert mon.send_errors == 1

    def test_on_assignment_start(self, *, mon, time):
        state = mon.on_assignment_start(Mock(name='assignor'))
        assert state['time_start'] == time()

    def test_on_assignment_completed(self, *, mon, time):
        other_time = 56.7
        assignor = Mock(name='assignor')
        assert mon.assignments_completed == 0
        mon.on_assignment_completed(assignor, {'time_start': other_time})
        assert mon.assignment_latency[-1] == time() - other_time
        assert mon.assignments_completed == 1

    def test_on_assignment_error(self, *, mon, time):
        other_time = 56.7
        assignor = Mock(name='assignor')
        assert mon.assignments_failed == 0
        mon.on_assignment_error(
            assignor, {'time_start': other_time}, KeyError())
        assert mon.assignment_latency[-1] == time() - other_time
        assert mon.assignments_failed == 1

    def test_on_rebalance_start(self, *, mon, time, app):
        assert mon.rebalances == 0
        app.rebalancing_count = 1
        state = mon.on_rebalance_start(app)
        assert state['time_start'] == time()
        assert mon.rebalances == 1

    def test_on_rebalance_return(self, *, mon, time, app):
        other_time = 56.7
        state = {'time_start': other_time}
        mon.on_rebalance_return(app, state)
        assert mon.rebalance_return_latency[-1] == time() - other_time
        assert state['time_return'] == time()
        assert state['latency_return'] == time() - other_time

    def test_on_rebalance_end(self, *, mon, time, app):
        other_time = 56.7
        state = {'time_start': other_time}
        mon.on_rebalance_end(app, state)
        assert mon.rebalance_end_latency[-1] == time() - other_time
        assert state['time_end'] == time()
        assert state['latency_end'] == time() - other_time

    def test_on_web_request_start(self, *, mon, time, app):
        request = Mock(name='request')
        view = Mock(name='view')
        state = mon.on_web_request_start(app, request, view=view)
        assert state['time_start'] == time()

    def test_on_web_request_end(self, *, mon, time, app):
        response = Mock(name='response')
        response.status = 404
        self.assert_on_web_request_end(mon, time, app, response,
                                       expected_status=404)

    def test_on_web_request_end__None_response(self, *, mon, time, app):
        self.assert_on_web_request_end(mon, time, app, None,
                                       expected_status=500)

    def assert_on_web_request_end(self, mon, time, app, response,
                                  expected_status):
        request = Mock(name='request')
        view = Mock(name='view')
        other_time = 156.9
        state = {'time_start': other_time}
        mon.on_web_request_end(app, request, response, state, view=view)
        assert state['time_end'] == time()
        assert state['latency_end'] == time() - other_time
        assert state['status_code'] == HTTPStatus(expected_status)

        assert mon.http_response_latency[-1] == time() - other_time
        assert mon.http_response_codes[HTTPStatus(expected_status)] == 1

    def test_TableState_asdict(self, *, mon, table):
        state = mon._table_or_create(table)
        assert isinstance(state, TableState)
        assert state.table is table
        assert state.keys_retrieved == 0
        assert state.keys_updated == 0
        assert state.keys_deleted == 0

        expected_asdict = {
            'keys_retrieved': 0,
            'keys_updated': 0,
            'keys_deleted': 0,
        }
        assert state.asdict() == expected_asdict
        assert state.__reduce_keywords__() == {
            **state.asdict(),
            'table': table,
        }

    def test_on_tp_commit(self, *, mon):
        topic = 'foo'
        for offset in range(20):
            partitions = list(range(4))
            tps = {TP(topic=topic, partition=p) for p in partitions}
            commit_offsets = {tp: offset for tp in tps}
            mon.on_tp_commit(commit_offsets)
            assert all(mon.tp_committed_offsets[tp] == commit_offsets[tp]
                       for tp in tps)
            offsets_dict = mon.asdict()['topic_committed_offsets'][topic]
            assert all(offsets_dict[p] == offset for p in partitions)

    def test_track_tp_end_offsets(self, *, mon):
        tp = TP(topic='foo', partition=2)
        for offset in range(20):
            mon.track_tp_end_offset(tp, offset)
            assert mon.tp_end_offsets[tp] == offset
            offsets_dict = mon.asdict()['topic_end_offsets'][tp.topic]
            assert offsets_dict[tp.partition] == offset

    @pytest.mark.asyncio
    async def test_service_sampler(self, *, mon):
        mon = Monitor()

        i = 0
        mon.events_runtime = []
        mon.sleep = AsyncMock(name='sleep')

        def on_sample(prev_events, prev_messages):
            nonlocal i
            mon.events_runtime.append(i + 0.34)
            i += 1
            if i > 10:
                mon._stopped.set()
            return prev_events, prev_messages
        mon._sample = Mock(name='_sample')
        mon._sample.side_effect = on_sample

        await mon._sampler(mon)

    def test__sample(self, *, mon):
        prev_event_total = 0
        prev_message_total = 0
        mon.events_runtime = []
        mon._sample(prev_event_total, prev_message_total)
        mon.events_runtime = deque(range(100))
        mon.rebalance_return_latency = deque(range(100))
        mon.rebalance_end_latency = deque(range(100))
        mon.http_response_latency = deque(range(100))
        prev_event_total = 0
        prev_message_total = 0
        mon._sample(prev_event_total, prev_message_total)

        assert mon.events_runtime_avg == median(mon.events_runtime)
        assert mon.events_s == 0  # XXX this is wrong!

        assert mon.rebalance_return_avg == median(mon.rebalance_return_latency)
        assert mon.rebalance_end_avg == median(mon.rebalance_end_latency)
        assert mon.http_response_latency_avg == median(
            mon.http_response_latency)

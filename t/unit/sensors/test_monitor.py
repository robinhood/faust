from typing import Any
import pytest
from faust import Event, Stream, Table, Topic
from faust.transport.consumer import Consumer
from faust.transport.producer import Producer
from faust.types import Message, TP
from faust.sensors.monitor import (
    Monitor,
    MonitorService,
    TableState,
)
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
            'topic_buffer_full': mon._topic_buffer_full_dict(),
            'metric_counts': mon._metric_counts_dict(),
            'tables': {
                name: table.asdict() for name, table in mon.tables.items()
            },
        }

    def test_cleanup(self, *, mon):
        mon._cleanup_max_avg_history = Mock(name='cleanup_max_avg')
        mon._cleanup_commit_latency_history = Mock(name='cleanup_commit')
        mon._cleanup_send_latency_history = Mock(name='cleanup_send')

        mon._cleanup()

        mon._cleanup_max_avg_history.assert_called_once_with()
        mon._cleanup_commit_latency_history.assert_called_once_with()
        mon._cleanup_send_latency_history.assert_called_once_with()

    def test_cleanup_max_avg_history(self, *, mon):
        mon.max_avg_history = 10

        mon.events_runtime = list(range(5))
        mon._cleanup_max_avg_history()
        assert mon.events_runtime == list(range(5))

        mon.events_runtime.extend(list(range(10)))
        mon._cleanup_max_avg_history()
        assert mon.events_runtime == list(range(10))

    def test_cleanup_commit_latency_history(self, *, mon):
        mon.max_commit_latency_history = 10

        mon.commit_latency = list(range(5))
        mon._cleanup_commit_latency_history()
        assert mon.commit_latency == list(range(5))

        mon.commit_latency.extend(list(range(10)))
        mon._cleanup_commit_latency_history()
        assert mon.commit_latency == list(range(10))

    def test_cleanup_send_latency_history(self, *, mon):
        mon.max_send_latency_history = 10

        mon.send_latency = list(range(5))
        mon._cleanup_send_latency_history()
        assert mon.send_latency == list(range(5))

        mon.send_latency.extend(list(range(10)))
        mon._cleanup_send_latency_history()
        assert mon.send_latency == list(range(10))

    def test_on_message_in(self, *, message, mon, time):
        for i in range(1, 11):
            mon.on_message_in(TP1, 3 + i, message)

            assert mon.messages_received_total == i
            assert mon.messages_active == i
            assert mon.messages_received_by_topic[TP1.topic] == i
            assert message.time_in is time()

    def test_on_stream_event_in(self, *, event, mon, stream, time):
        for i in range(1, 11):
            event.message.stream_meta = {}
            mon.on_stream_event_in(TP1, 3 + i, stream, event)

            assert mon.events_total == i
            assert mon.events_by_stream[stream] == i
            assert mon.events_by_task[stream.task_owner] == i
            assert mon.events_active == i
            assert event.message.stream_meta[id(stream)] == {
                'time_in': time(),
                'time_out': None,
                'time_total': None,
            }

    def test_on_stream_event_out(self, *, event, mon, stream, time):
        other_time = 303.3
        mon.events_active = 10
        for i in range(1, 11):
            event.message.stream_meta = {}
            event.message.stream_meta[id(stream)] = {
                'time_in': other_time,
                'time_out': None,
                'time_total': None,
            }
            mon.on_stream_event_out(TP1, 3 + i, stream, event)

            assert mon.events_active == 10 - i
            assert event.message.stream_meta[id(stream)] == {
                'time_in': other_time,
                'time_out': time(),
                'time_total': time() - other_time,
            }
            assert mon.events_runtime[-1] == time() - other_time

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
                Mock(name='producer', autospec=Producer), 'topic', 2, 4)
            assert mon.messages_sent == i
            assert mon.messages_sent_by_topic['topic'] == i
            assert state == time()

    def test_on_send_completed(self, *, mon, time):
        other_time = 56.7
        mon.on_send_completed(
            Mock(name='producer', autospec=Producer), other_time)
        assert mon.send_latency[-1] == time() - other_time

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

    @pytest.mark.asyncio
    async def test_service_sampler(self, *, mon):
        service = MonitorService(mon)

        i = 0
        mon.events_runtime = []
        service.sleep = AsyncMock(name='sleep')

        def on_cleanup():
            nonlocal i
            mon.events_runtime.append(i + 0.34)
            i += 1
            if i > 10:
                service._stopped.set()
        mon._cleanup = Mock(name='_cleanup')
        mon._cleanup.side_effect = on_cleanup

        await service._sampler(service)

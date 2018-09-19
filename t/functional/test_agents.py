import asyncio
import pytest
from collections import Counter
from itertools import cycle
from time import time
from typing import Any, Optional, Set
from faust.types import CodecT, Message, StreamT, TP
from mode import Service
from mode.utils.aiter import aenumerate
from mode.utils.logging import setup_logging

CURRENT_OFFSETS = Counter()


@pytest.mark.asyncio
@pytest.mark.parametrize('concurrency,num_messages', [
    (10, 100),
    (10, 1000),
    (2, 10),
    (2, 100),
    (100, 1000),
    (1000, 1000),
])
async def test_agent_concurrency__duplicates(concurrency, num_messages, *,
                                             app):
    setup_logging(loglevel='INFO', logfile=None)
    case = AgentTestCase(
        app=app,
        name='test_agent_shares_queue',
        num_messages=num_messages,
        concurrency=concurrency,
    )
    try:
        await case.start()
        await case.wait(case.finished)
        await case.stop()
        assert not case._crashed.is_set()
    except Exception as exc:
        print('AgentTestCase raised: %r' % (exc,))
        raise
    assert case.processed_total == num_messages


def next_offset(tp: TP, *, offsets=CURRENT_OFFSETS) -> int:
    offset = offsets[tp]
    offsets[tp] += 1
    return offset


def new_message(tp: TP,
                offset: int = None,
                timestamp: float = None,
                timestamp_type: int = 1,
                key: Optional[bytes] = None,
                value: Optional[bytes] = None,
                checksum: Optional[bytes] = None,
                **kwargs: Any) -> Message:
    return Message(
        topic=tp.topic,
        partition=tp.partition,
        offset=next_offset(tp) if offset is None else offset,
        timestamp=time() if timestamp is None else timestamp,
        timestamp_type=timestamp_type,
        key=key,
        value=value,
        checksum=checksum,
        tp=tp,
    )


class AgentTestCase(Service):
    wait_for_shutdown = True

    def __init__(self, app, name, *,
                 partition: int = 0,
                 concurrency: int = 10,
                 value_serializer: CodecT = 'raw',
                 num_messages: int = 100,
                 **kwargs: Any):
        super().__init__(**kwargs)
        self.app = app
        self.name = name
        self.partition = partition
        self.concurrency = concurrency
        assert self.concurrency > 0
        assert not self.concurrency % 2, 'concurrency is even'
        self.value_serializer = value_serializer
        self.num_messages = num_messages
        assert self.num_messages > 1

        self.expected_index = cycle(range(self.concurrency))

        self.topic_name = self.name
        self.tp = TP(self.topic_name, self.partition)
        self.seen_offsets = set()
        self.processed_by_concurrency_index = Counter()
        self.processed_total = 0
        self.agent_started = asyncio.Event(loop=self.loop)
        self.agent_started_processing = asyncio.Event(loop=self.loop)
        self.agent_stopped_processing = asyncio.Event(loop=self.loop)
        self.finished = asyncio.Event(loop=self.loop)

    async def on_start(self) -> None:
        app = self.app
        topic = app.topic(self.topic_name,
                          value_serializer=self.value_serializer)
        self.agent = app.agent(
            topic,
            concurrency=self.concurrency)(self.process)
        assert self.agent

    @Service.task
    async def _wait_for_success(self) -> None:
        await self.wait(self.finished)
        try:
            await self.assert_success()
        finally:
            self.set_shutdown()

    async def assert_success(self) -> None:
        assert self.processed_total == self.num_messages
        self.log.info('- Final coroutine distribution:\n%s',
                      self.processed_by_concurrency_index.most_common())
        max_ = None
        for _, total in self.processed_by_concurrency_index.most_common():
            if max_ is None:
                max_ = total
            assert total == max_

    async def process(self, stream: StreamT[bytes]) -> None:
        self.agent_started.set()
        try:
            async for i, event in aenumerate(stream.events()):
                cur_index = stream.concurrency_index

                # Test for: perfect round-robin, uniform distribution
                assert cur_index == next(self.expected_index)
                self.agent_started_processing.set()
                self.processed_by_concurrency_index[cur_index] += 1
                self.processed_total += 1

                key = event.message.tp, event.message.offset
                if key in self.seen_offsets:
                    print(f'EVENT PROCESSED TWICE: {key}')
                    await self.crash(
                        Exception(f'Event processed twice: {key}'))
                self.seen_offsets.add(key)
                assert key in self.seen_offsets
                if self.processed_total >= self.num_messages:
                    self.agent_stopped_processing.set()
                await self.sleep(0)
        except asyncio.CancelledError:
            if self.processed_total < self.num_messages:
                print('I WAS CANCELLED?!?!?')
            raise
        except Exception as exc:
            print('AGENT RAISED ERROR: %r' % (exc,))
            await self.crash(exc)
            raise

    @Service.task
    async def _send(self) -> None:
        app = self.app
        tp = self.tp

        async with app.agents:
            app.flow_control.resume()
            # Wait for agent to enter execution.
            await self.wait(self.agent_started, timeout=1.0)
            # Wait for Stream.__aiter__ to start up.
            await self.sleep(0.5)

            # Update indices and compile closures for agent delivery
            await self.conductor_setup(assigned={tp})

            # send first message
            await self.put(tp, key=str(0).encode(), value=str(0).encode())

            # Wait for first message to be processed
            await self.wait(self.agent_started_processing, timeout=10.0)

            # send 99 more messages
            for i in range(1, self.num_messages):
                await self.put(tp, key=str(i).encode(), value=str(i).encode())

            # Wait for last message to be processed
            await self.wait(self.agent_stopped_processing, timeout=1000.0)

        self.finished.set()

    async def conductor_setup(self, assigned: Set[TP]) -> None:
        await self.app.topics._update_indices()
        await self.app.topics.on_partitions_assigned(assigned)

    async def put(self, tp: TP, key: bytes, value: bytes,
                  **kwargs: Any) -> Message:
        # send first message
        message = new_message(tp=tp, key=key, value=bytes, **kwargs)
        await self.app.topics.on_message(message)
        return message

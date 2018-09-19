import asyncio
from collections import Counter
from itertools import cycle
from time import time
from typing import Any, List, Optional, Set
from faust.types import AppT, CodecT, EventT, Message as MessageT, StreamT, TP
from mode import Service
from mode.utils.aiter import aenumerate

CURRENT_OFFSETS = Counter()


class AgentCase(Service):
    name: str

    wait_for_shutdown = True

    @classmethod
    async def run_test(cls, app: AppT, *,
                       num_messages: int = 100,
                       **kwargs: Any) -> 'AgentCase':
        case = cls(app, num_messages=num_messages, **kwargs)
        try:
            async with case:
                await case.wait(case.finished)
        except Exception as exc:
            print('AgentConcurrencyCase raised: %r' % (exc,))
            raise
        else:
            assert not case._crashed.is_set()
        assert case.processed_total == num_messages
        return case

    def __init__(self, app: AppT, *,
                 partitions: List[int] = None,
                 concurrency: int = 10,
                 value_serializer: CodecT = 'raw',
                 num_messages: int = 100,
                 isolated_partitions: bool = False,
                 name: str = None,
                 **kwargs: Any):
        self.app = app
        if name is not None:
            self.name = name
        if partitions is None:
            partitions = [0]
        self.partitions = list(sorted(partitions))
        self.concurrency = concurrency
        self.value_serializer = value_serializer
        self.num_messages = num_messages
        assert self.num_messages > 1
        self.isolated_partitions = isolated_partitions

        self.topic_name = self.name
        self.tps = [TP(self.topic_name, p) for p in self.partitions]
        self.next_tp = cycle(self.tps)
        self.expected_tp = cycle(self.tps)
        self.seen_offsets = set()
        self.processed_total = 0

        super().__init__(**kwargs)

        self.agent_started = asyncio.Event(loop=self.loop)
        self.agent_started_processing = asyncio.Event(loop=self.loop)
        self.agent_stopped_processing = asyncio.Event(loop=self.loop)
        self.finished = asyncio.Event(loop=self.loop)

    async def on_start(self) -> None:
        app = self.app
        topic = app.topic(self.topic_name,
                          value_serializer=self.value_serializer)
        create_agent = app.agent(
            topic,
            concurrency=self.concurrency,
            isolated_partitions=self.isolated_partitions,
        )
        self.agent = create_agent(self.process)
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

    async def on_agent_event(
            self, stream: StreamT, event: EventT) -> None:
        ...

    async def process(self, stream: StreamT[bytes]) -> None:
        self.agent_started.set()
        try:
            async for i, event in aenumerate(stream.events()):
                self.agent_started_processing.set()
                self.processed_total += 1

                await self.on_agent_event(stream, event)

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

        async with app.agents:
            app.flow_control.resume()
            # Wait for agent to enter execution.
            await self.wait(self.agent_started, timeout=1.0)
            # Wait for Stream.__aiter__ to start up.
            await self.sleep(0.5)

            # Update indices and compile closures for agent delivery
            await self.conductor_setup(assigned=set(self.tps))

            # send first message
            await self.put(key=str(0).encode(), value=str(0).encode())

            # Wait for first message to be processed
            await self.wait(self.agent_started_processing, timeout=10.0)

            # send the rest of the messages
            for i in range(1, self.num_messages):
                await self.put(key=str(i).encode(), value=str(i).encode())

            # Wait for last message to be processed
            await self.wait(self.agent_stopped_processing, timeout=1000.0)

        self.finished.set()

    async def conductor_setup(self, assigned: Set[TP]) -> None:
        print('PARTITIONS ASSIGNED: %r' % (assigned,))
        await self.app.agents.on_partitions_revoked(assigned)
        await self.app.agents.on_partitions_assigned(assigned)
        await self.app.topics._update_indices()
        await self.app.topics.on_partitions_assigned(assigned)

    async def put(self, key: bytes, value: bytes, *,
                  tp: TP = None,
                  **kwargs: Any) -> MessageT:
        # send first message
        message = self.Message(tp=tp, key=key, value=bytes, **kwargs)
        await self.app.topics.on_message(message)
        return message

    def Message(self,
                tp: TP = None,
                offset: int = None,
                timestamp: float = None,
                timestamp_type: int = 1,
                key: Optional[bytes] = None,
                value: Optional[bytes] = None,
                checksum: Optional[bytes] = None,
                **kwargs: Any) -> MessageT:
        if tp is None:
            tp = next(self.next_tp)
        return MessageT(
            topic=tp.topic,
            partition=tp.partition,
            offset=self.next_offset(tp) if offset is None else offset,
            timestamp=time() if timestamp is None else timestamp,
            timestamp_type=timestamp_type,
            key=key,
            value=value,
            checksum=checksum,
            tp=tp,
        )

    def next_offset(self, tp: TP, *, offsets=CURRENT_OFFSETS) -> int:
        offset = offsets[tp]
        offsets[tp] += 1
        return offset

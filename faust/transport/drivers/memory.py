"""Experimental: In-memory transport."""
import asyncio
from collections import defaultdict, deque
from time import time
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    ClassVar,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)

from mode import Seconds
from mode.utils.futures import done_future
from mode.utils.imports import symbol_by_name
from mode.utils.typing import Deque

from faust.transport import base
from faust.types import HeadersArg, Message, RecordMetadata, TP
from faust.types.transports import ConsumerT, ProducerT

# XXX mypy borks on `import faust`
faust_version = symbol_by_name('faust:__version__')


class RebalanceListener:
    """In-memory rebalance listener."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...


class Consumer(base.Consumer):
    """In-memory consumer."""

    RebalanceListener: ClassVar[Type] = RebalanceListener

    consumer_stopped_errors: ClassVar[Tuple[Type[Exception], ...]] = ()

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 1000.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        ...

    async def subscribe(self, topics: Iterable[str]) -> None:
        await cast(Transport, self.transport).subscribe(topics)

    async def getmany(self,
                      timeout: float) -> AsyncIterator[Tuple[TP, Message]]:
        transport = cast(Transport, self.transport)
        max_per_partition = 100
        partitions = tuple(self.assignment())

        if not partitions:
            if await self.wait_for_stopped(transport._subscription_ready):
                return

        for tp in partitions:
            messages = transport._messages[tp.topic]
            if not messages:
                if await self.wait_for_stopped(transport._messages_ready):
                    return
                transport._messages_ready.clear()

            i = 0
            while messages:
                yield tp, messages.popleft()
                i += 1
                if i > max_per_partition:
                    break

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return TP(topic, partition)

    async def perform_seek(self) -> None:
        ...

    async def _commit(self, offsets: Mapping[TP, int]) -> bool:
        return True

    def pause_partitions(self, tps: Iterable[TP]) -> None:
        ...

    async def position(self, tp: TP) -> Optional[int]:
        return 0

    def resume_partitions(self, partitions: Iterable[TP]) -> None:
        ...

    async def seek_to_latest(self, *partitions: TP) -> None:
        ...

    async def seek_to_beginning(self, *partitions: TP) -> None:
        ...

    async def seek(self, partition: TP, offset: int) -> None:
        ...

    def assignment(self) -> Set[TP]:
        return {
            TP(t, 0)
            for t in cast(Transport, self.transport)._subscription
        }

    def highwater(self, tp: TP) -> int:
        return 0

    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        return {tp: 0 for tp in partitions}

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        return {tp: 0 for tp in partitions}


class Producer(base.Producer):
    """In-memory producer."""

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = None,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        ...

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        res = await self.send_and_wait(
            topic, key, value, partition, timestamp, headers)
        return cast(Awaitable[RecordMetadata], done_future(res))

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
        return await cast(Transport, self.transport).send(
            topic, value, key, partition, timestamp, headers)


class Transport(base.Transport):
    """In-memory transport."""

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    default_port = 9092
    driver_version = f'memory-{faust_version}'

    _subscription: Set[str]
    _messages: MutableMapping[str, Deque[Message]]
    _messages_ready: asyncio.Event
    _subscription_ready: asyncio.Event

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._subscription = set()
        self._messages = defaultdict(deque)
        self._messages_ready = asyncio.Event(loop=self.loop)
        self._subscription_ready = asyncio.Event(loop=self.loop)

    async def subscribe(self, topics: Iterable[str]) -> None:
        self._subscription_ready.clear()
        self._subscription.clear()
        self._subscription.update(topics)
        self._subscription_ready.set()

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg]) -> RecordMetadata:
        if partition is None:
            partition = 0
        message = Message(
            topic,
            partition=partition,
            offset=0,
            timestamp=timestamp or time(),
            timestamp_type=1 if timestamp else 0,
            headers=headers,
            key=key,
            value=value,
            checksum=None,
            serialized_key_size=len(key) if key else 0,
            serialized_value_size=len(value) if value else 0,
        )
        self._messages[topic].append(message)
        self._messages_ready.set()
        return RecordMetadata(
            topic=topic,
            partition=partition,
            topic_partition=message.tp,
            offset=0,
        )

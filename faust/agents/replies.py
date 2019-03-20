"""Agent replies: waiting for replies, sending them, etc."""
import asyncio
from collections import defaultdict
from typing import (
    Any,
    AsyncIterator,
    MutableMapping,
    MutableSet,
    NamedTuple,
    Optional,
)
from weakref import WeakSet
from mode import Service
from faust.types import AppT, ChannelT, TopicT
from .models import ReqRepResponse

__all__ = ['ReplyPromise', 'BarrierState', 'ReplyConsumer']


class ReplyTuple(NamedTuple):
    correlation_id: str
    value: Any


class ReplyPromise(asyncio.Future):
    """Reply promise can be :keyword:`await`-ed to wait until result ready."""

    reply_to: str
    correlation_id: str

    def __init__(self, reply_to: str, correlation_id: str,
                 **kwargs: Any) -> None:
        self.reply_to = reply_to
        self.correlation_id = correlation_id
        super().__init__(**kwargs)

    def fulfill(self, correlation_id: str, value: Any) -> None:
        # If it wasn't for BarrierState we would just use .set_result()
        # directly, but BarrierState.fulfill requires the correlation_id
        # to be sent with it. That way it can mark that part of the map
        # operation as completed.
        assert correlation_id == self.correlation_id
        self.set_result(value)


class BarrierState(asyncio.Future):
    """State of pending/complete barrier.

    A barrier is a synchronization primitive that will wait until
    a group of coroutines have completed.
    """

    reply_to: str

    #: This is the size while the messages are being sent.
    #: (it's a tentative total, added to until the total is finalized).
    size: int = 0

    #: This is the actual total when all messages have been sent.
    #: It's set by :meth:`finalize`.
    total: int = 0

    #: The number of results we have received.
    fulfilled: int = 0

    #: Internal queue where results are added to.
    _results: asyncio.Queue

    #: Set of pending replies that this barrier is composed of.
    pending: MutableSet[ReplyPromise]

    def __init__(self, reply_to: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.reply_to = reply_to
        self.pending = set()
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        self._results = asyncio.Queue(maxsize=1000, loop=loop)

    def add(self, p: ReplyPromise) -> None:
        self.pending.add(p)
        self.size += 1

    def finalize(self) -> None:
        self.total = self.size
        # The barrier may have been filled up already at this point,
        if self.fulfilled >= self.total:
            self.set_result(True)
            self._results.put_nowait(None)  # always wake-up .iterate()

    def fulfill(self, correlation_id: str, value: Any) -> None:
        # ReplyConsumer calls this whenever a new reply is received.
        self._results.put_nowait(ReplyTuple(correlation_id, value))
        self.fulfilled += 1
        if self.total:
            if self.fulfilled >= self.total:
                self.set_result(True)
                self._results.put_nowait(None)  # always wake-up .iterate()

    def get_nowait(self) -> ReplyTuple:
        """Return next reply, or raise :exc:`asyncio.QueueEmpty`."""
        for _ in range(10):  # remove sentinels
            value = self._results.get_nowait()
            if value is not None:
                return value
        raise asyncio.QueueEmpty()

    async def iterate(self) -> AsyncIterator[ReplyTuple]:
        """Iterate over results as arrive."""
        get = self._results.get
        get_nowait = self._results.get_nowait
        is_done = self.done
        while not is_done():
            value = await get()
            if value is not None:
                yield value
        while 1:
            try:
                value = get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                if value is not None:
                    yield value


class ReplyConsumer(Service):
    """Consumer responsible for redelegation of replies received."""

    _waiting: MutableMapping[str, MutableSet[ReplyPromise]]
    _fetchers: MutableMapping[str, Optional[asyncio.Future]]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self._waiting = defaultdict(WeakSet)
        self._fetchers = {}
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        if self.app.conf.reply_create_topic:
            await self._start_fetcher(self.app.conf.reply_to)

    async def add(self, correlation_id: str, promise: ReplyPromise) -> None:
        reply_topic = promise.reply_to
        if reply_topic not in self._fetchers:
            await self._start_fetcher(reply_topic)
        self._waiting[correlation_id].add(promise)

    async def _start_fetcher(self, topic_name: str) -> None:
        if topic_name not in self._fetchers:
            # set the key as a lock, so it doesn't happen twice
            self._fetchers[topic_name] = None
            # declare the topic
            topic = self._reply_topic(topic_name)
            await topic.maybe_declare()
            await self.sleep(3.0)
            # then create the future
            self._fetchers[topic_name] = self.add_future(
                self._drain_replies(topic))

    async def _drain_replies(self, channel: ChannelT) -> None:
        async for reply in channel.stream():
            for promise in self._waiting[reply.correlation_id]:
                promise.fulfill(reply.correlation_id, reply.value)

    def _reply_topic(self, topic: str) -> TopicT:
        return self.app.topic(
            topic,
            partitions=1,
            replicas=0,
            deleting=True,
            retention=self.app.conf.reply_expires,
            value_type=ReqRepResponse,
        )

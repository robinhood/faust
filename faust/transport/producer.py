"""Producer.

The Producer is responsible for:

   - Holds reference to the transport that created it
   - ... and the app via ``self.transport.app``.
   - Sending messages.
"""
import asyncio
from asyncio import QueueEmpty
from typing import Any, Awaitable, Mapping, Optional, cast
from mode import Seconds, Service
from faust.types import AppT, HeadersArg
from faust.types.tuples import FutureMessage, RecordMetadata, TP
from faust.types.transports import ProducerBufferT, ProducerT, TransportT

__all__ = ['Producer']


class ProducerBuffer(Service, ProducerBufferT):

    max_messages = 100

    def __post_init__(self) -> None:
        self.pending = asyncio.Queue()

    def put(self, fut: FutureMessage) -> None:
        """Add message to buffer.

        The message will be eventually produced, you can await
        the future to wait for that to happen.
        """
        self.pending.put_nowait(fut)

    async def on_stop(self) -> None:
        await self.flush()

    async def flush(self) -> None:
        """Flush all messages (draining the buffer)."""
        get_pending = self.pending.get_nowait
        send_pending = self._send_pending

        if self.size:
            while True:
                try:
                    msg = get_pending()
                except QueueEmpty:
                    break
                else:
                    await send_pending(msg)

    async def flush_atmost(self, n: int) -> int:
        """Flush at most ``n`` messages."""
        get_pending = self.pending.get_nowait
        send_pending = self._send_pending

        if self.size:
            for i in range(n):
                try:
                    msg = get_pending()
                except QueueEmpty:
                    return i
                else:
                    await send_pending(msg)
            return n
        else:
            return 0

    async def _send_pending(self, fut: FutureMessage) -> None:
        await fut.message.channel.publish_message(fut, wait=False)

    async def wait_until_ebb(self) -> None:
        """Wait until buffer is of an acceptable size.

        Modifying a table key is using the Python dictionary API,
        and as ``__getitem__`` is synchronous we have to add
        pending messages to a buffer.

        The ``__getitem__`` method cannot drain the buffer as doing
        so requires trampolining into the event loop.

        To solve this, we have the conductor wait until the buffer
        is of an acceptable size before resuming stream processing flow.
        """
        if self.size > self.max_messages:
            await self.flush_atmost(self.max_messages)

    @Service.task
    async def _handle_pending(self) -> None:
        get_pending = self.pending.get
        send_pending = self._send_pending
        while not self.should_stop:
            msg = await get_pending()
            await send_pending(msg)

    @property
    def size(self) -> int:
        """Current buffer size (messages waiting to be produced)."""
        queue_items = self.pending._queue  # type: ignore
        queue_items = cast(list, queue_items)
        return len(queue_items)


class Producer(Service, ProducerT):
    """Base Producer."""

    app: AppT

    _api_version: str

    def __init__(self, transport: TransportT,
                 loop: asyncio.AbstractEventLoop = None,
                 **kwargs: Any) -> None:
        self.transport = transport
        self.app = self.transport.app
        conf = self.transport.app.conf
        self.client_id = conf.broker_client_id
        self.linger_ms = conf.producer_linger_ms
        self.max_batch_size = conf.producer_max_batch_size
        self.acks = conf.producer_acks
        self.max_request_size = conf.producer_max_request_size
        self.compression_type = conf.producer_compression_type
        self.request_timeout = conf.producer_request_timeout
        self.ssl_context = conf.ssl_context
        self.credentials = conf.broker_credentials
        self.partitioner = conf.producer_partitioner
        api_version = self._api_version = conf.producer_api_version
        assert api_version is not None
        super().__init__(loop=loop or self.transport.loop, **kwargs)

        self.buffer = ProducerBuffer(loop=self.loop, beacon=self.beacon)

    async def on_start(self) -> None:
        await self.add_runtime_dependency(self.buffer)

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        """Schedule message to be sent by producer."""
        raise NotImplementedError()

    def send_soon(self, fut: FutureMessage) -> None:
        self.buffer.put(fut)

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
        """Send message and wait for it to be transmitted."""
        raise NotImplementedError()

    async def flush(self) -> None:
        """Flush all in-flight messages."""
        # XXX subclasses must call self.buffer.flush() here.
        ...

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
        """Create/declare topic on server."""
        raise NotImplementedError()

    def key_partition(self, topic: str, key: bytes) -> TP:
        """Hash key to determine partition."""
        raise NotImplementedError()

    async def begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction by id."""
        raise NotImplementedError()

    async def commit_transaction(self, transactional_id: str) -> None:
        """Commit transaction by id."""
        raise NotImplementedError()

    async def abort_transaction(self, transactional_id: str) -> None:
        """Abort and rollback transaction by id."""
        raise NotImplementedError()

    async def stop_transaction(self, transactional_id: str) -> None:
        """Stop transaction by id."""
        raise NotImplementedError()

    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction by id, if not already started."""
        raise NotImplementedError()

    async def commit_transactions(
            self,
            tid_to_offset_map: Mapping[str, Mapping[TP, int]],
            group_id: str,
            start_new_transaction: bool = True) -> None:
        """Commit transactions."""
        raise NotImplementedError()

    def supports_headers(self) -> bool:
        """Return :const:`True` if headers are supported by this transport."""
        return False

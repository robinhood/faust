"""Producer.

The Producer is responsible for:

   - Holds reference to the transport that created it
   - ... and the app via ``self.transport.app``.
   - Sending messages.
"""
import asyncio
from typing import Any, Awaitable, Mapping, Optional
from mode import Seconds, Service
from faust.types import AppT, HeadersArg
from faust.types.tuples import RecordMetadata, TP
from faust.types.transports import ProducerT, TransportT

__all__ = ['Producer']


class Producer(Service, ProducerT):
    """Base Producer."""

    app: AppT

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
        super().__init__(loop=loop or self.transport.loop, **kwargs)

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        raise NotImplementedError()

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
        raise NotImplementedError()

    async def flush(self) -> None:
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
        raise NotImplementedError()

    def key_partition(self, topic: str, key: bytes) -> TP:
        raise NotImplementedError()

    async def begin_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def commit_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def abort_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def stop_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        raise NotImplementedError()

    async def commit_transactions(
            self,
            tid_to_offset_map: Mapping[str, Mapping[TP, int]],
            group_id: str,
            start_new_transaction: bool = True) -> None:
        raise NotImplementedError()

    def supports_headers(self) -> bool:
        return False

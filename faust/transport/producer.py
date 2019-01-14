"""Producer.

The Producer is responsible for:

   - Holds reference to the transport that created it
   - ... and the app via ``self.transport.app``.
   - Sending messages.
"""
import asyncio
from typing import Any, Awaitable, Mapping, Optional
from mode import Seconds, Service
from faust.types import AppT
from faust.types.tuples import RecordMetadata, TP
from faust.types.transports import ProducerT, TransactionProducerT, TransportT

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
        self.partitioner = conf.producer_partitioner
        super().__init__(loop=loop or self.transport.loop, **kwargs)

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int]) -> Awaitable[RecordMetadata]:
        raise NotImplementedError()

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int]) -> RecordMetadata:
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


class TransactionProducer(Producer, TransactionProducerT):

    def __init__(self, transport: TransportT,
                 loop: asyncio.AbstractEventLoop = None,
                 *,
                 partition: int,
                 **kwargs: Any) -> None:
        self.partition = partition
        super().__init__(transport, loop, **kwargs)

    async def commit(self, offsets: Mapping[TP, int], group_id: str,
                     start_new_transaction: bool = True) -> None:
        conf = self.app.conf
        raise NotImplementedError(
            f'This transport does not support {conf.processing_guarantee}')

    @property
    def transaction_id(self) -> str:
        return f'{self.app.conf.id}-{self.partition}'

    @property
    def label(self) -> str:
        return f'{type(self).__name__}-{self.partition}'

"""Base message transport implementation.

The Transport is responsible for:

- Holds reference to the app that created it.
- Creates new consumers/producers.

To see a reference transport implementation go to:
faust/transport/drivers/aiokafka.py
"""
import asyncio
from typing import Any, ClassVar, List, Type

from mode.services import ServiceT
from yarl import URL

from faust.types import AppT
from faust.types.transports import (
    ConductorT,
    ConsumerCallback,
    ConsumerT,
    ProducerT,
    TransactionManagerT,
    TransportT,
)

from .conductor import Conductor
from .consumer import Consumer, Fetcher, TransactionManager
from .producer import Producer

__all__ = ['Conductor', 'Consumer', 'Fetcher', 'Producer', 'Transport']


class Transport(TransportT):
    """Message transport implementation."""

    #: Consumer subclass used for this transport.
    Consumer: ClassVar[Type[ConsumerT]]
    Consumer = Consumer

    #: Producer subclass used for this transport.
    Producer: ClassVar[Type[ProducerT]]
    Producer = Producer

    TransactionManager: ClassVar[Type[TransactionManagerT]]
    TransactionManager = TransactionManager

    Conductor: ClassVar[Type[ConductorT]]
    Conductor = Conductor

    #: Service that fetches messages from the broker.
    Fetcher: ClassVar[Type[ServiceT]] = Fetcher

    driver_version: str

    def __init__(self,
                 url: List[URL],
                 app: AppT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = url
        self.app = app
        self.loop = loop or asyncio.get_event_loop()

    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        return self.Consumer(self, callback=callback,
                             loop=self.loop,
                             **kwargs)

    def create_producer(self, **kwargs: Any) -> ProducerT:
        return self.Producer(self, **kwargs)

    def create_transaction_manager(self,
                                   consumer: ConsumerT,
                                   producer: ProducerT,
                                   **kwargs: Any) -> TransactionManagerT:
        return self.TransactionManager(
            self,
            consumer=consumer,
            producer=producer,
            **kwargs,
        )

    def create_conductor(self, **kwargs: Any) -> ConductorT:
        return self.Conductor(app=self.app, loop=self.loop, **kwargs)

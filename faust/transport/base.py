"""Base message transport implementation.

The Transport is responsible for:

- Holds reference to the app that created it.
- Creates new consumers/producers.

To see a reference transport implementation go to:
faust/transport/drivers/aiokafka.py
"""
import asyncio
import typing
from typing import (
    Any,
    ClassVar,
    Type,
    Union,
)

from mode.services import ServiceT
from yarl import URL

from faust.types import AppT
from faust.types.transports import (
    ConductorT,
    ConsumerCallback,
    ConsumerT,
    ProducerT,
    TransportT,
)

from .conductor import Conductor
from .consumer import Consumer, Fetcher
from .producer import Producer

if typing.TYPE_CHECKING:  # pragma: no cover
    from faust.app import App
else:
    class App: ...  # noqa

__all__ = ['Conductor', 'Consumer', 'Fetcher', 'Producer', 'Transport']


class Transport(TransportT):
    """Message transport implementation."""

    #: Consumer subclass used for this transport.
    Consumer: ClassVar[Type[ConsumerT]]
    Consumer = Consumer

    #: Producer subclass used for this transport.
    Producer: ClassVar[Type[ProducerT]]
    Producer = Producer

    Conductor: ClassVar[Type[ConductorT]]
    Conductor = Conductor

    #: Service that fetches messages from the broker.
    Fetcher: ClassVar[Type[ServiceT]] = Fetcher

    driver_version: str

    def __init__(self,
                 url: Union[str, URL],
                 app: AppT,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = URL(url)
        self.app = app
        self.loop = loop or asyncio.get_event_loop()

    def create_consumer(self, callback: ConsumerCallback,
                        **kwargs: Any) -> ConsumerT:
        return self.Consumer(self, callback=callback, loop=self.loop, **kwargs)

    def create_producer(self, **kwargs: Any) -> ProducerT:
        return self.Producer(self, **kwargs)

    def create_conductor(self, **kwargs: Any) -> ConductorT:
        return self.Conductor(app=self.app, loop=self.loop, **kwargs)

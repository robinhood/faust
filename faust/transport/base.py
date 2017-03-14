import asyncio
import faust
from typing import Awaitable, Optional, cast
from ..types import ConsumerCallback, Topic
from ..utils.service import Service

CLIENT_ID = 'faust-{0}'.format(faust.__version__)


class Consumer(Service):
    topic: Topic
    client_id = CLIENT_ID
    transport: 'Transport'

    def __init__(self, transport: 'Transport',
                 *,
                 topic: Topic = None,
                 callback: ConsumerCallback = None) -> None:
        assert callback is not None
        self.transport = transport
        self.callback = callback
        self.topic = topic
        if self.topic.topics and self.topic.pattern:
            raise TypeError('Topic can specify either topics or pattern')
        super().__init__(loop=self.transport.loop)
_ConsumerT = Consumer


class Producer(Service):
    client_id = CLIENT_ID
    transport: 'Transport'

    def __init__(self, transport: 'Transport') -> None:
        self.transport = transport
        super().__init__(loop=self.transport.loop)

    async def send(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        raise NotImplementedError()

    async def send_and_wait(
            self,
            topic: str,
            key: Optional[bytes],
            value: bytes) -> Awaitable:
        raise NotImplementedError()
_ProducerT = Producer


class Transport:
    Consumer: type
    Producer: type

    url: str
    loop: asyncio.AbstractEventLoop

    def __init__(self,
                 url: str = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = url
        self.loop = loop

    def create_consumer(self, topic: Topic, callback: ConsumerCallback,
                        **kwargs) -> _ConsumerT:
        return cast(_ConsumerT, self.Consumer(
            self, topic=topic, callback=callback, **kwargs))

    def create_producer(self, **kwargs) -> _ProducerT:
        return cast(_ProducerT, self.Producer(self, **kwargs))

import asyncio
from typing import Sequence, Pattern
from .consumer import Consumer
from .types import K, V, Message, Serializer
from .utils.service import Service


class Stream(Service):

    _consumer: Consumer

    def __init__(self, name: str,
                 key_serializer: Serializer = None,
                 value_serializer: Serializer = None,
                 topics: Sequence[str] = None,
                 pattern: Pattern = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(loop=loop)
        self.name = name
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.topics = topics
        self.pattern = pattern
        self._consumer = None

    async def process(self, key: K, value: V) -> None:
        print('Received K/V: %r %r' % (key, value))

    async def on_start(self) -> None:
        self._consumer = self.get_consumer()
        await self._consumer.start()

    async def on_stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()

    async def on_message(self,
                         topic: str,
                         partition: str,
                         message: Message) -> None:
        print('Received message: %r' % (message,))
        await self.process(message.key, message.value)

    def get_consumer(self) -> Consumer:
        return Consumer(
            topics=self.topics,
            pattern=self.pattern,
            callback=self.on_message,
            loop=self.loop,
        )

import asyncio
from typing import Any, Callable, MutableMapping, Tuple, cast
from .consumer import Consumer
from .event import FieldDescriptor, from_tuple
from .types import K, V, Message, Topic
from .utils.service import Service


class stream:

    def __init__(self, topic: Topic,
                 group_by: FieldDescriptor = None,
                 **kwargs) -> None:
        self.topic = topic
        self.group_by = group_by

    def __call__(self, fun: Callable) -> 'GeneratorStream':
        return GeneratorStream(
            topic=self.topic,
            group_by=self.group_by,
            callback=fun,
        )


class Stream(Service):

    _consumer: Consumer

    def __init__(self, name: str = None,
                 topic: Topic = None,
                 group_by: FieldDescriptor = None,
                 callback: Callable = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        super().__init__(loop=loop)
        self.name = name
        self.topic = topic
        self.group_by = group_by
        self.callback = callback
        self._consumer = None
        self.type = self.topic.type
        self._quick_deserialize_k = self.topic.key_serializer
        self._quick_deserialize_v = self.topic.value_serializer
        self.on_init()

    def on_init(self) -> None:
        ...

    async def process(self, key: K, value: V) -> V:
        print('Received K/V: %r %r' % (key, value))
        if self.callback is not None:
            self.callback(key, value)
        return value

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
        k, v = self.to_KV(message)
        await self.process(k, v)

    def to_KV(self, message: Message) -> Tuple[K, V]:
        key = message.key
        if self._quick_deserialize_k:
            key = self._quick_deserialize_k(message.key)
        value = message.value
        if self._quick_deserialize_v:
            value = self._quick_deserialize_v(message.value)
        k = cast(K, key)
        return k, cast(V, from_tuple(self.type, k, value))

    def get_consumer(self) -> Consumer:
        return Consumer(
            topic=self.topic,
            callback=self.on_message,
            loop=self.loop,
        )


class Table(Stream):

    #: This maintains the state of the stream (the K/V store).
    _state: MutableMapping

    def on_init(self) -> None:
        self._state = {}

    async def on_message(self,
                         topic: str,
                         partition: str,
                         message: Message) -> None:
        k, v = self.to_KV(message)
        self._state[k] = await self.process(k, v)

    def __getitem__(self, key: Any) -> Any:
        return self._state[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        self._state[key] = value

    def __delitem__(self, key: Any) -> None:
        del self._state[key]


class GeneratorStream(Stream):

    async def process(self, key: K, value: V) -> None:
        self.callback.send(value)

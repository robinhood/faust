from typing import Any, Awaitable, Callable
from aiokafka.fetcher import ConsumerRecord as Message

__all__ = ['K', 'V', 'Serializer', 'Event']
K = str
V = Any
Serializer = Callable[[Any], Any]
ConsumerCallback = Callable[[str, str, Message], Awaitable]


class Event:
    key: K


class Task:
    ...   # TODO

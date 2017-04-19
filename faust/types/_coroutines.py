import abc
import asyncio
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Coroutine, Generator, Iterable, Union,
)
from faust.utils.services import ServiceT
from .models import Event

__all__ = [
    'InputStreamT',
    'StreamCoroutineCallback',
    'CoroCallbackT',
    'StreamCoroutine',
]


class InputStreamT(Iterable, AsyncIterable):
    queue: asyncio.Queue

    @abc.abstractmethod
    async def put(self, value: Event) -> None:
        ...

    @abc.abstractmethod
    async def next(self) -> Any:
        ...

    @abc.abstractmethod
    async def join(self, timeout: float = None):
        ...


StreamCoroutineCallback = Callable[[Event], Awaitable[None]]


class CoroCallbackT(ServiceT):

    def __init__(self,
                 inbox: InputStreamT,
                 callback: StreamCoroutineCallback = None,
                 **kwargs: Any) -> None:
        self.callback: StreamCoroutineCallback = callback

    async def send(self, value: Event) -> None:
        ...


StreamCoroutine = Union[
    Callable[[InputStreamT], Coroutine[Event, None, None]],
    Callable[[InputStreamT], AsyncIterable[Event]],
    Callable[[InputStreamT], Generator[Event, None, None]],
]

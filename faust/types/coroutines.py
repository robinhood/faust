import abc
import asyncio
from typing import (
    Any, AsyncIterable, Awaitable, Callable,
    Coroutine, Generator, Iterable, Union,
)
from .models import Event


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


class CoroCallbackT:

    def __init__(self, inbox: InputStreamT,
                 *,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    async def send(self,
                   value: Event,
                   callback: StreamCoroutineCallback) -> None:
        ...

    async def join(self) -> None:
        ...

    async def drain(self, callback: StreamCoroutineCallback) -> None:
        ...


StreamCoroutine = Union[
    Callable[[InputStreamT], Coroutine[Event, None, None]],
    Callable[[InputStreamT], AsyncIterable[Event]],
    Callable[[InputStreamT], Generator[Event, None, None]],
]

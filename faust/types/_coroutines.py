import abc
import asyncio
from typing import (
    Any, AsyncIterator, Awaitable, Callable,
    Coroutine, Generator, Iterable, Union,
)
from trish import ServiceT

__all__ = [
    'InputStreamT',
    'StreamCoroutineCallback',
    'CoroCallbackT',
    'StreamCoroutine',
]


class InputStreamT(Iterable, AsyncIterator):
    loop: asyncio.AbstractEventLoop
    queue: asyncio.Queue

    @abc.abstractmethod
    def __init__(self, *, loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    async def put(self, value: Any) -> None:
        ...

    @abc.abstractmethod
    async def next(self) -> Any:
        ...

    @abc.abstractmethod
    async def join(self, timeout: float = None):
        ...


StreamCoroutineCallback = Callable[[Any], Awaitable[None]]


class CoroCallbackT(ServiceT):

    @abc.abstractmethod
    def __init__(self,
                 inbox: InputStreamT,
                 callback: StreamCoroutineCallback = None,
                 **kwargs: Any) -> None:
        self.callback: StreamCoroutineCallback = callback

    @abc.abstractmethod
    async def send(self, value: Any) -> None:
        ...


StreamCoroutine = Union[
    Callable[[InputStreamT], Coroutine[Any, None, None]],
    Callable[[InputStreamT], AsyncIterator[Any]],
    Callable[[InputStreamT], Generator[Any, None, None]],
]

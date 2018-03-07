import abc
import typing
from typing import Any, Awaitable, Union
from mode.utils.compat import AsyncContextManager
from .codecs import CodecArg
from .core import K, V
from .tuples import Message, MessageSentCallback, RecordMetadata

if typing.TYPE_CHECKING:
    from .app import AppT
    from .channels import ChannelT
else:
    class AppT: ...  # noqa
    class ChannelT: ...  # noqa


class EventT(AsyncContextManager):
    app: AppT
    key: K
    value: V
    message: Message
    acked: bool

    __slots__ = ('app', 'key', 'value', 'message', 'acked')

    @abc.abstractmethod
    def __init__(self, app: AppT, key: K, value: V, message: Message) -> None:
        ...

    @abc.abstractmethod
    async def send(self,
                   channel: Union[str, ChannelT],
                   key: K = None,
                   value: V = None,
                   partition: int = None,
                   key_serializer: CodecArg = None,
                   value_serializer: CodecArg = None,
                   callback: MessageSentCallback = None,
                   force: bool = False) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    async def forward(self,
                      channel: Union[str, ChannelT],
                      key: Any = None,
                      value: Any = None,
                      partition: int = None,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None,
                      callback: MessageSentCallback = None,
                      force: bool = False) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    def ack(self) -> bool:
        ...

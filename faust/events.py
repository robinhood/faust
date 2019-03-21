"""Events received in streams."""
import typing
from types import TracebackType
from typing import Any, Awaitable, Optional, Type, Union, cast
from faust.types import (
    AppT,
    ChannelT,
    CodecArg,
    EventT,
    HeadersArg,
    K,
    Message,
    MessageSentCallback,
    RecordMetadata,
    V,
)

if typing.TYPE_CHECKING:  # pragma: no cover
    from .app.base import App as _App
else:
    class _App: ...  # noqa

USE_EXISTING_KEY = object()
USE_EXISTING_VALUE = object()
USE_EXISTING_HEADERS = object()


class Event(EventT):
    """An event received on a channel.

    Notes:
        - Events have a key and a value::

            event.key, event.value

        - They also have a reference to the original message
          (if available), such as a Kafka record:

            event.message.offset

        - Iteratiing over channels/topics yields Event:

            async for event in channel:
                ...

        - Iterating over a stream (that in turn iterate over channel) yields
          Event.value::

            async for value in channel.stream()  # value is event.value
                ...

        - If you only have a Stream object, you can also access underlying
          events by using ``Stream.events``.

            For example:

            .. sourcecode:: python

                async for event in channel.stream.events():
                    ...

          Also commonly used for finding the "current event" related to
          a value in the stream:

          .. sourcecode:: python

              stream = channel.stream()
              async for event in stream.events():
                  event = stream.current_event
                  message = event.message
                  topic = event.message.topic

          You can retrieve the current event in a stream to:

              - Get access to the serialized key+value.
              - Get access to message properties like, what topic+partition
                the value was received on, or its offset.

          If you want access to both key and value, you should use
          ``stream.items()`` instead.

            .. sourcecode:: python

                async for key, value in stream.items():
                    ...

            ``stream.current_event`` can also be accessed but you must take
            extreme care you are using the correct stream object. Methods
            such as ``.group_by(key)`` and ``.through(topic)`` returns cloned
            stream objects, so in the example:

            The best way to access the current_event in an agent is
            to use the contextvar:

            .. sourcecode:: python

                from faust import current_event

                @app.agent(topic)
                async def process(stream):
                    async for value in stream:
                        event = current_event()
    """

    def __init__(self,
                 app: AppT,
                 key: K,
                 value: V,
                 headers: Optional[HeadersArg],
                 message: Message) -> None:
        self.app: AppT = app
        self.key: K = key
        self.value: V = value
        self.message: Message = message
        if headers is not None:
            if not isinstance(headers, dict):
                self.headers = dict(headers)
            else:
                self.headers = headers
        else:
            self.headers = {}

        self.acked: bool = False

    async def send(self,
                   channel: Union[str, ChannelT],
                   key: K = USE_EXISTING_KEY,
                   value: V = USE_EXISTING_VALUE,
                   partition: int = None,
                   timestamp: float = None,
                   headers: Any = USE_EXISTING_HEADERS,
                   key_serializer: CodecArg = None,
                   value_serializer: CodecArg = None,
                   callback: MessageSentCallback = None,
                   force: bool = False) -> Awaitable[RecordMetadata]:
        """Send object to channel."""
        if key is USE_EXISTING_KEY:
            key = self.key
        if value is USE_EXISTING_VALUE:
            value = self.value
        if headers is USE_EXISTING_HEADERS:
            headers = self.headers
        return await self._send(
            channel,
            key,
            value,
            partition,
            timestamp,
            headers,
            key_serializer,
            value_serializer,
            callback,
            force=force,
        )

    async def forward(self,
                      channel: Union[str, ChannelT],
                      key: K = USE_EXISTING_KEY,
                      value: V = USE_EXISTING_VALUE,
                      partition: int = None,
                      timestamp: float = None,
                      headers: Any = USE_EXISTING_HEADERS,
                      key_serializer: CodecArg = None,
                      value_serializer: CodecArg = None,
                      callback: MessageSentCallback = None,
                      force: bool = False) -> Awaitable[RecordMetadata]:
        """Forward original message (will not be reserialized)."""
        if key is USE_EXISTING_KEY:
            key = self.message.key
        if value is USE_EXISTING_VALUE:
            value = self.message.value
        if headers is USE_EXISTING_HEADERS:
            headers = self.message.headers
            if not headers:
                headers = None
        return await self._send(
            channel,
            key,
            value,
            partition,
            timestamp,
            headers,
            key_serializer,
            value_serializer,
            callback,
            force=force,
        )

    async def _send(self,
                    channel: Union[str, ChannelT],
                    key: K = None,
                    value: V = None,
                    partition: int = None,
                    timestamp: float = None,
                    headers: HeadersArg = None,
                    key_serializer: CodecArg = None,
                    value_serializer: CodecArg = None,
                    callback: MessageSentCallback = None,
                    force: bool = False) -> Awaitable[RecordMetadata]:
        return await cast(_App, self.app)._attachments.maybe_put(
            channel,
            key,
            value,
            partition,
            timestamp,
            headers,
            key_serializer,
            value_serializer,
            callback,
            force=force,
        )

    def _attach(self,
                channel: Union[ChannelT, str],
                key: K = None,
                value: V = None,
                partition: int = None,
                timestamp: float = None,
                headers: HeadersArg = None,
                key_serializer: CodecArg = None,
                value_serializer: CodecArg = None,
                callback: MessageSentCallback = None,
                ) -> Awaitable[RecordMetadata]:
        return cast(_App, self.app)._attachments.put(
            self.message,
            channel,
            key,
            value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    def ack(self) -> bool:
        return self.message.ack(self.app.consumer)

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: k={self.key!r} v={self.value!r}>'

    async def __aenter__(self) -> EventT:
        return self

    async def __aexit__(self,
                        _exc_type: Type[BaseException] = None,
                        _exc_val: BaseException = None,
                        _exc_tb: TracebackType = None) -> Optional[bool]:
        self.ack()
        return None

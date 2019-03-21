"""Attachments - Deprecated module.

Attachments were used before transactions support.
"""
import asyncio
import typing
from collections import defaultdict
from heapq import heappop, heappush
from typing import (
    Awaitable, Callable, Iterator, List,
    MutableMapping, NamedTuple, Union, cast,
)

from mode.utils.objects import Unordered, cached_property

from faust.streams import current_event
from faust.types import AppT, ChannelT, CodecArg, RecordMetadata, TP
from faust.types.core import HeadersArg, K, V
from faust.types.tuples import FutureMessage, Message, MessageSentCallback

if typing.TYPE_CHECKING:
    from faust.events import Event as _Event
else:
    class _Event: ...       # noqa

__all__ = ['Attachment', 'Attachments']


class Attachment(NamedTuple):
    """Message attached to offset in source topic.

    The message will be published once that offset in the source
    topic is committed.
    """

    # Tuple used in heapq entry for Attachments._pending
    # These are used to delay producing of messages until source offset is
    # committed:
    #
    # @app.agent(source_topic)
    # async def process(stream):
    #    async for value in stream:
    #        await other_topic.send(value)  # does not send here!
    #
    # sending of the message is attached to the offset of value in the source
    # topic, so that when the event is acked, only then do we send the
    # message.  This gives better consistency: if we reprocess the event
    # we don't send messages twice.
    #
    # Note though: we need Kafka transactions to cover all cases of
    # inconsistencies.

    offset: int
    message: Unordered[FutureMessage]


class Attachments:
    """Attachments manager."""

    app: AppT

    # Mapping used to attach messages to a source message such that
    # only when the source message is acked, only then do we publish
    # its attached messages.
    #
    # The mapping maintains one list for each TopicPartition,
    # where the lists are used as heap queues containing tuples
    # of ``(source_message_offset, FutureMessage)``.
    _pending: MutableMapping[TP, List[Attachment]]

    def __init__(self, app: AppT) -> None:
        self.app = app
        self._pending = defaultdict(list)

    @cached_property
    def enabled(self) -> bool:
        return self.app.conf.stream_publish_on_commit

    async def maybe_put(self,
                        channel: Union[ChannelT, str],
                        key: K = None,
                        value: V = None,
                        partition: int = None,
                        timestamp: float = None,
                        headers: HeadersArg = None,
                        key_serializer: CodecArg = None,
                        value_serializer: CodecArg = None,
                        callback: MessageSentCallback = None,
                        force: bool = False) -> Awaitable[RecordMetadata]:
        # XXX The concept of attaching should be deprecated when we
        # have Kafka transaction support (:kip:`KIP-98`).
        # This is why the interface related to attaching is private.

        # attach message to current event if there is one.
        send: Callable = self.app.send
        if self.enabled and not force:
            event = current_event()
            if event is not None:
                return cast(_Event, event)._attach(
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
        return await send(
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

    def put(self,
            message: Message,
            channel: Union[str, ChannelT],
            key: K,
            value: V,
            partition: int = None,
            timestamp: float = None,
            headers: HeadersArg = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        # This attaches message to be published when source message' is
        # acknowledged.  To be replaced by transactions in :kip:`KIP-98`.

        # get heap queue for this TopicPartition
        # items in this list are ``(source_offset, Unordered[FutureMessage])``
        # tuples.
        buf = self._pending[message.tp]
        chan = self.app.topic(channel) if isinstance(channel, str) else channel
        fut = chan.as_future_message(
            key, value, partition, timestamp, headers,
            key_serializer, value_serializer, callback)
        # Note: Since FutureMessage have members that are unhashable
        # we wrap it in an Unordered object to stop heappush from crashing.
        # Unordered simply orders by random order, which is fine
        # since offsets are always unique.
        heappush(buf, Attachment(message.offset, Unordered(fut)))
        return fut

    async def commit(self, tp: TP, offset: int) -> None:
        await asyncio.wait(
            await self.publish_for_tp_offset(tp, offset),
            return_when=asyncio.ALL_COMPLETED,
            loop=self.app.loop,
        )

    async def publish_for_tp_offset(
            self, tp: TP, offset: int) -> List[Awaitable[RecordMetadata]]:
        # publish pending messages attached to this TP+offset

        # make shallow copy to allow concurrent modifications (append)
        attached = list(self._attachments_for(tp, offset))
        return [
            await fut.message.channel.publish_message(fut, wait=False)
            for fut in attached
        ]

    def _attachments_for(self, tp: TP,
                         commit_offset: int) -> Iterator[FutureMessage]:
        # Return attached messages for TopicPartition within committed offset.
        attached = self._pending.get(tp)
        while attached:
            # get the entry with the smallest offset in this TP
            entry = heappop(attached)

            # if the entry offset is smaller or equal to the offset
            # being committed
            if entry[0] <= commit_offset:
                # we use it by extracting the FutureMessage
                # from Attachment tuple, where entry.message is
                # Unordered[FutureMessage].
                yield entry.message.value
            else:
                # else we put it back and exit (this was the smallest offset).
                heappush(attached, entry)
                break

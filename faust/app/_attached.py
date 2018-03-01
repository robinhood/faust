import asyncio
import typing
from collections import defaultdict
from heapq import heappop, heappush
from typing import (
    Awaitable, Callable, Iterator, List,
    MutableMapping, NamedTuple, Union, cast,
)
from ..streams import current_event
from ..types import AppT, ChannelT, CodecArg, K, RecordMetadata, TP, V
from ..types.tuples import FutureMessage, Message, MessageSentCallback
from ..utils.objects import Unordered

if typing.TYPE_CHECKING:
    from ..channels import Event
else:
    class Event: ...       # noqa

__all__ = ['Attachment', 'Attachments']


class Attachment(NamedTuple):
    # Tuple used in heapq entry for Attachments._pending
    # These are used to send messages when an offset is committed
    # (sending of the message is attached to an offset in a source topic).
    offset: int
    message: Unordered[FutureMessage]


class Attachments:
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

    async def maybe_put(
            self,
            channel: Union[ChannelT, str],
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            force: bool = False) -> Awaitable[RecordMetadata]:
        # XXX The concept of attaching should be deprecated when we
        # have Kafka transaction support (:kip:`KIP-98`).
        # This is why the interface related to attaching is private.

        # attach message to current event if there is one.
        send: Callable = self.app.send
        if not force:
            event = current_event()
            if event is not None:
                send = cast(Event, event)._attach
        return await send(
            channel, key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            callback=callback,
        )

    def put(
            self,
            message: Message,
            channel: Union[str, ChannelT],
            key: K,
            value: V,
            partition: int = None,
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
            key, value, partition,
            key_serializer, value_serializer, callback)
        # Note: Since FutureMessage have members that are unhashable
        # we wrap it in an Unordered object to stop heappush from crashing.
        # Unordered simply orders by random order, which is fine
        # since offsets are always unique.
        heappush(buf, Attachment(message.offset, Unordered(fut)))
        return fut

    async def commit(self, tp: TP, offset: int) -> None:
        # publish pending messages attached to this TP+offset

        # make shallow copy to allow concurrent modifications (append)
        attached = list(self._attachments_for(tp, offset))
        if attached:
            await asyncio.wait(
                [await fut.message.channel.publish_message(fut, wait=False)
                 for fut in attached],
                return_when=asyncio.ALL_COMPLETED,
                loop=self.app.loop,
            )

    def _attachments_for(
            self, tp: TP, commit_offset: int) -> Iterator[FutureMessage]:
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
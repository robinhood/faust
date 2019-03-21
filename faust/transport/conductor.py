"""The conductor delegates messages from the consumer to the streams."""
import asyncio
import os
import typing
from collections import defaultdict
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    MutableSet,
    Optional,
    Set,
    Tuple,
    cast,
)
from mode import Service, get_logger
from mode.utils.futures import notify

from faust.exceptions import KeyDecodeError, ValueDecodeError
from faust.types import AppT, EventT, K, Message, TP, V
from faust.types.topics import TopicT
from faust.types.transports import ConductorT, ConsumerCallback, TPorTopicSet
from faust.types.tuples import tp_set_to_map
from faust.utils.tracing import traced_from_parent_span

if typing.TYPE_CHECKING:  # pragma: no cover
    from faust.topics import Topic as _Topic
else:
    class _Topic: ...  # noqa

NO_CYTHON = bool(os.environ.get('NO_CYTHON', False))

if not NO_CYTHON:  # pragma: no cover
    try:
        from ._cython.conductor import ConductorHandler
    except ImportError:
        ConductorHandler = None
else:  # pragma: no cover
    ConductorHandler = None

__all__ = ['Conductor', 'ConductorCompiler']

logger = get_logger(__name__)


class ConductorCompiler:  # pragma: no cover
    """Pregenerate a function to handle the messages for a topic+partition."""

    def build(self,
              conductor: 'Conductor',
              tp: TP,
              channels: MutableSet[_Topic]) -> ConsumerCallback:
        # This method localizes variables and attribute access
        # for better performance.  This is part of the inner loop
        # of a Faust worker, so tiny improvements here has big impact.

        topic, partition = tp
        app = conductor.app
        on_topic_buffer_full = app.sensors.on_topic_buffer_full
        acquire_flow_control: Callable = app.flow_control.acquire
        len_: Callable[[Any], int] = len

        async def on_message(message: Message) -> None:
            # when a message is received we find all channels
            # that subscribe to this message
            await acquire_flow_control()
            channels_n = len_(channels)
            if channels_n:
                # we increment the reference count for this message in bulk
                # immediately, so that nothing will get a chance to decref to
                # zero before we've had the chance to pass it to all channels
                message.incref(channels_n)
                event: Optional[EventT] = None
                event_keyid: Optional[Tuple[K, V]] = None

                # forward message to all channels subscribing to this topic

                # keep track of the number of channels we delivered to,
                # so that if a DecodeError is raised we can propagate
                # that errors to the remaining channels.
                delivered: Set[_Topic] = set()
                full: List[Tuple[EventT, _Topic]] = []
                try:
                    for chan in channels:
                        keyid = chan.key_type, chan.value_type
                        if event is None:
                            # first channel deserializes the payload:
                            event = await chan.decode(message, propagate=True)
                            event_keyid = keyid

                            queue = chan.queue
                            if queue.full():
                                full.append((event, chan))
                                continue
                            queue.put_nowait(event)
                        else:
                            # subsequent channels may have a different
                            # key/value type pair, meaning they all can
                            # deserialize the message in different ways

                            dest_event: EventT
                            if keyid == event_keyid:
                                # Reuse the event if it uses the same keypair:
                                dest_event = event
                            else:
                                dest_event = await chan.decode(
                                    message, propagate=True)
                            queue = chan.queue
                            if queue.full():
                                full.append((dest_event, chan))
                                continue
                            queue.put_nowait(dest_event)
                        delivered.add(chan)
                    if full:
                        for _, dest_chan in full:
                            on_topic_buffer_full(dest_chan)
                        await asyncio.wait(
                            [dest_chan.put(dest_event)
                             for dest_event, dest_chan in full],
                            return_when=asyncio.ALL_COMPLETED,
                        )
                except KeyDecodeError as exc:
                    remaining = channels - delivered
                    message.ack(app.consumer, n=len(remaining))
                    for channel in remaining:
                        await channel.on_key_decode_error(exc, message)
                        delivered.add(channel)
                except ValueDecodeError as exc:
                    remaining = channels - delivered
                    message.ack(app.consumer, n=len(remaining))
                    for channel in remaining:
                        await channel.on_value_decode_error(exc, message)
                        delivered.add(channel)
        return on_message


class Conductor(ConductorT, Service):
    """Manages the channels that subscribe to topics.

    - Consumes messages from topic using a single consumer.
    - Forwards messages to all channels subscribing to a topic.
    """

    logger = logger

    #: Fast index to see if Topic is registered.
    _topics: MutableSet[TopicT]

    #: Map of (topic,partition) to set of channels that subscribe to that TP.
    _tp_index: MutableMapping[TP, MutableSet[TopicT]]

    #: Map str topic name to set of channels that subscribe
    #: to that topic.
    _topic_name_index: MutableMapping[str, MutableSet[TopicT]]

    #: For every TP assigned we compile a callback closure,
    #: and when we receive a message to a TP we look up which callback
    #: to call here.
    _tp_to_callback: MutableMapping[TP, ConsumerCallback]

    #: Whenever a change is made, i.e. a Topic is added/removed, we notify
    #: the background task responsible for resubscribing.
    _subscription_changed: Optional[asyncio.Event]

    _subscription_done: Optional[asyncio.Future]

    _acking_topics: Set[str]

    _compiler: ConductorCompiler

    #: We wait for 45 seconds after a resubscription request, to make
    #: sure any later requests are handled at the same time.
    _resubscribe_sleep_lock_seconds: float = 45.0

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self._topics = set()
        self._topic_name_index = defaultdict(set)
        self._tp_index = defaultdict(set)
        self._tp_to_callback = {}
        self._acking_topics = set()
        self._subscription_changed = None
        self._subscription_done = None
        self._compiler = ConductorCompiler()
        # we compile the closure used for receive messages
        # (this just optimizes symbol lookups, localizing variables etc).
        self.on_message: ConsumerCallback
        self.on_message = self._compile_message_handler()

    async def commit(self, topics: TPorTopicSet) -> bool:
        return await self.app.consumer.commit(topics)

    def acks_enabled_for(self, topic: str) -> bool:
        return topic in self._acking_topics

    def _compile_message_handler(self) -> ConsumerCallback:
        # This method localizes variables and attribute access
        # for better performance.  This is part of the inner loop
        # of a Faust worker, so tiny improvements here has big impact.

        get_callback_for_tp = self._tp_to_callback.__getitem__

        async def on_message(message: Message) -> None:
            return await get_callback_for_tp(message.tp)(message)

        return on_message

    @Service.task
    async def _subscriber(self) -> None:  # pragma: no cover
        # the first time we start, we will wait two seconds
        # to give agents a chance to start up and register their
        # streams.  This way we won't have N subscription requests at the
        # start.
        await self.sleep(2.0)
        if not self.should_stop:
            # tell the consumer to subscribe to the topics.
            await self.app.consumer.subscribe(await self._update_indices())
            notify(self._subscription_done)

            # Now we wait for changes
            ev = self._subscription_changed = asyncio.Event(loop=self.loop)
        while not self.should_stop:
            # Wait for something to add/remove topics from subscription.
            await ev.wait()
            if self.app.rebalancing:
                # we do not want to perform a resubscribe if the application
                # is rebalancing.
                ev.clear()
            else:
                # The change could be in reaction to something like "all agents
                # restarting", in that case it would be bad if we resubscribe
                # over and over, so we wait for 45 seconds to make sure any
                # further subscription requests will happen during the same
                # rebalance.
                await self.sleep(self._resubscribe_sleep_lock_seconds)
                subscribed_topics = await self._update_indices()
                await self.app.consumer.subscribe(subscribed_topics)

            # clear the subscription_changed flag, so we can wait on it again.
            ev.clear()
            # wake-up anything waiting for the subscription to be done.
            notify(self._subscription_done)

    async def wait_for_subscriptions(self) -> None:
        if self._subscription_done is not None:
            await self._subscription_done

    async def _update_indices(self) -> Iterable[str]:
        self._topic_name_index.clear()
        self._tp_to_callback.clear()
        for channel in self._topics:
            if channel.internal:
                await channel.maybe_declare()
            for topic in channel.topics:
                if channel.acks:
                    self._acking_topics.add(topic)
                self._topic_name_index[topic].add(channel)

        return self._topic_name_index

    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        T = traced_from_parent_span()
        self._tp_index.clear()
        T(self._update_tp_index)(assigned)
        T(self._update_callback_map)()

    def _update_tp_index(self, assigned: Set[TP]) -> None:
        assignmap = tp_set_to_map(assigned)
        tp_index = self._tp_index
        for topic in self._topics:
            if topic.active_partitions is not None:
                # Isolated Partitions: One agent per partition.
                if topic.active_partitions:
                    if assigned:
                        assert topic.active_partitions.issubset(assigned)
                    for tp in topic.active_partitions:
                        tp_index[tp].add(topic)
            else:
                # Default: One agent receives messages for all partitions.
                for subtopic in topic.topics:
                    for tp in assignmap[subtopic]:
                        tp_index[tp].add(topic)

    def _update_callback_map(self) -> None:
        self._tp_to_callback.update(
            (tp, self._build_handler(tp, cast(MutableSet[_Topic], channels)))
            for tp, channels in self._tp_index.items()
        )

    def _build_handler(self,
                       tp: TP,
                       channels: MutableSet[_Topic]) -> ConsumerCallback:
        if ConductorHandler is not None:  # pragma: no cover
            return ConductorHandler(self, tp, channels)
        else:
            return self._compiler.build(self, tp, channels)

    def clear(self) -> None:
        self._topics.clear()
        self._topic_name_index.clear()
        self._tp_index.clear()
        self._tp_to_callback.clear()
        self._acking_topics.clear()

    def __contains__(self, value: Any) -> bool:
        return value in self._topics

    def __iter__(self) -> Iterator[TopicT]:
        return iter(self._topics)

    def __len__(self) -> int:
        return len(self._topics)

    def __hash__(self) -> int:
        return object.__hash__(self)

    def add(self, topic: Any) -> None:
        if topic not in self._topics:
            self._topics.add(topic)
            if self._topic_contain_unsubscribed_topics(topic):
                self._flag_changes()

    def _topic_contain_unsubscribed_topics(self, topic: TopicT) -> bool:
        index = self._topic_name_index
        return bool(index and any(t not in index for t in topic.topics))

    def discard(self, topic: Any) -> None:
        self._topics.discard(topic)

    def _flag_changes(self) -> None:
        if self._subscription_changed is not None:
            self._subscription_changed.set()
        if self._subscription_done is None:
            self._subscription_done = asyncio.Future(loop=self.loop)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}({len(self._topics)})'

    @property
    def shortlabel(self) -> str:
        return type(self).__name__

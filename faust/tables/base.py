"""Base class Collection for Tables/Sets/etc."""
import abc
from collections import defaultdict
from datetime import datetime
from heapq import heappop, heappush
from typing import (
    Any, Callable, Iterable, Iterator, List, Mapping,
    MutableMapping, MutableSet, Optional, Union, cast,
)
from mode import Seconds, Service
from yarl import URL
from .. import stores
from ..channels import Event
from ..streams import current_event
from ..streams import joins
from ..types import (
    AppT, EventT, FieldDescriptorT, FutureMessage, JoinT,
    RecordMetadata, TopicPartition, TopicT,
)
from ..types.models import ModelArg
from ..types.stores import StoreT
from ..types.streams import JoinableT, StreamT
from ..types.tables import CollectionT
from ..types.windows import WindowRange, WindowT

__all__ = ['Collection']

TABLE_CLEANING = 'CLEANING'


class Collection(Service, CollectionT):

    _store: URL
    _changelog_topic: TopicT
    _timestamp_keys: MutableMapping[float, MutableSet]
    _timestamps: List[float]

    @abc.abstractmethod
    def _get_key(self, key: Any) -> Any:
        ...

    @abc.abstractmethod
    def _set_key(self, key: Any, value: Any) -> None:
        ...

    @abc.abstractmethod
    def _del_key(self, key: Any) -> None:
        ...

    def __init__(self, app: AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: Union[str, URL] = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 help: str = None,
                 **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self.name = name
        self.default = default
        self._store = URL(store) if store else None
        self.key_type = key_type
        self.value_type = value_type
        self.partitions = partitions
        self.window = window
        self.changelog_topic = changelog_topic
        self.help = help

        # Table key expiration
        self._timestamp_keys = defaultdict(set)
        self._timestamps = []

        if self.StateStore is not None:
            self.data = self.StateStore(url=None, app=app, loop=self.loop)
        else:
            url = self._store or self.app.store
            self.data = stores.by_url(url)(
                url, app,
                table_name=self.name,
                loop=self.loop)

        # Table.start() also starts Store
        self.add_dependency(cast(StoreT, self.data))

        # Aliases
        self._sensor_on_get = self.app.sensors.on_table_get
        self._sensor_on_set = self.app.sensors.on_table_set
        self._sensor_on_del = self.app.sensors.on_table_del

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app.tables mapping.
        return object.__hash__(self)

    async def on_start(self) -> None:
        await self.changelog_topic.maybe_declare()

    def info(self) -> Mapping[str, Any]:
        # Used to recreate object in .clone()
        return {
            'app': self.app,
            'name': self.name,
            'store': self._store,
            'default': self.default,
            'key_type': self.key_type,
            'value_type': self.value_type,
            'changelog_topic': self._changelog_topic,
            'window': self.window,
        }

    def persisted_offset(self, tp: TopicPartition) -> Optional[int]:
        return self.data.persisted_offset(tp)

    async def need_active_standby_for(self, tp: TopicPartition) -> bool:
        return await self.data.need_active_standby_for(tp)

    def reset_state(self) -> None:
        self.data.reset_state()

    def _send_changelog(self, key: Any, value: Any) -> None:
        event = current_event()
        partition: int = None
        if event is not None:
            send = cast(Event, event)._attach
            partition = event.message.partition
        else:
            send = self.app.send_soon
        send(self.changelog_topic, key, value,
             partition=partition,
             key_serializer='json',
             value_serializer='json',
             callback=self._on_changelog_sent)

    def _on_changelog_sent(self, fut: FutureMessage) -> None:
        res: RecordMetadata = fut.result()
        self.data.set_persisted_offset(res.topic_partition, res.offset)

    @Service.task
    @Service.transitions_to(TABLE_CLEANING)
    async def _clean_data(self) -> None:
        if self._should_expire_keys():
            timestamps = self._timestamps
            window = self.window
            while not self.should_stop:
                while timestamps and window.stale(timestamps[0]):
                    timestamp = heappop(timestamps)
                    for key in self._timestamp_keys[timestamp]:
                        del self.data[key]
                    del self._timestamp_keys[timestamp]
                await self.sleep(self.app.table_cleanup_interval)

    def _should_expire_keys(self) -> bool:
        window = self.window
        return not (window is None or window.expires is None)

    def _maybe_set_key_ttl(self, key: Any) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        heappush(self._timestamps, window_range.end)
        self._timestamp_keys[window_range.end].add(key)

    def _maybe_del_key_ttl(self, key: Any) -> None:
        if not self._should_expire_keys():
            return
        _, window_range = key
        ts_keys = self._timestamp_keys.get(window_range.end)
        ts_keys.discard(key)

    def _changelog_topic_name(self) -> str:
        return f'{self.app.id}-{self.name}-changelog'

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.LeftJoin(stream=self, fields=fields))

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.InnerJoin(stream=self, fields=fields))

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.OuterJoin(stream=self, fields=fields))

    def _join(self, join_strategy: JoinT) -> StreamT:
        # TODO
        raise NotImplementedError('TODO')

    def clone(self, **kwargs: Any) -> Any:
        return self.__class__(**{**self.info(), **kwargs})

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        # TODO
        raise NotImplementedError('TODO')

    def _new_changelog_topic(self, *,
                             retention: Seconds = None,
                             compacting: bool = True,
                             deleting: bool = None) -> TopicT:
        return self.app.topic(
            self._changelog_topic_name(),
            key_type=self.key_type,
            value_type=self.value_type,
            key_serializer='json',
            value_serializer='json',
            partitions=self.partitions,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            acks=False,
        )

    def __copy__(self) -> Any:
        return self.clone()

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def _apply_window_op(self,
                         op: Callable[[Any, Any], Any],
                         key: Any,
                         value: Any,
                         event: EventT = None) -> None:
        get_ = self._get_key
        set_ = self._set_key
        for window_range in self._window_ranges(event):
            set_((key, window_range), op(get_((key, window_range)), value))

    def _set_windowed(self, key: Any, value: Any,
                      event: EventT = None) -> None:
        for window_range in self._window_ranges(event):
            self._set_key((key, window_range), value)

    def _del_windowed(self, key: Any, event: EventT = None) -> None:
        for window_range in self._window_ranges(event):
            self._del_key((key, window_range))

    def _window_ranges(self, event: EventT = None) -> Iterator[WindowRange]:
        timestamp = self._get_timestamp(event)
        for window_range in self.window.ranges(timestamp):
            yield window_range

    def _windowed_now(self, key: Any) -> Any:
        now = datetime.utcnow().timestamp()
        return self._get_key((key, self.window.current(now)))

    def _windowed_current(self, key: Any, event: EventT = None) -> Any:
        return self._get_key(
            (key, self.window.current(self._get_timestamp(event))))

    def _windowed_delta(self, key: Any, d: Seconds,
                        event: EventT = None) -> Any:
        return self._get_key(
            (key, self.window.delta(self._get_timestamp(event), d)))

    def _get_timestamp(self, event: EventT = None) -> float:
        return (event or current_event()).message.timestamp

    async def on_partitions_assigned(
            self, assigned: Iterable[TopicPartition]) -> None:
        await self.data.on_partitions_assigned(self, assigned)

    async def on_partitions_revoked(
            self, revoked: Iterable[TopicPartition]) -> None:
        await self.data.on_partitions_revoked(self, revoked)

    @property
    def label(self) -> str:
        return f'{self.shortlabel}@{self._store}'

    @property
    def shortlabel(self) -> str:
        return f'{type(self).__name__}: {self.name}'

    @property
    def changelog_topic(self) -> TopicT:
        if self._changelog_topic is None:
            self._changelog_topic = self._new_changelog_topic(compacting=True)
        return self._changelog_topic

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        self._changelog_topic = topic

    def apply_changelog_batch(self, batch: Iterable[EventT]) -> None:
        self.data.apply_changelog_batch(
            batch,
            to_key=self._to_key,
            to_value=self._to_value,
        )

    def _to_key(self, k: Any) -> Any:
        if isinstance(k, list):
            # Lists are not hashable, and windowed-keys are json
            # serialized into a list.
            return tuple(tuple(v) if isinstance(v, list) else v for v in k)
        return k

    def _to_value(self, v: Any) -> Any:
        return v


__flake8_RecordMetadata_is_used: RecordMetadata  # XXX flake8 bug

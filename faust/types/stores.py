import abc
import typing
from typing import Any, Callable, Iterable, MutableMapping, Optional, Union
from mode import ServiceT
from yarl import URL
from .channels import EventT
from .codecs import CodecArg
from .tuples import TopicPartition

if typing.TYPE_CHECKING:
    from .app import AppT
    from .tables import CollectionT
else:
    class AppT: ...    # noqa
    class CollectionT: ...  # noqa

__all__ = ['StoreT']


class StoreT(ServiceT, MutableMapping):

    url: URL
    app: AppT
    table_name: str
    key_serializer: CodecArg
    value_serializer: CodecArg

    @abc.abstractmethod
    def __init__(self, url: Union[str, URL], app: AppT,
                 *,
                 table_name: str = '',
                 key_serializer: CodecArg = '',
                 value_serializer: CodecArg = '',
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def persisted_offset(self, tp: TopicPartition) -> Optional[int]:
        ...

    @abc.abstractmethod
    def set_persisted_offset(self, tp: TopicPartition, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def need_active_standby_for(self, tp: TopicPartition) -> bool:
        ...

    @abc.abstractmethod
    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        ...

    @abc.abstractmethod
    def reset_state(self) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(
            self,
            table: CollectionT,
            assigned: Iterable[TopicPartition]) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(
            self,
            table: CollectionT,
            revoked: Iterable[TopicPartition]) -> None:
        ...

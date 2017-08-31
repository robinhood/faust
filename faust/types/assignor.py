import abc
import typing
from typing import Iterable, List, MutableMapping
from .core import K
from .tables import CollectionT
from .topics import TopicPartition


TopicPartitionsMap = MutableMapping[str, List[int]]
HostPartitionsMap = MutableMapping[str, TopicPartitionsMap]


if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...      # noqa


class PartitionAssignorT(abc.ABC):

    replicas: int
    app: AppT

    @abc.abstractmethod
    def assigned_standbys(self) -> Iterable[TopicPartition]:
        ...

    @abc.abstractmethod
    def assigned_actives(self) -> Iterable[TopicPartition]:
        ...

    @abc.abstractmethod
    def key_store(self, table: CollectionT, key: K) -> str:
        ...

    @abc.abstractmethod
    def table_metadata(self, table: CollectionT) -> HostPartitionsMap:
        ...

    @abc.abstractmethod
    def tables_metadata(self) -> HostPartitionsMap:
        ...

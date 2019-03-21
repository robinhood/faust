import abc
import typing
from typing import List, MutableMapping, Set

from mode import ServiceT
from yarl import URL

from .tuples import TP

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
else:
    class _AppT: ...      # noqa

__all__ = [
    'TopicToPartitionMap',
    'HostToPartitionMap',
    'PartitionAssignorT',
    'LeaderAssignorT',
]

TopicToPartitionMap = MutableMapping[str, List[int]]
HostToPartitionMap = MutableMapping[str, TopicToPartitionMap]


class PartitionAssignorT(abc.ABC):

    replicas: int
    app: _AppT

    @abc.abstractmethod
    def __init__(self, app: _AppT, replicas: int = 0) -> None:
        ...

    @abc.abstractmethod
    def group_for_topic(self, topic: str) -> int:
        ...

    @abc.abstractmethod
    def assigned_standbys(self) -> Set[TP]:
        ...

    @abc.abstractmethod
    def assigned_actives(self) -> Set[TP]:
        ...

    @abc.abstractmethod
    def is_active(self, tp: TP) -> bool:
        ...

    @abc.abstractmethod
    def is_standby(self, tp: TP) -> bool:
        ...

    @abc.abstractmethod
    def key_store(self, topic: str, key: bytes) -> URL:
        ...

    @abc.abstractmethod
    def table_metadata(self, topic: str) -> HostToPartitionMap:
        ...

    @abc.abstractmethod
    def tables_metadata(self) -> HostToPartitionMap:
        ...


class LeaderAssignorT(ServiceT):

    app: _AppT

    @abc.abstractmethod
    def is_leader(self) -> bool:
        ...

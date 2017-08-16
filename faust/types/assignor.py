import abc
from typing import Iterable
from .topics import TopicPartition


class PartitionAssignorT(abc.ABC):

    replicas: int

    @abc.abstractmethod
    def assigned_standbys(self) -> Iterable[TopicPartition]:
        ...

    @abc.abstractmethod
    def assigned_actives(self) -> Iterable[TopicPartition]:
        ...

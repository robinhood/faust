import abc
from typing import Any, List
from .graphs import DependencyGraphT

__all__ = ['NodeT']


class NodeT(abc.ABC):
    root: 'NodeT'
    children: List[Any]
    prev: 'NodeT'
    data: Any

    @classmethod
    @abc.abstractmethod
    def _new_node(cls, data: Any, **kwargs: Any) -> 'NodeT':
        ...

    @abc.abstractmethod
    def new(self, data: Any) -> 'NodeT':
        ...

    @abc.abstractmethod
    def add(self, data: Any) -> None:
        ...

    @abc.abstractmethod
    def discard(self, data: Any) -> None:
        ...

    @abc.abstractmethod
    def reattach(self, parent: 'NodeT') -> 'NodeT':
        ...

    @abc.abstractmethod
    def depth(self) -> int:
        ...

    @abc.abstractmethod
    def as_graph(self) -> DependencyGraphT:
        ...

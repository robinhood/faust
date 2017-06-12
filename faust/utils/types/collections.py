import abc
from typing import Any, Iterator, List
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
    def traverse(self) -> Iterator['NodeT']:
        ...

    @abc.abstractmethod
    def walk(self) -> Iterator['NodeT']:
        ...

    @abc.abstractmethod
    def as_graph(self) -> DependencyGraphT:
        ...

    @property
    @abc.abstractmethod
    def prev(self) -> 'NodeT':
        ...

    @prev.setter
    def prev(self, node: 'NodeT') -> None:
        ...

    @property
    @abc.abstractmethod
    def root(self) -> 'NodeT':
        ...

    @root.setter
    def root(self, ndoe: 'NodeT') -> None:
        ...

    @property
    @abc.abstractmethod
    def depth(self) -> int:
        ...

    @property
    @abc.abstractmethod
    def path(self) -> str:
        ...

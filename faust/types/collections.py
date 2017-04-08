import abc
from typing import Any, List

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

import abc
from typing import Any, IO, Iterable, Mapping, MutableMapping, Sequence


class GraphFormatterT(abc.ABC):

    scheme: Mapping[str, Any]
    edge_scheme: Mapping[str, Any]
    node_scheme: Mapping[str, Any]
    term_scheme: Mapping[str, Any]
    graph_scheme: Mapping[str, Any]

    @abc.abstractmethod
    def __init__(self,
                 root: Any = None,
                 type: str = None,
                 id: str = None,
                 indent: int = 0,
                 inw: str = ' ' * 4,
                 **scheme: Any) -> None:
        ...

    @abc.abstractmethod
    def attr(self, name: str, value: Any) -> str:
        ...

    @abc.abstractmethod
    def attrs(self, d: Mapping = None, scheme: Mapping = None) -> str:
        ...

    @abc.abstractmethod
    def head(self, **attrs: Any) -> str:
        ...

    @abc.abstractmethod
    def tail(self) -> str:
        ...

    @abc.abstractmethod
    def label(self, obj: Any) -> str:
        ...

    @abc.abstractmethod
    def node(self, obj: Any, **attrs: Any) -> str:
        ...

    @abc.abstractmethod
    def terminal_node(self, obj: Any, **attrs: Any) -> str:
        ...

    @abc.abstractmethod
    def edge(self, a: Any, b: Any, **attrs: Any) -> str:
        ...

    @abc.abstractmethod
    def FMT(self, fmt: str, *args: Any, **kwargs: Any) -> str:
        ...

    @abc.abstractmethod
    def draw_edge(self, a: Any, b: Any,
                  scheme: Mapping = None,
                  attrs: Mapping = None) -> str:
        ...

    @abc.abstractmethod
    def draw_node(self, obj: Any,
                  scheme: Mapping = None,
                  attrs: Mapping = None) -> str:
        ...


class DependencyGraphT(abc.ABC, Mapping):

    adjacent: MutableMapping

    @abc.abstractmethod
    def __init__(self,
                 it: Iterable = None,
                 formatter: GraphFormatterT = None) -> None:
        ...

    @abc.abstractmethod
    def add_arc(self, obj: Any) -> None:
        ...

    @abc.abstractmethod
    def add_edge(self, A: Any, B: Any) -> None:
        ...

    @abc.abstractmethod
    def connect(self, graph: 'DependencyGraphT') -> None:
        ...

    @abc.abstractmethod
    def topsort(self) -> Sequence:
        ...

    @abc.abstractmethod
    def valency_of(self, obj: Any) -> int:
        ...

    @abc.abstractmethod
    def update(self, it: Iterable) -> None:
        ...

    @abc.abstractmethod
    def edges(self) -> Iterable:
        ...

    @abc.abstractmethod
    def to_dot(self, fh: IO, *, formatter: GraphFormatterT = None) -> None:
        ...

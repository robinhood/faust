from typing import (
    Any, ItemsView, Iterator, KeysView, List,
    MutableMapping, ValuesView, cast,
)
from collections import UserDict, deque
from .graphs import DependencyGraph
from .types.collections import NodeT
from .types.graphs import DependencyGraphT

__all__ = ['Node', 'FastUserDict', 'ManagedUserDict']


class Node(NodeT):

    @classmethod
    def _new_node(cls, data: Any, **kwargs: Any) -> NodeT:
        return cls(data, **kwargs)  # type: ignore

    def __init__(self, data: Any,
                 *,
                 root: NodeT = None,
                 prev: NodeT = None,
                 children: List[NodeT] = None) -> None:
        self.data = data
        self.root = root
        self.prev = prev
        self.children = children or []

    def new(self, data: Any) -> NodeT:
        node = self._new_node(
            data,
            root=self.root if self.root is not None else self,
            prev=self,
        )
        self.children.append(node)
        return node

    def add(self, data: Any) -> None:
        self.children.append(data)

    def as_graph(self) -> DependencyGraphT:
        graph = DependencyGraph()
        stack = deque([self])
        while stack:
            node = stack.popleft()
            for child in node.children:
                graph.add_arc(node.data)
                if isinstance(child, NodeT):
                    stack.append(cast(Node, child))
                    graph.add_edge(node.data, child.data)
                else:
                    graph.add_edge(node.data, child)
        return graph


class FastUserDict(UserDict):

    data: MutableMapping

    # Mypy forces us to redefine these, for some reason:

    def __getitem__(self, key: Any) -> Any:
        if not hasattr(self, '__missing__'):
            return self.data[key]
        if key in self.data:
            return self.data[key]
        return self.__missing__(key)  # type: ignore

    def __setitem__(self, key: Any, value: Any) -> None:
        self.data[key] = value

    def __delitem__(self, key: Any) -> None:
        del self.data[key]

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator:
        return iter(self.data)

    # Rest is fast versions of generic slow MutableMapping methods.

    def __contains__(self, key: Any) -> bool:
        return key in self.data

    def update(self, *args: Any, **kwargs: Any) -> None:
        self.data.update(*args, **kwargs)

    def clear(self) -> None:
        self.data.clear()

    def items(self) -> ItemsView:
        return cast(ItemsView, self.data.items())

    def keys(self) -> KeysView:
        return cast(KeysView, self.data.keys())

    def values(self) -> ValuesView:
        return self.data.values()


class ManagedUserDict(FastUserDict):

    def on_key_get(self, key: Any) -> None:
        ...

    def on_key_set(self, key: Any, value: Any) -> None:
        ...

    def on_key_del(self, key: Any) -> None:
        ...

    def on_clear(self) -> None:
        ...

    def __getitem__(self, key: Any) -> Any:
        self.on_key_get(key)
        return super().__getitem__(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        self.on_key_set(key, value)
        self.data[key] = value

    def __delitem__(self, key: Any) -> None:
        self.on_key_del(key)
        del self.data[key]

    def update(self, *args: Any, **kwargs: Any) -> None:
        for d in args:
            for key, value in d.items():
                self.on_key_set(key, value)
        for key, value in kwargs.items():
            self.on_key_set(key, value)
        self.data.update(*args, **kwargs)

    def clear(self) -> None:
        self.on_clear()
        self.data.clear()

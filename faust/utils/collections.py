from collections import UserDict, deque
from contextlib import suppress
from typing import (
    Any, Deque, ItemsView, Iterator, KeysView, List,
    MutableMapping, MutableSet, ValuesView, cast,
)
from .graphs import DependencyGraph
from .objects import shortlabel
from .types.collections import NodeT
from .types.graphs import DependencyGraphT

__flake8_Deque_is_used: Deque  # XXX flake8 bug

__all__ = [
    'Node',
    'FastUserDict',
    'ManagedUserDict',
    'FastUserSet',
    'ManagedUserSet',
]


class Node(NodeT):
    """Tree node.

    Notes:
        Nodes have a link to

            - the ``.root`` node (or None if this is the top-most node)
            - the ``.parent`` node (if this is a child node).
            - a list of children

        A Node may have ``.data`` associated with it, and arbitrary
        data may also be stored in ``.children``.

    Arguments:
        data (Any): Data to associate with node.

    Keyword Arguments:
        root (NodeT): Root node.
        parent (NodeT): Parent node.
        children (List[NodeT]): List of child nodes.
    """

    _root: NodeT = None
    _parent: NodeT = None

    @classmethod
    def _new_node(cls, data: Any, **kwargs: Any) -> NodeT:
        return cls(data, **kwargs)  # type: ignore

    def __init__(self, data: Any,
                 *,
                 root: NodeT = None,
                 parent: NodeT = None,
                 children: List[NodeT] = None) -> None:
        self.data = data
        self.root = root
        self.parent = parent
        self.children = children or []

    def new(self, data: Any) -> NodeT:
        """Create new node from this node."""
        node = self._new_node(
            data,
            root=self.root if self.root is not None else self,
            parent=self,
        )
        self.children.append(node)
        return node

    def reattach(self, parent: NodeT) -> NodeT:
        """Attach this node to `parent` node."""
        self.root = parent.root if parent.root is not None else parent
        self.parent = parent
        parent.add(self)
        return self

    def add(self, data: Any) -> None:
        """Add node as a child node."""
        self.children.append(data)

    def discard(self, data: Any) -> None:
        # XXX slow
        with suppress(ValueError):
            self.children.remove(data)

    def traverse(self) -> Iterator[NodeT]:
        stack: Deque[NodeT] = deque([self])
        while stack:
            node = stack.popleft()
            yield node
            for child in node.children:
                if isinstance(child, NodeT):
                    stack.append(child)
                else:
                    yield child

    def walk(self) -> Iterator[NodeT]:
        node: NodeT = self
        while node:
            yield node
            node = node.parent

    def as_graph(self) -> DependencyGraphT:
        """Convert to :class:`~faust.utils.graphs.DependencyGraph`."""
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

    def __repr__(self) -> str:
        return f'{type(self).__name__}: {self.path}'

    @property
    def depth(self) -> int:
        return self._find_depth()

    def _find_depth(self) -> int:
        return sum(1 for _ in enumerate(self.walk()))

    @property
    def path(self) -> str:
        return '/'.join(reversed([
            shortlabel(node.data) for node in self.walk()
        ]))

    @property
    def parent(self) -> NodeT:
        return self._parent

    @parent.setter
    def parent(self, node: NodeT) -> None:
        if node is self:
            raise ValueError('Parent node cannot be itself.')
        self._parent = node

    @property
    def root(self) -> NodeT:
        return self._root

    @root.setter
    def root(self, node: NodeT) -> None:
        if node is self:
            raise ValueError('Root node cannot be itself.')
        self._root = node


class FastUserDict(UserDict):
    """Like UserDict but reimplements some methods for speed."""

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


class FastUserSet(MutableSet):
    data: MutableMapping[Any, bool]

    def __contains__(self, key: Any) -> bool:
        return key in self.data

    def __iter__(self) -> Iterator:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def add(self, key: Any) -> None:
        self.data[key] = True

    def discard(self, key: Any) -> None:
        self.data.pop(key, None)

    # Rest is fast versions of generic slow MutableSet methods.

    def clear(self) -> None:
        self.data.clear()


class ManagedUserSet(FastUserSet):
    """A MutableSet that adds callbacks for when keys are get/set/deleted."""

    def on_key_get(self, key: Any) -> None:
        ...

    def on_key_set(self, key: Any) -> None:
        ...

    def on_key_del(self, key: Any) -> None:
        ...

    def on_clear(self) -> None:
        ...

    def __contains__(self, key: Any) -> bool:
        self.on_key_get(key)
        return super().__contains__(key)

    def add(self, key: Any) -> None:
        self.on_key_set(key)
        return super().add(key)

    def discard(self, key: Any) -> None:
        self.on_key_del(key)
        return super().discard(key)

    def clear(self) -> None:
        self.on_clear()
        return super().clear()

    def raw_update(self, *args: Any, **kwargs: Any) -> None:
        self.data.update(*args, **kwargs)


class ManagedUserDict(FastUserDict):
    """A UserDict that adds callbacks for when keys are get/set/deleted."""

    def on_key_get(self, key: Any) -> None:
        """Called when a key is retrieved."""
        ...

    def on_key_set(self, key: Any, value: Any) -> None:
        """Called when a key is set."""
        ...

    def on_key_del(self, key: Any) -> None:
        """Called when a key is deleted."""
        ...

    def on_clear(self) -> None:
        """Called when the dict is cleared."""
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

    def raw_update(self, *args: Any, **kwargs: Any) -> None:
        self.data.update(*args, **kwargs)

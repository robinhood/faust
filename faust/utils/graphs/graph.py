from functools import partial
from typing import (
    Any, Callable, ItemsView, Iterable, Iterator, IO,
    List, MutableMapping, Set, Sequence, cast,
)
from collections import Counter
from ..types.graphs import DependencyGraphT, GraphFormatterT
from .formatter import GraphFormatter

_flake8_List_is_used: List
_flake8_Set_is_used: Set


class CycleError(Exception):
    """A cycle was detected in an acyclic graph."""


class DependencyGraph(DependencyGraphT):
    """A directed acyclic graph of objects and their dependencies.

    Supports a robust topological sort
    to detect the order in which they must be handled.

    Takes an optional iterator of ``(obj, dependencies)``
    tuples to build the graph from.

    Warning:
        Does not support cycle detection.
    """
    adjacent: MutableMapping

    def __init__(self,
                 it: Iterable = None,
                 formatter: GraphFormatterT = None) -> None:
        self.formatter = formatter or GraphFormatter()
        self.adjacent = {}
        if it is not None:
            self.update(it)

    def add_arc(self, obj: Any) -> None:
        """Add an object to the graph."""
        self.adjacent.setdefault(obj, [])

    def add_edge(self, A: Any, B: Any) -> None:
        """Add an edge from object ``A`` to object ``B``.

        I.e. ``A`` depends on ``B``.
        """
        self[A].append(B)

    def connect(self, graph: DependencyGraphT) -> None:
        """Add nodes from another graph."""
        self.adjacent.update(graph.adjacent)

    def topsort(self) -> Sequence:
        """Sort the graph topologically.

        Returns:
            List: of objects in the order in which they must be handled.
        """
        graph = DependencyGraph()
        components = self._tarjan72()

        NC = {
            node: component for component in components for node in component
        }
        for component in components:
            graph.add_arc(component)
        for node in self:
            node_c = NC[node]
            for successor in self[node]:
                successor_c = NC[successor]
                if node_c != successor_c:
                    graph.add_edge(node_c, successor_c)
        return [t[0] for t in graph._khan62()]

    def valency_of(self, obj: Any) -> int:
        """Return the valency (degree) of a vertex in the graph."""
        try:
            l = [len(self[obj])]
        except KeyError:
            return 0
        for node in self[obj]:
            l.append(self.valency_of(node))
        return sum(l)

    def update(self, it: Iterable) -> None:
        """Update graph with data from a list of ``(obj, deps)`` tuples."""
        tups = list(it)
        for obj, _ in tups:
            self.add_arc(obj)
        for obj, deps in tups:
            for dep in deps:
                self.add_edge(obj, dep)

    def edges(self) -> Iterable:
        """Return generator that yields for all edges in the graph."""
        return (obj for obj, adj in self.items() if adj)

    def _khan62(self) -> Sequence:
        """Perform Khan's simple topological sort algorithm from '62.

        See https://en.wikipedia.org/wiki/Topological_sorting
        """
        count: Counter = Counter()
        result = []

        for node in self:
            for successor in self[node]:
                count[successor] += 1
        ready = [node for node in self if not count[node]]

        while ready:
            node = ready.pop()
            result.append(node)

            for successor in self[node]:
                count[successor] -= 1
                if count[successor] == 0:
                    ready.append(successor)
        result.reverse()
        return result

    def _tarjan72(self) -> Sequence:
        """Perform Tarjan's algorithm to find strongly connected components.

        See Also:
            :wikipedia:`Tarjan%27s_strongly_connected_components_algorithm`
        """
        result: List = []
        stack: List = []
        low: List = []

        def visit(node: Any) -> None:
            if node in low:
                return
            num = len(low)
            low[node] = num
            stack_pos = len(stack)
            stack.append(node)

            for successor in self[node]:
                visit(successor)
                low[node] = min(low[node], low[successor])

            if num == low[node]:
                component = tuple(stack[stack_pos:])
                stack[stack_pos:] = []
                result.append(component)
                for item in component:
                    low[item] = len(self)

        for node in self:
            visit(node)

        return result

    def to_dot(self, fh: IO, *, formatter: GraphFormatterT = None) -> None:
        """Convert the graph to DOT format.

        Arguments:
            fh (IO): A file, or a file-like object to write the graph to.
            formatter (celery.utils.graph.GraphFormatter): Custom graph
                formatter to use.
        """
        seen: Set = set()
        draw = formatter or self.formatter
        write = partial(print, file=fh)

        def if_not_seen(fun: Callable[[Any], str], obj: Any) -> None:
            if draw.label(obj) not in seen:
                write(fun(obj))
                seen.add(draw.label(obj))

        write(draw.head())
        for obj, adjacent in self.items():
            if not adjacent:
                if_not_seen(draw.terminal_node, obj)
            for req in adjacent:
                if_not_seen(draw.node, obj)
                write(draw.edge(obj, req))
        write(draw.tail())

    def __iter__(self) -> Iterator:
        return iter(self.adjacent)

    def __getitem__(self, node: Any) -> Any:
        return self.adjacent[node]

    def __len__(self) -> int:
        return len(self.adjacent)

    def __contains__(self, obj: Any) -> bool:
        return obj in self.adjacent

    def items(self) -> ItemsView:
        return cast(ItemsView, self.adjacent.items())

    def __repr__(self) -> str:
        return '\n'.join(self._repr_node(N) for N in self)

    def _repr_node(self, obj: Any,
                   level: int = 1,
                   fmt: str = '{0}({1})') -> str:
        output = [fmt.format(obj, self.valency_of(obj))]
        if obj in self:
            for other in self[obj]:
                d = fmt.format(other, self.valency_of(other))
                output.append('     ' * level + d)
                output.extend(
                    self._repr_node(other, level + 1).split('\n')[1:])
        return '\n'.join(output)

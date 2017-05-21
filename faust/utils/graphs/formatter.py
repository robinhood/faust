from functools import singledispatch
from typing import Any, Mapping
from ..types.graphs import GraphFormatterT


def dedent_initial(s: str, n: int = 4) -> str:
    """Remove identation from first line of text."""
    return s[n:] if s[:n] == ' ' * n else s


def dedent(s: str, n: int = 4, sep: str = '\n') -> str:
    """Remove identation."""
    return sep.join(dedent_initial(l) for l in s.splitlines())


class DOT:
    """Constants related to the dot format."""

    HEAD = dedent("""
        {IN}{type} {id} {{
        {INp}graph [{attrs}]
    """)
    ATTR = '{name}={value}'
    NODE = '{INp}"{0}" [{attrs}]'
    EDGE = '{INp}"{0}" {dir} "{1}" [{attrs}]'
    ATTRSEP = ', '
    DIRS = {'graph': '--', 'digraph': '->'}
    TAIL = '{IN}}}'


@singledispatch
def _label(s: Any) -> str:
    return str(
        getattr(s, 'label', None) or
        getattr(s, 'name', None) or
        getattr(s, '__qualname__', None) or
        getattr(s, '__name__', None) or
        getattr(type(s), '__qualname__', None) or
        type(s).__name__
    )


@_label.register(str)
def _(s: str) -> str:
    return s


class GraphFormatter(GraphFormatterT):
    """Format dependency graphs."""

    _attr = DOT.ATTR.strip()
    _node = DOT.NODE.strip()
    _edge = DOT.EDGE.strip()
    _head = DOT.HEAD.strip()
    _tail = DOT.TAIL.strip()
    _attrsep = DOT.ATTRSEP
    _dirs = dict(DOT.DIRS)

    scheme: Mapping[str, Any] = {
        'shape': 'box',
        'arrowhead': 'vee',
        'style': 'filled',
        'fontname': 'HelveticaNeue',
    }
    edge_scheme: Mapping[str, Any] = {
        'color': 'darkseagreen4',
        'arrowcolor': 'black',
        'arrowsize': 0.7,
    }
    node_scheme: Mapping[str, Any] = {
        'fillcolor': 'palegreen3',
        'color': 'palegreen4',
    }
    term_scheme: Mapping[str, Any] = {
        'fillcolor': 'palegreen1',
        'color': 'palegreen2',
    }
    graph_scheme: Mapping[str, Any] = {
        'bgcolor': 'mintcream',
    }

    def __init__(self,
                 root: Any = None,
                 type: str = None,
                 id: str = None,
                 indent: int = 0,
                 inw: str = ' ' * 4,
                 **scheme: Any) -> None:
        self.id = id or 'dependencies'
        self.root = root
        self.type = type or 'digraph'
        self.direction = self._dirs[self.type]
        self.IN = inw * (indent or 0)
        self.INp = self.IN + inw
        self.scheme = dict(self.scheme, **scheme)
        self.graph_scheme = dict(self.graph_scheme, root=self.label(self.root))

    def attr(self, name: str, value: Any) -> str:
        return self.FMT(self._attr, name=name, value=f'"{value}"')

    def attrs(self, d: Mapping = None, scheme: Mapping = None) -> str:
        scheme = {**self.scheme, **scheme} if scheme else self.scheme
        d = {**scheme, **d} if d else scheme
        return self._attrsep.join(
            str(self.attr(k, v)) for k, v in d.items()
        )

    def head(self, **attrs: Any) -> str:
        return self.FMT(
            self._head, id=self.id, type=self.type,
            attrs=self.attrs(attrs, self.graph_scheme),
        )

    def tail(self) -> str:
        return self.FMT(self._tail)

    def label(self, obj: Any) -> str:
        return _label(obj)

    def node(self, obj: Any, **attrs: Any) -> str:
        return self.draw_node(obj, self.node_scheme, attrs)

    def terminal_node(self, obj: Any, **attrs: Any) -> str:
        return self.draw_node(obj, self.term_scheme, attrs)

    def edge(self, a: Any, b: Any, **attrs: Any) -> str:
        return self.draw_edge(a, b, **attrs)

    def _enc(self, s: str) -> str:
        return s.encode('utf-8', 'ignore').decode()

    def FMT(self, fmt: str, *args: Any, **kwargs: Any) -> str:
        return self._enc(fmt.format(
            *args, **dict(kwargs, IN=self.IN, INp=self.INp)
        ))

    def draw_edge(self, a: Any, b: Any,
                  scheme: Mapping = None,
                  attrs: Mapping = None) -> str:
        return self.FMT(
            self._edge, self.label(a), self.label(b),
            dir=self.direction, attrs=self.attrs(attrs, self.edge_scheme),
        )

    def draw_node(self, obj: Any,
                  scheme: Mapping = None,
                  attrs: Mapping = None) -> str:
        return self.FMT(
            self._node, self.label(obj), attrs=self.attrs(attrs, scheme),
        )

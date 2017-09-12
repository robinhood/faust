"""Object utilities."""
import sys
from contextlib import suppress
from functools import singledispatch, total_ordering
from pathlib import Path
from typing import (
    AbstractSet, Any, Callable, Dict, FrozenSet, Generic,
    Iterable, List, Mapping, MutableMapping, MutableSequence,
    MutableSet, Sequence, Set, Tuple, Type, TypeVar, cast,
)

__all__ = [
    'FieldMapping',
    'DefaultsMapping',
    'Unordered',
    'KeywordReduce',
    'qualname',
    'canoname',
    'label',
    'shortlabel',
    'annotations',
    'iter_mro_reversed',
    'guess_concrete_type',
    'cached_property',
]

_T = TypeVar('_T')

#: Mapping of attribute name to attribute type.
FieldMapping = Mapping[str, Type]

#: Mapping of attribute name to attributes default value.
DefaultsMapping = Mapping[str, Any]

SET_TYPES: Tuple[Type, ...] = (AbstractSet, FrozenSet, MutableSet, Set)
LIST_TYPES: Tuple[Type, ...] = (
    List,
    Sequence,
    MutableSequence,
)
DICT_TYPES: Tuple[Type, ...] = (Dict, Mapping, MutableMapping)
# XXX cast required for mypy bug
TUPLE_TYPES: Tuple[Type, ...] = cast(Tuple[Type, ...], (Tuple,))


@total_ordering
class Unordered(Generic[_T]):

    # Used to put anything inside a heapq, even things that can not be ordered
    # like dicts and lists.

    def __init__(self, value: _T) -> None:
        self.value = value

    def __le__(self, other: Any) -> bool:
        return True


def _restore_from_keywords(typ: Type, kwargs: Dict) -> Any:
    # This function is used to restore pickled KeywordReduce object.
    return typ(**kwargs)


class KeywordReduce:
    """Mixin class for objects that can be pickled.

    Traditionally Python's __reduce__ method operates on
    positional arguments, this adds support for restoring
    an object using keyword arguments.

    Your class needs to define a ``__reduce_keywords__`` method
    that returns the keyword arguments used to reconstruct the object
    as a mapping.
    """

    def __reduce_keywords__(self) -> Mapping:
        raise NotImplemented()

    def __reduce__(self) -> Tuple:
        return _restore_from_keywords, (type(self), self.__reduce_keywords__())


def qualname(obj: Any) -> str:
    """Get object qualified name."""
    if not hasattr(obj, '__name__') and hasattr(obj, '__class__'):
        obj = obj.__class__
    return '.'.join((obj.__module__, obj.__name__))


def canoname(obj: Any, *, main_name: str = None) -> str:
    """Get qualname of obj, trying to resolve the real name of ``__main__``."""
    name = qualname(obj)
    parts = name.split('.')
    if parts[0] == '__main__':
        return '.'.join([main_name or _detect_main_name()] + parts[1:])
    return name


def _detect_main_name() -> str:
    path = Path(sys.modules['__main__'].__file__).absolute()
    node = path.parent
    seen = []
    while node:
        if (node / '__init__.py').exists():
            seen.append(node.stem)
            node = node.parent
        else:
            break
    return '.'.join(seen + [path.stem])


@singledispatch
def label(s: Any) -> str:
    return str(
        getattr(s, 'label', None) or
        getattr(s, 'name', None) or
        getattr(s, '__qualname__', None) or
        getattr(s, '__name__', None) or
        getattr(type(s), '__qualname__', None) or
        type(s).__name__)


@label.register(str)
def _(s: str) -> str:
    return s


@singledispatch
def shortlabel(s: Any) -> str:
    return str(
        getattr(s, 'shortlabel', None) or
        getattr(s, 'name', None) or
        getattr(s, '__qualname__', None) or
        getattr(s, '__name__', None) or
        getattr(type(s), '__qualname__', None) or
        type(s).__name__)


@shortlabel.register(str)  # noqa
def _s(s: str) -> str:
    return s


def annotations(cls: Type,
                *,
                stop: Type = object) -> Tuple[FieldMapping, DefaultsMapping]:
    """Get class field definition in MRO order.

    Arguments:
        cls: Class to get field information from.
        stop: Base class to stop at (default is ``object``).

    Returns:
        Tuple[FieldMapping, DefaultsMapping]: Tuple with two dictionaries,
            the first containing a map of field names to their types,
            the second containing a map of field names to their default
            value.  If a field is not in the second map, it means the field
            is required.

    Examples:
        >>> class Point:
        ...    x: float
        ...    y: float

        >>> class 3DPoint(Point):
        ...     z: float = 0.0

        >>> fields, defaults = annotations(3DPoint)
        >>> fields
        {'x': float, 'y': float, 'z': 'float'}
        >>> defaults
        {'z': 0.0}
    """
    fields: Dict[str, Type] = {}
    defaults: Dict[str, Any] = {}  # noqa: E704 (flake8 bug)
    for subcls in iter_mro_reversed(cls, stop=stop):
        defaults.update(subcls.__dict__)
        with suppress(AttributeError):
            annotations = subcls.__annotations__
            fields.update(annotations)
    return fields, defaults


def iter_mro_reversed(cls: Type, stop: Type) -> Iterable[Type]:
    """Iterate over superclasses, in reverse Method Resolution Order.

    The stop argument specifies a base class that when seen will
    stop iterating (well actually start, since this is in reverse, see Example
    for demonstration).

    Arguments:
        cls (Type): Target class.
        stop (Type): A base class in which we stop iteration.

    Notes:
        The last item produced will be the class itself (`cls`).

    Examples:
        >>> class A: ...
        >>> class B(A): ...
        >>> class C(B): ...

        >>> list(iter_mro_reverse(C, object))
        [A, B, C]

        >>> list(iter_mro_reverse(C, A))
        [B, C]

    Yields:
        Iterable[Type]: every class.
    """
    wanted = False
    for subcls in reversed(cls.__mro__):
        if wanted:
            yield cast(Type, subcls)
        else:
            wanted = subcls == stop


def guess_concrete_type(
        typ: Type,
        *,
        set_types: Tuple[Type, ...] = SET_TYPES,
        list_types: Tuple[Type, ...] = LIST_TYPES,
        tuple_types: Tuple[Type, ...] = TUPLE_TYPES,
        dict_types: Tuple[Type, ...] = DICT_TYPES) -> Tuple[Type, Type]:
    if (typ.__class__.__name__ == '_Union' and
            hasattr(typ, '__args__') and
            typ.__args__[1] is type(None)):  # noqa
        # Optional[x] actually returns Union[x, type(None)]
        typ = typ.__args__[0]
    if not issubclass(typ, (str, bytes)):
        if issubclass(typ, set_types):
            # Set[x]
            return set, _unary_type_arg(typ)
        elif issubclass(typ, list_types):
            # List[x]
            return list, _unary_type_arg(typ)
        elif issubclass(typ, DICT_TYPES):
            # Dict[_, x]
            return dict, (
                typ.__args__[1]
                if typ.__args__ and len(typ.__args__) > 1 else Any)
        elif issubclass(typ, tuple_types):
            # Tuple[x]
            return tuple, _unary_type_arg(typ)
    raise TypeError('Nuot a generic type')


def _unary_type_arg(typ: Type) -> Type:
    return typ.__args__[0] if typ.__args__ else Any


class cached_property:
    """Cached property.

    A property descriptor that caches the return value
    of the get function.

    Examples:
        .. sourcecode:: python

            @cached_property
            def connection(self):
                return Connection()

            @connection.setter  # Prepares stored value
            def connection(self, value):
                if value is None:
                    raise TypeError('Connection must be a connection')
                return value

            @connection.deleter
            def connection(self, value):
                # Additional action to do at del(self.attr)
                if value is not None:
                    print(f'Connection {value!r} deleted')
    """

    def __init__(self,
                 fget: Callable,
                 fset: Callable = None,
                 fdel: Callable = None,
                 doc: str = None,
                 class_attribute: str = None) -> None:
        self.__get = fget
        self.__set = fset
        self.__del = fdel
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__
        self.__module__ = fget.__module__
        self.class_attribute = class_attribute

    def __get__(self, obj: Any, type: Type = None) -> Any:
        if obj is None:
            if type is not None and self.class_attribute:
                return getattr(type, self.class_attribute)
            return self
        try:
            return obj.__dict__[self.__name__]
        except KeyError:
            value = obj.__dict__[self.__name__] = self.__get(obj)
            return value

    def __set__(self, obj: Any, value: Any) -> None:
        if self.__set is not None:
            value = self.__set(obj, value)
        obj.__dict__[self.__name__] = value

    def __delete__(self, obj: Any, _sentinel: Any = object()) -> None:
        value = obj.__dict__.pop(self.__name__, _sentinel)
        if self.__del is not None and value is not _sentinel:
            self.__del(obj, value)

    def setter(self, fset: Callable) -> 'cached_property':
        return self.__class__(self.__get, fset, self.__del)

    def deleter(self, fdel: Callable) -> 'cached_property':
        return self.__class__(self.__get, self.__set, fdel)

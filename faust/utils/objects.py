"""Object utilities."""
import sys
from contextlib import suppress
from functools import total_ordering
from pathlib import Path
from typing import (
    AbstractSet, Any, ClassVar, Dict, FrozenSet, Generic,
    Iterable, List, Mapping, MutableMapping, MutableSequence,
    MutableSet, Sequence, Set, Tuple, Type, TypeVar, cast,
)
from mode.utils.objects import cached_property

try:
    from typing import _ClassVar  # type: ignore
except ImportError:
    from typing import _GenericAlias  # type: ignore

    def _is_class_var(x: Any) -> bool:  # noqa
        return isinstance(x, _GenericAlias) and x.__origin__ is ClassVar
else:
    def _is_class_var(x: Any) -> bool:
        return type(x) is _ClassVar


__all__ = [
    'FieldMapping',
    'DefaultsMapping',
    'Unordered',
    'KeywordReduce',
    'qualname',
    'canoname',
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
# "expression has type Tuple[_SpecialForm]"
TUPLE_TYPES: Tuple[Type, ...] = cast(Tuple[Type, ...], (Tuple,))


@total_ordering
class Unordered(Generic[_T]):
    """Shield object from being ordered in heapq/``__le__``/etc."""

    # Used to put anything inside a heapq, even things that cannot be ordered
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
    try:
        filename = sys.modules['__main__'].__file__
    except (AttributeError, KeyError):  # ipython/REPL
        return '__main__'
    else:
        path = Path(filename).absolute()
        node = path.parent
        seen = []
        while node:
            if (node / '__init__.py').exists():
                seen.append(node.stem)
                node = node.parent
            else:
                break
        return '.'.join(seen + [path.stem])


def annotations(cls: Type,
                *,
                stop: Type = object,
                skip_classvar: bool = False) -> Tuple[
                    FieldMapping, DefaultsMapping]:
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
        .. sourcecode:: text

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
            if skip_classvar:
                fields.update({
                    name: typ
                    for name, typ in annotations.items()
                    if not _is_class_var(typ)
                })
            else:
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
    """Try to find the real type of an abstract type."""
    if (typ.__class__.__name__ == '_Union' and
            hasattr(typ, '__args__') and
            typ.__args__[1] is type(None)):  # noqa
        # Optional[x] actually returns Union[x, type(None)]
        typ = typ.__args__[0]
    if not issubclass(typ, (str, bytes)):
        if issubclass(typ, tuple_types):
            # Tuple[x]
            return tuple, _unary_type_arg(typ)
        elif issubclass(typ, set_types):
            # Set[x]
            return set, _unary_type_arg(typ)
        elif issubclass(typ, list_types):
            # List[x]
            return list, _unary_type_arg(typ)
        elif issubclass(typ, dict_types):
            # Dict[_, x]
            return dict, (
                typ.__args__[1]
                if typ.__args__ and len(typ.__args__) > 1 else Any)
    raise TypeError('Not a generic type')


def _unary_type_arg(typ: Type) -> Type:
    return typ.__args__[0] if typ.__args__ else Any

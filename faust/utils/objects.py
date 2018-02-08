"""Object utilities."""
import sys
import typing
from contextlib import suppress
from functools import total_ordering
from pathlib import Path
from typing import (
    AbstractSet, Any, ClassVar, Dict, FrozenSet, Generic,
    Iterable, List, Mapping, MutableMapping, MutableSequence,
    MutableSet, Sequence, Set, Tuple, Type, TypeVar, cast,
)
from typing import _eval_type, _type_check  # type: ignore
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

try:
    from typing import ForwardRef  # type: ignore
except ImportError:
    from typing import _ForwardRef as ForwardRef  # type: ignore

__all__ = [
    'FieldMapping',
    'DefaultsMapping',
    'Unordered',
    'KeywordReduce',
    'InvalidAnnotation',
    'qualname',
    'canoname',
    'annotations',
    'eval_type',
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


class InvalidAnnotation(Exception):
    """Raised by :func:`annotations` when encountering an invalid type."""


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
                invalid_types: Set = None,
                skip_classvar: bool = False,
                globalns: Dict[str, Any] = None,
                localns: Dict[str, Any] = None) -> Tuple[
                    FieldMapping, DefaultsMapping]:
    """Get class field definition in MRO order.

    Arguments:
        cls: Class to get field information from.
        stop: Base class to stop at (default is ``object``).
        invalid_types: Set of types that if encountered should raise
          :exc:`InvalidAnnotation` (does not test for subclasses).
        globalns: Global namespace to use when evaluating forward
            references (see :class:`typing.ForwardRef`).
        localns: Local namespace to use when evaluating forward
            references (see :class:`typing.ForwardRef`).

    Returns:
        Tuple[FieldMapping, DefaultsMapping]: Tuple with two dictionaries,
            the first containing a map of field names to their types,
            the second containing a map of field names to their default
            value.  If a field is not in the second map, it means the field
            is required.

    Raises:
        InvalidAnnotation: if a list of invalid types are provided and an
            invalid type is encountered.

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
            fields.update(_resolve_refs(
                subcls.__annotations__,
                globalns if globalns is not None else _get_globalns(subcls),
                localns,
                invalid_types or set(),
                skip_classvar,
            ))
    return fields, defaults


def _resolve_refs(d: Dict[str, Any],
                  globalns: Dict[str, Any] = None,
                  localns: Dict[str, Any] = None,
                  invalid_types: Set = None,
                  skip_classvar: bool = False) -> Iterable[Tuple[str, Type]]:
    invalid_types = invalid_types or set()
    for k, v in d.items():
        v = eval_type(v, globalns, localns, invalid_types)
        if skip_classvar and _is_class_var(v):
            pass
        else:
            yield k, v


def eval_type(typ: Any,
              globalns: Dict[str, Any] = None,
              localns: Dict[str, Any] = None,
              invalid_types: Set = None) -> Type:
    """Convert (possible) string annotation to actual type.

    Examples:
        >>> eval_type('List[int]') == typing.List[int]
    """
    invalid_types = invalid_types or set()
    if isinstance(typ, str):
        typ = ForwardRef(typ)
    if isinstance(typ, ForwardRef):
        # On 3.6/3.7 _eval_type crashes if str references ClassVar
        typ = _ForwardRef_safe_eval(typ, globalns, localns)
    typ = _eval_type(typ, globalns, localns)
    if typ in invalid_types:
        raise InvalidAnnotation(typ)
    return typ


def _ForwardRef_safe_eval(ref: ForwardRef,
                          globalns: Dict[str, Any] = None,
                          localns: Dict[str, Any] = None) -> Type:
    # On 3.6/3.7 ForwardRef._evaluate crashes if str references ClassVar
    if not ref.__forward_evaluated__:
        if globalns is None and localns is None:
            globalns = localns = {}
        elif globalns is None:
            globalns = localns
        elif localns is None:
            localns = globalns
        val = eval(ref.__forward_code__, globalns, localns)
        if not _is_class_var(val):
            val = _type_check(
                val, 'Forward references must evaluate to types.')
        ref.__forward_value__ = val
    return ref.__forward_value__


def _get_globalns(typ: Type) -> Dict[str, Any]:
    return sys.modules[typ.__module__].__dict__


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
    args = getattr(typ, '__args__', ())
    if typ.__class__.__name__ == '_GenericAlias':  # Py3.7
        if typ.__origin__ is typing.Union:
            for arg in args:
                if arg is not type(None):  # noqa
                    typ = arg
                    break
        else:
            typ = typ.__origin__  # for List this is list, etc.
    elif (typ.__class__.__name__ == '_Union' and  # Py3.6
            args and args[1] is type(None)):  # noqa
        # Optional[x] actually returns Union[x, type(None)]
        typ = args[0]
    if not issubclass(typ, (str, bytes)):
        if issubclass(typ, tuple_types):
            # Tuple[x]
            return tuple, _unary_type_arg(args)
        elif issubclass(typ, set_types):
            # Set[x]
            return set, _unary_type_arg(args)
        elif issubclass(typ, list_types):
            # List[x]
            return list, _unary_type_arg(args)
        elif issubclass(typ, dict_types):
            # Dict[_, x]
            return dict, args[1] if args and len(args) > 1 else Any
    raise TypeError(f'Not a generic type: {typ!r}')


def _unary_type_arg(args: List[Type]) -> Any:
    return args[0] if args else Any

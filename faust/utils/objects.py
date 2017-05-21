"""Object utilities."""
from contextlib import suppress
from typing import Any, Callable, Dict, Iterable, Mapping, Tuple, Type, cast

__flake8_Dict_is_used: Dict   # silence flake8 bug

__all__ = [
    'FieldMapping', 'DefaultsMapping', 'KeywordReduce',
    'annotations', 'iter_mro_reversed', 'cached_property',
]

#: Mapping of attribute name to attribute type.
FieldMapping = Mapping[str, Type]

#: Mapping of attribute name to attributes default value.
DefaultsMapping = Mapping[str, Any]


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


class cached_property:
    """Cached property.

    A property descriptor that caches the return value
    of the get function.

    Examples:
        .. code-block:: python

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

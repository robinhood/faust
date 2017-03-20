from typing import Any, Dict, FrozenSet, Iterable, Mapping, Tuple, Type, cast
from .types import K, Message, Request
from .utils.serialization import dumps, loads

__foobar: Dict  # flake8 thinks Dict is unused for some reason


def _itermro(cls: Type, stop: Type) -> Iterable[Type]:
    wanted = False
    for subcls in reversed(cls.__mro__):
        if wanted:
            yield cast(Type, subcls)
        else:
            wanted = subcls == stop


class Event:
    req: Request = None
    _fields: Mapping[str, Type]
    _fieldset = FrozenSet[str]
    _optionalset = FrozenSet[str]

    @classmethod
    def from_message(cls, key: K, message: Message) -> 'Event':
        request = Request(key, message)
        return cls.loads(message.value, req=request)

    @classmethod
    def loads(cls, s: Any, **kwargs) -> 'Event':
        return cls(**kwargs, **loads(cls.serializer, s))  # type: ignore

    def __init_subclass__(cls, serializer: str = None, **kwargs) -> None:
        super().__init_subclass__(**kwargs)  # type: ignore
        cls.serializer = serializer
        fields: Dict = {}
        optional: Dict = {}
        for subcls in _itermro(cls, stop=Event):
            optional.update(subcls.__dict__)
            try:
                annotations = subcls.__annotations__  # type: ignore
            except AttributeError:
                pass
            else:
                fields.update(annotations)
        cls._fields = cast(Mapping, fields)
        cls._fieldset = frozenset(fields)
        cls._optionalset = frozenset(optional)
        for field, typ in cls._fields.items():
            setattr(cls, field, FieldDescriptor(field, typ, cls))

    def __init__(self, req=None, **fields):
        fieldset = frozenset(fields)
        missing = self._fieldset - fieldset - self._optionalset
        if missing:
            raise TypeError('{} missing required arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(missing))))
        extraneous = fieldset - self._fieldset
        if extraneous:
            raise TypeError('{} got unexpected arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(extraneous))))
        self.__dict__.update(fields)
        self.req = req

    def dumps(self) -> Any:
        return dumps(self.serializer, self._asdict())

    def _asdict(self) -> Mapping:
        return dict(self._asitems())

    def _asitems(self) -> Iterable[Tuple[Any, Any]]:
        for key in self._fields:
            yield key, self.__dict__[key]

    def __repr__(self) -> str:
        return '<{}: {}>'.format(type(self).__name__, _kvrepr(self.__dict__))


def _kvrepr(d: Mapping[str, Any],
            sep: str = ', ',
            fmt: str = '{0}={1!r}') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    return sep.join(
        fmt.format(k, v) for k, v in d.items()
    )


class FieldDescriptor:
    """Describes a field.

    Used for every field in Event so that they can be used in join's
    /group_by etc:

        group_by=Withdrawal.amount
    """

    field: str
    type: Type

    def __init__(self, field: str, type: Type, event: Type) -> None:
        self.field = field
        self.type = type
        self.event = event

    def __get__(self, instance: Any, owner: Type) -> Any:
        if instance is None:
            return self
        return instance.__dict__[self.field]

    def __set__(self, instance: Any, value: Any) -> None:
        instance.__dict__[self.field] = value

    def __repr__(self) -> str:
        return '<{name}: {self.field}>'.format(
            name=type(self).__name__, self=self)

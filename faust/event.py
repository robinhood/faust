from typing import Any, Iterable, Mapping, NamedTuple, Tuple
from .types import K, Message
from .utils.serialization import dumps, loads


class Request(NamedTuple):
    key: K
    topic: str
    partition: int
    message: Message


class Event:
    req: Request = None

    @classmethod
    def from_message(cls,
                     key: K,
                     topic: str,
                     partition: int,
                     message: Message) -> 'Event':
        request = Request(key, topic, partition, message)
        return cls.loads(message.value, req=request)

    @classmethod
    def loads(cls, s: Any, **kwargs) -> 'Event':
        return cls(**kwargs, **loads(cls.serializer, s))

    def __init_subclass__(cls, serializer: str = None, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.serializer = serializer

    def __init__(self, **fields):
        self.__dict__.update(fields)

    def dumps(self) -> Any:
        return dumps(self.serializer, self._asdict())

    def _asdict(self) -> Mapping:
        return dict(self._asitems())

    def _asitems(self) -> Iterable[Tuple[Any, Any]]:
        for anno in self._iteranno():
            for k in anno:
                yield k, self.__dict__[k]

    @classmethod
    def _iteranno(cls) -> Iterable[Mapping]:
        for cls in cls.__mro__:
            try:
                yield cls.__annotations__  # type: ignore
            except AttributeError:
                break

    def __repr__(self) -> str:
        return '<{}: {!r}>'.format(type(self).__name__, self.__dict__)


class FieldDescriptor:
    """Describes a field.

    Used in join, etc.::

        A.type.amount & B.type.amount

    where ``.type`` basically creates one descriptor for every
    field in the record.
    """

    field: str

    def __init__(self, field: str) -> None:
        self.field = field

    def join(self, other: 'FieldDescriptor'):  # TODO Returns StreamT
        print('join %r with %r' % (self.field, other.field))
        # TODO Return StreamT

    def __and__(self, other: 'FieldDescriptor') -> Any:
        return self.join(other)

    def __repr__(self) -> str:
        return '<{name}: {self.field}>'.format(
            name=type(self).__name__, self=self)


class BoundEvent:
    """A bound event holds reference to specific stream.

    :class:`FieldDescriptor``'s will be added for every field
    in the event.

    """

    #: The event type object.
    event: type

    #: The object we are bound to.
    obj: Any

    def __init__(self, event: type, obj: Any) -> None:
        self.event = event
        self.obj = obj

        # mypy says 'type' objects don't have __annotations__
        annotations: Mapping = self.event.__annotations__  # type: ignore

        self.__dict__.update({
            field: self._make_field_descriptor(field)
            for field in annotations.keys()
        })

    def _make_field_descriptor(self, field: str) -> FieldDescriptor:
        return FieldDescriptor(field)

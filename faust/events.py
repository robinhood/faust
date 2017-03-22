"""Events: Describing how messages are serialized/deserialized."""
from typing import Any, Dict, FrozenSet, Iterable, Mapping, Tuple, Type, cast
from .types import EventT, FieldDescriptorT, K, Message, Request, SerializerArg
from .utils.objects import iter_mro_reversed
from .utils.serialization import dumps, loads

__all__ = ['Event', 'FieldDescriptor']

# flake8 thinks Dict is unused for some reason
__flake8_ignore_this_Dict: Dict  # XXX

# NOTES:
# - Events are described in the same notation as named tuples in Python 3.6.
#   To accomplish this ``__init_subclass__`` defined in :pep:`487` is used.
#
# - Sometimes field descriptions are passed around as arguments to functions,
#   for example when joining a stream together, we need to specify the fields
#   to use as the basis for the join.
#
#   When accessed on the Event class, the attributes are actually field
#   descriptors that return information about the field:
#       >>> Point.x
#       <FieldDescriptor: Point.x: int>
#
#   This field descriptor holds information about the name of the field, the
#   value type of the field, and also what Event subclass it belongs to.
#
#   FieldDescriptor is also an actual Python descriptor:  In Python object
#   attributes can override what happens when they are get/set/deleted:
#
#       class MyDescriptor:
#
#           def __get__(self, instance, cls):
#               if instance is None:
#                   print('ACCESS ON CLASS ATTRIBUTE')
#                   return self
#               print('ACCESS ON INSTANCE')
#               return 42
#
#       class Example:
#           foo = MyDescriptor()
#
#   The above descriptor only overrides getting, so is executed when you access
#   the attribute:
#       >>> Example.foo
#       ACCESS ON CLASS ATTRIBUTE
#       <__main__.MyDescriptor at 0x1049caac8>
#
#       >>> x = Example()
#       >>> x.foo
#       ACCESS ON INSTANCE
#       42


class Event(EventT):
    """Describes how messages in a topic is serialized.

    Examples:
        >>> class LogEvent(Event, serializer='json'):
        ...     severity: str
        ...     message: str
        ...     timestamp: float
        ...     optional_field: str = 'default value'

        >>> event = LogEvent(
        ...     severity='error',
        ...     message='Broken pact',
        ...     timestamp=666.0,
        ... )

        >>> event.severity
        'error'

        >>> serialized = event.dumps()
        '{"severity": "error", "message": "Broken pact", "timestamp": 666.0}'

        >>> restored = LogEvent.loads(serialized)
        <LogEvent: severity='error', message='Broken pact', timestamp=666.0>

        >>> # You can also subclass an Event to create a new event
        >>> # with derived fields
        >>> class RemoteLogEvent(LogEvent):
        ...     url: str

        >>> # You can also refer to event fields and pass them around:
        >>> LogEvent.severity
        >>> <FieldDescriptor: LogEvent.severity (str)>
    """

    #: When an Event is received as a message, this field is populated with
    #: the :class:`Request` it originated from.
    req: Request = None

    # Index: Flattened view of __annotations__ in MRO order.
    _fields: Mapping[str, Type]

    # Index: Set of required field names, for fast argument checking.
    _fieldset = FrozenSet[str]

    # Index: Set of optional field names, for fast argument checking.
    _optionalset = FrozenSet[str]

    @classmethod
    def loads(cls, s: bytes,
              *,
              default_serializer: SerializerArg = None,
              **kwargs) -> 'Event':
        """Deserialize event from bytes.

        Keyword Arguments:
            default_serializer (SerializerArg): Default serializer to use
                if no custom serializer was set for this Event subclass.
            **kwargs: Additional attributes to set on the event object.
                Note, these are regarded as defaults, and any fields also
                present in the message takes precedence.
        """
        # the double starargs means dicts are merged
        return cls(**kwargs,  # type: ignore
                   **loads(cls.serializer or default_serializer, s))

    @classmethod
    def from_message(cls, key: K, message: Message,
                     *,
                     default_serializer: SerializerArg = None) -> 'Event':
        """Create event from message.

        The Consumer uses this to convert a message to an event.

        Arguments:
            key (K): Deserialized key.
            message (Message): Message object holding serialized event.

        Keyword Arguments:
            default_serializer (SerializerArg): Default serializer to use
                if no custom serializer was set for this Event subclass.
        """
        return cls.loads(
            message.value,
            default_serializer=default_serializer,
            req=Request(key, message),
        )

    def __init_subclass__(cls, serializer: str = None, **kwargs) -> None:
        # Python 3.6 added the new __init_subclass__ function to make it
        # possible to initialize subclasses without using metaclasses
        # (:pep:`487`).
        super().__init_subclass__(**kwargs)  # type: ignore

        # Can set serializer using:
        #    class X(Event, serializer='avro')
        cls.serializer = serializer

        # Find attributes and their types, and create indexes for these
        # for performance at runtime.
        fields: Dict = {}
        optional: Dict = {}
        for subcls in iter_mro_reversed(cls, stop=Event):
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

        # Add FieldDescriptor's for every field.
        for field, typ in cls._fields.items():
            try:
                default, required = optional[field], False
            except KeyError:
                default, required = None, True
            setattr(cls, field, FieldDescriptor(
                field, typ, cls, required, default))

    def __init__(self, req=None, **fields):
        fieldset = frozenset(fields)

        # Check all required arguments.
        missing = self._fieldset - fieldset - self._optionalset
        if missing:
            raise TypeError('{} missing required arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(missing))))

        # Check for unknown arguments.
        extraneous = fieldset - self._fieldset
        if extraneous:
            raise TypeError('{} got unexpected arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(extraneous))))

        # Fast: This sets attributes from kwargs.
        self.__dict__.update(fields)

        # Req is only set by the Consumer, when the event originates
        # from message received.
        self.req = req

    def dumps(self) -> bytes:
        """Serialize event to the target serialization format."""
        return dumps(self.serializer, self._asdict())

    def _asdict(self) -> Mapping[str, Any]:
        # Convert known fields to mapping of ``{field: value}``.
        return dict(self._asitems())

    def _asitems(self) -> Iterable[Tuple[Any, Any]]:
        # Iterate over known fields as items-tuples.
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


class FieldDescriptor(FieldDescriptorT):
    """Describes a field.

    Used for every field in Event so that they can be used in join's
    /group_by etc.

    Examples:
        >>> class Withdrawal(Event):
        ...    account_id: str
        ...    amount: float = 0.0

        >>> Withdrawal.account_id
        <FieldDescriptor: Withdrawal.account_id: str>
        >>> Withdrawal.amount
        <FieldDescriptor: Withdrawal.amount: float = 0.0>

    Arguments:
        field (str): Name of field.
        type (Type): Field value type.
        event (Type): Event class the field belongs to.
        required (bool): Set to false if field is optional.
        default (Any): Default value when `required=False`.
    """

    field: str
    type: Type
    event: Type
    required: bool = True
    default: Any = None  # noqa: E704

    def __init__(self,
                 field: str,
                 type: Type,
                 event: Type,
                 required: bool = True,
                 default: Any = None) -> None:
        self.field = field
        self.type = type
        self.event = event
        self.required = required
        self.default = default

    def __get__(self, instance: Any, owner: Type) -> Any:
        # class attribute accessed
        if instance is None:
            return self

        # instance attribute accessed
        try:
            return instance.__dict__[self.field]
        except KeyError:
            if self.required:
                raise
            return self.default

    def __set__(self, instance: Any, value: Any) -> None:
        instance.__dict__[self.field] = value

    def __repr__(self) -> str:
        return '<{name}: {event}.{field}: {type}{default}>'.format(
            name=type(self).__name__,
            event=self.event.__name__,
            field=self.field,
            type=self.type.__name__,
            default='' if self.required else ' = {!r}'.format(self.default),
        )

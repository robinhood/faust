"""Serializing/deserializing message keys and values."""
from typing import (
    Any, Dict, Iterable, Mapping, Sequence, Tuple, Type, Union, cast,
)
from .codecs import dumps, loads
from .types import (
    AppT, CodecArg, Event, FieldDescriptorT, K,
    Message, ModelOptions, ModelT, Request, Topic,
)
from .utils.avro.utils import to_avro_type
from .utils.objects import annotations

__all__ = ['Model', 'Record', 'FieldDescriptor']

# flake8 thinks Dict is unused for some reason
__flake8_ignore_this_Dict: Dict  # XXX

# NOTES:
# - Records are described in the same notation as named tuples in Python 3.6.
#   To accomplish this ``__init_subclass__`` defined in :pep:`487` is used.
#
# - Sometimes field descriptions are passed around as arguments to functions,
#   for example when joining a stream together, we need to specify the fields
#   to use as the basis for the join.
#
#   When accessed on the Record class, the attributes are actually field
#   descriptors that return information about the field:
#       >>> Point.x
#       <FieldDescriptor: Point.x: int>
#
#   This field descriptor holds information about the name of the field, the
#   value type of the field, and also what Record subclass it belongs to.
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


class Model(ModelT):
    """Describes how messages in a topic is serialized."""

    __abstract__ = True

    #: When an Event is received as a message, this field is populated with
    #: the :class:`Request` it originated from.
    req: Request = None

    _schema_type: str = None

    @classmethod
    def loads(
            cls, s: bytes,
            *,
            default_serializer: CodecArg = None,
            **kwargs) -> ModelT:
        """Deserialize event from bytes.

        Keyword Arguments:
            default_serializer (CodecArg): Default serializer to use
                if no custom serializer was set for this Event subclass.
            **kwargs: Additional attributes to set on the event object.
                Note, these are regarded as defaults, and any fields also
                present in the message takes precedence.
        """
        # the double starargs means dicts are merged
        return cls(**kwargs,  # type: ignore
                   **loads(cls._options.serializer or default_serializer, s))

    @classmethod
    def from_message(
            cls, key: K, message: Message, app: AppT,
            *,
            default_serializer: CodecArg = None) -> Event:
        """Create event from message.

        The Consumer uses this to convert a message to an event.

        Arguments:
            key (K): Deserialized key.
            message (Message): Message object holding serialized event.

        Keyword Arguments:
            default_serializer (CodecArg): Default serializer to use
                if no custom serializer was set for this Event subclass.
        """
        return cast(Event, cls.loads(
            message.value,
            default_serializer=default_serializer,
            req=Request(app, key, message),
        ))

    @classmethod
    def as_schema(cls) -> Mapping:
        return {
            'namespace': cls._options.namespace,
            'type': cls._schema_type,
            'name': cls.__name__,
            'fields': cls._schema_fields(),
        }

    @classmethod
    def _schema_fields(cls) -> Any:
        raise NotImplementedError()

    def __init_subclass__(cls,
                          serializer: str = None,
                          namespace: str = None,
                          **kwargs) -> None:
        if cls.__abstract__:
            cls.__abstract__ = False
            return
        # Python 3.6 added the new __init_subclass__ function to make it
        # possible to initialize subclasses without using metaclasses
        # (:pep:`487`).
        super().__init_subclass__(**kwargs)  # type: ignore

        # Can set serializer using:
        #    class X(Event, serializer='avro'):
        #        ...
        custom_options = getattr(cls, '_options', None)
        options = ModelOptions()
        if custom_options:
            options.__dict__.update(custom_options.__dict__)
        if serializer is not None:
            options.serializer = serializer
        if namespace is not None:
            options.namespace = namespace

        # Add introspection capabilities
        cls._contribute_to_options(options)
        # Add FieldDescriptor's for every field.
        cls._contribute_field_descriptors(options)

        # Store options on new subclass.
        cls._options = options

    @classmethod
    def _contribute_to_options(
            cls, options: ModelOptions) -> None:
        raise NotImplementedError()

    @classmethod
    def _contribute_field_descriptors(
            cls, options: ModelOptions) -> None:
        raise NotImplementedError()

    def __init__(self, req=None, **fields):
        # Req is only set by the Consumer, when the event originates
        # from message received.
        self.req = req

        # Set fields from keyword arguments.
        self._init_fields(fields)

    def derive(self, *objects: ModelT, **fields) -> ModelT:
        return self._derive(objects, fields)

    def _derive(self, objects: Tuple[ModelT, ...], fields: Dict) -> ModelT:
        raise NotImplementedError()

    async def forward(self, topic: Union[str, Topic]) -> None:
        await self.req.app.send(topic, self.req.key, self)

    def dumps(self) -> bytes:
        """Serialize event to the target serialization format."""
        return dumps(self._options.serializer, self._to_representation())

    def _to_representation(self) -> Any:
        raise NotImplementedError()

    def _humanize(self) -> str:
        raise NotImplementedError()

    def __repr__(self) -> str:
        return '<{}: {}>'.format(type(self).__name__, self._humanize())


class Record(Model):
    """Describes a model type that is a record (Mapping).

    Examples:
        >>> class LogEvent(Record, serializer='json'):
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

        >>> # You can also subclass a Record to create a new record
        >>> # with additional fields
        >>> class RemoteLogEvent(LogEvent):
        ...     url: str

        >>> # You can also refer to record fields and pass them around:
        >>> LogEvent.severity
        >>> <FieldDescriptor: LogEvent.severity (str)>
    """
    _schema_type = 'record'

    @classmethod
    def _schema_fields(cls) -> Sequence[Mapping]:
        return [
            {'name': key, 'type': to_avro_type(typ)}
            for key, typ in cls._options.fields.items()
        ]

    @classmethod
    def _contribute_to_options(cls, options: ModelOptions):
        # Find attributes and their types, and create indexes for these
        # for performance at runtime.
        fields, defaults = annotations(cls, stop=Record)
        options.fields = cast(Mapping, fields)
        options.fieldset = frozenset(fields)
        options.optionalset = frozenset(defaults)
        options.defaults = defaults

    @classmethod
    def _contribute_field_descriptors(cls, options: ModelOptions) -> None:
        fields = options.fields
        defaults = options.defaults
        for field, typ in fields.items():
            try:
                default, required = defaults[field], False
            except KeyError:
                default, required = None, True
            setattr(cls, field, FieldDescriptor(
                field, typ, cls, required, default))

    def _init_fields(self, fields):
        fieldset = frozenset(fields)
        options = self._options

        # Check all required arguments.
        missing = options.fieldset - fieldset - options.optionalset
        if missing:
            raise TypeError('{} missing required arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(missing))))

        # Check for unknown arguments.
        extraneous = fieldset - options.fieldset
        if extraneous:
            raise TypeError('{} got unexpected arguments: {}'.format(
                type(self).__name__, ', '.join(sorted(extraneous))))

        # Fast: This sets attributes from kwargs.
        self.__dict__.update(fields)

    def _derive(self, objects: Tuple[ModelT, ...], fields: Dict) -> ModelT:
        data = cast(Dict, self._to_representation())
        for obj in objects:
            data.update(cast(Record, obj)._to_representation())
        return type(self)(req=self.req, **{**data, **fields})

    def _to_representation(self) -> Mapping[str, Any]:
        # Convert known fields to mapping of ``{field: value}``.
        return dict(self._asitems())

    def _asitems(self) -> Iterable[Tuple[Any, Any]]:
        # Iterate over known fields as items-tuples.
        for key in self._options.fields:
            yield key, self.__dict__[key]

    def _humanize(self) -> str:
        return _kvrepr(self.__dict__)


def _kvrepr(d: Mapping[str, Any],
            sep: str = ', ',
            fmt: str = '{0}={1!r}') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    return sep.join(
        fmt.format(k, v) for k, v in d.items()
    )


class FieldDescriptor(FieldDescriptorT):
    """Describes a field.

    Used for every field in Record so that they can be used in join's
    /group_by etc.

    Examples:
        >>> class Withdrawal(Record):
        ...    account_id: str
        ...    amount: float = 0.0

        >>> Withdrawal.account_id
        <FieldDescriptor: Withdrawal.account_id: str>
        >>> Withdrawal.amount
        <FieldDescriptor: Withdrawal.amount: float = 0.0>

    Arguments:
        field (str): Name of field.
        type (Type): Field value type.
        event (Type): Record class the field belongs to.
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

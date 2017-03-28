"""Serializing/deserializing message keys and values."""
from typing import (
    Any, Dict, Iterable, List, Mapping, MutableSequence,
    Set, Sequence, Tuple, Type, Union, cast,
)
from avro import schema
from .codecs import dumps, loads
from .types.codecs import CodecArg
from .types.models import FieldDescriptorT, ModelT, ModelOptions
from .types.tuples import Request, Topic
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
    _schema_cache: schema.Schema = None   # XXX ClassVar

    @classmethod
    def loads(
            cls, s: bytes,
            *,
            default_serializer: CodecArg = None,
            req: Request = None) -> ModelT:
        """Deserialize event from bytes.

        Keyword Arguments:
            default_serializer (CodecArg): Default serializer to use
                if no custom serializer was set for this Event subclass.
            **kwargs: Additional attributes to set on the event object.
                Note, these are regarded as defaults, and any fields also
                present in the message takes precedence.
        """
        return cls(  # type: ignore
            loads(cls._options.serializer or default_serializer, s),
            req=req,
        )

    @classmethod
    def as_schema(cls) -> Mapping:
        return {
            'namespace': cls._options.namespace,
            'type': cls._schema_type,
            'name': cls.__name__,
            'fields': cls._schema_fields(),
        }

    @classmethod
    def as_avro_schema(cls) -> schema.Schema:
        if cls._schema_cache is None:
            cls._schema_cache = cls._as_avro_schema()
        return cls._schema_cache

    @classmethod
    def _as_avro_schema(cls) -> schema.Schema:
        names = schema.Names()
        return schema.SchemaFromJSONData(cls.as_schema(), names)

    @classmethod
    def _schema_fields(cls) -> Any:
        raise NotImplementedError()

    def __init_subclass__(cls,
                          serializer: str = None,
                          namespace: str = None,
                          **kwargs: Any) -> None:
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

    def derive(self, *objects: ModelT, **fields) -> ModelT:
        return self._derive(objects, fields)

    def _derive(self, objects: Tuple[ModelT, ...], fields: Dict) -> ModelT:
        raise NotImplementedError()

    async def forward(self, topic: Union[str, Topic]) -> None:
        await self.req.app.send(topic, self.req.key, self)

    def dumps(self) -> bytes:
        """Serialize event to the target serialization format."""
        return dumps(self._options.serializer, self.to_representation())

    def to_representation(self) -> Any:
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
        options.models = {
            field: typ for field, typ in fields.items()
            if issubclass(typ, ModelT)
        }
        options.modelset = frozenset(options.models)

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

    def __init__(self, _data: Any = None, *, req=None, **fields: Any) -> None:
        # Req is only set by the Consumer, when the event originates
        # from message received.
        self.req = req

        if _data is not None:
            assert not fields
            self._init_fields(_data, using_args=True)
        else:
            # Set fields from keyword arguments.
            self._init_fields(fields, using_args=False)

    def _init_fields(self, fields, using_args):
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

        # then reconstruct child models
        for _field, _typ in self._options.models.items():
            try:
                _data = fields[_field]
            except KeyError:
                pass
            else:
                if not isinstance(_data, ModelT):
                    self.__dict__[_field] = _typ(_data)

    def _derive(self, objects: Tuple[ModelT, ...], fields: Dict) -> ModelT:
        data = cast(Dict, self.to_representation())
        for obj in objects:
            data.update(cast(Record, obj).to_representation())
        return type(self)(req=self.req, **{**data, **fields})

    def to_representation(self) -> Mapping[str, Any]:
        # Convert known fields to mapping of ``{field: value}``.
        return dict(self._asitems())

    def _asitems(self) -> Iterable[Tuple[Any, Any]]:
        # Iterate over known fields as items-tuples.
        modelset = self._options.modelset
        for key in self._options.fields:
            value = self.__dict__[key]
            if key in modelset:
                value = value.to_representation()
            yield key, value

    def _humanize(self) -> str:
        return _kvrepr(self.__dict__, skip={'req'})


def _kvrepr(d: Mapping[str, Any],
            *,
            skip: Set[str] = None,
            sep: str = ', ',
            fmt: str = '{0}={1!r}') -> str:
    """Represent dict as `k='v'` pairs separated by comma."""
    skip = skip or set()
    return sep.join(
        fmt.format(k, v) for k, v in d.items()
        if k not in skip
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


class ArrayType(type):

    __fields__: Tuple[Type, ...] = None
    __descr__: List[FieldDescriptor]

    def __getitem__(cls, fields: Any) -> type:
        if cls.__fields__ is None:
            return type('Array', (cls,), {'__fields__': fields})
        return cls.__descr__[fields]

    def __repr__(cls):
        return 'Array:{}[{}]'.format(cls.__name__, ', '.join(
            t.__name__ for t in cls.__fields__))


class Array(Model, metaclass=ArrayType):
    __abstract__ = True
    _schema_type = 'array'

    __values__: List[Any]

    @classmethod
    def _contribute_to_options(cls, options: ModelOptions):
        # Find attributes and their types, and create indexes for these
        # for performance at runtime.
        options.fields = cls.__fields__
        options.fieldset = frozenset()
        options.optionalset = frozenset()
        options.defaults = {}

    @classmethod
    def _contribute_field_descriptors(cls, options: ModelOptions) -> None:
        cls.__descr__ = [
            FieldDescriptor('Array{}Value'.format(i + 1), typ, cls, True, None)
            for i, typ in enumerate(cls.__fields__)
        ]

    @classmethod
    def _schema_fields(cls) -> Sequence[Mapping]:
        return [
            {'name': d.field, 'type': to_avro_type(d.type)}
            for d in cls.__descr__
        ]

    def __init__(self, s: Sequence[Any], *, req: Request = None) -> None:
        # Req is only set by the Consumer, when the event originates
        # from message received.
        self.req = req
        if isinstance(s, MutableSequence):
            self.__values__ = cast(List, s)
        else:
            self.__values__ = list(s)

    def to_representation(self) -> List[Any]:
        return self.__values__

    def __getitem__(self, index: int) -> Any:
        return self.__values__[index]

    def __setitem__(self, index: int, value: Any) -> None:
        self.__values__[index] = value

    def __delitem__(self, index: int) -> None:
        del self.__values__[index]

    def __repr__(self) -> str:
        return repr(self.__values__)

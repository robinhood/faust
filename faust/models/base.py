"""Serializing/deserializing message keys and values."""
from typing import Any, ClassVar, Dict, Mapping, Tuple, Type, Union
from avro import schema
from ..serializers.codecs import CodecArg, dumps, loads
from ..types import K, V, Request, TopicT
from ..types.models import FieldDescriptorT, ModelT, ModelOptions

__all__ = ['Model', 'FieldDescriptor']

SENTINEL = object()

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

    #: If you want to make a custom Model base class, you have to
    #: set this attribute to True in your class.
    #: It forces __init_subclass__ to skip class initialization.
    __abstract__: ClassVar[bool] = True

    #: Name used in Avro schema's "type" field (e.g. "record").
    _schema_type: ClassVar[str] = None

    #: Cache for ``.as_avro_schema()``.
    _schema_cache: ClassVar[schema.Schema] = None

    #: Request associated with event.
    #: When an model (Event) is received as a message, this field is populated
    #: with the :class:`~faust.types.Request` it originated from.
    req: Request = None

    @classmethod
    def loads(
            cls, s: bytes,
            *,
            default_serializer: CodecArg = None,
            req: Request = None) -> ModelT:
        """Deserialize model object from bytes.

        Keyword Arguments:
            default_serializer (CodecArg): Default serializer to use
                if no custom serializer was set for this model subclass.
            **kwargs: Additional attributes to set on the model object.
                Note, these are regarded as defaults, and any fields also
                present in the message takes precedence.
        """
        return cls(  # type: ignore
            loads(cls._options.serializer or default_serializer, s),
            req=req,
        )

    @classmethod
    def as_schema(cls) -> Mapping:
        """Return Avro schema as mapping."""
        return {
            'namespace': cls._options.namespace,
            'type': cls._schema_type,
            'name': cls.__name__,
            'fields': cls._schema_fields(),
        }

    @classmethod
    def as_avro_schema(cls) -> schema.Schema:
        """Return Avro schema as :class:`avro.schema.Schema`."""
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
        # Python 3.6 added the new __init_subclass__ function to make it
        # possible to initialize subclasses without using metaclasses
        # (:pep:`487`).
        super().__init_subclass__(**kwargs)  # type: ignore

        # mypy does not recognize `__init_subclass__` as a classmethod
        # and so thinks we are mutating a ClassVar when setting
        #   cls.__abstract__ = False
        # To fix this we simply delegate to a _init_subclass classmethod.
        cls._init_subclass(serializer, namespace)

    @classmethod
    def _init_subclass(cls,
                       serializer: str = None,
                       namespace: str = None) -> None:
        if cls.__abstract__:
            # Custom base classes can set this to skip class initialization.
            cls.__abstract__ = False
            return

        # Can set serializer/namespace/etc. using:
        #    class X(Record, serializer='avro', namespace='com.vandelay.X'):
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

    def derive(self, *objects: ModelT, **fields: Any) -> ModelT:
        return self._derive(objects, fields)

    def _derive(self, objects: Tuple[ModelT, ...], fields: Dict) -> ModelT:
        raise NotImplementedError()

    async def send(self, topic: Union[str, TopicT],
                   *,
                   key: Any = SENTINEL) -> None:
        """Serialize and send object to topic."""
        if key is SENTINEL:
            key = self.req.key
        await self.req.app.send(topic, key, self)

    async def forward(self, topic: Union[str, TopicT],
                      *,
                      key: Any = SENTINEL) -> None:
        """Forward original message (will not be reserialized)."""
        if key is SENTINEL:
            key = self.req.key
        await self.req.app.send(topic, key, self.req.message.value)

    def attach(self, topic: Union[str, TopicT], key: K, value: V,
               *,
               partition: int = None,
               key_serializer: CodecArg = None,
               value_serializer: CodecArg = None) -> None:
        self.req.app.send_attached(
            self.req.message, topic, key, value,
            partition=partition,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )

    def ack(self) -> None:
        req = self.req
        message = req.message
        # decrement the reference count
        message.decref()
        # if no more references, ack message
        if not message.refcount:
            self.req.app.sources.ack_message(message)

    async def __aenter__(self) -> 'ModelT':
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        self.ack()

    def __enter__(self) -> 'ModelT':
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.ack()

    def dumps(self, *, serializer: CodecArg = None) -> bytes:
        """Serialize object to the target serialization format."""
        return dumps(serializer or self._options.serializer,
                     self.to_representation())

    def to_representation(self) -> Any:
        """Convert object to JSON serializable object."""
        raise NotImplementedError()

    def _humanize(self) -> str:
        """String representation of object for debugging purposes."""
        raise NotImplementedError()

    def __repr__(self) -> str:
        return '<{}: {}>'.format(type(self).__name__, self._humanize())


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
        model (Type): Model class the field belongs to.
        required (bool): Set to false if field is optional.
        default (Any): Default value when `required=False`.
    """

    #: Name of attribute on Model.
    field: str

    #: Type of value (e.g. ``int``).
    type: Type

    #: The model this is field is associated with.
    model: Type

    #: Set if a value for this field is required (cannot be :const:`None`).
    required: bool = True

    #: Default value for non-required field.
    default: Any = None  # noqa: E704

    def __init__(self,
                 field: str,
                 type: Type,
                 model: Type,
                 required: bool = True,
                 default: Any = None) -> None:
        self.field = field
        self.type = type
        self.model = model
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
        return '<{name}: {ident}: {type}{default}>'.format(
            name=type(self).__name__,
            model=self.model.__name__,
            field=self.field,
            type=self.type.__name__,
            default='' if self.required else ' = {!r}'.format(self.default),
        )

    @property
    def ident(self) -> str:
        return '{}.{}'.format(self.model.__name__, self.field)

"""Serializing/deserializing message keys and values."""
from typing import (
    Any, ClassVar, Dict, Mapping, MutableMapping, Optional, Tuple, Type,
)
from avro import schema
from ..serializers.codecs import CodecArg, dumps, loads
from ..types.models import FieldDescriptorT, ModelOptions, ModelT
from ..utils.objects import canoname

__all__ = ['Model', 'FieldDescriptor', 'registry']

# NOTES:
# - Records are described in the same notation as named tuples in Python 3.6.
#   To accomplish this ``__init_subclass__`` defined in :pep:`487` is used.
#
#   When accessed on the Record class, the attributes are actually field
#   descriptors that return information about the field:
#       >>> Point.x
#       <FieldDescriptor: Point.x: int>
#
#   This field descriptor holds information about the name of the field, the
#   value type of the field, and also what Record subclass it belongs to.
#
# - Sometimes field descriptions are passed around as arguments to functions.
#
# - A stream of deposits may be joined with a stream of orders if
#   both have an ``account`` field.  Field descriptors are used to
#   specify the field.
#
# - order_instance.account is data
#   (it holds the string account for this particular order).
#
# - order_instance.__class__.account is the field descriptor for the field,
#   it's not data but metadata that enables introspection, and it can be
#   passed around to describe a field we want to extract or similar.
#
# - FieldDescriptors are Python descriptors: In Python object
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

#: Global map of namespace -> Model, used to find model classes by name.
#: Every single model defined is added here automatically on class creation.
registry: MutableMapping[str, Type[ModelT]] = {}


class Model(ModelT):
    """Meta description model for serialization."""

    #: If you want to make a custom Model base class, you have to
    #: set this attribute to True in your class.
    #: It forces __init_subclass__ to skip class initialization.
    __abstract__: ClassVar[bool] = True

    #: Name used in Avro schema's "type" field (e.g. "record").
    _schema_type: ClassVar[str] = None

    #: Cache for ``.as_avro_schema()``.
    _schema_cache: ClassVar[schema.Schema] = None

    #: Serialized data may contain a "blessed key" that mandates
    #: how the data should be deserialized.  This probably only
    #: applies to records, but we need to support it at Model level.
    #: The blessed key has a dictionary value with a ``ns`` key:
    #:   data = {.., '__faust': {'ns': 'examples.simple.Withdrawal'}}
    #: When ``Model._maybe_reconstruct` sees this key it will look
    #: up that namespace in the :data:`registry`, and if it exists
    #: select it as the target model to use for serialization.
    #:
    #: Is this similar to how unsafe deserialization in pickle/yaml/etc.
    #: works?  No! pickle/pyyaml allow for arbitrary types to be
    #: deserialized (and worse in pickle's case), whereas the blessed
    #: key can only deserialize to a hardcoded list of types that are
    #: already under the remote control of messages anyway.
    #: For example it's not possible to perform remote code execution
    #: by providing a blessed key namespace of "os.system", simply
    #: because os.system is not in the registry of allowed types.
    _blessed_key = '__faust'

    @classmethod
    def _maybe_namespace(cls, data: Any) -> Optional[Type[ModelT]]:
        # The serialized data may contain a ``__faust`` blessed key
        # holding the name of the model it should be deserialized as.
        # So even if value_type=MyModel, the data may mandata that it
        # should be deserialized using "foo.bar.baz" instead.

        # This is how we deal with Kafka's lack of message headers,
        # as needed by the RPC mechanism, without wrapping all data.
        if isinstance(data, Mapping) and cls._blessed_key in data:
            return registry[data[cls._blessed_key]['ns']]
        return None

    @classmethod
    def _maybe_reconstruct(cls, data: Any) -> Any:
        model = cls._maybe_namespace(data)
        return model(data) if model else data

    @classmethod
    def loads(
            cls, s: bytes,
            *,
            default_serializer: CodecArg = None) -> ModelT:
        """Deserialize model object from bytes.

        Keyword Arguments:
            default_serializer (CodecArg): Default serializer to use
                if no custom serializer was set for this model subclass.
            **kwargs: Additional attributes to set on the model object.
                Note, these are regarded as defaults, and any fields also
                present in the message takes precedence.
        """
        data = loads(cls._options.serializer or default_serializer, s)
        self_cls = cls._maybe_namespace(data)
        return self_cls(data) if self_cls else cls(data)

    @classmethod
    def as_schema(cls) -> Mapping:
        'Return Avro schema as mapping.'
        return {
            'namespace': cls._options.namespace,
            'type': cls._schema_type,
            'name': cls.__name__,
            'fields': cls._schema_fields(),
        }

    @classmethod
    def as_avro_schema(cls) -> schema.Schema:
        'Return Avro schema as :class:`avro.schema.Schema`.'
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
                          include_metadata: bool = True,
                          isodates: bool = False,
                          **kwargs: Any) -> None:
        # Python 3.6 added the new __init_subclass__ function that
        # makes it possible to initialize subclasses without using
        # metaclasses (:pep:`487`).
        super().__init_subclass__(**kwargs)  # type: ignore

        # mypy does not recognize `__init_subclass__` as a classmethod
        # and thinks we're mutating a ClassVar when setting:
        #   cls.__abstract__ = False
        # To fix this we simply delegate to a _init_subclass classmethod.
        cls._init_subclass(serializer, namespace, include_metadata, isodates)

    @classmethod
    def _init_subclass(cls,
                       serializer: str = None,
                       namespace: str = None,
                       include_metadata: bool = True,
                       isodates: bool = False) -> None:
        if cls.__abstract__:
            # Custom base classes can set this to skip class initialization.
            cls.__abstract__ = False
            return

        # Can set serializer/namespace/etc. using:
        #    class X(Record, serializer='avro', namespace='com.vandelay.X'):
        #        ...
        try:
            custom_options = cls.Options
        except AttributeError:
            custom_options = None
        else:
            delattr(cls, 'Options')
        options = ModelOptions()
        if custom_options:
            options.__dict__.update(custom_options.__dict__)
        if serializer is not None:
            options.serializer = serializer
        options.include_metadata = include_metadata
        options.namespace = namespace or canoname(cls)
        options.isodates = isodates

        # Add introspection capabilities
        cls._contribute_to_options(options)
        # Add FieldDescriptor's for every field.
        cls._contribute_field_descriptors(options)

        # Store options on new subclass.
        cls._options = options

        # Register in the global registry, so we can look up
        # models by namespace.
        registry[options.namespace] = cls

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

    def dumps(self, *, serializer: CodecArg = None) -> bytes:
        'Serialize object to the target serialization format.'
        return dumps(serializer or self._options.serializer,
                     self.to_representation())

    def to_representation(self) -> Any:
        'Convert object to JSON serializable object.'
        raise NotImplementedError()

    def _humanize(self) -> str:
        'String representation of object for debugging purposes.'
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self._humanize()}>'


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

    #: Type of value (e.g. ``int``, or ``Optional[int]``)).
    type: Type

    #: The model class this field is associated with.
    model: Type[ModelT]

    #: Set if a value for this field is required (cannot be :const:`None`).
    required: bool = True

    #: Default value for non-required field.
    default: Any = None  # noqa: E704

    def __init__(self,
                 field: str,
                 type: Type,
                 model: Type[ModelT],
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
        default = '' if self.required else f' = {self.default!r}'
        typ = self.type.__name__
        return f'<{type(self).__name__}: {self.ident}: {typ}{default}>'

    @property
    def ident(self) -> str:
        return f'{self.model.__name__}.{self.field}'


# flake8 thinks Dict is unused for some reason
__flake8_ignore_this_Dict: Dict  # XXX

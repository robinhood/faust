"""Model descriptions.

The model describes the components of a data structure, kind of like a struct
in C, but there's no limitation of what type of data structure the model is,
or what it's used for.

A record (faust.models.record) is a model type that serialize into
dictionaries, so the model describe the fields, and their types:

.. sourcecode:: python

    >>> class Point(Record):
    ...    x: int
    ...    y: int

    >>> p = Point(10, 3)
    >>> assert p.x == 10
    >>> assert p.y == 3
    >>> p
    <Point: x=10, y=3>
    >>> payload = p.dumps(serializer='json')
    '{"x": 10, "y": 3, "__faust": {"ns": "__main__.Point"}}'
    >>> p2 = Record.loads(payload)
    >>> p2
    <Point: x=10, y=3>

Models are mainly used for describing the data in messages: both keys and
values can be described as models.
"""
import abc
import inspect
from operator import attrgetter
from typing import (
    Any,
    Callable,
    ClassVar,
    Iterable,
    MutableMapping,
    Optional,
    Tuple,
    Type,
)

from mode.utils.objects import canoname

from faust.serializers.codecs import CodecArg, dumps, loads
from faust.types.models import FieldDescriptorT, ModelOptions, ModelT

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
#   The above descriptor overrides __get__, which is called when the attribute
#   is accessed (a descriptor may also override __set__ and __del__).
#
#
#   You can see the difference in what happens when you access the attribute
#   on the class, vs. the instance:

#       >>> Example.foo
#       ACCESS ON CLASS ATTRIBUTE
#       <__main__.MyDescriptor at 0x1049caac8>
#
#       >>> x = Example()
#       >>> x.foo
#       ACCESS ON INSTANCE
#       42

#: Global map of namespace -> Model, used to find model classes by name.
#: Every single model defined is added here automatically when a model
#: class is defined.
registry: MutableMapping[str, Type[ModelT]] = {}


class Model(ModelT):
    """Meta description model for serialization."""

    #: Set to True if this is an abstract base class.
    __is_abstract__: ClassVar[bool] = True

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
    def _maybe_namespace(
            cls, data: Any,
            *,
            preferred_type: Type[ModelT] = None,
            fast_types: Tuple[Type, ...] = (bytes, str),
            isinstance: Callable = isinstance) -> Optional[Type[ModelT]]:
        # The serialized data may contain a ``__faust`` blessed key
        # holding the name of the model it should be deserialized as.
        # So even if value_type=MyModel, the data may mandata that it
        # should be deserialized using "foo.bar.baz" instead.

        # This is how we deal with Kafka's lack of message headers,
        # as needed by the RPC mechanism, without wrapping all data.
        if data is None or isinstance(data, fast_types):
            return None
        try:
            ns = data[cls._blessed_key]['ns']
        except (KeyError, TypeError):
            pass
        else:
            # we only allow blessed keys when type=None, or type=Model
            type_is_abstract = (preferred_type is None or
                                preferred_type is ModelT or
                                preferred_type is Model)
            try:
                model = registry[ns]
            except KeyError:
                if type_is_abstract:
                    raise
                return None
            else:
                if type_is_abstract or model._options.allow_blessed_key:
                    return model
        return None

    @classmethod
    def _maybe_reconstruct(cls, data: Any) -> Any:
        model = cls._maybe_namespace(data)
        return model.from_data(data) if model else data

    @classmethod
    def loads(cls, s: bytes, *, default_serializer: CodecArg = None) -> ModelT:
        """Deserialize model object from bytes.

        Arguments:
            default_serializer (CodecArg): Default serializer to use
                if no custom serializer was set for this model subclass.
            **kwargs: Additional attributes to set on the model object.
                Note, these are regarded as defaults, and any fields also
                present in the message takes precedence.
        """
        data = loads(cls._options.serializer or default_serializer, s)
        return cls.from_data(data)

    def __init_subclass__(cls,
                          serializer: str = None,
                          namespace: str = None,
                          include_metadata: bool = True,
                          isodates: bool = False,
                          abstract: bool = False,
                          allow_blessed_key: bool = False,
                          **kwargs: Any) -> None:
        # Python 3.6 added the new __init_subclass__ function that
        # makes it possible to initialize subclasses without using
        # metaclasses (:pep:`487`).
        super().__init_subclass__(**kwargs)

        # mypy does not recognize `__init_subclass__` as a classmethod
        # and thinks we're mutating a ClassVar when setting:
        #   cls.__is_abstract__ = False
        # To fix this we simply delegate to a _init_subclass classmethod.
        cls._init_subclass(
            serializer,
            namespace,
            include_metadata,
            isodates,
            abstract,
            allow_blessed_key,
        )

    @classmethod
    def _init_subclass(cls,
                       serializer: str = None,
                       namespace: str = None,
                       include_metadata: bool = True,
                       isodates: bool = False,
                       abstract: bool = False,
                       allow_blessed_key: bool = False) -> None:
        if abstract:
            # Custom base classes can set this to skip class initialization.
            cls.__is_abstract__ = True
            return
        cls.__is_abstract__ = False

        # Can set serializer/namespace/etc. using:
        #    class X(Record, serializer='json', namespace='com.vandelay.X'):
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
        options.allow_blessed_key = allow_blessed_key

        # Add introspection capabilities
        cls._contribute_to_options(options)
        # Add FieldDescriptors for every field.
        cls._contribute_field_descriptors(cls, options)

        # Store options on new subclass.
        cls._options = options

        cls._contribute_methods()

        # Register in the global registry, so we can look up
        # models by namespace.
        registry[options.namespace] = cls

        cls._model_init = cls._BUILD_init()
        if '__init__' not in cls.__dict__:
            cls.__init__ = cls._model_init

    @classmethod
    @abc.abstractmethod
    def _contribute_to_options(
            cls, options: ModelOptions) -> None:  # pragma: no cover
        ...

    @classmethod
    def _contribute_methods(cls) -> None:  # pragma: no cover
        ...

    @classmethod
    @abc.abstractmethod
    def _contribute_field_descriptors(
            cls,
            target: Type,
            options: ModelOptions,
            parent: FieldDescriptorT = None) -> None:  # pragma: no cover
        ...

    @classmethod
    @abc.abstractmethod
    def _BUILD_init(cls) -> Callable[[], None]:  # pragma: no cover
        ...

    @abc.abstractmethod
    def to_representation(self) -> Any:  # pragma: no cover
        """Convert object to JSON serializable object."""

    @abc.abstractmethod
    def _humanize(self) -> str:  # pragma: no cover
        """Return string representation of object for debugging purposes."""
        ...

    def derive(self, *objects: ModelT, **fields: Any) -> ModelT:
        return self._derive(*objects, **fields)

    @abc.abstractmethod  # pragma: no cover
    def _derive(self, *objects: ModelT, **fields: Any) -> ModelT:
        raise NotImplementedError()

    def dumps(self, *, serializer: CodecArg = None) -> bytes:
        """Serialize object to the target serialization format."""
        return dumps(serializer or self._options.serializer,
                     self.to_representation())

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self._humanize()}>'


def _is_concrete_model(typ: Type = None) -> bool:
    return (typ is not None and
            inspect.isclass(typ) and
            issubclass(typ, ModelT) and
            typ is not ModelT and
            not getattr(typ, '__is_abstract__', False))


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
                 default: Any = None,
                 parent: FieldDescriptorT = None) -> None:
        self.field = field
        self.type = type
        self.model = model
        self.required = required
        self.default = default
        self.parent = parent
        self._copy_descriptors(self.type)

    def _copy_descriptors(self, typ: Type = None) -> None:
        if typ is not None and _is_concrete_model(typ):
            typ._contribute_field_descriptors(self, typ._options, parent=self)

    def __get__(self, instance: Any, owner: Type) -> Any:
        # class attribute accessed
        if instance is None:
            return self

        # instance attribute accessed
        return instance.__dict__[self.field]

    def getattr(self, obj: ModelT) -> Any:
        return attrgetter('.'.join(reversed(list(self._parents_path()))))(obj)

    def _parents_path(self) -> Iterable[str]:
        node: Optional[FieldDescriptorT] = self
        while node:
            yield node.field
            node = node.parent

    def __set__(self, instance: Any, value: Any) -> None:
        instance.__dict__[self.field] = value

    def __repr__(self) -> str:
        default = '' if self.required else f' = {self.default!r}'
        typ = self.type.__name__
        return f'<{type(self).__name__}: {self.ident}: {typ}{default}>'

    @property
    def ident(self) -> str:
        return f'{self.model.__name__}.{self.field}'

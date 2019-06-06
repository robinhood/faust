import abc
import typing
from typing import (
    Any,
    Callable,
    ClassVar,
    FrozenSet,
    Generic,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)
from faust.exceptions import ValidationError  # XXX !!coupled
from .codecs import CodecArg

__all__ = [
    'CoercionHandler',
    'FieldDescriptorT',
    'FieldMap',
    'IsInstanceArgT',
    'ModelArg',
    'ModelOptions',
    'ModelT',
    'TypeCoerce',
    'TypeInfo',
]

FieldMap = Mapping[str, 'FieldDescriptorT']

T = TypeVar('T')

# Workaround for https://bugs.python.org/issue29581
try:
    @typing.no_type_check  # type: ignore
    class _InitSubclassCheck(metaclass=abc.ABCMeta):

        def __init_subclass__(self,
                              *args: Any,
                              ident: int = 808,
                              **kwargs: Any) -> None:
            self.ident = ident
            super().__init__(*args, **kwargs)  # type: ignore

    @typing.no_type_check  # type: ignore
    class _UsingKwargsInNew(_InitSubclassCheck, ident=909):
        ...
except TypeError:
    abc_compatible_with_init_subclass = False
else:
    abc_compatible_with_init_subclass = True

ModelArg = Union[Type['ModelT'], Type[bytes], Type[str]]
IsInstanceArgT = Union[Type, Tuple[Type, ...]]
CoercionHandler = Callable[[Any], Any]
CoercionMapping = MutableMapping[IsInstanceArgT, CoercionHandler]


class TypeCoerce(NamedTuple):
    target: Type
    handler: CoercionHandler


class TypeInfo(NamedTuple):
    generic_type: Optional[Type]
    member_type: Type


class ModelOptions(abc.ABC):
    serializer: Optional[CodecArg] = None
    namespace: str
    include_metadata: bool = True
    polymorphic_fields: bool = False
    allow_blessed_key: bool = False  # XXX compat
    isodates: bool = False
    decimals: bool = False
    validation: bool = False
    coerce: bool = False
    coercions: CoercionMapping = cast(CoercionMapping, None)

    # If we set `attr = None` mypy will think the values can be None
    # on the instance, but if we don't set it Sphinx will find
    # that the attributes don't exist on the class.
    # Very annoying - we could set .e.g `fields = {}` instead of None,
    # but then we might accidentally forget to initialize it,
    # so seems safer for it to be None.

    #: Index: Flattened view of __annotations__ in MRO order.
    fields: Mapping[str, Type] = cast(Mapping[str, Type], None)

    #: Index: Set of required field names, for fast argument checking.
    fieldset: FrozenSet[str] = cast(FrozenSet[str], None)

    #: Index: Mapping of field name to field descriptor.
    descriptors: FieldMap = cast(FieldMap, None)

    #: Index: Positional argument index to field name.
    #: Used by Record.__init__ to map positional arguments to fields.
    fieldpos: Mapping[int, str] = cast(Mapping[int, str], None)

    #: Index: Set of optional field names, for fast argument checking.
    optionalset: FrozenSet[str] = cast(FrozenSet[str], None)

    #: Index: Mapping of fields that are ModelT
    models: Mapping[str, Type['ModelT']] = cast(
        Mapping[str, Type['ModelT']], None)

    # Index: Set of field names that are ModelT and there concrete type
    modelattrs: Mapping[str, Optional[Type]] = cast(
        Mapping[str, Optional[Type]], None)

    #: Index: Mapping of fields that need to be coerced.
    #: Key is the name of the field, value is the coercion handler function.
    field_coerce: Mapping[str, TypeCoerce] = cast(
        Mapping[str, TypeCoerce], None)

    #: Mapping of field names to default value.
    defaults: Mapping[str, Any] = cast(  # noqa: E704 (flake8 bug)
        Mapping[str, Any], None)

    #: Mapping of init field conversion callbacks.
    initfield: Mapping[str, CoercionHandler] = cast(
        Mapping[str, CoercionHandler], None)

    #: Index of field to polymorphic type
    polyindex: Mapping[str, TypeInfo] = cast(
        Mapping[str, TypeInfo], None)

    def clone_defaults(self) -> 'ModelOptions':
        new_options = type(self)()
        new_options.serializer = self.serializer
        new_options.namespace = self.namespace
        new_options.include_metadata = self.include_metadata
        new_options.polymorphic_fields = self.polymorphic_fields
        new_options.allow_blessed_key = self.allow_blessed_key
        new_options.isodates = self.isodates
        new_options.decimals = self.decimals
        new_options.coerce = self.coerce
        new_options.coercions = dict(self.coercions)
        return new_options


base = abc.ABC if abc_compatible_with_init_subclass else object


class ModelT(base):  # type: ignore
    __is_model__: ClassVar[bool] = True

    _options: ClassVar[ModelOptions]

    @classmethod
    @abc.abstractmethod
    def from_data(cls, data: Any, *,
                  preferred_type: Type['ModelT'] = None) -> 'ModelT':
        ...

    @classmethod
    @abc.abstractmethod
    def loads(cls, s: bytes, *,
              default_serializer: CodecArg = None,  # XXX use serializer
              serializer: CodecArg = None) -> 'ModelT':
        ...

    @abc.abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def dumps(self, *, serializer: CodecArg = None) -> bytes:
        ...

    @abc.abstractmethod
    def derive(self, *objects: 'ModelT', **fields: Any) -> 'ModelT':
        ...

    @abc.abstractmethod
    def to_representation(self) -> Any:
        ...

    @abc.abstractmethod
    def is_valid(self) -> bool:
        ...

    @abc.abstractmethod
    def validate(self) -> List[ValidationError]:
        ...

    @abc.abstractmethod
    def validate_or_raise(self) -> None:
        ...

    @property
    @abc.abstractmethod
    def validation_errors(self) -> List[ValidationError]:
        ...


class FieldDescriptorT(Generic[T]):

    field: str
    type: Type[T]
    model: Type[ModelT]
    required: bool = True
    default: Optional[T] = None  # noqa: E704
    parent: Optional['FieldDescriptorT']
    generic_type: Optional[Type]
    member_type: Optional[Type]

    @abc.abstractmethod
    def __init__(self, *,
                 field: str = None,
                 type: Type[T] = None,
                 model: Type[ModelT] = None,
                 required: bool = True,
                 default: T = None,
                 parent: 'FieldDescriptorT' = None,
                 generic_type: Type = None,
                 member_type: Type = None,
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def validate(self, value: T) -> Iterable[ValidationError]:
        ...

    @abc.abstractmethod
    def prepare_value(self, value: Any) -> Optional[T]:
        ...

    @abc.abstractmethod
    def getattr(self, obj: ModelT) -> T:
        ...

    @property
    @abc.abstractmethod
    def ident(self) -> str:
        ...


# XXX See top of module!  We redefine with actual ModelT for Sphinx,
# as it cannot read non-final types.
ModelArg = Union[Type[ModelT], Type[bytes], Type[str]]  # type: ignore

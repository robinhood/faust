import abc
import typing
from typing import (
    Any,
    Callable,
    ClassVar,
    FrozenSet,
    Mapping,
    NamedTuple,
    Optional,
    Type,
    Union,
    cast,
)
from .codecs import CodecArg

__all__ = [
    'Converter',
    'ModelArg',
    'ModelOptions',
    'ModelT',
    'FieldDescriptorT',
]

ModelArg = Union[Type['ModelT'], Type[bytes], Type[str]]

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


class Converter(NamedTuple):
    target: Type
    handler: Callable[[Type, Any], Any]


class ModelOptions(abc.ABC):
    serializer: Optional[CodecArg] = None
    namespace: str
    include_metadata: bool = True
    allow_blessed_key: bool = False
    isodates: bool = False

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

    #: Index: Mapping of fields that are not builtin-types.
    #: E.g. datetime.
    converse: Mapping[str, Converter] = cast(Mapping[str, Converter], None)

    #: Mapping of field names to default value.
    defaults: Mapping[str, Any] = cast(  # noqa: E704 (flake8 bug)
        Mapping[str, Any], None)

    #: Mapping of init field conversion callbacks.
    initfield: Mapping[str, Callable[[Any], Any]] = cast(
        Mapping[str, Callable[[Any], Any]], None)


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
              default_serializer: CodecArg = None) -> 'ModelT':
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


class FieldDescriptorT(abc.ABC):
    field: str
    type: Type
    model: Type[ModelT]
    required: bool = True
    default: Any = None  # noqa: E704
    parent: Optional['FieldDescriptorT']

    @abc.abstractmethod
    def __init__(self,
                 field: str,
                 type: Type,
                 model: Type[ModelT],
                 required: bool = True,
                 default: Any = None,
                 parent: 'FieldDescriptorT' = None) -> None:
        ...

    @abc.abstractmethod
    def getattr(self, obj: ModelT) -> Any:
        ...

    @property
    @abc.abstractmethod
    def ident(self) -> str:
        ...


# XXX See top of module!  We redefine with actual ModelT for Sphinx,
# as it cannot read non-final types.
ModelArg = Union[Type[ModelT], Type[bytes], Type[str]]  # type: ignore

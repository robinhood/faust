import abc
import typing
from typing import (
    Any, Callable, ClassVar, FrozenSet, Mapping, NamedTuple, Type, Union,
)
from .codecs import CodecArg

if typing.TYPE_CHECKING:
    from avro.schema import Schema
else:
    class Schema: ...   # noqa

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
    @typing.no_type_check
    class _InitSubclassCheck(metaclass=abc.ABCMeta):

        def __init_subclass__(self, *args: Any,
                              ident: int = 808,
                              **kwargs: Any) -> None:
            self.ident = ident
            super().__init__(*args, **kwargs)  # type: ignore

    @typing.no_type_check
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
    serializer: CodecArg = None
    namespace: str = None
    include_metadata: bool = True
    isodates: bool = False

    # Index: Flattened view of __annotations__ in MRO order.
    fields: Mapping[str, Type]

    # Index: Set of required field names, for fast argument checking.
    fieldset: FrozenSet[str]

    # Index: Set of optional field names, for fast argument checking.
    optionalset: FrozenSet[str]

    # Index: Mapping of fields that are ModelT
    models: Mapping[str, Type['ModelT']]

    # Index: Set of field names that are ModelT
    modelset: FrozenSet[str]

    # Index: Mapping of fields that are not builtin-types.
    # E.g. datetime.
    converse: Mapping[str, Converter]

    #: Mapping of field names to default value.
    defaults: Mapping[str, Any]  # noqa: E704 (flake8 bug)


base = abc.ABC if abc_compatible_with_init_subclass else object


class ModelT(base):  # type: ignore

    _options: ClassVar[ModelOptions]

    @classmethod
    @abc.abstractmethod
    def as_schema(cls) -> Mapping:
        ...

    @classmethod
    @abc.abstractmethod
    def as_avro_schema(cls) -> Schema:
        ...

    @classmethod
    @abc.abstractmethod
    def loads(
            cls, s: bytes,
            *,
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
    parent: 'FieldDescriptorT' = None

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

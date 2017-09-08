import abc
import typing
from typing import Any, ClassVar, FrozenSet, Mapping, Type, Union
from .codecs import CodecArg

if typing.TYPE_CHECKING:  # pragma: no cover
    from avro.schema import Schema
else:
    class Schema: ...   # noqa

__all__ = ['ModelArg', 'ModelOptions', 'ModelT', 'FieldDescriptorT']

ModelArg = Union[Type['ModelT'], Type[bytes], Type[str]]


class ModelOptions(abc.ABC):
    serializer: CodecArg = None
    namespace: str = None
    include_metadata: bool = True

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

    #: Mapping of field names to default value.
    defaults: Mapping[str, Any]  # noqa: E704 (flake8 bug)


class ModelT:
    # uses __init_subclass__ so cannot use ABCMeta

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


class FieldDescriptorT:
    field: str
    type: Type
    model: Type[ModelT]
    required: bool = True
    default: Any = None  # noqa: E704

    def __init__(self,
                 field: str,
                 type: Type,
                 model: Type[ModelT],
                 required: bool = True,
                 default: Any = None) -> None:
        ...

    @property
    def ident(self) -> str:
        ...

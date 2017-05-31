import typing
from typing import Any, ClassVar, FrozenSet, Mapping, Type, Union
from .core import K, V
from .codecs import CodecArg

if typing.TYPE_CHECKING:  # pragma: no cover
    from avro.schema import Schema
    from .topics import TopicT
else:
    class Schema: ...   # noqa
    class TopicT: ...   # noqa

__all__ = ['ModelOptions', 'ModelT', 'FieldDescriptorT']


class ModelOptions:
    serializer: CodecArg = None
    namespace: str = None

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
    def as_schema(cls) -> Mapping:
        ...

    @classmethod
    def as_avro_schema(cls) -> Schema:
        ...

    @classmethod
    def loads(
            cls, s: bytes,
            *,
            default_serializer: CodecArg = None) -> 'ModelT':
        ...

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        ...

    def dumps(self, *, serializer: CodecArg = None) -> bytes:
        ...

    def derive(self, *objects: 'ModelT', **fields: Any) -> 'ModelT':
        ...

    async def forward(self, topic: Union[str, TopicT],
                      *,
                      key: Any = None) -> None:
        ...

    def attach(self, topic: Union[str, TopicT], key: K, value: V,
               *,
               partition: int = None,
               key_serializer: CodecArg = None,
               value_serializer: CodecArg = None) -> None:
        ...

    def ack(self) -> None:
        ...

    def to_representation(self) -> Any:
        ...

    async def __aenter__(self) -> 'ModelT':
        ...

    async def __aexit__(self, *exc_info: Any) -> None:
        ...

    def __enter__(self) -> 'ModelT':
        ...

    def __exit__(self, *exc_info: Any) -> None:
        ...


class FieldDescriptorT:
    field: str
    type: Type
    model: Type[ModelT]
    required: bool = True
    default: Any = None  # noqa: E704

    @property
    def ident(self) -> str:
        ...

import typing
from typing import Any, ClassVar, FrozenSet, Mapping, NewType, Type, Union
from .serializers.codecs import CodecArg
from .tuples import Request, Topic

if typing.TYPE_CHECKING:  # pragma: no cover
    from avro.schema import Schema
else:
    class Schema: ...   # noqa

__all__ = ['ModelOptions', 'ModelT', 'FieldDescriptorT', 'Event']


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
    models: Mapping[str, Type]

    # Index: Set of field names that are ModelT
    modelset: FrozenSet[str]

    #: Mapping of field names to default value.
    defaults: Mapping[str, Any]  # noqa: E704 (flake8 bug)


class ModelT:
    # uses __init_subclass__ so cannot use ABCMeta

    req: Request

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
            default_serializer: CodecArg = None,
            req: Request = None) -> 'ModelT':
        ...

    def dumps(self) -> bytes:
        ...

    def derive(self, *objects: 'ModelT', **fields: Any) -> 'ModelT':
        ...

    async def forward(self, topic: Union[str, Topic],
                      *,
                      key: Any = None) -> None:
        ...

    def to_representation(self) -> Any:
        ...


class FieldDescriptorT:
    field: str
    type: Type
    model: Type
    required: bool = True
    default: Any = None  # noqa: E704

    @property
    def ident(self) -> str:
        ...


#: An event is a ModelT that was received as a message.
Event = NewType('Event', ModelT)

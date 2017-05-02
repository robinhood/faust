from typing import Any, Dict, List, Mapping, Sequence, Tuple, Type
from ..types.app import AppT
from ..types.models import ModelT
from ..types.serializers import AsyncSerializerT
from ..utils.objects import cached_property
from ..utils.avro import MessageSerializer, RegistryClient

__all__ = ['AvroSerializer', 'to_avro_type']

AVRO_FAST_TYPE: Mapping[Any, str] = {
    int: 'int',
    float: 'float',
    bool: 'boolean',
    str: 'string',
    list: 'array',
    List: 'array',
    Sequence: 'array',
    Tuple: 'array',
    Mapping: 'map',
    Dict: 'map',
    dict: 'map',
}


def to_avro_type(typ: Type) -> str:
    try:
        return AVRO_FAST_TYPE[typ]
    except KeyError:
        pass
    if issubclass(typ, Sequence):
        return 'array'
    elif issubclass(typ, Mapping):
        return 'map'
    elif issubclass(typ, ModelT):
        return typ.as_schema()  # type: ignore
    raise TypeError('Cannot convert type {!r} to Avro'.format(typ))


class AvroSerializer(AsyncSerializerT):
    key_subject = '{}-key'
    value_subject = '{}-value'

    def __init__(self, app: AppT) -> None:
        self.app = app

    async def loads(self, s: bytes) -> Any:
        return await self.serializer.loads(s)

    async def dumps_key(self, topic: str, s: ModelT) -> bytes:
        return await self._dumps(self.key_subject.format(topic), s)

    async def dumps_value(self, topic: str, s: ModelT) -> bytes:
        return await self._dumps(self.value_subject.format(topic), s)

    async def _dumps(self, subject: str, s: ModelT) -> bytes:
        return await self.serializer.dumps(
            subject, s.as_avro_schema(), s.to_representation())

    @cached_property
    def registry(self) -> RegistryClient:
        return RegistryClient(self.app.avro_registry_url)

    @cached_property
    def serializer(self) -> MessageSerializer:
        return MessageSerializer(self.registry)

from typing import Any
from faust.types import AsyncSerializerT, AppT, ModelT
from faust.utils.objects import cached_property
from .serializer import MessageSerializer
from .server import RegistryClient


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

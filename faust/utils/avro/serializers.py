"""Avro serialization/deserialization."""
import io
from functools import partial
from struct import pack, unpack
from typing import Any, Mapping, MutableMapping, Type
from avro.io import BinaryDecoder, BinaryEncoder, DatumReader, DatumWriter
from avro.schema import Schema
from .servers import RegistryClient
try:
    from fastavro.reader import read_data as _fast_read_data
except ImportError:  # prgama: no cover
    _fast_read_data = None  # noqa

__all__ = ['MessageSerializer']

MAGIC_BYTE = 0


class MessageSerializer:
    Decoder: Type = BinaryDecoder
    Encoder: Type = BinaryEncoder
    Reader: Type = DatumReader
    Writer: Type = DatumWriter

    _id_to_decoder: MutableMapping[int, partial]
    _id_to_writer: MutableMapping[int, DatumWriter]

    def __init__(self, registry_client: RegistryClient) -> None:
        self.registry_client = registry_client
        self._id_to_decoder = {}
        self._id_to_writer = {}

    async def dumps(self, subject: str, schema: Schema, data: Any) -> bytes:
        schema_id = await self.registry_client.register(subject, schema)
        writer = self.Writer(schema)
        self._id_to_writer[schema_id] = writer
        return self._dumps(schema_id, writer, data)

    def _dumps(
            self, subject_id: int, writer: DatumWriter, data: Any) -> bytes:
        with io.BytesIO() as buffer:
            buffer.write(pack('>bI', MAGIC_BYTE, subject_id))
            encoder = self.Encoder(buffer)
            writer.write(data, encoder)
            return buffer.getvalue()

    async def loads(self, message: bytes) -> Any:
        if len(message) <= 5:
            raise ValueError('Message is too small to decode')
        with io.BytesIO(message) as payload:
            magic, schema_id = unpack('>bI', payload.read(5))
            if magic != MAGIC_BYTE:
                raise ValueError('Message does not start with magic byte.')
            try:
                decoder = self._id_to_decoder[schema_id]
            except KeyError:
                return await self._decode_best(schema_id, payload)
            return decoder(payload)

    async def _decode_best(
            self, schema_id: int, payload: io.BytesIO) -> bytes:
        schema = await self.registry_client.get_by_id(schema_id)

        # try to deserialize with fastavro
        cur_pos = payload.tell()
        if _fast_read_data is not None:
            schema_dict = schema.to_json()
            try:
                obj = self._fast_decode(schema_dict, payload)
            except Exception:
                pass
            else:
                self._id_to_decoder[schema_id] = partial(
                    self._fast_decode, schema_dict)
                return obj
        # if that didn't work: use slowavro
        payload.seek(cur_pos)
        decoder = self._id_to_decoder[schema_id] = partial(
            self._slow_decode, schema)
        return decoder(payload)

    def _fast_decode(self, schema_dict: Mapping, payload: io.BytesIO) -> bytes:
        return _fast_read_data(payload, schema_dict)

    def _slow_decode(self, schema: Schema, payload: io.BytesIO) -> bytes:
        return self.Reader(schema).read(self.Decoder(payload))

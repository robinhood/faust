"""Avro schema registry service client (HTTP)."""
import asyncio
import aiohttp
from collections import defaultdict
from contextlib import suppress
from typing import DefaultDict, Dict, Iterable, Mapping, Optional, Tuple, cast
from avro.schema import Parse, Schema
from faust.utils import json
from faust.utils.logging import get_logger

__all__ = ['ClientError', 'RegistryClient']

logger = get_logger(__name__)

ACCEPT_TYPES: Iterable[str] = [
    'application/vnd.schemaregistry.v1+json',
    'application/vnd.schemaregistry+json',
    'application/json',
]

STATUS_TO_ERROR: Mapping[int, str] = {
    404: 'Not Found',
    409: 'Incompatible Avro schema',
    422: 'Invalid Avro schema'
}


class ClientError(Exception):
    ...


class RegistryClient:
    valid_levels = {'NONE', 'FULL', 'FORWARD', 'BACKWARD'}
    content_type = 'application/vnd.schemaregistry.v1+json'

    url: str
    max_schemas_per_subject: int
    #: subj => { schema => id}
    subject_to_schema_ids: DefaultDict[str, Dict[Schema, int]]
    #: subj => { schema => version }
    subject_to_schema_versions: DefaultDict[str, Dict[Schema, str]]
    #: id => avro_schema
    id_to_schema: DefaultDict[int, Dict]
    loop: asyncio.AbstractEventLoop

    _accept_types: str
    _session: aiohttp.ClientSession

    def __init__(self,
                 url: str,
                 *,
                 max_schemas_per_subject: int = 1000,
                 session: aiohttp.ClientSession = None,
                 accept: Iterable[str] = ACCEPT_TYPES,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.url = url.rstrip('/')
        self.max_schemas_per_subject = max_schemas_per_subject
        self.subject_to_schema_ids = defaultdict(dict)
        self.id_to_schema = defaultdict(dict)
        self.subject_to_schema_versions = defaultdict(dict)
        self.loop = loop
        self._accept_types = ', '.join(accept)
        self._session = session

    async def register(self, subject: str, schema: Schema) -> int:
        """Register schema with registry.

        Arguments:
            subject (str): Subject name to register as.
            schema (Schema): Schema to be registered.

        Returns:
            int: schema id.
        """
        # POST /subjects/{subject}/versions
        schemas_to_id = self.subject_to_schema_ids[subject]
        with suppress(KeyError):
            return schemas_to_id[schema]

        result = await self._send_request(
            f'{self.url}/subjects/{subject}/versions',
            method='post',
            unknown_error_message='Unable to register schema',
            body={
                'schema': json.dumps(schema.to_json()),
            },
        )
        schema_id = cast(int, result['id'])
        self._cache_schema(schema, schema_id, subject)
        return schema_id

    def _cache_schema(self, schema: Schema, schema_id: int,
                      subject: str = None,
                      version: str = None) -> None:
        # Don't overwrite anything
        schema = self.id_to_schema.setdefault(schema_id, schema)
        if subject:
            self.subject_to_schema_ids[subject][schema] = schema_id
            if version:
                self.subject_to_schema_versions[subject][schema] = version

    async def get_by_id(self, schema_id: int) -> Schema:
        try:
            return self.id_to_schema[schema_id]
        except KeyError:
            result = await self._send_request(
                f'{self.url}/schemas/ids/{schema_id}',
            )
            schema = self._parse_schema(cast(str, result.get('schema')))
            self._cache_schema(schema, schema_id)
            return schema

    def _parse_schema(self, payload: str) -> Schema:
        try:
            return Parse(payload)
        except Exception as exc:
            raise ClientError('Received bad schema from registry')

    async def get_latest_schema(
            self, subject: str) -> Tuple[int, Schema, str]:
        try:
            result = await self._send_request(
                f'{self.url}/subjects/{subject}/versions/latest',
            )
        except ClientError:
            return None, None, None
        else:
            schema_id: int = cast(int, result['id'])
            version: str = cast(str, result['version'])
            try:
                schema = self.id_to_schema[schema_id]
            except KeyError:
                schema = self._parse_schema(cast(str, result['schema']))
            self._cache_schema(schema, schema_id, subject, version)
            return schema_id, schema, version

    async def get_version(self, subject: str, schema: Schema) -> Optional[str]:
        schemas_to_version = self.subject_to_schema_versions[subject]
        with suppress(KeyError):
            return schemas_to_version[schema]

        try:
            result = await self._send_request(
                f'{self.url}/subjects/{subject}',
                method='post',
                body={
                    'schema': json.dumps(schema.to_json()),
                },
                unknown_error_message='Unable to get version of schema',
            )
        except ClientError:
            return None
        else:
            schema_id: int = cast(int, result['id'])
            version: str = cast(str, result['version'])
            self._cache_schema(schema, schema_id, subject, version)
            return version

    async def test_compatibility(
            self, subject: str, schema: Schema,
            version: str = 'latest') -> bool:
        url = self.url
        try:
            result = await self._send_request(
                f'{url}/compatibility/subjects/{subject}/versions/{version}',
                method='post',
                body={
                    'schema': json.dumps(schema.to_json()),
                },
            )
        except ClientError as exc:
            return False
        else:
            return cast(bool, result.get('is_compatible'))

    async def update_compatibility(
            self, level: str, subject: str = None) -> str:
        if level not in self.valid_levels:
            raise ClientError(f'Invalid level specified: {level!r}')

        result = await self._send_request(
            self._compatibility_url(subject),
            method='put',
            body={'compatibility': 'level'},
            unknown_error_message=f'Unable to update level: {level}',
        )
        return result['compatibility']

    async def get_compatibility(self, subject: str = None) -> str:
        result = await self._send_request(
            self._compatibility_url(subject),
        )
        return cast(str, result.get(
            'compatibility', result.get('compatibilityLevel')))

    def _compatibility_url(self, subject: str = None) -> str:
        path = f'/{subject}' if subject else ''
        return f'{self.url}/config{path}'

    async def _send_request(
            self, url: str,
            method: str = 'get',
            body: Mapping = None,
            headers: Mapping[str, str] = None,
            unknown_error_message: str = 'Unknown error') -> Mapping:
        _body: bytes = None
        _headers: Dict[str, str] = {'Accept': self._accept_types}
        if body:
            _body = json.dumps(body).encode('utf-8')
            _headers.update({
                'Content-Length': str(len(body)),
                'Content-Type': self.content_type,
            })
        _headers.update(headers or {})

        response = await self.session.request(
            method.lower(),
            body=_body,
            headers=_headers,
        )
        if response.ok:
            return response.json()
        code = response.status_code

        error = STATUS_TO_ERROR.get(code, unknown_error_message)
        message = f'{error}: code={code!r} url={url!r} body={body!r}'
        logger.exception(message)
        raise ClientError(message)

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(loop=self.loop)
        return self._session

    @session.setter
    def session(self, session: aiohttp.ClientSession) -> None:
        self._session = session

import hashlib
from functools import wraps
from typing import Any, Callable, ClassVar, List, Optional, Type, Union, cast
from urllib.parse import quote

from mode.utils.times import Seconds, want_seconds
from mode.utils.logging import get_logger
from mode.utils.objects import cached_property

from faust.exceptions import ImproperlyConfigured
from faust.types import AppT
from faust.types.web import (
    CacheBackendT,
    CacheT,
    Request,
    Response,
    View,
)

logger = get_logger(__name__)

IDENT: str = 'faustweb.cache.view'


class BaseCache(CacheT):
    ident: ClassVar[str] = IDENT

    timeout: Seconds
    key_prefix: str
    cache_allowed_methods = frozenset({'GET', 'HEAD'})

    def __init__(self,
                 timeout: Seconds = None,
                 key_prefix: str = '',
                 backend: Union[Type[CacheBackendT], str] = None,
                 **kwargs: Any) -> None:
        self.timeout = timeout
        self.key_prefix = key_prefix or ''
        self._backend = backend

    def view(self,
             timeout: Seconds = None,
             key_prefix: str = None,
             **kwargs: Any) -> Callable[[Callable], Callable]:

        def _inner(fun: Callable) -> Callable:

            @wraps(fun)
            async def cached(view: View, request: Request,
                             *args: Any, **kwargs: Any) -> Response:
                key: Optional[str] = None
                cache = cast('Cache', self.resolve(view.app))
                if cache.can_cache_request(request):
                    key = cache.key_for_request(request, key_prefix, 'GET')

                if key is not None:
                    response = await cache.get_view(key, view)
                    if response is not None:
                        logger.info('Found cached response for %r', key)
                        return response
                    if request.method.upper() == 'HEAD':
                        response = await cache.get_view(
                            cache.key_for_request(request, key_prefix, 'HEAD'),
                            view)
                        if response is not None:
                            logger.info('Found cached HEAD response for %r',
                                        key)
                            return response

                logger.info('No cache found for %r', key)
                view_response = await fun(view, request, *args, **kwargs)

                if key is not None and view_response.status == 200:
                    logger.info('Saving cache for key %r', key)
                    await cache.set_view(key, view, view_response, timeout)
                return view_response
            return cached
        return _inner

    async def get_view(self,
                       key: str, view: View) -> Optional[Response]:
        try:
            payload = await self.backend.get(key)
            if payload is not None:
                return view.bytes_to_response(payload)
        except self.backend.Unavailable:
            return None
        return None

    async def set_view(self,
                       key: str,
                       view: View,
                       response: Response,
                       timeout: Seconds) -> None:
        try:
            return await self.backend.set(
                key,
                view.response_to_bytes(response),
                want_seconds(timeout if timeout is not None else self.timeout),
            )
        except self.backend.Unavailable:
            pass

    def can_cache_request(self, request: Request) -> bool:
        return request.method.upper() in self.cache_allowed_methods

    def key_for_request(self,
                        request: Request,
                        prefix: str = None,
                        method: str = None) -> str:
        actual_method: str = request.method if method is None else method
        if prefix is None:
            prefix = self.key_prefix
        return self.build_key(request, actual_method, prefix, [])

    def build_key(self,
                  request: Request,
                  method: str,
                  prefix: str,
                  headers: List[str]) -> str:
        context = hashlib.md5(
            b''.join(v.encode() for v in headers if v is not None)).hexdigest()
        url = hashlib.md5(
            iri_to_uri(str(request.url)).encode('ascii')).hexdigest()
        return f'{self.ident}.{prefix}.{method}.{url}.{context}'

    def resolve(self, app: AppT) -> CacheT:
        return self

    @cached_property
    def backend(self) -> CacheBackendT:
        if self._backend is None:
            raise ImproperlyConfigured(f'Cache {self} does not have backend')
        return cast(CacheBackendT, self._backend)


class Cache(BaseCache):
    app: AppT

    def __init__(self,
                 timeout: Seconds = None,
                 key_prefix: str = '',
                 backend: Union[Type[CacheBackendT], str] = None,
                 *,
                 app: AppT,
                 **kwargs: Any) -> None:
        self.app = app
        super().__init__(timeout, key_prefix, backend, **kwargs)

    @cached_property
    def backend(self) -> CacheBackendT:
        if self._backend is None:
            self._backend = self.app.cache
        return cast(CacheBackendT, self._backend)


def iri_to_uri(iri: str) -> str:
    # The list of safe characters here is constructed from the "reserved" and
    # "unreserved" characters specified in sections 2.2 and 2.3 of RFC 3986:
    #     reserved    = gen-delims / sub-delims
    #     gen-delims  = ":" / "/" / "?" / "#" / "[" / "]" / "@"
    #     sub-delims  = "!" / "$" / "&" / "'" / "(" / ")"
    #                   / "*" / "+" / "," / ";" / "="
    #     unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
    # Of the unreserved characters, urllib.parse.quote() already considers all
    # but the ~ safe.
    # The % character is also added to the list of safe characters here, as the
    # end of section 3.1 of RFC 3987 specifically mentions that % must not be
    # converted.
    return quote(iri, safe='''/#%[]=:;$&()+,!?*@'~''')

"""Cache interface."""
import hashlib
from contextlib import suppress
from functools import wraps
from typing import Any, Callable, ClassVar, List, Optional, Type, Union, cast
from urllib.parse import quote

from mode.utils.times import Seconds, want_seconds
from mode.utils.logging import get_logger

from faust.types.web import (
    CacheBackendT,
    CacheT,
    Request,
    Response,
    View,
)

logger = get_logger(__name__)

IDENT: str = 'faustweb.cache.view'


class Cache(CacheT):
    """Cache interface."""

    ident: ClassVar[str] = IDENT

    def __init__(self,
                 timeout: Seconds = None,
                 key_prefix: str = None,
                 backend: Union[Type[CacheBackendT], str] = None,
                 **kwargs: Any) -> None:
        self.timeout = timeout
        self.key_prefix = key_prefix or ''
        self.backend = backend

    def view(self,
             timeout: Seconds = None,
             key_prefix: str = None,
             **kwargs: Any) -> Callable[[Callable], Callable]:
        """Decorate view to be cached."""
        def _inner(fun: Callable) -> Callable:

            @wraps(fun)
            async def cached(view: View, request: Request,
                             *args: Any, **kwargs: Any) -> Response:
                key: Optional[str] = None
                is_head = request.method.upper() == 'HEAD'
                if self.can_cache_request(request):
                    key = self.key_for_request(request, key_prefix, 'GET')

                    response = await self.get_view(key, view)
                    if response is not None:
                        logger.info('Found cached response for %r', key)
                        return response
                    if is_head:
                        response = await self.get_view(
                            self.key_for_request(request, key_prefix, 'HEAD'),
                            view,
                        )
                        if response is not None:
                            logger.info(
                                'Found cached HEAD response for %r', key)
                            return response

                logger.info('No cache found for %r', key)
                res = await fun(view, request, *args, **kwargs)

                if key is not None and self.can_cache_response(request, res):
                    logger.info('Saving cache for key %r', key)
                    if is_head:
                        key = self.key_for_request(
                            request, key_prefix, 'HEAD')
                    await self.set_view(key, view, res, timeout)
                return res
            return cached
        return _inner

    async def get_view(self,
                       key: str, view: View) -> Optional[Response]:
        backend = self._view_backend(view)
        with suppress(backend.Unavailable):
            payload = await backend.get(key)
            if payload is not None:
                return view.bytes_to_response(payload)
        return None

    def _view_backend(self, view: View) -> CacheBackendT:
        return cast(CacheBackendT, self.backend or view.app.cache)

    async def set_view(self,
                       key: str,
                       view: View,
                       response: Response,
                       timeout: Seconds) -> None:
        backend = self._view_backend(view)
        with suppress(backend.Unavailable):
            return await backend.set(
                key,
                view.response_to_bytes(response),
                want_seconds(timeout if timeout is not None else self.timeout),
            )

    def can_cache_request(self, request: Request) -> bool:
        return True

    def can_cache_response(self,
                           request: Request,
                           response: Response) -> bool:
        return response.status == 200

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


def iri_to_uri(iri: str) -> str:
    """Convert IRI to URI."""
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

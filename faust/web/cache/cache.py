"""Cache interface."""
import hashlib

from contextlib import suppress
from functools import wraps
from typing import (
    Any,
    Callable,
    ClassVar,
    Mapping,
    Optional,
    Type,
    Union,
    cast,
)
from urllib.parse import quote

from mode.utils.compat import want_bytes
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
                 include_headers: bool = False,
                 key_prefix: str = None,
                 backend: Union[Type[CacheBackendT], str] = None,
                 **kwargs: Any) -> None:
        self.timeout = timeout
        self.include_headers = include_headers
        self.key_prefix = key_prefix or ''
        self.backend = backend

    def view(self,
             timeout: Seconds = None,
             include_headers: bool = False,
             key_prefix: str = None,
             **kwargs: Any) -> Callable[[Callable], Callable]:
        """Decorate view to be cached."""

        def _inner(fun: Callable) -> Callable:
            @wraps(fun)
            async def cached(view: View, request: Request, *args: Any,
                             **kwargs: Any) -> Response:
                key: Optional[str] = None
                is_head = request.method.upper() == 'HEAD'
                if self.can_cache_request(request):
                    key = self.key_for_request(request, key_prefix, 'GET',
                                               include_headers)

                    response = await self.get_view(key, view)
                    if response is not None:
                        logger.info('Found cached response for %r', key)
                        return response
                    if is_head:
                        response = await self.get_view(
                            self.key_for_request(request, key_prefix, 'HEAD',
                                                 include_headers),
                            view,
                        )
                        if response is not None:
                            logger.info('Found cached HEAD response for %r',
                                        key)
                            return response

                logger.info('No cache found for %r', key)
                res = await fun(view, request, *args, **kwargs)

                if key is not None and self.can_cache_response(request, res):
                    logger.info('Saving cache for key %r', key)
                    if is_head:
                        key = self.key_for_request(request, key_prefix, 'HEAD',
                                                   include_headers)
                    await self.set_view(key, view, res, timeout)
                return res

            return cached

        return _inner

    async def get_view(self,
                       key: str, view: View) -> Optional[Response]:
        """Get cached value for HTTP view request."""
        backend = self._view_backend(view)
        with suppress(backend.Unavailable):
            payload = await backend.get(key)
            if payload is not None:
                return view.bytes_to_response(payload)
        return None

    def _view_backend(self, view: View) -> CacheBackendT:
        return cast(CacheBackendT, self.backend or view.app.cache)

    async def set_view(self, key: str, view: View, response: Response,
                       timeout: Seconds = None) -> None:
        """Set cached value for HTTP view request."""
        backend = self._view_backend(view)
        _timeout = timeout if timeout is not None else self.timeout
        with suppress(backend.Unavailable):
            return await backend.set(
                key,
                view.response_to_bytes(response),
                want_seconds(_timeout) if _timeout is not None else None,
            )

    def can_cache_request(self, request: Request) -> bool:
        """Return :const:`True` if we can cache this type of HTTP request."""
        return True

    def can_cache_response(self, request: Request, response: Response) -> bool:
        """Return :const:`True` for HTTP status codes we CAN cache."""
        return response.status == 200

    def key_for_request(self,
                        request: Request,
                        prefix: str = None,
                        method: str = None,
                        include_headers: bool = False) -> str:
        """Return a cache key created from web request."""
        actual_method: str = request.method if method is None else method
        headers = request.headers if include_headers else {}
        if prefix is None:
            prefix = self.key_prefix
        return self.build_key(request, actual_method, prefix, headers)

    def build_key(self, request: Request, method: str, prefix: str,
                  headers: Mapping[str, str]) -> str:
        """Build cache key from web request and environment."""
        context = hashlib.md5(b''.join(
            want_bytes(k) + want_bytes(v)
            for k, v in headers.items())).hexdigest()
        url = hashlib.md5(iri_to_uri(str(
            request.url)).encode('ascii')).hexdigest()
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

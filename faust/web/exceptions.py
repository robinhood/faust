import http
from typing import Any, Dict, cast
from faust.exceptions import FaustError

__all__ = [
    'WebError',
    'ServerError',
    'ValidationError',
    'ParseError',
    'AuthenticationFailed',
    'NotAuthenticated',
    'PermissionDenied',
    'NotFound',
    'MethodNotAllowed',
    'NotAcceptable',
    'UnsupportedMediaType',
    'Throttled',
]


class WebError(FaustError):
    code: int = cast(int, None)
    detail: str = 'Default not set on class'
    extra_context: Dict

    def __init__(self, detail: str = None, *,
                 code: int = None,
                 **extra_context: Any) -> None:
        if detail:
            self.detail = detail
        if code:
            self.code = code
        self.extra_context = extra_context
        super().__init__(self, detail, code, extra_context)


class ServerError(WebError):
    code = http.HTTPStatus.INTERNAL_SERVER_ERROR
    detail = 'Internal server error.'


class ValidationError(WebError):
    code = http.HTTPStatus.BAD_REQUEST
    detail = 'Invalid input.'


class ParseError(WebError):
    code = http.HTTPStatus.BAD_REQUEST
    detail = 'Malformed request.'


class AuthenticationFailed(WebError):
    code = http.HTTPStatus.UNAUTHORIZED
    detail = 'Incorrect authentication credentials'


class NotAuthenticated(WebError):
    code = http.HTTPStatus.UNAUTHORIZED
    detail = 'Authentication credentials were not provided.'


class PermissionDenied(WebError):
    code = http.HTTPStatus.FORBIDDEN
    detail = 'You do not have permission to perform this action.'


class NotFound(WebError):
    code = http.HTTPStatus.NOT_FOUND
    detauil = 'Not found.'


class MethodNotAllowed(WebError):
    code = http.HTTPStatus.METHOD_NOT_ALLOWED
    detail = 'Method not allowed.'


class NotAcceptable(WebError):
    code = http.HTTPStatus.NOT_ACCEPTABLE
    detail = 'Could not satisfy the request Accept header.'


class UnsupportedMediaType(WebError):
    code = http.HTTPStatus.UNSUPPORTED_MEDIA_TYPE
    detail = 'Unsupported media type in request.'


class Throttled(WebError):
    code = http.HTTPStatus.TOO_MANY_REQUESTS
    detail = 'Request was throttled.'

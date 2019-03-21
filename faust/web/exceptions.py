"""HTTP and related errors."""
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
    """Web related error.

    Web related errors will have a status :attr:`code`,
    and a :attr:`detail` for the human readable error string.

    It may also keep :attr:`extra_context`.
    """

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
    """Internal Server Error (500)."""

    code = http.HTTPStatus.INTERNAL_SERVER_ERROR
    detail = 'Internal server error.'


class ValidationError(WebError):
    """Invalid input in POST data (400)."""

    code = http.HTTPStatus.BAD_REQUEST
    detail = 'Invalid input.'


class ParseError(WebError):
    """Malformed request (400)."""

    code = http.HTTPStatus.BAD_REQUEST
    detail = 'Malformed request.'


class AuthenticationFailed(WebError):
    """Incorrect authentication credentials (401)."""

    code = http.HTTPStatus.UNAUTHORIZED
    detail = 'Incorrect authentication credentials'


class NotAuthenticated(WebError):
    """Authentication credentials were not provided (401)."""

    code = http.HTTPStatus.UNAUTHORIZED
    detail = 'Authentication credentials were not provided.'


class PermissionDenied(WebError):
    """No permission to perform action (403)."""

    code = http.HTTPStatus.FORBIDDEN
    detail = 'You do not have permission to perform this action.'


class NotFound(WebError):
    """Resource not found (404)."""

    code = http.HTTPStatus.NOT_FOUND
    detauil = 'Not found.'


class MethodNotAllowed(WebError):
    """HTTP Method not allowed (405)."""

    code = http.HTTPStatus.METHOD_NOT_ALLOWED
    detail = 'Method not allowed.'


class NotAcceptable(WebError):
    """Not able to satisfy the request ``Accept`` header (406)."""

    code = http.HTTPStatus.NOT_ACCEPTABLE
    detail = 'Could not satisfy the request Accept header.'


class UnsupportedMediaType(WebError):
    """Request contains unsupported media type (415)."""

    code = http.HTTPStatus.UNSUPPORTED_MEDIA_TYPE
    detail = 'Unsupported media type in request.'


class Throttled(WebError):
    """Client is sending too many requests to server (429)."""

    code = http.HTTPStatus.TOO_MANY_REQUESTS
    detail = 'Request was throttled.'


class ServiceUnavailable(WebError):
    """Service is temporarily unavailable (503)."""

    code = http.HTTPStatus.SERVICE_UNAVAILABLE
    detail = 'Service unavailable. Try again later.'

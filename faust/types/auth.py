import ssl
from enum import Enum
from typing import Any, Optional, Union
from faust.exceptions import ImproperlyConfigured

__all__ = [
    'AuthProtocol',
    'CredentialsT',
    'SASLCredentials',
    'SSLCredentials',
    'CredentialsArg',
    'to_credentials',
]


class AuthProtocol(Enum):
    SSL = 'SSL'
    PLAINTEXT = 'PLAINTEXT'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'


AUTH_PROTOCOLS_SSL = {AuthProtocol.SSL, AuthProtocol.SASL_SSL}
AUTH_PROTOCOLS_SASL = {AuthProtocol.SASL_PLAINTEXT, AuthProtocol.SASL_SSL}


class CredentialsT:
    protocol: AuthProtocol


class SASLCredentials(CredentialsT):
    protocol = AuthProtocol.SASL_PLAINTEXT
    mechanism: str = 'PLAIN'

    username: Optional[str]
    password: Optional[str]

    def __init__(self, *, username: str = None, password: str = None) -> None:
        self.username = username
        self.password = password

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: username={self.username}>'


class SSLCredentials(CredentialsT):
    protocol = AuthProtocol.SSL
    context: ssl.SSLContext

    def __init__(self, context: ssl.SSLContext = None, *,
                 purpose: Any = None,
                 cafile: Optional[str] = None,
                 capath: Optional[str] = None,
                 cadata: Optional[str] = None) -> None:
        if context is None:
            context = ssl.create_default_context(
                purpose=purpose,
                cafile=cafile,
                capath=capath,
                cadata=cadata,
            )
        self.context = context

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: context={self.context}>'


CredentialsArg = Union[CredentialsT, ssl.SSLContext]


def to_credentials(obj: CredentialsArg = None) -> Optional[CredentialsT]:
    if obj is not None:
        if isinstance(obj, ssl.SSLContext):
            return SSLCredentials(obj)
        if isinstance(obj, CredentialsT):
            return obj
        raise ImproperlyConfigured(
            f'Unknown credentials type {type(obj)}: {obj}')
    return None

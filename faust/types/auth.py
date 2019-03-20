import ssl
from enum import Enum
from typing import Optional, Union

__all__ = [
    'AUTH_PROTOCOLS_SSL',
    'AUTH_PROTOCOLS_SASL',
    'AuthProtocol',
    'CredentialsArg',
    'CredentialsT',
    'SASLMechanism',
    'to_credentials',
]


class AuthProtocol(Enum):
    SSL = 'SSL'
    PLAINTEXT = 'PLAINTEXT'
    SASL_PLAINTEXT = 'SASL_PLAINTEXT'
    SASL_SSL = 'SASL_SSL'


class SASLMechanism(Enum):
    PLAIN = 'PLAIN'
    GSSAPI = 'GSSAPI'


AUTH_PROTOCOLS_SSL = {AuthProtocol.SSL, AuthProtocol.SASL_SSL}
AUTH_PROTOCOLS_SASL = {AuthProtocol.SASL_PLAINTEXT, AuthProtocol.SASL_SSL}


class CredentialsT:
    protocol: AuthProtocol


CredentialsArg = Union[CredentialsT, ssl.SSLContext]


def to_credentials(obj: CredentialsArg = None) -> Optional[CredentialsT]:
    if obj is not None:
        from faust.auth import SSLCredentials  # XXX :(
        if isinstance(obj, ssl.SSLContext):
            return SSLCredentials(obj)
        if isinstance(obj, CredentialsT):
            return obj
        from faust.exceptions import ImproperlyConfigured
        raise ImproperlyConfigured(
            f'Unknown credentials type {type(obj)}: {obj}')
    return None

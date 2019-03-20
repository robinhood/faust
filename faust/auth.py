import ssl
from typing import Any, Optional, Union
from faust.types.auth import AuthProtocol, CredentialsT, SASLMechanism

__all__ = [
    'Credentials',
    'SASLCredentials',
    'GSSAPICredentials',
    'SSLCredentials',
]


class Credentials(CredentialsT):
    ...


class SASLCredentials(Credentials):
    protocol = AuthProtocol.SASL_PLAINTEXT
    mechanism: SASLMechanism = SASLMechanism.PLAIN

    username: Optional[str]
    password: Optional[str]

    def __init__(self, *,
                 username: str = None,
                 password: str = None,
                 mechanism: Union[str, SASLMechanism] = None) -> None:
        self.username = username
        self.password = password
        if mechanism is not None:
            self.mechanism = SASLMechanism(mechanism)

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: username={self.username}>'


class GSSAPICredentials(Credentials):
    protocol = AuthProtocol.SASL_PLAINTEXT
    mechanism: str = SASLMechanism.GSSAPI

    def __init__(self, *,
                 kerberos_service_name: str = 'kafka',
                 kerberos_domain_name: str = None,
                 mechanism: Union[str, SASLMechanism] = None) -> None:
        self.kerberos_service_name = kerberos_service_name
        self.kerberos_domain_name = kerberos_domain_name
        if mechanism is not None:
            self.mechanism = SASLMechanism(mechanism)

    def __repr__(self) -> str:
        return '<{0}: kerberos service={1!r} domain={2!r}'.format(
            type(self).__name__,
            self.kerberos_service_name,
            self.kerberos_domain_name,
        )


class SSLCredentials(Credentials):
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

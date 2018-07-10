"""Kafka protocol extensions."""
# pragma: no cover
import abc
from typing import Type
from rhkafka.protocol.struct import Struct
from rhkafka.protocol.types import Schema


class Response(Struct, metaclass=abc.ABCMeta):
    """API Response."""

    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request/response."""
        pass

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request/response version."""
        pass

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """Return instance of Schema() representing the response structure."""
        pass


class Request(Struct, metaclass=abc.ABCMeta):
    """API Request."""

    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request."""
        pass

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request version."""
        pass

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """Return instance of Schema() representing the request structure."""
        pass

    @property
    @abc.abstractmethod
    def RESPONSE_TYPE(self) -> Type[Response]:
        """Response class associated with the api request."""
        pass

    def expect_response(self) -> bool:
        """Return True if request type does not always return response."""
        return True

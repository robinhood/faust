import abc
from typing import Type
from kafka.protocol.struct import Struct
from kafka.protocol.types import Schema


class Response(Struct, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request/response"""
        pass

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request/response version"""
        pass

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the response structure"""
        pass


class Request(Struct, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request"""
        pass

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request version"""
        pass

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the request structure"""
        pass

    @property
    @abc.abstractmethod
    def RESPONSE_TYPE(self) -> Type[Response]:
        """The Response class associated with the api request"""
        pass

    def expect_response(self) -> bool:
        """Override this method if an api request does not always
        generate a response"""
        return True

from __future__ import absolute_import

import abc

from kafka.protocol.struct import Struct
from kafka.protocol.types import Schema


class Response(Struct):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def API_KEY(self) -> int:
        """Integer identifier for api request/response"""
        pass

    @abc.abstractproperty
    def API_VERSION(self) -> int:
        """Integer of api request/response version"""
        pass

    @abc.abstractproperty
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the response structure"""
        pass


class Request(Struct):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def API_KEY(self) -> int:
        """Integer identifier for api request"""
        pass

    @abc.abstractproperty
    def API_VERSION(self) -> int:
        """Integer of api request version"""
        pass

    @abc.abstractproperty
    def SCHEMA(self) -> Schema:
        """An instance of Schema() representing the request structure"""
        pass

    @abc.abstractproperty
    def RESPONSE_TYPE(self) -> Response:
        """The Response class associated with the api request"""
        pass

    def expect_response(self) -> bool:
        """Override this method if an api request does not always
        generate a response"""
        return True

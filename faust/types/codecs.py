import abc
from typing import Any, Optional, Union

__all__ = ['CodecArg', 'CodecT']

# `serializer` argument can be str or Codec instance.
CodecArg = Optional[Union['CodecT', str]]


class CodecT(metaclass=abc.ABCMeta):
    """Abstract type for an encoder/decoder.

    See Also:
        :class:`faust.codecs.Codec`.
    """

    @abc.abstractmethod
    def dumps(self, obj: Any) -> bytes:
        ...

    @abc.abstractmethod
    def loads(self, s: bytes) -> Any:
        ...

    @abc.abstractmethod
    def clone(self, *children: 'CodecT') -> 'CodecT':
        ...

    @abc.abstractmethod
    def __or__(self, other: Any) -> Any:
        ...

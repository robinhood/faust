import abc
from typing import Any, Optional, Tuple, Union

__all__ = ['CodecArg', 'CodecT']


class CodecT(metaclass=abc.ABCMeta):
    """Abstract type for an encoder/decoder.

    See Also:
        :class:`faust.serializers.codecs.Codec`.
    """

    @abc.abstractmethod
    def __init__(self, children: Tuple['CodecT', ...] = None,
                 **kwargs: Any) -> None:
        ...

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


# `serializer` argument can be str or Codec instance.
CodecArg = Optional[Union[CodecT, str]]

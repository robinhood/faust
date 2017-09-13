import abc
import typing
from typing import Any, Callable, Iterable, MutableMapping, Optional
from .channels import EventT
from .codecs import CodecArg
from .tuples import TopicPartition
from ..utils.types.services import ServiceT

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...    # noqa

__all__ = ['StoreT']


class StoreT(ServiceT, MutableMapping):

    url: str
    app: AppT
    table_name: str
    key_serializer: CodecArg
    value_serializer: CodecArg

    @abc.abstractmethod
    def __init__(self, url: str, app: AppT,
                 *,
                 table_name: str = '',
                 key_serializer: CodecArg = '',
                 value_serializer: CodecArg = '',
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def persisted_offset(self, tp: TopicPartition) -> Optional[int]:
        ...

    @abc.abstractmethod
    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        ...

    @abc.abstractmethod
    def reset_state(self) -> None:
        ...

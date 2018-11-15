import abc
import typing
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    Set,
    TypeVar,
    Union,
)

from mode import ServiceT
from mode.utils.collections import FastUserDict
from yarl import URL

from .codecs import CodecArg
from .events import EventT
from .tuples import TP

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelArg
    from .tables import CollectionT
else:
    class AppT: ...  # noqa
    class ModelArg: ...  # noqa
    class CollectionT: ...  # noqa

__all__ = ['StoreT']

KT = TypeVar('KT')
VT = TypeVar('VT')


class StoreT(ServiceT, FastUserDict[KT, VT]):

    url: URL
    app: AppT
    table: CollectionT
    table_name: str
    key_type: Optional[ModelArg]
    value_type: Optional[ModelArg]
    key_serializer: CodecArg
    value_serializer: CodecArg

    @abc.abstractmethod
    def __init__(self,
                 url: Union[str, URL],
                 app: AppT,
                 table: CollectionT,
                 *,
                 table_name: str = '',
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 key_serializer: CodecArg = '',
                 value_serializer: CodecArg = '',
                 **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def persisted_offset(self, tp: TP) -> Optional[int]:
        ...

    @abc.abstractmethod
    def set_persisted_offset(self, tp: TP, offset: int) -> None:
        ...

    @abc.abstractmethod
    async def need_active_standby_for(self, tp: TP) -> bool:
        ...

    @abc.abstractmethod
    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], KT],
                              to_value: Callable[[Any], VT]) -> None:
        ...

    @abc.abstractmethod
    def reset_state(self) -> None:
        ...

    @abc.abstractmethod
    async def on_rebalance(self,
                           table: CollectionT,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        ...

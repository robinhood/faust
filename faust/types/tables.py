from typing import Any, ClassVar, MutableMapping, Type
from .streams import StreamT
from .tuples import Topic
from .windows import WindowT


class TableT(StreamT, MutableMapping):
    StateStore: ClassVar[Type] = None

    table_name: str
    changelog_topic: Topic
    default: Any  # noqa: E704
    window: WindowT = None

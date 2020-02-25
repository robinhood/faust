import typing
from enum import Enum
from typing import Any, Callable, Dict, Generic, Optional, Type, TypeVar, cast

__all__ = [
    'SectionType',
    'Section',
    'SECTIONS',
    'Agent',
    'Broker',
    'Common',
    'Extension',
    'Producer',
    'RPC',
    'Serialization',
    'Stream',
    'Table',
    'Topic',
    'WebServer',
    'Worker',
]

IT = TypeVar('IT')   # Input type.
OT = TypeVar('OT')   # Output type.

if typing.TYPE_CHECKING:
    from .params import Param as _Param
else:
    class _Param(Generic[IT, OT]): ...  # noqa: E701


class SectionType(Enum):
    AGENT = 'AGENT'
    BROKER = 'BROKER'
    COMMON = 'COMMON'
    CONSUMER = 'CONSUMER'
    EXTENSION = 'EXTENSION'
    PRODUCER = 'PRODUCER'
    RPC = 'RPC'
    SERIALIZATION = 'SERIALIZATION'
    STREAM = 'STREAM'
    TABLE = 'TABLE'
    TOPIC = 'TOPIC'
    WEB_SERVER = 'WEB_SERVER'
    WORKER = 'WORKER'


class Section:
    """Configuration section."""
    type: SectionType
    title: str
    refid: str
    content: Optional[str]

    def __init__(self, type: SectionType, title: str, refid: str, *,
                 content: str = None) -> None:
        self.type = type
        self.title = title
        self.refid = refid
        self.content = content
        SECTIONS[self.type] = self

    def setting(self, param: Type[_Param[IT, OT]],
                **kwargs: Any) -> Callable[[Callable], OT]:
        """Decorate to define new setting in this section."""
        def inner(fun: Callable) -> OT:
            setting = param(
                name=fun.__name__,
                section=self,
                help=fun.__doc__,
                **kwargs,
            )
            return cast(OT, setting)
        return inner

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self.type}>'


SECTIONS: Dict[SectionType, Section] = {}

Common = Section(
    type=SectionType.COMMON,
    title='Commonly Used Settings',
    refid='settings-common',
)

Serialization = Section(
    type=SectionType.SERIALIZATION,
    title='Serialization Settings',
    refid='settings-serialization',
)

Topic = Section(
    type=SectionType.TOPIC,
    title='Topic Settings',
    refid='settings-topic',
)

Broker = Section(
    type=SectionType.BROKER,
    title='Advanced Broker Settings',
    refid='settings-broker',
)

Consumer = Section(
    type=SectionType.CONSUMER,
    title='Advanced Consumer Settings',
    refid='settings-consumer',
)

Producer = Section(
    type=SectionType.PRODUCER,
    title='Advanced Producer Settings',
    refid='settings-producer',
)

Table = Section(
    type=SectionType.TABLE,
    title='Advanced Table Settings',
    refid='settings-table',
)

Stream = Section(
    type=SectionType.STREAM,
    title='Advanced Stream Settings',
    refid='setting-stream',
)

Worker = Section(
    type=SectionType.WORKER,
    title='Advanced Worker Settings',
    refid='settings-worker',
)

WebServer = Section(
    type=SectionType.WEB_SERVER,
    title='Advanced Web Server Settings',
    refid='setting-web',
)

Agent = Section(
    type=SectionType.AGENT,
    title='Advanced Agent Settings',
    refid='settings-agent',
)

RPC = Section(
    type=SectionType.RPC,
    title='Agent RPC Settings',
    refid='settings-rpc',
)

Extension = Section(
    type=SectionType.EXTENSION,
    title='Extension Settings',
    refid='settings-extending',
)

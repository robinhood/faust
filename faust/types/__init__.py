"""Abstract types for static typing."""
from mode import ServiceT

from .agents import AgentManagerT, AgentT
from .app import AppT
from .channels import ChannelT
from .codecs import CodecArg, CodecT
from .core import HeadersArg, K, V
from .enums import ProcessingGuarantee
from .events import EventT
from .fixups import FixupT
from .joins import JoinT
from .models import FieldDescriptorT, ModelArg, ModelOptions, ModelT
from .sensors import SensorT
from .serializers import RegistryT
from .stores import StoreT
from .streams import (
    JoinableT,
    Processor,
    StreamT,
)
from .tables import CollectionT, TableT
from .topics import TopicT
from .transports import (
    ConsumerCallback,
    ConsumerT,
    ProducerT,
    TransactionManagerT,
    TransportT,
)
from .tuples import (
    ConsumerMessage,
    FutureMessage,
    Message,
    MessageSentCallback,
    PendingMessage,
    RecordMetadata,
    TP,
)
from .windows import WindowRange, WindowT

__all__ = [
    # :pypi:`mode`
    'ServiceT',

    # types.agents
    'AgentManagerT',
    'AgentT',

    # types.app
    'AppT',

    # types.codecs
    'CodecArg',
    'CodecT',

    # types.core
    'HeadersArg',
    'K',
    'V',

    # types.fixups
    'FixupT',

    # types.joins
    'JoinT',

    # types.models
    'FieldDescriptorT',
    'ModelArg',
    'ModelOptions',
    'ModelT',

    # types.sensors
    'SensorT',

    # types.serializers
    'RegistryT',

    # types.stores
    'StoreT',

    # types.streams
    'JoinableT',
    'Processor',
    'StreamT',

    # types.tables
    'CollectionT',
    'TableT',

    # types.enums
    'ProcessingGuarantee',

    # types.topics
    'ChannelT',
    'EventT',
    'TopicT',

    # types.transports
    'ConsumerCallback',
    'ConsumerT',
    'ProducerT',
    'TransactionManagerT',
    'TransportT',

    # types.tuples
    'ConsumerMessage',
    'FutureMessage',
    'Message',
    'MessageSentCallback',
    'PendingMessage',
    'RecordMetadata',
    'TP',

    # types.windows
    'WindowRange',
    'WindowT',
]

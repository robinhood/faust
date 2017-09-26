"""Abstract types for static typing."""
from .actors import ActorT
from .app import AppT
from .channels import ChannelT, EventT
from .codecs import CodecArg, CodecT
from .core import K, V
from .joins import JoinT
from .models import FieldDescriptorT, ModelArg, ModelOptions, ModelT
from .sensors import SensorT
from .serializers import RegistryT
from .stores import StoreT
from .streams import (
    JoinableT,
    Processor,
    StreamCoroutine,
    StreamT,
)
from .tables import CollectionT, SetT, TableT
from .topics import TopicT
from .transports import (
    ConsumerCallback,
    ConsumerT,
    ProducerT,
    TransportT,
)
from .tuples import (
    FutureMessage, Message, MessageSentCallback, PendingMessage,
    RecordMetadata, TopicPartition,
)
from .windows import WindowRange, WindowT
from ..utils.services import ServiceT

__all__ = [
    # types.actors
    'ActorT',

    # types.app
    'AppT',

    # types.codecs
    'CodecArg', 'CodecT',

    # types.core
    'K', 'V',

    # types.joins
    'JoinT',

    # types.models
    'FieldDescriptorT', 'ModelArg', 'ModelOptions', 'ModelT',

    # types.sensors
    'SensorT',

    # types.serializers
    'RegistryT',

    # utils.services
    'ServiceT',

    # types.stores
    'StoreT',

    # types.streams
    'JoinableT', 'Processor', 'StreamCoroutine', 'StreamT',

    # types.tables
    'CollectionT', 'SetT', 'TableT',

    # types.topics
    'ChannelT', 'EventT', 'TopicT',

    # types.transports
    'ConsumerCallback', 'ConsumerT', 'ProducerT', 'TransportT',

    # types.tuples
    'FutureMessage', 'Message', 'MessageSentCallback', 'PendingMessage',
    'RecordMetadata', 'TopicPartition',

    # types.windows
    'WindowRange', 'WindowT',
]

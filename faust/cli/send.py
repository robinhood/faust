"""Program ``faust send`` used to send events to agents and topics."""
import asyncio
import random
from typing import Any
from faust.types import CodecArg, K, RecordMetadata, V
from .base import AppCommand, argument, option

__all__ = ['send']


class send(AppCommand):
    """Send message to agent/topic."""

    topic: Any
    key: K
    key_serializer: CodecArg
    value: V
    value_serializer: CodecArg
    repeat: int
    min_latency: float
    max_latency: float

    options = [
        option('--key-type', '-K',
               help='Name of model to serialize key into.'),
        option('--key-serializer',
               help='Override default serializer for key.'),
        option('--value-type', '-V',
               help='Name of model to serialize value into.'),
        option('--value-serializer',
               help='Override default serializer for value.'),
        option('--key', '-k',
               help='String value for key (use json if model).'),
        option('--partition', type=int,
               help='Specific partition to send to.'),
        option('--repeat', '-r', type=int, default=1,
               help='Send message n times.'),
        option('--min-latency', type=float, default=0.0,
               help='Minimum delay between sending.'),
        option('--max-latency', type=float, default=0.0,
               help='Maximum delay between sending.'),

        argument('entity'),
        argument('value', default=None, required=False),
    ]

    async def run(self,
                  entity: str,
                  value: str,
                  *args: Any,
                  key: str = None,
                  key_type: str = None,
                  key_serializer: str = None,
                  value_type: str = None,
                  value_serializer: str = None,
                  partition: int = 1,
                  timestamp: float = None,
                  repeat: int = 1,
                  min_latency: float = 0.0,
                  max_latency: float = 0.0,
                  **kwargs: Any) -> Any:
        if key is not None:
            key = self.to_key(key_type, key)
        if value is not None:
            value = self.to_value(value_type, value)
        topic = self.to_topic(entity)
        for i in range(repeat):
            self.carp(f'k={key!r} v={value!r} -> {topic!r}...')
            fut_send_complete = await topic.send(
                key=key,
                value=value,
                partition=partition,
                timestamp=timestamp,
                key_serializer=key_serializer,
                value_serializer=value_serializer,
            )
            meta: RecordMetadata = await fut_send_complete
            self.say(self.dumps(meta._asdict()))
            if i and max_latency:
                await asyncio.sleep(random.uniform(min_latency, max_latency))
        await self.app.producer.stop()

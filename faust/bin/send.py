import asyncio
import random
from typing import Any
import click
from .base import AppCommand
from ..types import CodecArg, K, V

__all__ = ['send']


class send(AppCommand):
    """Send message to actor/topic."""

    topic: Any
    key: K
    key_serializer: CodecArg
    value: V
    value_serializer: CodecArg
    repeat: int
    min_latency: float
    max_latency: float

    options = [
        click.option('--key-type', '-K',
                     help='Name of model to serialize key into'),
        click.option('--key-serializer',
                     help='Override default serializer for key.'),
        click.option('--value-type', '-V',
                     help='Name of model to serialize value into'),
        click.option('--value-serializer',
                     help='Override default serializer for value.'),
        click.option('--key', '-k',
                     help='Key value'),
        click.option('--partition', type=int,
                     help='Specific partition to send to'),
        click.option('--repeat', '-r', type=int, default=1,
                     help='Send message n times'),
        click.option('--latency', '-l', default='0,0',
                     help='Delay between sending as min,max or max'),
        click.argument('entity'),
        click.argument('value', default=None, required=False),
    ]

    def init_options(self,
                     entity: str,
                     value: str,
                     *args: Any,
                     key: str = None,
                     key_type: str = None,
                     value_type: str = None,
                     partition: int = None,
                     repeat: int = 1,
                     latency: str = '0.0',
                     **kwargs: Any) -> None:
        self.key = self.to_key(key_type, key)
        self.value = self.to_value(value_type, value)
        self.topic = self.to_topic(entity)
        self.partition = partition
        self.repeat = repeat
        if ',' in latency:
            min_, _, max_ = latency.partition(',')
        else:
            min_, max_ = '0.0', latency
        self.min_latency, self.max_latency = float(min_), float(max_)
        super().init_options(*args, **kwargs)

    async def run(self) -> Any:
        for i in range(self.repeat):
            self.carp(f'k={self.key!r} v={self.value!r} -> {self.topic!r}...')
            fut = await self.topic.send(
                key=self.key,
                value=self.value,
                partition=self.partition,
                key_serializer=self.key_serializer,
                value_serializer=self.value_serializer,
            )
            res = await fut
            self.say(self.dumps(res._asdict()))
            if i and self.max_latency:
                await asyncio.sleep(
                    random.uniform(self.min_latency, self.max_latency))
        await self.app.producer.stop()

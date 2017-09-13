from typing import Any
import click
from .base import AppCommand, cli
from ..types import CodecArg, K, V

__all__ = ['send']


@cli.command(help='Send message to actor/topic')
@click.pass_context
@click.option('--key-type', '-K',
              help='Name of model to serialize key into')
@click.option('--key-serializer',
              help='Override default serializer for key.')
@click.option('--value-type', '-V',
              help='Name of model to serialize value into')
@click.option('--value-serializer',
              help='Override default serializer for value.')
@click.option('--key', '-k',
              help='Key value')
@click.option('--partition', type=int, help='Specific partition to send to')
@click.argument('entity')
@click.argument('value', default=None, required=False)
class send(AppCommand):

    topic: Any
    key_serializer: CodecArg
    value_serializer: CodecArg

    key: K
    value: V

    def __init__(self,
                 ctx: click.Context,
                 entity: str,
                 value: str,
                 key: str = None,
                 key_type: str = None,
                 key_serializer: CodecArg = None,
                 value_type: str = None,
                 value_serializer: CodecArg = None,
                 partition: int = None) -> None:
        super().__init__(
            ctx,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
        )
        self.key = self.to_key(key_type, key)
        self.value = self.to_value(value_type, value)
        self.topic = self.to_topic(entity)
        self.partition = partition
        self()

    async def run(self) -> Any:
        self.carp(f'Send k={self.key!r} v={self.value!r} -> {self.topic!r}...')
        fut = await self.topic.send(
            key=self.key,
            value=self.value,
            partition=self.partition,
            key_serializer=self.key_serializer,
            value_serializer=self.value_serializer,
        )
        res = await fut
        self.say(self.dumps(res._asdict()))
        await self.app.producer.stop()

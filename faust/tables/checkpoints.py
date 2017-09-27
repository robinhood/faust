import json
import os
from contextlib import suppress
from typing import Any, MutableMapping, Optional
from mode import Service
from ..types import AppT, TopicPartition
from ..types.tables import CheckpointManagerT

__all__ = ['CheckpointManager']


class CheckpointManager(CheckpointManagerT, Service):

    _offsets: MutableMapping[TopicPartition, int]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self._offsets = {}
        Service.__init__(self, **kwargs)

    async def on_start(self) -> None:
        with suppress(FileNotFoundError):
            with open(self.app.checkpoint_path, 'r') as fh:
                data = json.load(fh)
            self._offsets.update((
                (self._get_tp(k), int(v))
                for k, v in data.items()
            ))

    async def on_stop(self) -> None:
        with open(self.app.checkpoint_path, 'w') as fh:
            json.dump({
                f'{tp.topic}\0{tp.partition}': v
                for tp, v in self._offsets.items()
            }, fh)

    def reset_state(self) -> None:
        with suppress(FileNotFoundError):
            os.remove(self.app.checkpoint_path)

    @classmethod
    def _get_tp(cls, key: str) -> TopicPartition:
        topic, partition = key.split('\0')
        return TopicPartition(topic, int(partition))

    def get_offset(self, tp: TopicPartition) -> Optional[int]:
        return self._offsets.get(tp)

    def set_offset(self, tp: TopicPartition, offset: int) -> None:
        self._offsets[tp] = offset

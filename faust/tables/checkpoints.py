import json
import os
from contextlib import suppress
from typing import Any, Iterator, Mapping, MutableMapping, Optional, Tuple
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
        self._update_offsets_from_file()

    def _update_offsets_from_file(self) -> None:
        self._offsets.update(self._read_checkpoints())

    def _read_checkpoints(self) -> Iterator[Tuple[TopicPartition, int]]:
        data: Mapping = None
        with suppress(FileNotFoundError):
            with open(self.app.checkpoint_path, 'r') as fh:
                data = json.load(fh)
            for k, v in data.items():
                yield self._get_tp(k), int(v)

    async def on_stop(self) -> None:
        await self.sync()

    async def sync(self) -> None:
        self._write_offsets_to_file(self._offsets)

    def _write_offsets_to_file(
            self, offsets: Mapping[TopicPartition, int]) -> None:
        with open(self.app.checkpoint_path, 'w') as fh:
            json.dump(self._as_encoded_offsets(offsets), fh)

    def _as_encoded_offsets(
            self, offsets: Mapping[TopicPartition, int]) -> Mapping[str, int]:
        return {
            f'{tp.topic}\0{tp.partition}': v
            for tp, v in offsets.items()
        }

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

from typing import Any
from ..types import AppT, TopicPartition, TopicT
from ..types.assignor import LeaderAssignorT
from ..utils.objects import cached_property
from ..utils.services import Service


class LeaderAssignor(Service, LeaderAssignorT):

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app

    async def on_start(self) -> None:
        await self._leader_topic.maybe_declare()
        self.app.topics.add(self._leader_topic)

    @cached_property
    def _leader_topic(self) -> TopicT:
        return self.app.topic(
            self._leader_topic_name,
            partitions=1,
            acks=False,
        )

    @cached_property
    def _leader_topic_name(self) -> str:
        return f'{self.app.id}-__assignor-__leader'

    @cached_property
    def _leader_tp(self) -> TopicPartition:
        return TopicPartition(self._leader_topic_name, 0)

    def is_leader(self) -> bool:
        return self._leader_tp in self.app.consumer.assignment()

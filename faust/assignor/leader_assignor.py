"""Leader assignor."""
from typing import Any
from mode import Service
from mode.utils.objects import cached_property
from faust.types import AppT, TP, TopicT
from faust.types.assignor import LeaderAssignorT

__all__ = ['LeaderAssignor']


class LeaderAssignor(Service, LeaderAssignorT):
    """Leader assignor, ensures election of a leader."""

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app

    async def on_start(self) -> None:
        leader_topic = self._leader_topic
        await leader_topic.maybe_declare()
        self.app.topics.add(leader_topic)
        self.app.consumer.randomly_assigned_topics.add(
            leader_topic.get_topic_name())

    @cached_property
    def _leader_topic(self) -> TopicT:
        return self.app.topic(
            self._leader_topic_name,
            partitions=1,
            acks=False,
            internal=True,
        )

    @cached_property
    def _leader_topic_name(self) -> str:
        return f'{self.app.conf.id}-__assignor-__leader'

    @cached_property
    def _leader_tp(self) -> TP:
        return TP(self._leader_topic_name, 0)

    def is_leader(self) -> bool:
        return self._leader_tp in self.app.consumer.assignment()

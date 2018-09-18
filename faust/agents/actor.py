from typing import Any, AsyncIterator, Set
from mode import Service

from faust.types import StreamT, TP
from faust.types.agents import (
    ActorT,
    AgentT,
    AsyncIterableActorT,
    AwaitableActorT,
    _T,
)
__all__ = ['Actor', 'AsyncIterableActor', 'AwaitableActor']


class Actor(ActorT, Service):
    """An actor is a specific agent instance."""
    mundane_level = 'debug'

    # Agent will start n * concurrency actors.

    def __init__(self,
                 agent: AgentT,
                 stream: StreamT,
                 it: _T,
                 index: int = None,
                 active_partitions: Set[TP] = None,
                 **kwargs: Any) -> None:
        self.agent = agent
        self.stream = stream
        self.it = it
        self.index = index
        self.active_partitions = active_partitions
        self.actor_task = None
        Service.__init__(self, **kwargs)

    async def on_start(self) -> None:
        assert self.actor_task
        self.add_future(self.actor_task)

    async def on_stop(self) -> None:
        self.cancel()

    async def on_isolated_partition_revoked(self, tp: TP) -> None:
        self.log.debug('Cancelling current task in actor for partition %r', tp)
        self.cancel()
        self.log.info('Stopping actor for revoked partition %r...', tp)
        await self.stop()
        self.log.debug('Actor for revoked partition %r stopped')

    async def on_isolated_partition_assigned(self, tp: TP) -> None:
        self.log.dev('Actor was assigned to %r', tp)

    def cancel(self) -> None:
        if self.actor_task:
            self.actor_task.cancel()

    def __repr__(self) -> str:
        return f'<{self.shortlabel}>'

    @property
    def label(self) -> str:
        s = self.agent._agent_label(name_suffix='*')
        if self.stream.active_partitions:
            partitions = {
                tp.partition for tp in self.stream.active_partitions
            }
            s += f' isolated={partitions}'
        return s


class AsyncIterableActor(AsyncIterableActorT, Actor):
    """Used for agent function that yields."""

    def __aiter__(self) -> AsyncIterator:
        return self.it.__aiter__()


class AwaitableActor(AwaitableActorT, Actor):
    """Used for actor function that do not yield."""

    def __await__(self) -> Any:
        return self.it.__await__()

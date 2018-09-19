import pytest
from collections import Counter
from pprint import pformat
from typing import List, NamedTuple
from faust.exceptions import ImproperlyConfigured
from faust.types import EventT, StreamT
from .helpers import AgentCase


class _param(NamedTuple):
    partitions: List[int]
    num_messages: int
    isolated_partitions: bool = False


@pytest.mark.asyncio
@pytest.mark.parametrize(_param._fields, [
    _param([0], 100, isolated_partitions=True),
    _param([0], 100, isolated_partitions=False),
    _param(list(range(10)), 100, isolated_partitions=True),
    _param(list(range(10)), 100, isolated_partitions=False),
    _param(list(range(100)), 1000, isolated_partitions=True),
    _param(list(range(10)), 1000, isolated_partitions=True),
    _param(list(range(10)), 1000, isolated_partitions=False),
])
async def test_agent_isolated_partitions(
        partitions,
        num_messages,
        isolated_partitions,
        *,
        app,
        logging):
    await AgentIsolatedCase.run_test(
        app=app,
        num_messages=num_messages,
        concurrency=1,
        partitions=partitions,
        isolated_partitions=isolated_partitions,
    )


@pytest.mark.asyncio
async def test_agent_isolated_partitions__concurrency(*, app, logging):
    with pytest.raises(ImproperlyConfigured):
        await AgentIsolatedCase.run_test(
            app=app,
            concurrency=2,
            isolated_partitions=True,
        )


class AgentIsolatedCase(AgentCase):
    name = 'test_agent_isolated_partitions'

    def on_init(self) -> None:
        # isolated_partitions=True requires concurrency=1
        self.processed_by_tp = Counter()

    async def on_agent_event(
            self, stream: StreamT, event: EventT) -> None:
        # when an event is processed by our agent
        if self.isolated_partitions:
            # make sure the event tp matches the tps for this actor
            assert stream.active_partitions is not None
            assert len(stream.active_partitions) == 1
            assert event.message.tp in stream.active_partitions
        else:
            assert stream.active_partitions is None

        # tests: ordering/uniform distribution
        assert event.message.tp == next(self.expected_tp)

        self.processed_by_tp[event.message.tp] += 1

    async def assert_success(self) -> None:
        await super().assert_success()
        self.log.info(
            '- Final TopicPartition distribution by coroutine:\n%s',
            pformat([
                (tp.partition, total)
                for tp, total in self.processed_by_tp.most_common()
            ]),
        )
        max_ = None
        for _, total in self.processed_by_tp.most_common():
            if max_ is None:
                max_ = total
            assert total == max_

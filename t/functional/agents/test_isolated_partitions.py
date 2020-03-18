import pytest
from collections import Counter
from pprint import pformat
from typing import Any, List, Mapping, NamedTuple
from faust.exceptions import ImproperlyConfigured
from faust.types import AppT, EventT, Message as MessageT, StreamT
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


@pytest.mark.asyncio
async def test_agent_isolated_partitions_rebalancing(*, app, logging):
    await AgentIsolatedRebalanceCase.run_test(
        app=app,
        num_messages=100,
        concurrency=1,
        partitions=[0, 1, 2, 3],
        reassign_partitions={
            10: [0],
            20: [1],
            30: [0, 1],
            40: [2, 3],
            50: [0, 1, 2, 3],
            60: [4, 5, 6, 7],
        },
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


class AgentIsolatedRebalanceCase(AgentCase):
    name = 'test_agent_isolated_partitions_rebalancing'

    @classmethod
    async def run_test(cls, app: AppT, *,
                       reassign_partitions: Mapping[int, List[int]],
                       **kwargs: Any) -> 'AgentCase':
        return await super().run_test(app,
                                      reassign_partitions=reassign_partitions,
                                      **kwargs)

    def __init__(self, app: AppT, *,
                 reassign_partitions: Mapping[int, List[int]],
                 **kwargs: Any):
        super().__init__(app, **kwargs)
        self.reassign_partitions = reassign_partitions

    async def put(self, key: bytes, value: bytes, **kwargs: Any) -> MessageT:
        message = await super().put(key, value, **kwargs)

        new_partitions = self.reassign_partitions.get(int(message.key))
        if new_partitions is not None:
            await self.simulate_rebalance(new_partitions)

        return message

    async def simulate_rebalance(self, partitions: List[int]):
        await self.sleep(.1)
        self.partitions = sorted(partitions)
        current_tps = set(self.tps)
        self._set_tps_from_partitions()
        assigned = set(self.tps)
        revoked = current_tps - assigned
        await self.conductor_setup(assigned=assigned, revoked=revoked)

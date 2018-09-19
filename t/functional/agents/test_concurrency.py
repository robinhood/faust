import pytest
from collections import Counter
from itertools import cycle
from pprint import pformat
from typing import NamedTuple
from faust.types import EventT, StreamT
from .helpers import AgentCase


class _param(NamedTuple):
    concurrency: int
    num_messages: int


@pytest.mark.asyncio
@pytest.mark.parametrize(_param._fields, [
    _param(10, 100),
    _param(10, 1000),
    _param(2, 10),
    _param(2, 100),
    _param(100, 1000),
    _param(1000, 1000),
])
async def test_agent_concurrency__duplicates(
        concurrency,
        num_messages,
        *,
        app,
        logging):
    await AgentConcurrencyCase.run_test(
        app=app,
        num_messages=num_messages,
        concurrency=concurrency,
    )


class AgentConcurrencyCase(AgentCase):
    name = 'test_agent_concurrency_duplicates'

    def on_init(self) -> None:
        assert self.concurrency > 0
        assert not self.concurrency % 2, 'concurrency is even'

        self.expected_index = cycle(range(self.concurrency))
        self.processed_by_concurrency_index = Counter()

    async def on_agent_event(
            self, stream: StreamT, event: EventT) -> None:
        cur_index = stream.concurrency_index

        # Test for: perfect round-robin, uniform distribution
        assert cur_index == next(self.expected_index)
        self.processed_by_concurrency_index[cur_index] += 1

    async def assert_success(self) -> None:
        await super().assert_success()
        self.log.info(
            '- Final coroutine distribution:\n%s',
            pformat(self.processed_by_concurrency_index.most_common()))
        max_ = None
        for _, total in self.processed_by_concurrency_index.most_common():
            if max_ is None:
                max_ = total
            assert total == max_

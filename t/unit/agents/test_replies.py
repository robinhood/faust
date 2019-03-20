import asyncio
import pytest
from faust.agents.models import ReqRepResponse
from faust.agents.replies import (
    BarrierState,
    ReplyConsumer,
    ReplyPromise,
)
from mode.utils.mocks import AsyncMock, Mock


def test_ReplyPromise():
    r = ReplyPromise(reply_to='rt', correlation_id='id1')
    assert r.reply_to == 'rt'
    assert r.correlation_id == 'id1'
    assert not r.done()
    r.fulfill('id1', 'value')
    assert r.result() == 'value'


class test_BarrierState:

    @pytest.mark.asyncio
    async def test_parallel_join(self):
        p = BarrierState(reply_to='rt')
        assert not p.pending
        assert p.size == 0

        await asyncio.gather(
            self.adder(p),
            self.fulfiller(p),
            self.finalizer(p, 1.0),
            self.joiner(p),
        )

    @pytest.mark.asyncio
    async def test_get_nowait__exhaust_sentinels(self):
        p = BarrierState(reply_to='rt')
        for _ in range(20):
            p._results.put_nowait(None)
        with pytest.raises(asyncio.QueueEmpty):
            p.get_nowait()

    @pytest.mark.asyncio
    async def test_iterate__completion(self):
        p = BarrierState(reply_to='rt')
        p.done = Mock(name='done')
        p.done.return_value = False
        p._results.put_nowait(None)
        p._results.get = AsyncMock(name='get')

        def se():
            p.done.return_value = True
            return None

        p._results.get.coro.side_effect = se

        assert [x async for x in p.iterate()] == []

    @pytest.mark.asyncio
    async def test_parallel_iterate(self):
        p = BarrierState(reply_to='rt')
        assert not p.pending
        assert p.size == 0

        done, pending = await asyncio.wait(
            [
                self.adder(p),
                self.fulfiller(p),
                self.finalizer(p, 1.0),
                self.consumer(p),
            ],
            timeout=5.0,
        )

        if pending:
            raise Exception(
                f'Test did not return in 5s:\n'
                f'  DONE_TASKS={done}\n'
                f'  PENDING_TASKS={pending}\n'
                f'  size={p.size}\n'
                f'  total={p.total}\n'
                f'  fulfilled={p.fulfilled}\n'
                f'  pending={len(p.pending)}\n'
                f'  done={p.done()}'
                f'  result={p.result() if p.done() else None}')

    @pytest.mark.asyncio
    async def test_sync_join(self):
        p = BarrierState(reply_to='rt')
        assert not p.pending
        assert p.size == 0

        await self.adder(p)
        await self.fulfiller(p)
        await self.finalizer(p, 0.0)
        await self.joiner(p)
        assert p.get_nowait()

    @pytest.mark.asyncio
    async def test_sync_iterate(self):
        p = BarrierState(reply_to='rt')
        assert not p.pending
        assert p.size == 0

        await self.adder(p)
        await self.finalizer(p, 0.0)
        await self.fulfiller(p)
        assert p.done()
        await self.consumer(p)

    async def adder(self, p: BarrierState):
        for i in range(100):
            assert p.size == i
            r = ReplyPromise(reply_to=p.reply_to, correlation_id=str(i))
            p.add(r)
            assert len(p.pending) == i + 1
        await asyncio.sleep(0.5)
        assert p.size == i + 1

    async def finalizer(self, p: BarrierState, sleep_for):
        await asyncio.sleep(sleep_for)
        p.finalize()
        assert p.total == 100

    async def fulfiller(self, p: BarrierState):
        for i in range(100):
            await asyncio.sleep(0.01)
            p.fulfill(correlation_id=str(i), value=str(i))

    async def joiner(self, p: BarrierState):
        await p

    async def consumer(self, p: BarrierState):
        results = [None] * 100
        async for correlation_id, value in p.iterate():
            results[int(correlation_id)] = (correlation_id, value)
        for i, (correlation_id, value) in enumerate(results):
            assert correlation_id == str(i)
            assert value == str(i)


class test_ReplyConsumer:

    @pytest.fixture()
    def c(self, *, app):
        return ReplyConsumer(app)

    @pytest.mark.asyncio
    async def test_on_start__disabled(self, *, c, app):
        c._start_fetcher = AsyncMock()
        app.conf.reply_create_topic = False
        await c.on_start()
        c._start_fetcher.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_start__enabled(self, *, c, app):
        c._start_fetcher = AsyncMock()
        app.conf.reply_create_topic = True
        await c.on_start()
        c._start_fetcher.assert_called_once_with(app.conf.reply_to)

    @pytest.mark.asyncio
    async def test_add(self, *, c):
        assert not c._waiting
        c._start_fetcher = AsyncMock()
        p = ReplyPromise(reply_to='rt', correlation_id='id1')
        await c.add('id1', p)
        assert 'id1' in c._waiting
        assert p in list(c._waiting['id1'])

        p2 = ReplyPromise(reply_to='rt', correlation_id='id1')
        c._fetchers[p2.reply_to] = Mock()
        await c.add('id1', p2)
        assert p in list(c._waiting['id1'])
        assert p2 in list(c._waiting['id1'])

        c._start_fetcher.assert_called_once_with(p.reply_to)

    @pytest.mark.asyncio
    async def test_start_fetcher(self, *, c):
        c._drain_replies = Mock()
        c._reply_topic = Mock(
            return_value=Mock(
                maybe_declare=AsyncMock(),
            ),
        )
        c.sleep = AsyncMock()
        c.add_future = Mock()

        await c._start_fetcher('topic1')
        assert 'topic1' in c._fetchers
        assert c._fetchers['topic1'] is c.add_future.return_value
        await c._start_fetcher('topic1')

        c._reply_topic.assert_called_once_with('topic1')
        topic = c._reply_topic.return_value
        topic.maybe_declare.assert_called_once_with()

        c.add_future.assert_called_once_with(
            c._drain_replies.return_value,
        )
        c._drain_replies.assert_called_once_with(topic)
        c.sleep.assert_called_once_with(3.0)

    @pytest.mark.asyncio
    async def test_drain_replies(self, *, c):
        responses = [
            ReqRepResponse(key='key1', value='value1', correlation_id='id1'),
            ReqRepResponse(key='key2', value='value2', correlation_id='id2'),
        ]
        channel = Mock(
            stream=Mock(return_value=self._response_stream(responses)),
        )
        p1 = Mock()
        p2 = Mock()
        p3 = Mock()
        c._waiting['id1'] = {p1, p2}
        c._waiting['id2'] = {p3}

        await c._drain_replies(channel)

        p1.fulfill.assert_called_once_with('id1', 'value1')
        p2.fulfill.assert_called_once_with('id1', 'value1')
        p3.fulfill.assert_called_once_with('id2', 'value2')

    async def _response_stream(self, responses):
        for response in responses:
            yield response

    def test_reply_topic(self, *, c, app):
        topic = c._reply_topic('foo')
        assert topic.get_topic_name() == 'foo'
        assert topic.partitions == 1
        assert topic.replicas == 0
        assert topic.deleting
        assert topic.retention == app.conf.reply_expires
        assert topic.value_type is ReqRepResponse
